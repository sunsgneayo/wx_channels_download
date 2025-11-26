package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// GlobalHub 简单的连接池管理
type GlobalHub struct {
	clients map[*websocket.Conn]bool
	mutex   sync.Mutex
}

var hub = GlobalHub{
	clients: make(map[*websocket.Conn]bool),
}

// [新增] 用于存储正在等待结果的 HTTP 请求
// Key: string (ObjectId/OriginId), Value: chan BrowserResponse
type PendingMap struct {
	requests map[string]chan BrowserResponse
	mutex    sync.Mutex
}

var pendingRequests = PendingMap{
	requests: make(map[string]chan BrowserResponse),
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Command 定义发送给浏览器的指令格式
type Command struct {
	Type          string `json:"type"`
	ObjectId      string `json:"objectid"`
	ObjectNonceId string `json:"objectNonceId"`
}

// BrowserResponse 定义浏览器回传的数据格式
type BrowserResponse struct {
	Type     string      `json:"type"`
	OriginId string      `json:"origin_id"` // 必须与请求的 ObjectId 一致
	Data     interface{} `json:"data"`
	Msg      string      `json:"msg,omitempty"`
}

func main() {
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/api/send_task", sendTaskHandler)

	port := "8888"
	fmt.Printf("服务启动成功: http://127.0.0.1:%s\n", port)
	fmt.Printf("   - WebSocket 地址: ws://127.0.0.1:%s/ws\n", port)
	fmt.Printf("   - 下发任务接口:   http://127.0.0.1:%s/api/send_task (POST)\n", port)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	hub.mutex.Lock()
	hub.clients[conn] = true
	hub.mutex.Unlock()

	log.Printf("[WS] 新浏览器已连接: %s", conn.RemoteAddr())

	// 循环读取消息
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[WS] 连接断开: %v", err)
			break
		}
		handleMessage(message)
	}

	hub.mutex.Lock()
	delete(hub.clients, conn)
	hub.mutex.Unlock()
}

// [修改] handleMessage 现在负责将结果分发给对应的 HTTP 请求
func handleMessage(msg []byte) {
	var resp BrowserResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		log.Printf("[MSG] 解析 JSON 失败: %v | Raw: %s", err, string(msg))
		return
	}

	fmt.Printf("[WS收到] ID: %s, Type: %s\n", resp.OriginId, resp.Type)

	// [核心逻辑] 检查是否有 HTTP 请求在等待这个 ID
	pendingRequests.mutex.Lock()
	resultChan, exists := pendingRequests.requests[resp.OriginId]
	pendingRequests.mutex.Unlock()

	if exists {
		// 如果有等待者，将结果发送到通道中
		// 使用非阻塞发送，防止通道已关闭导致的 panic (虽然逻辑上不太可能)
		select {
		case resultChan <- resp:
			fmt.Println(" -> 已转发给 HTTP 等待者")
		default:
			fmt.Println(" -> 通道阻塞或已关闭，无法转发")
		}
	} else {
		// 没人等待这个消息（可能是超时的消息，或者是浏览器主动推送的其他消息）
		fmt.Println(" -> 无等待者 (可能已超时)，仅打印结果")
		if resp.Type == "video_result" {
			prettyJSON, _ := json.MarshalIndent(resp.Data, "", "  ")
			fmt.Printf("数据内容: %s\n", string(prettyJSON))
		}
	}
}

// [修改] sendTaskHandler 改为同步等待模式
func sendTaskHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var cmd Command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	cmd.Type = "fetch_video"

	// 1. 检查是否有浏览器在线
	hub.mutex.Lock()
	activeCount := len(hub.clients)
	hub.mutex.Unlock()
	if activeCount == 0 {
		http.Error(w, "No active browser workers", http.StatusServiceUnavailable)
		return
	}

	// 2. [核心逻辑] 创建一个接收结果的通道
	responseChan := make(chan BrowserResponse, 1) // 带缓冲，防止阻塞

	// 3. 将通道注册到全局 Map 中
	pendingRequests.mutex.Lock()
	pendingRequests.requests[cmd.ObjectId] = responseChan
	pendingRequests.mutex.Unlock()

	// 确保函数结束时清理 Map，防止内存泄漏
	defer func() {
		pendingRequests.mutex.Lock()
		delete(pendingRequests.requests, cmd.ObjectId)
		pendingRequests.mutex.Unlock()
		// 这里不需要 close(responseChan)，让 GC 处理即可，防止多次关闭 panic
	}()

	// 4. 发送任务给浏览器
	// 这里简单起见，发给任意一个或者全部。如果并发高，建议改为轮询负载均衡。
	hub.mutex.Lock()
	for conn := range hub.clients {
		if err := conn.WriteJSON(cmd); err != nil {
			log.Printf("发送指令失败: %v", err)
			conn.Close()
			delete(hub.clients, conn)
		} else {
			// 只要成功发给一个就跳出（避免重复抓取），除非你需要广播
			break
		}
	}
	hub.mutex.Unlock()

	// 5. [核心逻辑] 阻塞等待结果或超时
	w.Header().Set("Content-Type", "application/json")

	select {
	case result := <-responseChan:
		// 收到 WebSocket 传回的数据！
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"data":   result,
		})
	case <-time.After(30 * time.Second):
		// 超时处理
		w.WriteHeader(http.StatusGatewayTimeout)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "timeout",
			"msg":    "Browser did not respond in time",
			"id":     cmd.ObjectId,
		})
	}
}
