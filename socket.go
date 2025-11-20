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

// GlobalHub 简单的连接池管理，用于存储当前连接的浏览器 WebSocket
type GlobalHub struct {
	clients map[*websocket.Conn]bool
	mutex   sync.Mutex
}

var hub = GlobalHub{
	clients: make(map[*websocket.Conn]bool),
}

var upgrader = websocket.Upgrader{
	// 允许跨域，因为浏览器端是 channels.weixin.qq.com，服务端是 localhost
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
	OriginId string      `json:"origin_id"`
	Data     interface{} `json:"data"`          // 具体的视频详情数据
	Msg      string      `json:"msg,omitempty"` // 错误信息等
}

func main() {
	// 1. WebSocket 连接处理 (浏览器注入的脚本连接这里)
	http.HandleFunc("/ws", wsHandler)

	// 2. 任务下发接口 (外部程序调用这里，触发浏览器抓取)
	http.HandleFunc("/api/send_task", sendTaskHandler)

	port := "8888"
	fmt.Printf("服务启动成功: http://127.0.0.1:%s\n", port)
	fmt.Printf("   - WebSocket 地址: ws://127.0.0.1:%s/ws\n", port)
	fmt.Printf("   - 下发任务接口:   http://127.0.0.1:%s/api/send_task (POST)\n", port)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

// wsHandler 处理浏览器发来的 WebSocket 连接
func wsHandler(w http.ResponseWriter, r *http.Request) {
	// 升级 HTTP 为 WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	// 注册连接到全局池
	hub.mutex.Lock()
	hub.clients[conn] = true
	hub.mutex.Unlock()

	log.Printf("[WS] 新浏览器已连接: %s", conn.RemoteAddr())

	// 发送一个测试指令 (可选，验证连接是否通畅)
	// 测试用 ID，你可以注释掉这几行
	go func() {
		time.Sleep(5 * time.Second) // 等待连接稳定
		testCmd := Command{
			Type:          "fetch_video",
			ObjectId:      "14626319645964310960", // 示例 ID
			ObjectNonceId: "5566337981470565121_0_0_0_0_0_7eb7e05e-be02-11f0-9f3b-f9527b21811b",
		}
		if err := conn.WriteJSON(testCmd); err != nil {
			log.Println("发送测试指令失败:", err)
		} else {
			fmt.Println("[WS] 已自动发送测试指令...")
		}
	}()

	// 循环读取浏览器回传的消息
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[WS] 连接断开: %v", err)
			break
		}
		handleMessage(message)
	}

	// 清理连接
	hub.mutex.Lock()
	delete(hub.clients, conn)
	hub.mutex.Unlock()
}

// handleMessage 处理浏览器传回来的 JSON 数据
func handleMessage(msg []byte) {
	var resp BrowserResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		log.Printf("[MSG] 解析 JSON 失败: %v | Raw: %s", err, string(msg))
		return
	}

	if resp.Type == "video_result" {
		fmt.Printf("\n================ [收到抓取结果] ================\n")
		fmt.Printf("视频ID: %s\n", resp.OriginId)

		// 格式化打印数据部分
		prettyJSON, _ := json.MarshalIndent(resp.Data, "", "  ")
		fmt.Printf("数据内容: %s\n", string(prettyJSON))
		fmt.Printf("================================================\n\n")
	} else if resp.Type == "error" {
		fmt.Printf("[Browser Error] %s\n", resp.Msg)
	} else {
		fmt.Printf("[MSG] 未知类型: %s\n", string(msg))
	}
}

// sendTaskHandler 提供给外部调用的 HTTP 接口
// 用法: POST http://localhost:8888/api/send_task Body: {"objectid": "...", "objectNonceId": "..."}
func sendTaskHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 解析请求体
	var cmd Command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	cmd.Type = "fetch_video" // 强制设置类型

	// 广播给所有连接的浏览器 (或者你可以设计逻辑只发给其中一个)
	hub.mutex.Lock()
	activeCount := len(hub.clients)
	sentCount := 0
	for conn := range hub.clients {
		if err := conn.WriteJSON(cmd); err == nil {
			sentCount++
		} else {
			log.Printf("向浏览器 %s 发送指令失败: %v", conn.RemoteAddr(), err)
			conn.Close()
			delete(hub.clients, conn)
		}
	}
	hub.mutex.Unlock()

	// 返回结果给 API 调用者
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"status":       "success",
		"active_conns": activeCount,
		"sent_count":   sentCount,
		"task":         cmd,
	}
	json.NewEncoder(w).Encode(response)
}
