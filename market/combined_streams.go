package market

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type CombinedStreamsClient struct {
	conn        *websocket.Conn
	mu          sync.RWMutex
	subscribers map[string]chan []byte
	reconnect   bool
	done        chan struct{}
	batchSize   int // 每批订阅的流数量
	writeCh     chan wsRequest
	readyMu     sync.Mutex
	readyCond   *sync.Cond
	ready       bool

	// 测试用 hook（生产环境为 nil）
	// 重连时调用，传入需要重新订阅的流列表
	onReconnectSubscribeFunc func(streams []string)
}

func NewCombinedStreamsClient(batchSize int) *CombinedStreamsClient {
	c := &CombinedStreamsClient{
		subscribers: make(map[string]chan []byte),
		reconnect:   true,
		done:        make(chan struct{}),
		batchSize:   batchSize,
		writeCh:     make(chan wsRequest, 128),
	}
	c.readyCond = sync.NewCond(&c.readyMu)
	go c.writeLoop()
	return c
}

type wsRequest struct {
	payload map[string]interface{}
	resp    chan error
}

func (c *CombinedStreamsClient) Connect() error {
	proxyFunc := http.ProxyFromEnvironment
	if proxyURL, err := getCustomProxyURL(); err != nil {
		return fmt.Errorf("组合流代理配置错误: %w", err)
	} else if proxyURL != nil {
		log.Printf("组合流WebSocket将通过代理连接: %s", proxyURL)
		proxyFunc = func(*http.Request) (*url.URL, error) {
			return proxyURL, nil
		}
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Proxy:            proxyFunc,
	}

	// 组合流使用不同的端点
	conn, _, err := dialer.Dial("wss://fstream.binance.com/stream", nil)
	if err != nil {
		return fmt.Errorf("组合流WebSocket连接失败: %v", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	log.Println("组合流WebSocket连接成功")
	c.readyMu.Lock()
	c.ready = true
	c.readyCond.Broadcast()
	c.readyMu.Unlock()
	go c.readMessages()

	return nil
}

// BatchSubscribeKlines 批量订阅K线
func (c *CombinedStreamsClient) BatchSubscribeKlines(symbols []string, interval string) error {
	// 将symbols分批处理
	batches := c.splitIntoBatches(symbols, c.batchSize)

	for i, batch := range batches {
		log.Printf("订阅第 %d 批, 数量: %d", i+1, len(batch))

		streams := make([]string, len(batch))
		for j, symbol := range batch {
			streams[j] = fmt.Sprintf("%s@kline_%s", strings.ToLower(symbol), interval)
		}

		if err := c.subscribeStreams(streams); err != nil {
			return fmt.Errorf("第 %d 批订阅失败: %v", i+1, err)
		}

		// 批次间延迟，避免被限制
		if i < len(batches)-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	return nil
}

// splitIntoBatches 将切片分成指定大小的批次
func (c *CombinedStreamsClient) splitIntoBatches(symbols []string, batchSize int) [][]string {
	var batches [][]string

	for i := 0; i < len(symbols); i += batchSize {
		end := i + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}
		batches = append(batches, symbols[i:end])
	}

	return batches
}

// subscribeStreams 订阅多个流
func (c *CombinedStreamsClient) subscribeStreams(streams []string) error {
	if err := c.waitForConnection(); err != nil {
		return err
	}
	subscribeMsg := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": streams,
		"id":     time.Now().UnixNano(),
	}

	log.Printf("订阅流: %v", streams)
	return c.enqueueRequest(subscribeMsg)
}

func (c *CombinedStreamsClient) enqueueRequest(payload map[string]interface{}) error {
	if payload == nil {
		return fmt.Errorf("WebSocket请求为空")
	}

	req := wsRequest{
		payload: payload,
		resp:    make(chan error, 1),
	}

	select {
	case c.writeCh <- req:
	case <-c.done:
		return fmt.Errorf("WebSocket已关闭")
	case <-time.After(5 * time.Second):
		return fmt.Errorf("WebSocket写入排队超时")
	}

	return <-req.resp
}

func (c *CombinedStreamsClient) writeLoop() {
	for {
		select {
		case <-c.done:
			c.drainPendingRequests()
			return
		case req := <-c.writeCh:
			if req.payload == nil {
				req.resp <- fmt.Errorf("WebSocket请求为空")
				continue
			}
			err := c.writeJSON(req.payload)
			req.resp <- err
		}
	}
}

func (c *CombinedStreamsClient) drainPendingRequests() {
	for {
		select {
		case req := <-c.writeCh:
			if req.resp != nil {
				req.resp <- fmt.Errorf("WebSocket已关闭")
			}
		default:
			return
		}
	}
}

func (c *CombinedStreamsClient) writeJSON(payload map[string]interface{}) error {
	if err := c.waitForConnection(); err != nil {
		return err
	}

	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("WebSocket未连接")
	}

	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}

	return conn.WriteJSON(payload)
}

func (c *CombinedStreamsClient) readMessages() {
	for {
		select {
		case <-c.done:
			return
		default:
			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			if conn == nil {
				time.Sleep(1 * time.Second)
				continue
			}

			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("读取组合流消息失败: %v", err)
				c.handleReconnect()
				return
			}

			c.handleCombinedMessage(message)
		}
	}
}

func (c *CombinedStreamsClient) handleCombinedMessage(message []byte) {
	var combinedMsg struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}

	if err := json.Unmarshal(message, &combinedMsg); err != nil {
		log.Printf("解析组合消息失败: %v", err)
		return
	}

	c.mu.RLock()
	ch, exists := c.subscribers[combinedMsg.Stream]
	c.mu.RUnlock()

	if exists {
		select {
		case ch <- combinedMsg.Data:
		default:
			log.Printf("订阅者通道已满: %s", combinedMsg.Stream)
		}
	}
}

func (c *CombinedStreamsClient) AddSubscriber(stream string, bufferSize int) <-chan []byte {
	ch := make(chan []byte, bufferSize)
	c.mu.Lock()
	c.subscribers[stream] = ch
	c.mu.Unlock()
	return ch
}

func (c *CombinedStreamsClient) handleReconnect() {
	if !c.reconnect {
		return
	}

	c.readyMu.Lock()
	c.ready = false
	c.readyCond.Broadcast()
	c.readyMu.Unlock()

	log.Println("组合流尝试重新连接...")
	time.Sleep(3 * time.Second)

	if err := c.Connect(); err != nil {
		log.Printf("组合流重新连接失败: %v", err)
		go c.handleReconnect()
		return
	}

	// ✅ FIX: 重连成功后，重新订阅所有流
	// 这是解决数据卡住问题的关键：重连后必须发送 SUBSCRIBE 消息
	c.mu.RLock()
	streams := make([]string, 0, len(c.subscribers))
	for stream := range c.subscribers {
		streams = append(streams, stream)
	}
	c.mu.RUnlock()

	if len(streams) > 0 {
		log.Printf("重新订阅 %d 个流", len(streams))

		// 调用测试 hook（如果存在）
		if c.onReconnectSubscribeFunc != nil {
			c.onReconnectSubscribeFunc(streams)
		}

		if err := c.subscribeStreams(streams); err != nil {
			log.Printf("⚠️  重新订阅失败: %v", err)
		} else {
			log.Printf("✅ 重新订阅成功")
		}
	}
}

func (c *CombinedStreamsClient) Close() {
	c.reconnect = false
	close(c.done)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	for stream, ch := range c.subscribers {
		close(ch)
		delete(c.subscribers, stream)
	}

	c.readyMu.Lock()
	c.ready = false
	c.readyCond.Broadcast()
	c.readyMu.Unlock()
}

func (c *CombinedStreamsClient) waitForConnection() error {
	c.readyMu.Lock()
	defer c.readyMu.Unlock()

	for !c.ready && c.reconnect {
		c.readyCond.Wait()
	}

	if !c.ready {
		return fmt.Errorf("WebSocket未连接")
	}
	return nil
}

func getCustomProxyURL() (*url.URL, error) {
	for _, key := range []string{"PROXY", "proxy"} {
		if raw, ok := os.LookupEnv(key); ok {
			value := strings.TrimSpace(raw)
			if value == "" {
				continue
			}
			parsed, err := url.Parse(value)
			if err != nil {
				return nil, fmt.Errorf("无效的代理地址 %q: %w", value, err)
			}
			return parsed, nil
		}
	}
	return nil, nil
}
