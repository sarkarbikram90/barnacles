package stream

import (
	"encoding/json"
	"time"

	"github.com/gorilla/websocket"
)

// Message is the standard JSON envelope for WebSocket stream communication.
type Message struct {
	Type string `json:"type"`           // "log", "recent_batch", "ping", "error"
	Data any    `json:"data,omitempty"` // payload
}

// Client represents a connected WebSocket browser client.
type Client struct {
	hub           *Hub
	conn          *websocket.Conn
	send          chan []byte
	pingInterval  time.Duration
	writeDeadline time.Duration
}

// NewClient creates a new client instance.
func NewClient(hub *Hub, conn *websocket.Conn, bufferSize int, pingInterval, writeDeadline time.Duration) *Client {
	if bufferSize <= 0 {
		bufferSize = 256
	}
	if pingInterval <= 0 {
		pingInterval = 30 * time.Second
	}
	if writeDeadline <= 0 {
		writeDeadline = 5 * time.Second
	}

	return &Client{
		hub:           hub,
		conn:          conn,
		send:          make(chan []byte, bufferSize),
		pingInterval:  pingInterval,
		writeDeadline: writeDeadline,
	}
}

// ReadPump listens for incoming client frames (pongs, closes).
func (c *Client) ReadPump() {
	defer func() {
		c.hub.Unregister(c, "connection_closed")
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(64 * 1024)
	_ = c.conn.SetReadDeadline(time.Now().Add(c.pingInterval + 10*time.Second))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.pingInterval + 10*time.Second))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

// WritePump pushes queued log events and periodic ping heartbeats to the client.
func (c *Client) WritePump() {
	ticker := time.NewTicker(c.pingInterval)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeDeadline))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			_, _ = w.Write(msg)

			// Drain any additional messages in the channel to pack in the same frame if desired
			n := len(c.send)
			for i := 0; i < n; i++ {
				_, _ = w.Write([]byte{'\n'})
				_, _ = w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeDeadline))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// SendMessage serializes and enqueues a Message for the client.
// Returns false if client buffer is full.
func (c *Client) SendMessage(msg Message) bool {
	data, err := json.Marshal(msg)
	if err != nil {
		return true
	}

	select {
	case c.send <- data:
		return true
	default:
		return false
	}
}
