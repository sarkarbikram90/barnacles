package stream

import (
	"errors"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/sarkarbikram90/barnacles/internal/config"
	"github.com/sarkarbikram90/barnacles/internal/logentry"
	"github.com/sarkarbikram90/barnacles/internal/metrics"
)

var (
	// ErrMaxClientsReached indicates the server cannot accept more WebSocket connections.
	ErrMaxClientsReached = errors.New("maximum websocket clients reached")
)

// Hub orchestrates all active WebSocket client connections and event broadcasting.
type Hub struct {
	cfg          config.StreamSettings
	recentBuffer *RecentBuffer
	metrics      *metrics.ServerMetrics

	mu      sync.RWMutex
	clients map[*Client]struct{}
	closed  bool

	upgrader websocket.Upgrader
}

// NewHub creates a new WebSocket stream Hub.
func NewHub(cfg config.StreamSettings, recent *RecentBuffer, m *metrics.ServerMetrics) *Hub {
	if recent == nil {
		recent = NewRecentBuffer(cfg.RecentEvents)
	}

	return &Hub{
		cfg:          cfg,
		recentBuffer: recent,
		metrics:      m,
		clients:      make(map[*Client]struct{}),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow dashboard connections
			},
		},
	}
}

// Upgrade upgrades an incoming HTTP request to a WebSocket client connection,
// registers the client, replays recent logs, and begins read/write pumps.
func (h *Hub) Upgrade(w http.ResponseWriter, r *http.Request) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
		return errors.New("hub is closed")
	}
	if len(h.clients) >= h.cfg.MaxClients {
		h.mu.Unlock()
		http.Error(w, "too many clients connected", http.StatusServiceUnavailable)
		return ErrMaxClientsReached
	}
	h.mu.Unlock()

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return err
	}

	client := NewClient(h, conn, h.cfg.ClientBufferSize, h.cfg.PingInterval, h.cfg.WriteDeadline)

	h.Register(client)

	// Replay recent buffer to newly connected client
	snapshot := h.recentBuffer.Snapshot(100)
	client.SendMessage(Message{
		Type: "recent_batch",
		Data: snapshot,
	})

	go client.WritePump()
	go client.ReadPump()

	return nil
}

// Register adds a client to the hub.
func (h *Hub) Register(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		_ = c.conn.Close()
		return
	}

	h.clients[c] = struct{}{}
	if h.metrics != nil {
		h.metrics.WebsocketClients.Set(float64(len(h.clients)))
	}
}

// Unregister removes a client from the hub and updates metrics.
func (h *Hub) Unregister(c *Client, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)

		if h.metrics != nil {
			h.metrics.WebsocketClients.Set(float64(len(h.clients)))
			if reason != "" {
				h.metrics.WebsocketDisconnectsTotal.WithLabelValues(reason).Inc()
			}
		}
	}
}

// Broadcast dispatches new log entries to all connected clients and appends to recent buffer.
// Slow clients whose buffers are full are safely disconnected without blocking the hub.
func (h *Hub) Broadcast(entries []logentry.LogEntry) {
	if len(entries) == 0 {
		return
	}

	// Add to ring buffer
	h.recentBuffer.AddBatch(entries)

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}

	var slowClients []*Client

	for _, entry := range entries {
		msg := Message{
			Type: "log",
			Data: entry,
		}

		for client := range h.clients {
			if !client.SendMessage(msg) {
				slowClients = append(slowClients, client)
			}
		}
	}

	// Disconnect slow clients whose buffers filled up
	for _, slow := range slowClients {
		if _, ok := h.clients[slow]; ok {
			delete(h.clients, slow)
			close(slow.send)
			if h.metrics != nil {
				h.metrics.WebsocketDisconnectsTotal.WithLabelValues("buffer_overflow").Inc()
			}
		}
	}

	if h.metrics != nil {
		h.metrics.WebsocketClients.Set(float64(len(h.clients)))
		h.metrics.EventsBroadcastTotal.Add(float64(len(entries)))
	}
}

// ClientCount returns the number of currently connected clients.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// Recent returns the underlying RecentBuffer.
func (h *Hub) Recent() *RecentBuffer {
	return h.recentBuffer
}

// Close disconnects all active clients and closes the hub.
func (h *Hub) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true

	for client := range h.clients {
		delete(h.clients, client)
		close(client.send)
	}

	if h.metrics != nil {
		h.metrics.WebsocketClients.Set(0)
	}
	return nil
}
