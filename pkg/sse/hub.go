package sse

import (
	"context"
	"sync"
)

// Hub manages SSE clients and broadcasts events.
type Hub struct {
	mu      sync.RWMutex
	clients map[chan string]struct{}
}

// NewHub creates a new Hub.
func NewHub() *Hub {
	return &Hub{
		clients: make(map[chan string]struct{}),
	}
}

// Subscribe registers a new client and returns a channel to read events.
func (h *Hub) Subscribe() chan string {
	ch := make(chan string, 10)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

// Unsubscribe removes a client.
func (h *Hub) Unsubscribe(ch chan string) {
	h.mu.Lock()
	if _, ok := h.clients[ch]; ok {
		delete(h.clients, ch)
		close(ch)
	}
	h.mu.Unlock()
}

// Publish sends an event to all clients (non-blocking per client).
func (h *Hub) Publish(event string) {
	h.mu.RLock()
	for ch := range h.clients {
		select {
		case ch <- event:
		default:
			// Drop if client is slow; keeps hub healthy under backpressure.
		}
	}
	h.mu.RUnlock()
}

// Broadcast is kept for backward compatibility; it delegates to Publish.
func (h *Hub) Broadcast(s string) {
	h.Publish(s)
}

// Optional utility: publish with context (timeouts, cancel).
func (h *Hub) PublishCtx(ctx context.Context, event string) {
	done := make(chan struct{}, 1)
	go func() {
		h.Publish(event)
		done <- struct{}{}
	}()
	select {
	case <-ctx.Done():
	case <-done:
	}
}
