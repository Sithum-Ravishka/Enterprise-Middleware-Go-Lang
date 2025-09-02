package sse

import (
	"context"
	"sync"
)

type Hub struct {
	mu      sync.RWMutex
	clients map[string]map[chan string]struct{} // clientID → set of channels
}

func NewHub() *Hub {
	return &Hub{clients: make(map[string]map[chan string]struct{})}
}

// Subscribe adds a client for a given clientID.
func (h *Hub) Subscribe(clientID string) chan string {
	ch := make(chan string, 10)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[clientID] == nil {
		h.clients[clientID] = make(map[chan string]struct{})
	}
	h.clients[clientID][ch] = struct{}{}
	return ch
}

// Unsubscribe removes a client for a clientID.
func (h *Hub) Unsubscribe(clientID string, ch chan string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if set, ok := h.clients[clientID]; ok {
		delete(set, ch)
		close(ch)
		if len(set) == 0 {
			delete(h.clients, clientID)
		}
	}
}

// Publish sends event to all clients of this clientID.
func (h *Hub) Publish(clientID, event string) {
	h.mu.RLock()
	set := h.clients[clientID]
	h.mu.RUnlock()
	if set == nil {
		return
	}
	for ch := range set {
		select {
		case ch <- event:
		default:
			// drop if client is slow
		}
	}
}

// Optional safe publish with context.
func (h *Hub) PublishCtx(ctx context.Context, clientID, event string) {
	done := make(chan struct{}, 1)
	go func() {
		h.Publish(clientID, event)
		done <- struct{}{}
	}()
	select {
	case <-ctx.Done():
	case <-done:
	}
}
