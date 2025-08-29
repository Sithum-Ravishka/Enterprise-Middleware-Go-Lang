package sse

import (
	"bufio"
	"net/http"
	"time"
)

// Handler returns an http.Handler that streams SSE events from the Hub.
// Mount it like: mux.Handle("/events", sse.Handler(hub))
func Handler(h *Hub) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Basic CORS (adjust to your needs).
		w.Header().Set("Access-Control-Allow-Origin", "*")
		// SSE headers.
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")

		// Ensure we can flush.
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		// Subscribe this client.
		ch := h.Subscribe()
		defer h.Unsubscribe(ch)

		// Send an initial comment to open the stream.
		_, _ = w.Write([]byte(":ok\n\n"))
		flusher.Flush()

		// Heartbeat to keep connections alive (Heroku/ELB-friendly).
		heartbeat := time.NewTicker(30 * time.Second)
		defer heartbeat.Stop()

		// If the client goes away, context will be canceled.
		ctx := r.Context()

		// Some proxies buffer unless we flush after each write.
		bw := bufio.NewWriter(w)
		flush := func() {
			_ = bw.Flush()
			flusher.Flush()
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-heartbeat.C:
				_, _ = bw.WriteString(": ping\n\n")
				flush()
			case msg, ok := <-ch:
				if !ok {
					return
				}
				// Emit as a single 'data:' block. If you need event names, add "event: name\n".
				_, _ = bw.WriteString("data: " + msg + "\n\n")
				flush()
			}
		}
	})
}
