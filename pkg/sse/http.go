package sse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"time"
)

var idRe = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// Handler returns an http.Handler that streams SSE events from the Hub.
func Handler(h *Hub) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Preflight CORS
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "X-Client-Id, Content-Type")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Prefer query param, then header
		clientID := r.URL.Query().Get("client_id")
		if clientID == "" {
			clientID = r.Header.Get("X-Client-Id")
		}
		if clientID == "" {
			http.Error(w, "missing client_id", http.StatusBadRequest)
			return
		}
		if !idRe.MatchString(clientID) {
			http.Error(w, "invalid client_id", http.StatusBadRequest)
			return
		}

		// SSE headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		// Subscribe this client
		ch := h.Subscribe(clientID)
		defer h.Unsubscribe(clientID, ch)

		fmt.Println("[DEBUG] SSE subscribed client_id:", clientID)

		// Helper write with immediate flush
		write := func(s string) error {
			if _, err := w.Write([]byte(s)); err != nil {
				return err
			}
			flusher.Flush()
			return nil
		}

		// Retry advice
		_ = write("retry: 5000\n\n")

		// Initial hello event
		_ = write(":ok\n\n")
		_ = write("event: hello\n")
		_ = write(fmt.Sprintf(
			"data: {\"client_id\":\"%s\",\"connected_at\":\"%s\"}\n\n",
			clientID, time.Now().UTC().Format(time.RFC3339Nano),
		))

		// Heartbeat ticker
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				fmt.Println("[DEBUG] SSE closed for client_id:", clientID)
				return

			case <-ticker.C:
				// heartbeat
				if err := write(": ping\n\n"); err != nil {
					fmt.Println("[DEBUG] SSE heartbeat write failed:", err)
					return
				}

			case msg, ok := <-ch:
				if !ok {
					return
				}
				fmt.Println("[DEBUG] SSE handler sending to client_id:", clientID, "msg:", msg)
				if err := write("data: " + msg + "\n\n"); err != nil {
					fmt.Println("[DEBUG] SSE message write failed:", err)
					return
				}

				// 👇 detect "done" marker to close connection immediately
				var ev struct {
					Event string `json:"event"`
				}
				if json.Unmarshal([]byte(msg), &ev) == nil && len(ev.Event) > 0 {
					if len(ev.Event) >= 5 && ev.Event[len(ev.Event)-5:] == ".done" {
						fmt.Println("[DEBUG] Closing SSE after done event for client:", clientID)
						return
					}
				}
			}
		}
	})
}
