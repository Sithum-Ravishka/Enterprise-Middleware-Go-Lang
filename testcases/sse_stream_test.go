//go:build ignore
// +build ignore

package testcases

import (
    "testing"
    "time"

    "github.com/example/user-platform/pkg/sse"
    "github.com/stretchr/testify/require"
)

// TestSSE verifies that events are published to subscribers.
func TestSSE(t *testing.T) {
	hub := sse.NewHub()
	ch1 := hub.Subscribe()
	defer hub.Unsubscribe(ch1)
	// Publish an event
	hub.Publish("hello")
	select {
	case msg := <-ch1:
		require.Equal(t, "hello", msg)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}
