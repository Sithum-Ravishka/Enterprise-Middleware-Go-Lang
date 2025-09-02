package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/example/user-platform/pkg/kafka"
	"github.com/example/user-platform/pkg/sse"
)

// RunEventBridge consumes Kafka "events" topics and publishes into SSE hub.
func RunEventBridge(ctx context.Context, hub *sse.Hub, brokers []string) error {
	// Create Kafka consumer for user.events.v1
	consumer := kafka.NewByteAuditConsumer(brokers, "user.events.v1", "gateway-realtime", 1, func(ctx context.Context, val []byte) error {
		clientID := "*"
		payload := string(val)

		// Try to parse client_id if JSON has one
		var tmp struct {
			ClientID string `json:"client_id"`
		}
		if err := json.Unmarshal(val, &tmp); err == nil && tmp.ClientID != "" {
			clientID = tmp.ClientID
		}

		fmt.Printf("[EventBridge] client_id=%s payload=%s\n", clientID, payload)

		// Publish to hub
		hub.Publish(clientID, payload)
		return nil
	})

	// Start consumer loop
	consumer.Start(ctx)

	// Block until context is canceled
	<-ctx.Done()
	return ctx.Err()
}
