//go:build ignore
// +build ignore

package testcases

import (
    "context"
    "encoding/json"
    "testing"
    "time"

    "github.com/example/user-platform/pkg/kafka"
    "github.com/stretchr/testify/require"
)

func TestKafkaAuditFlow(t *testing.T) {
	// Use a Kafka producer and consumer with local brokers.
	brokers := []string{"localhost:9092"}
	producer := kafka.NewProducer(brokers, kafka.UserAuditTopic)
	defer producer.Close()
	// Prepare a consumer
	events := make([][]byte, 0, 1)
	consumer := kafka.NewAuditConsumer(brokers, kafka.UserAuditTopic, "test-group", 1, func(ctx context.Context, msg []byte) error {
		events = append(events, msg)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer.Start(ctx)
	// Publish an audit event
	payload := kafka.AuditEvent{
		ID:            "id",
		Timestamp:     time.Now().Unix(),
		UserID:        "user",
		EventType:     "user.test",
		CorrelationID: "correlation",
		PayloadJSON:   "{}",
	}
	data, _ := json.Marshal(payload)
	err := producer.Publish(context.Background(), []byte(payload.UserID), data)
	require.NoError(t, err)
	// Wait for consumer to process
	time.Sleep(2 * time.Second)
	require.NotEmpty(t, events)
	consumer.Close()
}
