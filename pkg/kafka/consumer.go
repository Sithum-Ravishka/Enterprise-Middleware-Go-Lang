package kafka

import (
	"context"
	"encoding/json"

	"github.com/example/user-platform/pkg/concurrency"
	"github.com/segmentio/kafka-go"
)

// AuditConsumer consumes audit events from Kafka.
type AuditConsumer struct {
	reader *kafka.Reader
	pool   *concurrency.WorkerPool
}

// NewAuditConsumer creates a new consumer with workers.
func NewAuditConsumer(brokers []string, topic string, groupID string, workerCount int, handler func(context.Context, []byte) error) *AuditConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
	pool := concurrency.NewWorkerPool(workerCount, handler)
	return &AuditConsumer{
		reader: r,
		pool:   pool,
	}
}

// Start begins consuming messages.
func (c *AuditConsumer) Start(ctx context.Context) {
	go func() {
		for {
			m, err := c.reader.FetchMessage(ctx)
			if err != nil {
				return
			}
			// Process message asynchronously
			c.pool.Submit(ctx, m.Value)
			_ = c.reader.CommitMessages(ctx, m)
		}
	}()
}

// Close closes the consumer and worker pool.
func (c *AuditConsumer) Close() error {
	c.pool.Shutdown()
	return c.reader.Close()
}

// AuditEvent represents an audit event consumed from Kafka.
type AuditEvent struct {
	ID            string `json:"id"`
	Timestamp     int64  `json:"ts"`
	UserID        string `json:"user_id"`
	EventType     string `json:"event_type"`
	CorrelationID string `json:"correlation_id"`
	PayloadJSON   string `json:"payload_json"`
}

// Decode decodes a JSON message into an AuditEvent.
func Decode(data []byte) (*AuditEvent, error) {
	var evt AuditEvent
	err := json.Unmarshal(data, &evt)
	return &evt, err
}
