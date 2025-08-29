package kafka

import (
    "context"
    "time"

    "github.com/example/user-platform/pkg/retry"
    "github.com/segmentio/kafka-go"
)

// Producer publishes messages to Kafka with retry logic.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer constructs a new Kafka producer.
func NewProducer(brokers []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	return &Producer{writer: w}
}

// Publish writes a message to Kafka with retries.
func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
	fn := func() error {
		return p.writer.WriteMessages(ctx, kafka.Message{
			Key:   key,
			Value: value,
			Time:  time.Now(),
		})
	}
	return retry.WithBackoff(ctx, fn, 3)
}

// Close closes the producer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
