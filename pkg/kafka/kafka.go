package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const UserAuditTopic = "user.audit.v1"

type ProducerOptions struct {
	Topic string
}

type Producer struct{ writer *kafka.Writer }

func NewProducer(brokers []string, opts ProducerOptions) *Producer {
	topic := strings.TrimSpace(opts.Topic)
	if topic == "" {
		panic("kafka.NewProducer: Topic must be provided")
	}
	var addrs []string
	for _, b := range brokers {
		if t := strings.TrimSpace(b); t != "" {
			addrs = append(addrs, t)
		}
	}
	if len(addrs) == 0 {
		addrs = []string{"localhost:9092"}
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(addrs...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	return &Producer{writer: w}
}

func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
	if p == nil || p.writer == nil {
		return errors.New("kafka.Producer.Publish: producer is nil")
	}
	msg := kafka.Message{Key: key, Value: value, Time: time.Now().UTC()}
	return p.writer.WriteMessages(ctx, msg)
}
func (p *Producer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

type AuditEvent struct {
	ID            string `json:"id"`
	Timestamp     int64  `json:"ts"`
	UserID        string `json:"user_id"`
	EventType     string `json:"event_type"`
	CorrelationID string `json:"correlation_id"`
	PayloadJSON   string `json:"payload_json"`
}

type ByteAuditConsumer struct {
	reader      *kafka.Reader
	handler     func(context.Context, []byte) error
	concurrency int
	closedCh    chan struct{}
}

func NewByteAuditConsumer(brokers []string, topic, group string, concurrency int, handler func(context.Context, []byte) error) *ByteAuditConsumer {
	if concurrency < 1 {
		concurrency = 1
	}
	var addrs []string
	for _, b := range brokers {
		if t := strings.TrimSpace(b); t != "" {
			addrs = append(addrs, t)
		}
	}
	if len(addrs) == 0 {
		addrs = []string{"localhost:9092"}
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  addrs,
		GroupID:  group,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10_000_000,
	})
	return &ByteAuditConsumer{reader: r, handler: handler, concurrency: concurrency, closedCh: make(chan struct{})}
}

func (c *ByteAuditConsumer) Start(ctx context.Context) {
	if c == nil || c.reader == nil || c.handler == nil {
		return
	}
	for i := 0; i < c.concurrency; i++ {
		go func() {
			for {
				m, err := c.reader.FetchMessage(ctx)
				if err != nil {
					select {
					case <-ctx.Done():
						return
					default:
						time.Sleep(500 * time.Millisecond)
						continue
					}
				}
				if err := c.handler(ctx, m.Value); err == nil {
					_ = c.reader.CommitMessages(ctx, m)
				}
			}
		}()
	}
}

func (c *ByteAuditConsumer) Close() error {
	if c == nil || c.reader == nil {
		return nil
	}
	return c.reader.Close()
}

// ---- Gateway convenience consumer ----

type ConsumerOpts struct {
	Brokers  []string
	Topic    string
	GroupID  string
	MinBytes int
	MaxBytes int
	DLQTopic string
}

type Consumer struct {
	reader  *kafka.Reader
	handler func(context.Context, AuditEvent) error
	logger  *zap.Logger
}

func NewAuditConsumer(opts ConsumerOpts, handler func(context.Context, AuditEvent) error, logger *zap.Logger) *Consumer {
	minBytes := opts.MinBytes
	if minBytes == 0 {
		minBytes = 1 << 10
	}
	maxBytes := opts.MaxBytes
	if maxBytes == 0 {
		maxBytes = 10 << 20
	}
	var addrs []string
	for _, b := range opts.Brokers {
		if t := strings.TrimSpace(b); t != "" {
			addrs = append(addrs, t)
		}
	}
	if len(addrs) == 0 {
		addrs = []string{"localhost:9092"}
	}
	if strings.TrimSpace(opts.Topic) == "" {
		opts.Topic = UserAuditTopic
	}
	if strings.TrimSpace(opts.GroupID) == "" {
		opts.GroupID = "default-group"
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  addrs,
		GroupID:  opts.GroupID,
		Topic:    opts.Topic,
		MinBytes: minBytes,
		MaxBytes: maxBytes,
	})
	return &Consumer{reader: r, handler: handler, logger: logger}
}

func (c *Consumer) Start(ctx context.Context) {
	if c == nil || c.reader == nil || c.handler == nil {
		return
	}
	go func() {
		for {
			m, err := c.reader.FetchMessage(ctx)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					if c.logger != nil {
						c.logger.Warn("kafka fetch failed", zap.Error(err))
					}
					time.Sleep(500 * time.Millisecond)
					continue
				}
			}
			var ev AuditEvent
			if err := json.Unmarshal(m.Value, &ev); err != nil {
				if c.logger != nil {
					c.logger.Warn("failed to unmarshal audit event", zap.Error(err))
				}
				_ = c.reader.CommitMessages(ctx, m)
				continue
			}
			if err := c.handler(ctx, ev); err == nil {
				_ = c.reader.CommitMessages(ctx, m)
			} else if c.logger != nil {
				c.logger.Error("audit handler failed", zap.Error(err))
			}
		}
	}()
}

func (c *Consumer) Close() error {
	if c == nil || c.reader == nil {
		return nil
	}
	return c.reader.Close()
}
