package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/example/user-platform/pkg/kafka"
	"github.com/google/uuid"
)

type AuditEmitter interface {
	Emit(ctx context.Context, userID, eventType, correlationID string, payload map[string]any) error
}

type AuditService struct {
	producer *kafka.Producer
	sess     any // reserved for future use
}

func NewAuditService(producer *kafka.Producer, session any) *AuditService {
	return &AuditService{producer: producer, sess: session}
}

func (a *AuditService) Emit(ctx context.Context, userID, eventType, correlationID string, payload map[string]any) error {
	if a == nil || a.producer == nil {
		return nil
	}
	if correlationID == "" {
		correlationID = uuid.NewString()
	}
	paybytes, err := json.Marshal(payload)
	if err != nil {
		paybytes = []byte("{}")
	}
	ev := kafka.AuditEvent{
		ID:            uuid.NewString(),
		Timestamp:     time.Now().Unix(),
		UserID:        userID,
		EventType:     eventType,
		CorrelationID: correlationID,
		PayloadJSON:   string(paybytes),
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return a.producer.Publish(ctx, []byte(userID), data)
}
