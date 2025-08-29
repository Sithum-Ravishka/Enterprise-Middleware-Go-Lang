package service

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/example/user-platform/pkg/kafka"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

// AuditService handles audit events.
// AuditService handles audit events. It writes events to a Cassandra table and
// publishes them to Kafka for downstream consumers. The Cassandra keyspace
// and table must already exist; see cmd/logger-service for schema creation.
type AuditService struct {
	Producer *kafka.Producer
	Session  *gocql.Session
}

// NewAuditService creates a new AuditService with the given Kafka producer
// and Cassandra session. Either may be nil; if the session is nil, events
// will only be published to Kafka. If the producer is nil, events will only
// be written to Cassandra.
func NewAuditService(p *kafka.Producer, session *gocql.Session) *AuditService {
	return &AuditService{Producer: p, Session: session}
}

// Emit emits an audit event to Kafka.
func (a *AuditService) Emit(ctx context.Context, userID, eventType, correlationID string, payload map[string]interface{}) error {
	// Build the audit event. Payload is encoded separately.
	evt := kafka.AuditEvent{
		ID:            uuid.New().String(),
		Timestamp:     time.Now().Unix(),
		UserID:        userID,
		EventType:     eventType,
		CorrelationID: correlationID,
		PayloadJSON:   "",
	}

	var payloadStr string
	if payload != nil {
		b, _ := json.Marshal(payload)
		payloadStr = string(b)
		evt.PayloadJSON = payloadStr
	}
	// Insert into Cassandra if session is configured.
	if a.Session != nil {
		// Use QUORUM consistency for both reads and writes.
		if err := a.Session.Query(`INSERT INTO audit_logs (id, ts, user_id, event_type, correlation_id, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?, toTimestamp(now()))`,
			evt.ID, time.Unix(evt.Timestamp, 0), evt.UserID, evt.EventType, evt.CorrelationID, payloadStr).WithContext(ctx).Exec(); err != nil {
			return err
		}
	}
	// Publish to Kafka if producer is configured.
	if a.Producer != nil {
		data, _ := json.Marshal(evt)
		return a.Producer.Publish(ctx, []byte(userID), data)
	}
	return errors.New("no audit sink configured")
}
