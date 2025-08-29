-- name: InsertAuditLog :one
INSERT INTO audit_logs (id, ts, user_id, event_type, correlation_id, payload)
VALUES ($1, $2, $3, $4, $5, $6::jsonb)
RETURNING id, ts, user_id, event_type, correlation_id, payload, created_at;

-- name: ListAuditLogsForUser :many
SELECT id, ts, user_id, event_type, correlation_id, payload, created_at
FROM audit_logs
WHERE user_id = $1
ORDER BY ts DESC
LIMIT $2 OFFSET $3;
