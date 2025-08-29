-- name: CreateUser :one
INSERT INTO users (id, email, username, password_hash)
VALUES ($1, $2, $3, $4)
RETURNING id, email, username, password_hash, created_at, updated_at;

-- name: GetUserByEmail :one
-- Uses UNIQUE + covering index for index-only scans.
SELECT id, email, username, password_hash, created_at, updated_at
FROM users
WHERE email = $1
LIMIT 1;

-- name: GetUserByID :one
SELECT id, email, username, password_hash, created_at, updated_at
FROM users
WHERE id = $1;

-- name: UpdatePassword :exec
UPDATE users
SET password_hash = $2 -- updated_at set by trigger
WHERE id = $1;

-- Optional: Keyset-paginated user list (newest first)
-- name: ListUsersPage :many
SELECT id, email, username, created_at, updated_at
FROM users
WHERE ($1::timestamptz IS NULL OR (created_at, id) < ($1, $2::uuid))
ORDER BY created_at DESC, id DESC
LIMIT $3;

-- Sessions

-- name: CreateSession :one
INSERT INTO sessions (id, user_id, token_hash, expires_at)
VALUES ($1, $2, $3, $4)
RETURNING id, user_id, token_hash, expires_at, created_at, revoked_at;

-- name: GetSessionByTokenHash :one
SELECT id, user_id, token_hash, expires_at, created_at, revoked_at
FROM sessions
WHERE token_hash = $1
LIMIT 1;

-- name: RevokeSessionByID :exec
UPDATE sessions
SET revoked_at = now()
WHERE id = $1;

-- Optional: Delete expired, already-revoked sessions (housekeeping)
-- name: CleanupOldSessions :exec
DELETE FROM sessions
WHERE revoked_at IS NOT NULL
  AND expires_at < now() - interval '7 days';
