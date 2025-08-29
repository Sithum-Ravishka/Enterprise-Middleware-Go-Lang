package repo

import (
	"context"
	"testing"
	"time"

	db "github.com/example/user-platform/internal/user/sqlc/gen"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

// --- shared constants for test literals ---
const (
	emailAlice = "alice@example.com"
	emailBob   = "bob@example.com"
	userAlice  = "alice"
	userBob    = "bob"
	passHash   = "hash"
	tokenStr   = "token"
)

var oneHour = time.Hour

// --- helpers ---

func pgUUID(u uuid.UUID) pgtype.UUID        { return pgtype.UUID{Bytes: u, Valid: true} }
func pgTime(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }
func uuidFromPg(u pgtype.UUID) uuid.UUID    { return u.Bytes }

// TestInMemoryRepoUserLifecycle tests creating and retrieving a user in the in-memory repo.
func TestInMemoryRepoUserLifecycle(t *testing.T) {
	t.Parallel()

	r := NewInMemoryRepo()
	ctx := context.Background()

	now := time.Unix(1_700_000_000, 0).UTC() // fixed time for determinism
	uid := uuid.New()

	u := db.User{
		ID:           pgUUID(uid),
		Email:        emailAlice,
		Username:     userAlice,
		PasswordHash: passHash,
		CreatedAt:    pgTime(now),
		UpdatedAt:    pgTime(now),
	}

	_, err := r.CreateUser(ctx, u)
	require.NoError(t, err)

	got, err := r.GetUserByEmail(ctx, u.Email)
	require.NoError(t, err)
	require.Equal(t, uid, uuidFromPg(got.ID))

	gotByID, err := r.GetUserByID(ctx, uid)
	require.NoError(t, err)
	require.Equal(t, u.Email, gotByID.Email)

	// Duplicate email should error (prefer checking sentinel if available)
	_, err = r.CreateUser(ctx, u)
	if ErrEmailExists != nil { // if your package defines this sentinel
		require.ErrorIs(t, err, ErrEmailExists)
	} else {
		require.Error(t, err)
	}
}

// TestInMemoryRepoSessionLifecycle tests creating, retrieving and revoking sessions.
func TestInMemoryRepoSessionLifecycle(t *testing.T) {
	t.Parallel()

	r := NewInMemoryRepo()
	ctx := context.Background()

	now := time.Unix(1_700_000_000, 0).UTC() // fixed time for determinism

	// create user to associate sessions
	userID := uuid.New()
	u := db.User{
		ID:           pgUUID(userID),
		Email:        emailBob,
		Username:     userBob,
		PasswordHash: passHash,
		CreatedAt:    pgTime(now),
		UpdatedAt:    pgTime(now),
	}
	_, _ = r.CreateUser(ctx, u)

	sessID := uuid.New()
	sess := db.Session{
		ID:        pgUUID(sessID),
		UserID:    pgUUID(userID),
		TokenHash: tokenStr,
		ExpiresAt: pgTime(now.Add(oneHour)),
		CreatedAt: pgTime(now),
	}

	_, err := r.CreateSession(ctx, sess)
	require.NoError(t, err)

	got, err := r.GetSessionByTokenHash(ctx, tokenStr)
	require.NoError(t, err)
	require.Equal(t, sessID, uuidFromPg(got.ID))

	// revoke session
	err = r.RevokeSession(ctx, sessID)
	require.NoError(t, err)

	got, err = r.GetSessionByTokenHash(ctx, tokenStr)
	require.NoError(t, err)
	require.True(t, got.RevokedAt.Valid)
}
