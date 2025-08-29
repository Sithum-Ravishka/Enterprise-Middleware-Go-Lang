package repo

import (
	"context"
	"errors"
	"sync"
	"time"

	db "github.com/example/user-platform/internal/user/sqlc/gen"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// Sentinel errors (single source of truth)
var (
	ErrNotFound        = errors.New("not found")
	ErrEmailExists     = errors.New("email exists")
	ErrSessionNotFound = errors.New("session not found")
)

// InMemoryRepo implements the Repository interface using in-memory maps.
type InMemoryRepo struct {
	mu       sync.RWMutex
	users    map[string]db.User    // key: userID string
	byEmail  map[string]string     // email -> userID string
	sessions map[string]db.Session // key: sessionID string
	byToken  map[string]string     // tokenHash -> sessionID string
}

func NewInMemoryRepo() *InMemoryRepo {
	return &InMemoryRepo{
		users:    make(map[string]db.User),
		byEmail:  make(map[string]string),
		sessions: make(map[string]db.Session),
		byToken:  make(map[string]string),
	}
}

func uuidString(u pgtype.UUID) string { return uuid.UUID(u.Bytes).String() }

// --- Users ---

func (r *InMemoryRepo) CreateUser(ctx context.Context, user db.User) (db.User, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.byEmail[user.Email]; exists {
		return db.User{}, ErrEmailExists
	}
	idStr := uuidString(user.ID)
	r.users[idStr] = user
	r.byEmail[user.Email] = idStr
	return user, nil
}

func (r *InMemoryRepo) GetUserByEmail(ctx context.Context, email string) (db.User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	id, ok := r.byEmail[email]
	if !ok {
		return db.User{}, ErrNotFound
	}
	return r.users[id], nil
}

func (r *InMemoryRepo) GetUserByID(ctx context.Context, id uuid.UUID) (db.User, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	usr, ok := r.users[id.String()]
	if !ok {
		return db.User{}, ErrNotFound
	}
	return usr, nil
}

// --- Sessions ---

func (r *InMemoryRepo) CreateSession(ctx context.Context, sess db.Session) (db.Session, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	idStr := uuidString(sess.ID)
	r.sessions[idStr] = sess
	r.byToken[sess.TokenHash] = idStr
	return sess, nil
}

func (r *InMemoryRepo) RevokeSession(ctx context.Context, id uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := id.String()
	sess, ok := r.sessions[key]
	if !ok {
		return ErrSessionNotFound
	}
	sess.RevokedAt = pgtype.Timestamptz{Time: time.Now(), Valid: true}
	r.sessions[key] = sess
	return nil
}

func (r *InMemoryRepo) GetSessionByTokenHash(ctx context.Context, tokenHash string) (db.Session, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	id, ok := r.byToken[tokenHash]
	if !ok {
		return db.Session{}, ErrNotFound
	}
	return r.sessions[id], nil
}
