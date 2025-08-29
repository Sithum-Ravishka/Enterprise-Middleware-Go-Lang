package testcases

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/example/user-platform/pkg/auth"
	"github.com/example/user-platform/pkg/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	auditservice "github.com/example/user-platform/internal/logger/service"
	userservice "github.com/example/user-platform/internal/user/service"
	db "github.com/example/user-platform/internal/user/sqlc/gen"
)

/* ---------------- shared test literals ---------------- */

const (
	testEmail    = "john@example.com"
	testUsername = "john"
	// Strong password: >=12 chars, upper, lower, digit, special
	testPass  = "VeryStrong#Password123"
	wrongPass = "wrongpassword"
)

var (
	errExists   = errors.New("exists")
	errNotFound = errors.New("not found")
)

/* ---------------- test cases ---------------- */

func TestRegisterAndLogin(t *testing.T) {
	svc, repo := newServiceForTests(t)

	ctx := context.Background()

	// Register
	userID, err := svc.Register(ctx, testEmail, testUsername, testPass)
	require.NoError(t, err)
	require.NotEmpty(t, userID)

	// Login ok
	access, refresh, err := svc.Login(ctx, testEmail, testPass)
	require.NoError(t, err)
	require.NotEmpty(t, access)
	require.NotEmpty(t, refresh)

	// Wrong password
	_, _, err = svc.Login(ctx, testEmail, wrongPass)
	require.Error(t, err)

	// Duplicate register
	_, err = svc.Register(ctx, testEmail, "johnny", testPass)
	require.Error(t, err)

	// Get profile ok
	prof, err := svc.GetProfile(ctx, userID)
	require.NoError(t, err)
	require.Equal(t, testEmail, prof.Email)

	// Get profile bad UUID
	_, err = svc.GetProfile(ctx, "not-a-uuid")
	require.Error(t, err)

	// Get profile not found
	_, err = svc.GetProfile(ctx, uuid.NewString())
	require.Error(t, err)

	// Refresh session ok
	newAccess, newRefresh, err := svc.RefreshSession(ctx, refresh)
	require.NoError(t, err)
	require.NotEmpty(t, newAccess)
	require.NotEmpty(t, newRefresh)
	require.NotEqual(t, refresh, newRefresh)

	// Old session should be revoked
	oldHash := sha256Hex(refresh)
	sess, ok := repo.sessionsByHash[oldHash]
	require.True(t, ok)
	require.True(t, sess.RevokedAt.Valid)

	// Refresh with invalid token
	_, _, err = svc.RefreshSession(ctx, "bogus-token")
	require.Error(t, err)

	// Logout revokes existing session:
	// First login again to create a fresh session
	_, ref2, err := svc.Login(ctx, testEmail, testPass)
	require.NoError(t, err)
	h2 := sha256Hex(ref2)

	err = svc.Logout(ctx, ref2)
	require.NoError(t, err)

	s2, ok := repo.sessionsByHash[h2]
	require.True(t, ok)
	require.True(t, s2.RevokedAt.Valid)
}

func TestValidationErrors(t *testing.T) {
	svc, _ := newServiceForTests(t)
	ctx := context.Background()

	// Bad email
	_, err := svc.Register(ctx, "not-an-email", "johnny", testPass)
	require.Error(t, err)

	// Short/weak password
	_, err = svc.Register(ctx, "a@b.co", "jjj", "short")
	require.Error(t, err)

	// Username too short
	_, err = svc.Register(ctx, "u@ex.com", "ab", testPass)
	require.Error(t, err)
}

/* ---------------- scaffolding & helpers ---------------- */

func newServiceForTests(t *testing.T) (*userservice.UserService, *inMemoryRepo) {
	t.Helper()

	repo := &inMemoryRepo{
		users:          make(map[string]db.User),
		sessionsByID:   make(map[string]db.Session),
		sessionsByHash: make(map[string]db.Session),
	}

	// Lightweight audit (won't actually reach a broker in tests)
	producer := kafka.NewProducer([]string{"localhost:9092"}, kafka.UserAuditTopic)
	audit := auditservice.NewAuditService(producer, nil)

	// Token manager uses file paths
	privPath, pubPath := writeTempKeyPair(t)
	tm, err := auth.NewTokenManager(privPath, pubPath)
	require.NoError(t, err)

	// Validator adapter implementing userservice.validator
	v := &svcValidator{}

	logger := zap.NewNop() // no-op logger for tests
	cache := noopCache{}   // <-- updated to match interface

	svc := userservice.NewUserService(repo, audit, tm, v, logger, cache)
	return svc, repo
}

// ---- validator adapter that matches userservice.validator ----

type svcValidator struct{}

// Adjust these to mirror your production rules.
var (
	reEmail    = regexp.MustCompile(`^[^\s@]+@[^\s@]+\.[^\s@]+$`)
	reUpper    = regexp.MustCompile(`[A-Z]`)
	reLower    = regexp.MustCompile(`[a-z]`)
	reDigit    = regexp.MustCompile(`[0-9]`)
	reSpecial  = regexp.MustCompile(`[!@#\$%\^&\*\-_+=\[\]\{\}\(\):;'",.<>\/\?\\|]`)
	minUserLen = 3
	maxUserLen = 32
	minPassLen = 12
)

func (v *svcValidator) ValidateRegister(requestID, email, username, password string) error {
	// email
	if email == "" || !reEmail.MatchString(email) {
		return errors.New("invalid email")
	}
	// username
	if l := len(username); l < minUserLen || l > maxUserLen {
		return errors.New("invalid username")
	}
	// password strength
	if len(password) < minPassLen ||
		!reUpper.MatchString(password) ||
		!reLower.MatchString(password) ||
		!reDigit.MatchString(password) ||
		!reSpecial.MatchString(password) {
		return errors.New("weak password")
	}
	return nil
}

// ---- no-op cache to satisfy userservice.Cache ----
// If your interface differs, tweak the method set below accordingly.

type noopCache struct{}

func (noopCache) Get(ctx context.Context, key string) (string, bool, error)           { return "", false, nil }
func (noopCache) Set(ctx context.Context, key, value string, ttl time.Duration) error { return nil }
func (noopCache) Del(ctx context.Context, keys ...string) error                       { return nil }

// ---- keypair / repo / helpers ----

// writeTempKeyPair generates an RSA keypair and writes to temp PEM files.
func writeTempKeyPair(t *testing.T) (string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	privDer, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	privPem := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDer})

	pubDer, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	pubPem := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDer})

	dir := t.TempDir()
	privPath := filepath.Join(dir, "key.pem")
	pubPath := filepath.Join(dir, "key.pub.pem")

	require.NoError(t, os.WriteFile(privPath, privPem, 0o600))
	require.NoError(t, os.WriteFile(pubPath, pubPem, 0o600))

	return privPath, pubPath
}

type inMemoryRepo struct {
	users          map[string]db.User
	sessionsByID   map[string]db.Session
	sessionsByHash map[string]db.Session
}

func (r *inMemoryRepo) CreateUser(ctx context.Context, user db.User) (db.User, error) {
	if _, ok := r.users[user.Email]; ok {
		return db.User{}, errExists
	}
	r.users[user.Email] = user
	return user, nil
}

func (r *inMemoryRepo) GetUserByEmail(ctx context.Context, email string) (db.User, error) {
	u, ok := r.users[email]
	if !ok {
		return db.User{}, errNotFound
	}
	return u, nil
}

func (r *inMemoryRepo) GetUserByID(ctx context.Context, id uuid.UUID) (db.User, error) {
	for _, user := range r.users {
		if equalUUID(user.ID, id) {
			return user, nil
		}
	}
	return db.User{}, errNotFound
}

func (r *inMemoryRepo) CreateSession(ctx context.Context, session db.Session) (db.Session, error) {
	idStr := uuidFromPg(session.ID).String()
	r.sessionsByID[idStr] = session
	// index by token hash for lookups
	if session.TokenHash != "" {
		r.sessionsByHash[session.TokenHash] = session
	}
	return session, nil
}

func (r *inMemoryRepo) RevokeSession(ctx context.Context, id uuid.UUID) error {
	idStr := id.String()
	s, ok := r.sessionsByID[idStr]
	if !ok {
		return nil
	}
	now := time.Now()
	s.RevokedAt = pgtype.Timestamptz{Time: now, Valid: true}
	r.sessionsByID[idStr] = s
	if s.TokenHash != "" {
		r.sessionsByHash[s.TokenHash] = s
	}
	return nil
}

func (r *inMemoryRepo) GetSessionByTokenHash(ctx context.Context, tokenHash string) (db.Session, error) {
	if s, ok := r.sessionsByHash[tokenHash]; ok {
		return s, nil
	}
	return db.Session{}, errNotFound
}

/* ---------------- tiny pg helpers ---------------- */

func equalUUID(p pgtype.UUID, u uuid.UUID) bool {
	return p.Valid && uuidFromPg(p) == u
}

func uuidFromPg(p pgtype.UUID) uuid.UUID {
	return uuid.UUID(p.Bytes)
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// (Optional) example time helper if you simulate expiry elsewhere
func nowPlus(days int) time.Time { return time.Now().Add(time.Duration(days) * 24 * time.Hour) }
