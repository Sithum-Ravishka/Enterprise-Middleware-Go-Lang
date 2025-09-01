// internal/user/service/service.go
package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	db "github.com/example/user-platform/internal/user/sqlc/gen"
	"github.com/example/user-platform/pkg/auth"
	intErr "github.com/example/user-platform/pkg/errors"
	"github.com/example/user-platform/pkg/trace"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
)

/* =============== Interfaces & types =============== */

// Repository is your existing storage port (sqlc-backed)
type Repository interface {
	CreateUser(ctx context.Context, user db.User) (db.User, error)
	GetUserByEmail(ctx context.Context, email string) (db.User, error)
	GetUserByID(ctx context.Context, id uuid.UUID) (db.User, error)

	CreateSession(ctx context.Context, session db.Session) (db.Session, error)
	RevokeSession(ctx context.Context, id uuid.UUID) error
	GetSessionByTokenHash(ctx context.Context, tokenHash string) (db.Session, error)
}

// validator interface for validation.
type validator interface {
	ValidateRegister(requestID, email, username, password string) error
}

// Cache abstracts Redis. A minimal footprint is enough for our needs.
type Cache interface {
	// Get returns (value, true) if found, else ("", false, nil). Any driver error -> err.
	Get(ctx context.Context, key string) (string, bool, error)
	// Set with TTL; value should be <= few KB; errors ignored on hot path if desired.
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
	// Del invalidates keys (best-effort)
	Del(ctx context.Context, keys ...string) error
}

// userCacheView is the small cached projection; keep it lean to reduce payload size.
type userCacheView struct {
	ID        uuid.UUID `json:"id"`
	Email     string    `json:"email"`
	Username  string    `json:"username"`
	CreatedAt int64     `json:"created_at_unix"`
	UpdatedAt int64     `json:"updated_at_unix"`
}

// toView converts the sqlc row into the cache view (copy minimal fields)
func toView(u db.User) userCacheView {
	return userCacheView{
		ID:        uuid.UUID(u.ID.Bytes),
		Email:     u.Email,
		Username:  u.Username,
		CreatedAt: u.CreatedAt.Time.Unix(),
		UpdatedAt: u.UpdatedAt.Time.Unix(),
	}
}

func (v userCacheView) toUserRow() db.User {
	return db.User{
		ID:           pgUUID(v.ID),
		Email:        v.Email,
		Username:     v.Username,
		PasswordHash: "", // never cache hashes
		CreatedAt:    pgTime(time.Unix(v.CreatedAt, 0)),
		UpdatedAt:    pgTime(time.Unix(v.UpdatedAt, 0)),
	}

}

/* =============== Service =============== */

type UserService struct {
	Repo      Repository
	Audit     AuditEmitter
	TokenMng  *auth.TokenManager
	Validator validator

	// New bits
	Log   *zap.Logger
	Cache Cache

	sf singleflight.Group

	// per-email login rate limiters (protects hashing & DB)
	//  - use a bounded TTL map in production or shard by sync.Map with a sweeper
	limiterLRU *limiterPool
}

func NewUserService(repo Repository, tm *auth.TokenManager, v validator, log *zap.Logger, cache Cache) *UserService {
	if log == nil {
		log = zap.NewNop()
	}
	return &UserService{
		Repo:       repo,
		TokenMng:   tm,
		Validator:  v,
		Log:        log,
		Cache:      cache,
		limiterLRU: newLimiterPool(10*time.Minute, 100_000), // keep hottest 100k keys
	}
}

// --- pgtype helpers ---
func pgUUID(id uuid.UUID) pgtype.UUID       { return pgtype.UUID{Bytes: id, Valid: true} }
func pgTime(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }

// --- cache keys & TTLs ---
const (
	cacheTTLUserHot    = 2 * time.Minute  // hot path reads (email/id)
	cacheTTLUserWarm   = 10 * time.Minute // less hot, optional
	cacheKeyByEmailFmt = "user:email:%s"
	cacheKeyByIDFmt    = "user:id:%s"
)

// format helpers
func keyByEmail(email string) string { return "user:email:" + email }
func keyByID(id uuid.UUID) string    { return "user:id:" + id.String() }

// Register registers a new user.
func (s *UserService) Register(ctx context.Context, email, username, password string) (string, error) {
	meta := trace.ExtractFromIncoming(ctx) // TraceID, APIPath, HTTPMethod
	reqID := meta.TraceID

	if err := s.Validator.ValidateRegister(reqID, email, username, password); err != nil {
		s.Log.Warn("register validation failed", zap.String("email", email), zap.Error(err))
		return "", err
	}

	// Hash password (cost configured in auth pkg)
	hash, err := auth.HashPassword(password)
	if err != nil {
		s.Log.Error("hash password failed", zap.Error(err))
		return "", intErr.StatusFromCode(intErr.ErrInternal, reqID)
	}

	// Existence check by email (hit cache first)
	if _, err := s.getUserByEmail(ctx, email); err == nil {
		return "", intErr.StatusFromCode(intErr.ErrEmailInUse, reqID)
	}

	// Create
	id := uuid.New()
	now := time.Now()
	u := db.User{
		ID:           pgUUID(id),
		Email:        email,
		Username:     username,
		PasswordHash: hash,
		CreatedAt:    pgTime(now),
		UpdatedAt:    pgTime(now),
	}
	if _, err := s.Repo.CreateUser(ctx, u); err != nil {
		s.Log.Error("repo.CreateUser failed", zap.String("email", email), zap.Error(err))
		return "", intErr.StatusFromCode(intErr.ErrInternal, reqID)
	}

	// Invalidate any stale cache keys (best-effort)
	if s.Cache != nil {
		_ = s.Cache.Del(ctx, keyByEmail(email), keyByID(id))
	}

	s.emitAudit(ctx,
		id.String(),
		"INFO",
		"user register successful",
		"user-service",
		meta.APIPath,
		meta.HTTPMethod,
		meta.TraceID,
		"user-register", // reason
	)

	return id.String(), nil
}

// Updated helper for new audit schema
func (s *UserService) emitAudit(
	ctx context.Context,
	userID string,
	logLevel string,
	message string,
	serviceName string,
	apiEndpoint string,
	httpMethod string,
	traceID string,
	reason string,
) {
	if s.Audit == nil {
		s.Log.Warn("audit service not configured; skipping emit",
			zap.String("event", message), zap.String("user_id", userID))
		return
	}
	// protect hot path from any panic inside downstream emit
	defer func() {
		if r := recover(); r != nil {
			s.Log.Error("panic while emitting audit event",
				zap.String("event", message), zap.Any("recover", r))
		}
	}()

	if err := s.Audit.Emit(ctx, userID, logLevel, message, serviceName, apiEndpoint, httpMethod, traceID, reason); err != nil {
		s.Log.Warn("audit emit failed",
			zap.String("event", message), zap.String("user_id", userID), zap.Error(err))
	}
}

// Login authenticates a user and returns tokens.
func (s *UserService) Login(ctx context.Context, email, password string) (string, string, error) {
	reqID := ""

	// Per-email rate limit (e.g., 5 req/s burst 10 for login)
	lim := s.limiterLRU.Get(email, rate.Every(200*time.Millisecond), 10)
	if !lim.Allow() {
		s.Log.Warn("login rate-limited", zap.String("email", email))
		return "", "", intErr.StatusFromCode(intErr.ErrRateLimited, reqID)
	}

	user, err := s.getUserByEmail(ctx, email)
	if err != nil {
		return "", "", intErr.StatusFromCode(intErr.ErrInvalidCredentials, reqID)
	}
	ok, _ := auth.ComparePassword(user.PasswordHash, password)
	if !ok {
		return "", "", intErr.StatusFromCode(intErr.ErrInvalidCredentials, reqID)
	}

	access, refresh, err := s.TokenMng.Generate(uuid.UUID(user.ID.Bytes).String())
	if err != nil {
		s.Log.Error("token generate failed", zap.Error(err))
		return "", "", intErr.StatusFromCode(intErr.ErrInternal, reqID)
	}

	// Hash refresh for storage
	sum := sha256.Sum256([]byte(refresh))
	hash := hex.EncodeToString(sum[:])
	now := time.Now()
	sess := db.Session{
		ID:        pgUUID(uuid.New()),
		UserID:    user.ID,
		TokenHash: hash,
		ExpiresAt: pgTime(now.Add(7 * 24 * time.Hour)),
		CreatedAt: pgTime(now),
	}
	if _, err := s.Repo.CreateSession(ctx, sess); err != nil {
		s.Log.Error("repo.CreateSession failed", zap.Error(err))
		return "", "", intErr.StatusFromCode(intErr.ErrInternal, reqID)
	}

	// SAFE: use helper (nil-safe + panic-guard)
	s.emitAudit(ctx,
		uuid.UUID(user.ID.Bytes).String(),
		"INFO",
		"user.login.succeeded",
		"user-service",
		"",                  // api_endpoint
		"",                  // http_method
		uuid.New().String(), // trace_id
		"",                  // reason
	)

	return access, refresh, nil
}

// GetProfile returns user profile by ID (cached).
func (s *UserService) GetProfile(ctx context.Context, userID string) (db.User, error) {
	reqID := ""

	id, err := uuid.Parse(userID)
	if err != nil {
		return db.User{}, intErr.StatusFromCode(intErr.ErrValidation, reqID)
	}

	u, err := s.getUserByID(ctx, id)
	if err != nil {
		return db.User{}, intErr.StatusFromCode(intErr.ErrNotFound, reqID)
	}
	// Never expose password hash in API layer; this is service layer returning db.User
	return u, nil
}

// RefreshSession rotates the refresh token and returns a new pair of tokens.
func (s *UserService) RefreshSession(ctx context.Context, refreshToken string) (string, string, error) {
	reqID := ""

	// Verify token and extract subject (user ID)
	userID, err := s.TokenMng.Verify(refreshToken)
	if err != nil {
		return "", "", intErr.StatusFromCode(intErr.ErrInvalidCredentials, reqID)
	}

	// Lookup session by hash
	sum := sha256.Sum256([]byte(refreshToken))
	hash := hex.EncodeToString(sum[:])
	session, err := s.Repo.GetSessionByTokenHash(ctx, hash)
	if err != nil {
		return "", "", intErr.StatusFromCode(intErr.ErrInvalidCredentials, reqID)
	}

	// Check revocation/expiry
	if session.RevokedAt.Valid || session.ExpiresAt.Time.Before(time.Now()) {
		return "", "", intErr.StatusFromCode(intErr.ErrInvalidCredentials, reqID)
	}

	// Revoke old session
	if err := s.Repo.RevokeSession(ctx, uuid.UUID(session.ID.Bytes)); err != nil {
		s.Log.Error("repo.RevokeSession failed", zap.Error(err))
		return "", "", intErr.StatusFromCode(intErr.ErrInternal, reqID)
	}

	// Issue new tokens
	access, newRefresh, err := s.TokenMng.Generate(userID)
	if err != nil {
		s.Log.Error("token generate failed", zap.Error(err))
		return "", "", intErr.StatusFromCode(intErr.ErrInternal, reqID)
	}

	// Store new session
	sumNew := sha256.Sum256([]byte(newRefresh))
	newHash := hex.EncodeToString(sumNew[:])
	now := time.Now()
	newSess := db.Session{
		ID:        pgUUID(uuid.New()),
		UserID:    session.UserID,
		TokenHash: newHash,
		ExpiresAt: pgTime(now.Add(7 * 24 * time.Hour)),
		CreatedAt: pgTime(now),
	}
	if _, err := s.Repo.CreateSession(ctx, newSess); err != nil {
		s.Log.Error("repo.CreateSession failed", zap.Error(err))
		return "", "", intErr.StatusFromCode(intErr.ErrInternal, reqID)
	}

	// SAFE: use helper
	s.emitAudit(ctx,
		uuid.UUID(session.UserID.Bytes).String(),
		"INFO",
		"user.session.refreshed",
		"user-service",
		"",                  // api_endpoint
		"",                  // http_method
		uuid.New().String(), // trace_id
		"",                  // reason
	)

	return access, newRefresh, nil
}

// Logout revokes the session for the provided refresh token.
func (s *UserService) Logout(ctx context.Context, refreshToken string) error {
	// Verify signature but ignore error to avoid leaking info.
	_, _ = s.TokenMng.Verify(refreshToken)

	// Hash and try lookup
	sum := sha256.Sum256([]byte(refreshToken))
	hash := hex.EncodeToString(sum[:])

	sess, err := s.Repo.GetSessionByTokenHash(ctx, hash)
	if err != nil {
		return nil
	}
	_ = s.Repo.RevokeSession(ctx, uuid.UUID(sess.ID.Bytes))

	// SAFE: use helper
	s.emitAudit(ctx,
		uuid.UUID(sess.UserID.Bytes).String(),
		"INFO",
		"user.logout",
		"user-service",
		"",                  // api_endpoint
		"",                  // http_method
		uuid.New().String(), // trace_id
		"",                  // reason
	)
	return nil
}

/* =============== High-performance cached getters =============== */

func (s *UserService) getUserByEmail(ctx context.Context, email string) (db.User, error) {
	// 1) try cache
	if s.Cache != nil {
		if raw, ok, err := s.Cache.Get(ctx, keyByEmail(email)); err == nil && ok {
			var v userCacheView
			if err := json.Unmarshal([]byte(raw), &v); err == nil {
				// cache never stores password hash; fetch hash from DB only if needed
				// For login, we need hash -> fall through to DB if caller needs hash.
				// Here: we’ll fetch from DB because Login requires hash.
			}
		}
	}

	// 2) collapse concurrent misses
	val, err, _ := s.sf.Do("user-email-"+email, func() (any, error) {
		u, err := s.Repo.GetUserByEmail(ctx, email)
		if err != nil {
			return db.User{}, err
		}
		// set cache (without hash)
		if s.Cache != nil {
			view := toView(u)
			if b, e := json.Marshal(view); e == nil {
				_ = s.Cache.Set(ctx, keyByEmail(email), string(b), cacheTTLUserHot)
				_ = s.Cache.Set(ctx, keyByID(view.ID), string(b), cacheTTLUserHot)
			}
		}
		return u, nil
	})
	if err != nil {
		return db.User{}, err
	}
	return val.(db.User), nil
}

func (s *UserService) getUserByID(ctx context.Context, id uuid.UUID) (db.User, error) {
	// 1) try cache
	if s.Cache != nil {
		if raw, ok, err := s.Cache.Get(ctx, keyByID(id)); err == nil && ok {
			var v userCacheView
			if err := json.Unmarshal([]byte(raw), &v); err == nil {
				return v.toUserRow(), nil
			}
		}
	}

	// 2) collapse concurrent misses
	val, err, _ := s.sf.Do("user-id-"+id.String(), func() (any, error) {
		u, err := s.Repo.GetUserByID(ctx, id)
		if err != nil {
			return db.User{}, err
		}
		if s.Cache != nil {
			view := toView(u)
			if b, e := json.Marshal(view); e == nil {
				_ = s.Cache.Set(ctx, keyByID(id), string(b), cacheTTLUserWarm)
				_ = s.Cache.Set(ctx, keyByEmail(view.Email), string(b), cacheTTLUserWarm)
			}
		}
		return u, nil
	})
	if err != nil {
		return db.User{}, err
	}
	return val.(db.User), nil
}

/* =============== In-memory limiter pool (per email) =============== */

type limiterPool struct {
	ttl     time.Duration
	maxKeys int

	items map[string]*limiterItem
}

type limiterItem struct {
	lim *rate.Limiter
	exp time.Time
}

func newLimiterPool(ttl time.Duration, maxKeys int) *limiterPool {
	return &limiterPool{
		ttl:     ttl,
		maxKeys: maxKeys,
		items:   make(map[string]*limiterItem, 1024),
	}
}

// Get returns an existing limiter or creates one with rate r, burst b.
// Very lightweight; for extremely high cardinality, shard & sweep periodically.
func (p *limiterPool) Get(key string, r rate.Limit, b int) *rate.Limiter {
	now := time.Now()
	if it, ok := p.items[key]; ok {
		it.exp = now.Add(p.ttl)
		return it.lim
	}
	// prune opportunistically if we exceed the size
	if len(p.items) >= p.maxKeys {
		// simple O(n) sweep; fine in practice with low cadence calls
		for k, it := range p.items {
			if now.After(it.exp) {
				delete(p.items, k)
			}
		}
	}
	lim := rate.NewLimiter(r, b)
	p.items[key] = &limiterItem{lim: lim, exp: now.Add(p.ttl)}
	return lim
}
