package repo

import (
	"context"

	db "github.com/example/user-platform/internal/user/sqlc/gen"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepo implements Repository using sqlc-generated queries.
type PostgresRepo struct {
	q *db.Queries
}

func NewPostgresRepo(pool *pgxpool.Pool) *PostgresRepo {
	return &PostgresRepo{q: db.New(pool)}
}

// Users

func (r *PostgresRepo) CreateUser(ctx context.Context, u db.User) (db.User, error) {
	return r.q.CreateUser(ctx, db.CreateUserParams{
		ID:           u.ID,
		Email:        u.Email,
		Username:     u.Username,
		PasswordHash: u.PasswordHash,
	})
}

func (r *PostgresRepo) GetUserByEmail(ctx context.Context, email string) (db.User, error) {
	return r.q.GetUserByEmail(ctx, email)
}

func (r *PostgresRepo) GetUserByID(ctx context.Context, id uuid.UUID) (db.User, error) {
	return r.q.GetUserByID(ctx, pgtype.UUID{Bytes: id, Valid: true})
}

// Sessions

func (r *PostgresRepo) CreateSession(ctx context.Context, s db.Session) (db.Session, error) {
	return r.q.CreateSession(ctx, db.CreateSessionParams{
		ID:        s.ID,
		UserID:    s.UserID,
		TokenHash: s.TokenHash,
		ExpiresAt: s.ExpiresAt,
	})
}

func (r *PostgresRepo) RevokeSession(ctx context.Context, id uuid.UUID) error {
	return r.q.RevokeSessionByID(ctx, pgtype.UUID{Bytes: id, Valid: true})
}

func (r *PostgresRepo) GetSessionByTokenHash(ctx context.Context, tokenHash string) (db.Session, error) {
	return r.q.GetSessionByTokenHash(ctx, tokenHash)
}
