-- Enable useful extensions
CREATE EXTENSION IF NOT EXISTS citext;   -- case-insensitive text (perfect for emails)
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- fast ILIKE prefix/contains search

-- users
CREATE TABLE IF NOT EXISTS users (
    id            uuid PRIMARY KEY,
    email         citext NOT NULL UNIQUE,      -- avoids duplicates due to case
    username      citext NOT NULL UNIQUE,
    password_hash text   NOT NULL CHECK (length(password_hash) BETWEEN 60 AND 200),
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- updated_at trigger
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger WHERE tgname = 'trg_users_set_updated_at'
  ) THEN
    CREATE TRIGGER trg_users_set_updated_at
      BEFORE UPDATE ON users
      FOR EACH ROW
      EXECUTE FUNCTION set_updated_at();
  END IF;
END$$;

-- Sessions (one row per refresh token; revoke via revoked_at)
CREATE TABLE IF NOT EXISTS sessions (
    id         uuid PRIMARY KEY,
    user_id    uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash text NOT NULL CHECK (length(token_hash) = 64), -- sha256 hex
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);

-- You typically want token_hash UNIQUE (tokens are unique).
-- If you allow duplicates (rare), drop UNIQUE and keep only the index.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'sessions_token_hash_uniq'
  ) THEN
    ALTER TABLE sessions ADD CONSTRAINT sessions_token_hash_uniq UNIQUE (token_hash);
  END IF;
END$$;

-- Cover the hottest lookups and pagination paths

-- 1) Fast login by email (unique already helps); add a small INCLUDE for index-only scans:
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'users_email_uq_cover') THEN
    CREATE UNIQUE INDEX users_email_uq_cover ON users(email) INCLUDE (id, password_hash, username, created_at, updated_at);
  END IF;
END$$;

-- 2) Case-insensitive prefix/contains search on emails (trigram)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'users_email_trgm') THEN
    CREATE INDEX users_email_trgm ON users USING gin (email gin_trgm_ops);
  END IF;
END$$;

-- 3) Keyset-ready listing (newest users first), covering index
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'users_created_desc_cover') THEN
    CREATE INDEX users_created_desc_cover
      ON users (created_at DESC, id DESC)
      INCLUDE (username, email);
  END IF;
END$$;

-- 4) Sessions: fast lookup by hash (unique), plus active (not revoked) sessions by user for housekeeping
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'sessions_user_active_idx') THEN
    CREATE INDEX sessions_user_active_idx
      ON sessions (user_id, expires_at DESC)
      WHERE revoked_at IS NULL;
  END IF;
END$$;
