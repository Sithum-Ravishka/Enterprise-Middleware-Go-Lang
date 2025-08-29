DROP INDEX IF EXISTS sessions_user_active_idx;
ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_token_hash_uniq;
DROP TABLE IF EXISTS sessions;

DROP TRIGGER IF EXISTS trg_users_set_updated_at ON users;
DROP FUNCTION IF EXISTS set_updated_at();
DROP INDEX IF EXISTS users_created_desc_cover;
DROP INDEX IF EXISTS users_email_trgm;
DROP INDEX IF EXISTS users_email_uq_cover;
DROP TABLE IF EXISTS users;

-- keep extensions; they may be used elsewhere
-- DROP EXTENSION IF EXISTS pg_trgm;
-- DROP EXTENSION IF EXISTS citext;
