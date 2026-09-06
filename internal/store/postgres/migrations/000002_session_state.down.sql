DROP INDEX IF EXISTS sessions_state_expiry_idx;
DROP INDEX IF EXISTS sessions_token_hash_idx;
DROP INDEX IF EXISTS invites_token_hash_idx;
ALTER TABLE sessions DROP COLUMN IF EXISTS state;
