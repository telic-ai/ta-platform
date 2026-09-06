ALTER TABLE sessions
    ADD COLUMN state text NOT NULL DEFAULT 'active'
    CHECK (state IN ('active', 'completed', 'expired', 'revoked'));

-- Bearer tokens do not carry a tenant identifier, so their hashes must be
-- globally unambiguous at lookup time.
CREATE UNIQUE INDEX invites_token_hash_idx ON invites (token_hash);
CREATE UNIQUE INDEX sessions_token_hash_idx ON sessions (token_hash);
CREATE INDEX sessions_state_expiry_idx ON sessions (state, expires_at);
