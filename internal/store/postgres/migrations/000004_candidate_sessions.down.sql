-- Candidate sessions have no user to restore, so they cannot survive the
-- NOT NULL constraint.
ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_principal_check;
DELETE FROM sessions WHERE user_id IS NULL;
ALTER TABLE sessions ALTER COLUMN user_id SET NOT NULL;
