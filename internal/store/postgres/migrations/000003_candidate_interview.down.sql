ALTER TABLE interviews DROP COLUMN IF EXISTS last_sequence_number;
DROP INDEX IF EXISTS sessions_company_interview_idx;
ALTER TABLE sessions DROP COLUMN IF EXISTS interview_id;
DROP INDEX IF EXISTS invites_company_interview_idx;
ALTER TABLE invites DROP COLUMN IF EXISTS interview_id;
