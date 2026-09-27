-- Candidates are not company users. A session belongs either to a company
-- member (user_id) or to a candidate's interview (interview_id), never both.
ALTER TABLE sessions ALTER COLUMN user_id DROP NOT NULL;
UPDATE sessions SET user_id = NULL WHERE interview_id IS NOT NULL;
ALTER TABLE sessions
    ADD CONSTRAINT sessions_principal_check
    CHECK ((user_id IS NULL) <> (interview_id IS NULL));
