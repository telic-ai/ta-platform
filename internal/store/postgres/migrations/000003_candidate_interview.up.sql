-- A candidate invite and the session exchanged for it belong to exactly one
-- interview; every session event carries that interview_id.
ALTER TABLE invites ADD COLUMN interview_id uuid;
ALTER TABLE invites
    ADD CONSTRAINT invites_interview_fkey FOREIGN KEY (company_id, interview_id)
        REFERENCES interviews (company_id, id) ON DELETE CASCADE;
CREATE INDEX invites_company_interview_idx
    ON invites (company_id, interview_id)
    WHERE interview_id IS NOT NULL;

ALTER TABLE sessions ADD COLUMN interview_id uuid;
ALTER TABLE sessions
    ADD CONSTRAINT sessions_interview_fkey FOREIGN KEY (company_id, interview_id)
        REFERENCES interviews (company_id, id) ON DELETE CASCADE;
CREATE INDEX sessions_company_interview_idx
    ON sessions (company_id, interview_id)
    WHERE interview_id IS NOT NULL;

-- Per-interview event sequence. Producers allocate the next number with an
-- atomic increment in the same transaction as the state change they record.
ALTER TABLE interviews
    ADD COLUMN last_sequence_number bigint NOT NULL DEFAULT 0
    CHECK (last_sequence_number >= 0);
