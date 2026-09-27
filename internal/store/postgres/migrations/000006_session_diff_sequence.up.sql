-- Highest client diff sequence accepted for a Candidate Workspace session.
-- A diff is accepted only if its client sequence is greater, which drops
-- stale and duplicate diffs atomically.
ALTER TABLE sessions
    ADD COLUMN last_diff_sequence bigint NOT NULL DEFAULT 0
    CHECK (last_diff_sequence >= 0);
