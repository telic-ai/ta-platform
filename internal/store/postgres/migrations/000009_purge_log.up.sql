-- One row per interview the Housekeeper purged. The interview row itself is
-- kept as a skeleton (ids, status, timestamps) with its personal data
-- removed; this log records when and why.
CREATE TABLE purge_log (
    company_id uuid NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    interview_id uuid NOT NULL,
    reason text NOT NULL CHECK (reason IN ('retention', 'erasure')),
    search_documents_deleted integer NOT NULL CHECK (search_documents_deleted >= 0),
    purged_at timestamptz NOT NULL,
    PRIMARY KEY (company_id, interview_id)
);
