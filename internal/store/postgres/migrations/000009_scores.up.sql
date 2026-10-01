-- One score per Candidate Workspace session, written by the Scoring
-- Service. Metrics are always present; the AI recommendation is advisory
-- and is null (with recommendation_error set) when it failed.
CREATE TABLE scores (
    company_id uuid NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    session_id uuid NOT NULL,
    interview_id uuid NOT NULL,
    trigger_event_type text NOT NULL
        CHECK (trigger_event_type IN ('session.submitted', 'session.expired')),
    metrics jsonb NOT NULL,
    metrics_complete boolean NOT NULL,
    recommendation jsonb,
    recommendation_model text NOT NULL DEFAULT '',
    recommendation_error text,
    computed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (company_id, session_id),
    FOREIGN KEY (company_id, interview_id)
        REFERENCES interviews (company_id, id) ON DELETE CASCADE,
    FOREIGN KEY (company_id, session_id)
        REFERENCES sessions (company_id, id) ON DELETE CASCADE,
    CHECK ((recommendation IS NULL) <> (recommendation_error IS NULL))
);

CREATE INDEX scores_company_interview_idx ON scores (company_id, interview_id);
