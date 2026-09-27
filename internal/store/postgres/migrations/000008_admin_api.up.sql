-- Company roles are a closed set; the Admin API's RBAC matrix is keyed by it.
ALTER TABLE users
    ADD CONSTRAINT users_role_known_check
    CHECK (role IN ('owner', 'admin', 'recruiter', 'interviewer', 'viewer'));

-- Scores proposed for an interview (by AI scoring) and the human decision on
-- each. Only a human decision moves a score out of 'proposed', and every
-- decided score records who decided it and when.
CREATE TABLE interview_scores (
    company_id uuid NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    interview_id uuid NOT NULL,
    dimension text NOT NULL CHECK (dimension <> ''),
    proposed_value double precision NOT NULL,
    rationale text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT 'proposed'
        CHECK (status IN ('proposed', 'human_approved', 'human_rejected', 'human_adjusted')),
    final_value double precision,
    decision_note text NOT NULL DEFAULT '',
    decided_by uuid,
    decided_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (company_id, id),
    UNIQUE (company_id, interview_id, dimension),
    FOREIGN KEY (company_id, interview_id)
        REFERENCES interviews (company_id, id) ON DELETE CASCADE,
    FOREIGN KEY (company_id, decided_by)
        REFERENCES users (company_id, id) ON DELETE SET NULL (decided_by),
    CHECK ((status = 'proposed') = (decided_at IS NULL))
);

-- Per-company feature toggles. A missing row means the policy's default.
CREATE TABLE company_policies (
    company_id uuid NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    key text NOT NULL CHECK (key <> ''),
    enabled boolean NOT NULL,
    updated_by uuid,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (company_id, key),
    FOREIGN KEY (company_id, updated_by)
        REFERENCES users (company_id, id) ON DELETE SET NULL (updated_by)
);

CREATE INDEX interviews_company_created_idx ON interviews (company_id, created_at DESC);
