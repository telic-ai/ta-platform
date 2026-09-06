CREATE TABLE companies (
    id uuid PRIMARY KEY,
    name text NOT NULL CHECK (name <> ''),
    slug text NOT NULL UNIQUE CHECK (slug <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE users (
    company_id uuid NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    email text NOT NULL CHECK (email <> ''),
    display_name text NOT NULL DEFAULT '',
    role text NOT NULL CHECK (role <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (company_id, id),
    UNIQUE (company_id, email)
);

CREATE TABLE interviews (
    company_id uuid NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    created_by uuid,
    candidate_name text NOT NULL CHECK (candidate_name <> ''),
    candidate_email text NOT NULL CHECK (candidate_email <> ''),
    status text NOT NULL CHECK (status <> ''),
    scheduled_at timestamptz,
    terminal_at timestamptz,
    erase_requested_at timestamptz,
    legal_hold boolean NOT NULL DEFAULT false,
    purged_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (company_id, id),
    FOREIGN KEY (company_id, created_by)
        REFERENCES users (company_id, id) ON DELETE SET NULL (created_by),
    CHECK (purged_at IS NULL OR terminal_at IS NOT NULL)
);

CREATE INDEX interviews_company_status_scheduled_idx
    ON interviews (company_id, status, scheduled_at);
CREATE INDEX interviews_company_retention_idx
    ON interviews (company_id, terminal_at)
    WHERE terminal_at IS NOT NULL AND purged_at IS NULL AND legal_hold = false;
CREATE INDEX interviews_company_erase_requested_idx
    ON interviews (company_id, erase_requested_at)
    WHERE erase_requested_at IS NOT NULL AND purged_at IS NULL;

CREATE TABLE tasks (
    company_id uuid NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    interview_id uuid,
    assignee_id uuid,
    title text NOT NULL CHECK (title <> ''),
    description text NOT NULL DEFAULT '',
    status text NOT NULL CHECK (status <> ''),
    due_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (company_id, id),
    FOREIGN KEY (company_id, interview_id)
        REFERENCES interviews (company_id, id) ON DELETE CASCADE,
    FOREIGN KEY (company_id, assignee_id)
        REFERENCES users (company_id, id) ON DELETE SET NULL (assignee_id)
);

CREATE INDEX tasks_company_status_due_idx
    ON tasks (company_id, status, due_at);
CREATE INDEX tasks_company_interview_idx
    ON tasks (company_id, interview_id)
    WHERE interview_id IS NOT NULL;
CREATE INDEX tasks_company_assignee_idx
    ON tasks (company_id, assignee_id, status)
    WHERE assignee_id IS NOT NULL;

CREATE TABLE invites (
    company_id uuid NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    invited_by uuid,
    email text NOT NULL CHECK (email <> ''),
    role text NOT NULL CHECK (role <> ''),
    token_hash bytea NOT NULL CHECK (octet_length(token_hash) > 0),
    expires_at timestamptz NOT NULL,
    accepted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (company_id, id),
    UNIQUE (company_id, token_hash),
    FOREIGN KEY (company_id, invited_by)
        REFERENCES users (company_id, id) ON DELETE SET NULL (invited_by)
);

CREATE INDEX invites_company_email_idx
    ON invites (company_id, email);
CREATE INDEX invites_company_expiry_idx
    ON invites (company_id, expires_at)
    WHERE accepted_at IS NULL;

CREATE TABLE sessions (
    company_id uuid NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    token_hash bytea NOT NULL CHECK (octet_length(token_hash) > 0),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz,
    PRIMARY KEY (company_id, id),
    UNIQUE (company_id, token_hash),
    FOREIGN KEY (company_id, user_id)
        REFERENCES users (company_id, id) ON DELETE CASCADE
);

CREATE INDEX sessions_company_user_expiry_idx
    ON sessions (company_id, user_id, expires_at);
CREATE INDEX sessions_company_expiry_idx
    ON sessions (company_id, expires_at)
    WHERE revoked_at IS NULL;
