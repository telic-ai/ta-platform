-- Transactional outbox: events are written in the same transaction as the
-- state change they record, then relayed to Kafka and deleted. Rows are
-- relayed in id order by a single relay holding an advisory lock.
CREATE TABLE event_outbox (
    id bigserial PRIMARY KEY,
    company_id uuid NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    topic text NOT NULL CHECK (topic <> ''),
    message_key bytea NOT NULL CHECK (octet_length(message_key) > 0),
    event_type text NOT NULL CHECK (event_type <> ''),
    envelope bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX event_outbox_company_created_idx
    ON event_outbox (company_id, created_at);
