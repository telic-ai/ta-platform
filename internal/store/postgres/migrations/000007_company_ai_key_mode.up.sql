-- Whose provider key pays for a company's AI completions: the platform's
-- (managed) or the company's own, stored envelope-encrypted in Secrets
-- Manager (byok).
ALTER TABLE companies
    ADD COLUMN ai_key_mode text NOT NULL DEFAULT 'managed'
    CHECK (ai_key_mode IN ('managed', 'byok'));
