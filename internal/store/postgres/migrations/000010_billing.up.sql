-- Plans price monthly usage. price_table is the billing package's
-- PriceTable: integer cents per unit, tokens per million.
CREATE TABLE plans (
    id text PRIMARY KEY CHECK (id <> ''),
    name text NOT NULL CHECK (name <> ''),
    currency text NOT NULL DEFAULT 'USD' CHECK (currency ~ '^[A-Z]{3}$'),
    price_table jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Companies without a plan are not invoiced.
ALTER TABLE companies ADD COLUMN plan_id text REFERENCES plans (id);

-- Prepaid or goodwill credit. It applies to invoices for periods that end
-- after it was granted and start before it expires; what remains is its
-- amount minus the credit lines already on other invoices.
CREATE TABLE billing_credits (
    company_id uuid NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    amount_cents bigint NOT NULL CHECK (amount_cents > 0),
    description text NOT NULL DEFAULT '',
    granted_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    PRIMARY KEY (company_id, id)
);

-- One invoice per company and calendar month. The billing-close job
-- recomputes drafts; issued and void invoices are never changed by it.
CREATE TABLE invoices (
    company_id uuid NOT NULL REFERENCES companies (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    period date NOT NULL CHECK (period = date_trunc('month', period)::date),
    status text NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'issued', 'void')),
    plan_id text NOT NULL REFERENCES plans (id),
    currency text NOT NULL,
    subtotal_cents bigint NOT NULL DEFAULT 0 CHECK (subtotal_cents >= 0),
    credits_cents bigint NOT NULL DEFAULT 0 CHECK (credits_cents >= 0),
    total_cents bigint NOT NULL DEFAULT 0 CHECK (total_cents >= 0),
    computed_at timestamptz NOT NULL DEFAULT now(),
    issued_at timestamptz,
    PRIMARY KEY (company_id, id),
    UNIQUE (company_id, period),
    CHECK (total_cents = subtotal_cents - credits_cents),
    CHECK ((status = 'issued') = (issued_at IS NOT NULL) OR status = 'void')
);

CREATE TABLE invoice_lines (
    company_id uuid NOT NULL,
    invoice_id uuid NOT NULL,
    line_no integer NOT NULL CHECK (line_no > 0),
    kind text NOT NULL CHECK (kind IN (
        'base_fee', 'sessions', 'managed_input_tokens', 'managed_output_tokens', 'runs', 'credit')),
    description text NOT NULL,
    quantity bigint NOT NULL CHECK (quantity >= 0),
    amount_cents bigint NOT NULL,
    credit_id uuid,
    PRIMARY KEY (company_id, invoice_id, line_no),
    FOREIGN KEY (company_id, invoice_id) REFERENCES invoices (company_id, id) ON DELETE CASCADE,
    FOREIGN KEY (company_id, credit_id) REFERENCES billing_credits (company_id, id),
    CHECK ((kind = 'credit') = (credit_id IS NOT NULL)),
    CHECK ((kind = 'credit' AND amount_cents < 0) OR (kind <> 'credit' AND amount_cents >= 0))
);

CREATE INDEX invoice_lines_credit_idx ON invoice_lines (company_id, credit_id) WHERE credit_id IS NOT NULL;
