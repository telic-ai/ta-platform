# ta-platform

Monorepo for the Lucid AI talent-acquisition platform: nine Go services under
`cmd/`, shared Go packages under `internal/`, and two pnpm workspace apps
(company + candidate) under `web/`.

## Vault

Design docs referenced by the build plan live in an external vault
(`docs/vault/` is a local placeholder — the source notes were not available
when this scaffold was created):

- Lucid AI – Architecture
- Deployment & Local Dev
- Event Catalog
- Candidate Workspace Service
- Admin API
- Postgres / Kafka / ClickHouse / Redis / S3

When those notes become available, drop them into `docs/vault/` and update
this file to link them, and reconcile `internal/events`, `api/openapi.yaml`,
and the service list in `cmd/` against their actual contents — the current
versions are reasonable placeholders, not verified against the source
design.

## Layout

- `cmd/<service>/main.go` — one binary per service (admin-api, candidate-api,
  auth-service, company-service, job-service, matching-service,
  notification-service, analytics-service, ingestion-service).
- `internal/platform` — config, structured logging, telemetry, shared by
  every service.
- `internal/store/{postgres,kafka,clickhouse,redis,s3}` — thin client
  wrappers behind interfaces, each with a local integration test gated by
  the `integration` build tag (needs `make up`).
- `internal/store/postgres/migrations` — embedded, transactional Postgres
  migrations. `cmd/migrate` applies them through `make migrate-up` and rolls
  back one version through `make migrate-down`.
- `internal/domain` — persistence-neutral domain records. Tenant-owned records
  carry `CompanyID`; Postgres keys and query helpers scope by it.
- `internal/events` — envelope type and per-event-type payloads shared
  across services and Kafka topics.
- `internal/outbox` — transactional outbox relay. Services write events to
  the Postgres `event_outbox` table in the same transaction as the state
  change; the relay publishes them to Kafka in order.
- `internal/auth` — opaque bearer-token hashing and session lookup. A
  session belongs either to a company member (`user_id`) or to a candidate's
  interview (`interview_id`).
- `internal/rbac` — resolves a company member's (company_id, user_id, role)
  from their bearer session and holds the role → permission matrix.
  `rbac.Middleware` authenticates; `rbac.Require` guards each route.
- `internal/adminapi` — Admin API (`cmd/admin-api`, `:8082` locally):
  company-scoped CRUD for company/users/interviews/tasks, tokenized invites,
  human-only score decisions (`human_*`), policy toggles, and erasure
  *marking* (`POST /interviews/{id}/erase` sets `erase_requested_at`, returns
  202, deletes nothing). Postgres side is `postgres.AdminStore`; every
  statement lives in `adminQueries` and a unit test asserts each is
  `company_id`-scoped.
- `internal/livemonitor` — Live Monitoring (`cmd/live-monitor`, `:8083`
  locally). A Kafka consumer group appends each session event to a capped
  per-interview Redis ring (sorted set by `sequence_number`, duplicates
  dropped) and `PUBLISH`es it atomically (Lua). `GET /interviews/{id}/live`
  is SSE with the sequence number as the event id: it subscribes, catches
  up after `Last-Event-ID` from the ring (ClickHouse via `replay.Store` when
  the ring no longer reaches back), then streams live, repairing pub/sub
  gaps from the ring. `MemoryBus` is the in-process Bus for tests.
- `internal/dashboard` — dashboards and replay as Admin API routes:
  ClickHouse materialized views (`company_daily_events`,
  `interview_activity`, created by admin-api at startup after the events
  table) behind `GET /dashboard/overview` and `GET /dashboard/interviews`,
  and `GET /interviews/{id}/timeline` (honours the `replay` policy). View
  counts are at-least-once (a view sees Kafka redeliveries the events table
  later deduplicates).
- `internal/search` — Typesense scoped keys: `POST /search/key` on the
  Admin API derives a short-lived (≤1h) key from `TYPESENSE_SEARCH_KEY` (a
  search-only parent key, never the admin key) that hard-embeds
  `filter_by: company_id:=<company>`. Typesense ANDs it into every search
  and rejects the key if it is altered; the integration test proves this
  against the local Typesense.
- `internal/demo` + `cmd/demo-seed` — `ENV=local` only: creates a company
  whose owner/interviewer/viewer have ready-made session tokens (member
  sign-in is not built yet) and prints them as JSON.
- `internal/aigateway` — AI Gateway (`cmd/ai-gateway`): `POST /v1/complete`
  streams a model completion as SSE and always emits `ai.response.completed`.
  `internal/aigateway/byok` resolves a company's own key (BYOK): an
  envelope (KMS data key + AES-GCM) stored in Secrets Manager, decrypted
  in-process into a TTL cache that zeroes keys on eviction and picks up
  rotation on the next call. Enabled with `BYOK_ENABLED=true`;
  `companies.ai_key_mode` selects managed or byok per company.
- `internal/sandbox` — Execution Sandbox runners (gVisor and local Docker)
  behind one `Runner` interface. Its integration tests need a Docker engine,
  the language images pulled, and `scripts/install-gvisor.sh` for the gVisor
  cases (skipped if `runsc` is not registered). Reference pod manifests are
  in `deploy/k8s/execution-sandbox/`.
- `internal/eventindexer` — Event Indexer (`cmd/event-indexer`): consumes
  session events and upserts prompts, AI responses and code diffs into the
  Typesense `session_events` collection, keyed by `event_id` so redelivery
  is a no-op. Searches must filter by `company_id`. Typesense is reached
  through `internal/store/typesense` (`TYPESENSE_URL`, `TYPESENSE_API_KEY`).
- `internal/scoring` — Scoring Service (`cmd/scoring-service`): on
  `session.submitted`/`session.expired` it waits (NACK + capped backoff)
  until the ClickHouse event log holds every sequence number up to the
  trigger, computes `internal/scoring/metrics` (pure functions; fixtures in
  `metrics/fixtures` are shared with the ClickHouse view test), asks for a
  strict-JSON AI recommendation, and stores one `scores` row per session with
  `score.computed` in the outbox. An AI failure stores a null recommendation
  with `recommendation_error`; it never blocks the row.
- `internal/analytics` — ClickHouse materialized views over `events`
  (`mv_session_metrics`, `mv_task_daily`, `mv_funnel`, `mv_billing_usage`),
  created (and backfilled when new) by `cmd/event-log-writer` at startup.
  Read them through the deduplicating views `v_session_metrics`,
  `v_task_daily`, `v_funnel`, `v_billing_usage`; never sum a view target
  without `FINAL`. `v_session_metrics` must stay equal to
  `internal/scoring/metrics` — the fixture test enforces it.
- `internal/housekeeper` — Housekeeper CronJob (`cmd/housekeeper`,
  `deploy/k8s/housekeeper`): purges interviews past `RETENTION` or with
  `erase_requested_at`, never under `legal_hold`. Holding the interview row
  lock, it deletes Typesense documents, then ClickHouse `events`,
  `session_metric_rows` and `interview_activity`, then skeletonizes
  Postgres (PII blanked; invites, sessions, scores and interview_scores
  deleted) and writes `purge_log`. It never touches S3
  (bucket lifecycle) or Kafka (topic retention). There is no `ai_calls`
  table, despite the vault note.
- `internal/billing` — monthly close (`cmd/billing-close`,
  `deploy/k8s/billing-close`): reads `v_billing_usage`, prices it with
  `plans.price_table` (pure `Price`; BYOK tokens are not billed), applies
  `billing_credits` (pure `ApplyCredits`, never below zero, balance net of
  other non-void invoices) and writes one draft per `(company_id, period)`
  into `invoices`/`invoice_lines`. Re-runs recompute drafts; issued invoices
  are never touched.
- `internal/e2e` — cross-service integration tests (`integration` tag).
  The live-monitoring and Admin API flows are covered here too.
- `api/openapi.yaml` — the single source of truth for candidate + admin
  HTTP APIs; `make generate` produces Go server stubs
  (`internal/apigen/...` via oapi-codegen) and the TS client in
  `web/packages/api-client` (via openapi-typescript).
- `web/apps/company` — Vite + React company app: dashboard, interviews
  (schedule, candidate invite links, erase), live view (resumes via
  Last-Event-ID) and replay (rebuilds files from `code.diff` patches on the
  shared workspace starters). `pnpm dev` serves :5174 and proxies `/api` to
  admin-api (:8082) and `/live-api` to live-monitor (:8083). Sign in with a
  token from `go run ./cmd/demo-seed`.
- `web/` — pnpm workspace: `apps/company`, `apps/candidate`,
  `packages/ui`, `packages/api-client`. `apps/candidate` is a Vite + React
  app that talks only to the Candidate Workspace (`pnpm dev` proxies
  `/session` to candidate-api). `packages/api-client` holds the typed
  workspace client, POST-SSE parsing and the diff debouncer; its
  `test/fixtures/patches.json` is shared with a Go contract test.
- `deploy/docker-compose.yml` — local dependency stack (postgres, kafka in
  KRaft mode, redis, typesense, clickhouse, minio).

## Commands

- `make build` — `go build ./...`
- `make test` — `go test ./...` (unit tests only)
- `make integration-test` — `go test -tags integration ./...`, requires `make up`
- `make migrate-up` / `make migrate-down` — apply all pending Postgres
  migrations or roll back the latest version.
- `make demo-seed` — create a local demo company and print member tokens.
- `make web-build` — `pnpm -r build` in `web/`
- `make web-test` — `pnpm -r test` (Vitest) in `web/`
- `make generate` — regenerate API stubs/clients from `api/openapi.yaml`
- `make up` / `make down` — start/stop the local dependency stack
- `make smoke` — verify the stack is reachable (`scripts/smoke.sh`)
- `make check` — build + test + web-build + web-test (the CI gate)

## Conventions

- Go module: `github.com/telic-ai/ta-platform`, single root `go.mod`.
- Services read config from env vars with local-dev defaults matching
  `deploy/docker-compose.yml` (see `internal/platform/config`).
- Integration tests that need real infra are behind `//go:build integration`
  and run against `make up`.
