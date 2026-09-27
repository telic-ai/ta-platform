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
- `internal/e2e` — cross-service integration tests (`integration` tag).
- `api/openapi.yaml` — the single source of truth for candidate + admin
  HTTP APIs; `make generate` produces Go server stubs
  (`internal/apigen/...` via oapi-codegen) and the TS client in
  `web/packages/api-client` (via openapi-typescript).
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
