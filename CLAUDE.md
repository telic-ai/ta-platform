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
- `api/openapi.yaml` — the single source of truth for candidate + admin
  HTTP APIs; `make generate` produces Go server stubs
  (`internal/apigen/...` via oapi-codegen) and the TS client in
  `web/packages/api-client` (via openapi-typescript).
- `web/` — pnpm workspace: `apps/company`, `apps/candidate`,
  `packages/ui`, `packages/api-client`.
- `deploy/docker-compose.yml` — local dependency stack (postgres, kafka in
  KRaft mode, redis, typesense, clickhouse, minio).

## Commands

- `make build` — `go build ./...`
- `make test` — `go test ./...` (unit tests only)
- `make integration-test` — `go test -tags integration ./...`, requires `make up`
- `make migrate-up` / `make migrate-down` — apply all pending Postgres
  migrations or roll back the latest version.
- `make web-build` — `pnpm -r build` in `web/`
- `make generate` — regenerate API stubs/clients from `api/openapi.yaml`
- `make up` / `make down` — start/stop the local dependency stack
- `make smoke` — verify the stack is reachable (`scripts/smoke.sh`)
- `make check` — build + test + web-build (the CI gate)

## Conventions

- Go module: `github.com/telic-ai/ta-platform`, single root `go.mod`.
- Services read config from env vars with local-dev defaults matching
  `deploy/docker-compose.yml` (see `internal/platform/config`).
- Integration tests that need real infra are behind `//go:build integration`
  and run against `make up`.
