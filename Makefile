.PHONY: build test integration-test lint generate check web-build web-lint up down smoke

build:
	go build ./...

test:
	go test ./...

# Requires `make up` and `make smoke` first.
integration-test:
	go test -tags integration ./...

lint:
	golangci-lint run ./...

generate:
	go generate ./...
	cd web/packages/api-client && pnpm run generate
	cd web && pnpm -r typecheck

web-build:
	cd web && pnpm install --frozen-lockfile=false && pnpm -r build

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down -v

smoke:
	./scripts/smoke.sh

check: build test web-build
