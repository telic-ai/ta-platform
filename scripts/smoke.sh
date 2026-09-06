#!/usr/bin/env bash
# Verifies that every service in deploy/docker-compose.yml is reachable.
set -euo pipefail

fail=0

check() {
  local name="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "ok   $name"
  else
    echo "FAIL $name"
    fail=1
  fi
}

check "postgres" bash -c 'PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d ta_platform -c "select 1"'
check "redis"    bash -c 'redis-cli -h localhost -p 6379 ping | grep -q PONG'
check "kafka"    bash -c 'echo > /dev/tcp/localhost/9092'
check "typesense" curl -sf -H "X-TYPESENSE-API-KEY: local-dev-key" http://localhost:8108/health
check "clickhouse" curl -sf http://localhost:8123/ping
check "minio"    curl -sf http://localhost:9100/minio/health/live

exit $fail
