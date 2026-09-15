#!/usr/bin/env bash
# Install pgTAP into the local docker-compose Postgres used by PgFlow tests.
# Required for upstream harness --suite pgtap (first-class profile; fails if missing).
# Safe to re-run; no-op when the package is already present.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMPOSE_FILE="$ROOT/test/support/db/compose.yaml"

CONTAINER_ID="$(docker compose -f "$COMPOSE_FILE" ps -q db)"
if [ -z "$CONTAINER_ID" ]; then
  echo "db service is not running; start it with: docker compose -f $COMPOSE_FILE up -d" >&2
  exit 1
fi

if ! docker exec "$CONTAINER_ID" bash -c 'command -v apt-get >/dev/null'; then
  echo "apt-get is unavailable in $CONTAINER_ID; install postgresql-*-pgtap on the host image manually" >&2
  exit 1
fi

docker exec "$CONTAINER_ID" bash -ex -c '
  apt-get update -qq
  apt-get install -y -qq postgresql-17-pgtap || apt-get install -y -qq postgresql-pgtap
'

echo "pgTAP package installed. Harness creates CREATE EXTENSION pgtap on fixture databases."
