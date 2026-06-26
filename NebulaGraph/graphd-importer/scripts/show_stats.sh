#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/../docker-compose/docker-compose.yaml}"

printf 'USE bitcoin;\nSHOW STATS;\nSHOW JOBS;\n' | docker compose -f "$COMPOSE_FILE" exec -T console \
  nebula-console -addr graphd -port 9669 -u root -p nebula
