#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <job_id>" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/../docker-compose/docker-compose.yaml}"
JOB_ID="$1"

printf 'USE bitcoin;\nSHOW JOB %s;\n' "$JOB_ID" | docker compose -f "$COMPOSE_FILE" exec -T console \
  nebula-console -addr graphd -port 9669 -u root -p nebula
