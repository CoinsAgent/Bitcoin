#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/../docker-compose/docker-compose.yaml}"
PARTITION="${1:-${BITCOIN_IMPORT_PARTITION:-202601}}"
IMPORT_ROOT="${IMPORT_ROOT:-/home/btc/nebula}"
CONFIG="$IMPORT_ROOT/$PARTITION/bitcoin_import.yaml"

if [[ ! -f "$CONFIG" ]]; then
  echo "Importer config not found: $CONFIG" >&2
  echo "Run scripts/export_partition_csv.sh $PARTITION first, or set IMPORT_ROOT." >&2
  exit 1
fi

BITCOIN_IMPORT_PARTITION="$PARTITION" docker compose -f "$COMPOSE_FILE" --profile tools run --rm bitcoin-importer
