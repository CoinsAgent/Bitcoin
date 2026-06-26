#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-/home/btc/nebula}"

if [[ $# -gt 0 ]]; then
  PARTITION="$1"
  shift
else
  PARTITION="${BITCOIN_IMPORT_PARTITION:-202601}"
fi

python3 "$ROOT_DIR/src/export_clickhouse_to_nebula_csv_by_partition.py" \
  --from-partition "$PARTITION" \
  --to-partition "$PARTITION" \
  --output-dir "$OUTPUT_DIR" \
  "$@"
