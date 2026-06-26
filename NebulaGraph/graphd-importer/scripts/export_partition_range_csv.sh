#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <from_yyyymm> <to_yyyymm> [exporter args...]" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FROM_PARTITION="$1"
TO_PARTITION="$2"
OUTPUT_DIR="${OUTPUT_DIR:-/home/btc/nebula}"
shift 2

python3 "$ROOT_DIR/src/export_clickhouse_to_nebula_csv_by_partition.py" \
  --from-partition "$FROM_PARTITION" \
  --to-partition "$TO_PARTITION" \
  --output-dir "$OUTPUT_DIR" \
  "$@"
