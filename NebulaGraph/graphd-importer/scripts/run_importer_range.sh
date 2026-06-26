#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <from_yyyymm> <to_yyyymm>" >&2
  exit 1
fi

from="$1"
to="$2"
year="${from:0:4}"
month="${from:4:2}"

while true; do
  partition="$(printf '%04d%02d' "$((10#$year))" "$((10#$month))")"
  if [[ "$partition" > "$to" ]]; then
    break
  fi
  "$(dirname "${BASH_SOURCE[0]}")/run_importer_partition.sh" "$partition"
  month=$((10#$month + 1))
  if [[ "$month" -eq 13 ]]; then
    year=$((10#$year + 1))
    month=1
  fi
done
