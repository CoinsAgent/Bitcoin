#!/usr/bin/env bash
set -euo pipefail

SST_OUTPUT="${1:-/home/btc/nebula_sst_clickhouse/sst_output}"
SPACE_ID="${SPACE_ID:-1}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-docker-compose}"

if [[ ! -d "$SST_OUTPUT" ]]; then
  echo "SST output directory not found: $SST_OUTPUT" >&2
  exit 1
fi

sst_count="$(find "$SST_OUTPUT" -maxdepth 2 -type f -name '*.sst' | wc -l)"
if [[ "$sst_count" -eq 0 ]]; then
  echo "No .sst files found under: $SST_OUTPUT" >&2
  exit 1
fi

for storage_id in 0 1 2; do
  container="${COMPOSE_PROJECT}-storaged${storage_id}-1"
  docker exec "$container" sh -lc "rm -rf /data/storage/nebula/$SPACE_ID/download && mkdir -p /data/storage/nebula/$SPACE_ID/download"
  docker cp "$SST_OUTPUT/." "$container:/data/storage/nebula/$SPACE_ID/download/"
  printf 'storage%s ' "$storage_id"
  docker exec "$container" sh -lc "find /data/storage/nebula/$SPACE_ID/download -maxdepth 2 -type f -name '*.sst' | wc -l && du -sh /data/storage/nebula/$SPACE_ID/download"
done
