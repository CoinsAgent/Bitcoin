#!/usr/bin/env bash
set -euo pipefail

SPACE="${SPACE:-bitcoin}"
GRAPH_ADDR="${GRAPH_ADDR:-graphd}"
GRAPH_PORT="${GRAPH_PORT:-9669}"
GRAPH_USER="${GRAPH_USER:-root}"
GRAPH_PASSWORD="${GRAPH_PASSWORD:-nebula}"
GRAPH_CONF_ADDRS="${GRAPH_CONF_ADDRS:-192.168.2.65:9669}"
META_ADDRS="${META_ADDRS:-172.20.0.2:9559,172.20.0.3:9559,172.20.0.4:9559}"
SPARK_SUBMIT="${SPARK_SUBMIT:-/opt/spark/bin/spark-submit}"
EXCHANGE_JAR="${EXCHANGE_JAR:-/opt/nebula-exchange/nebula-exchange_spark_3.0-3.8.0.jar}"
SPARK_MASTER="${SPARK_MASTER:-local[4]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-8g}"
SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-192.168.2.65}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-256}"
SPARK_TASK_MAX_FAILURES="${SPARK_TASK_MAX_FAILURES:-4}"
EXPECTED_SST_FILES="${EXPECTED_SST_FILES:-$((SHUFFLE_PARTITIONS * 3))}"
SPARK_DOCKER_IMAGE="${SPARK_DOCKER_IMAGE:-}"
SPARK_DOCKER_NETWORK="${SPARK_DOCKER_NETWORK:-nebula_nebula-net}"
SPARK_DOCKER_SPARK_DIR="${SPARK_DOCKER_SPARK_DIR:-/opt/spark-3.3.4-bin-hadoop3}"
SPARK_DOCKER_WORKDIR="${SPARK_DOCKER_WORKDIR:-/home/btc}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-nebula}"
CONSOLE_CONTAINER="${CONSOLE_CONTAINER:-nebula-console-1}"
STORAGE_CONTAINER_PREFIX="${STORAGE_CONTAINER_PREFIX:-nebula-storaged}"
STORAGE_CONTAINER_SUFFIX="${STORAGE_CONTAINER_SUFFIX:--1}"
STORAGE_COUNT="${STORAGE_COUNT:-3}"
REMOTE_ROOT="${REMOTE_ROOT:-/home/btc/nebula_sst_monthly}"
REPO_ROOT="${REPO_ROOT:-/home/btc/Bitcoin/NebulaGraph/sst-importer}"
BASE_CONF="${BASE_CONF:-$REPO_ROOT/conf/clickhouse_sst_host.conf}"
RUN_CONF_DIR="${RUN_CONF_DIR:-$REPO_ROOT/conf/generated_monthly}"
LOG_DIR="${LOG_DIR:-$REMOTE_ROOT/logs}"
KEEP_SST="${KEEP_SST:-1}"
MONTH_START="${MONTH_START:-200901}"
MONTH_END="${MONTH_END:-202312}"
RESET_SPACE="${RESET_SPACE:-0}"
TZ=UTC
export TZ SPARK_LOCAL_IP

mkdir -p "$REMOTE_ROOT" "$RUN_CONF_DIR" "$LOG_DIR"

log() {
  printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

console() {
  docker exec -i "$CONSOLE_CONTAINER" nebula-console \
    -addr "$GRAPH_ADDR" -port "$GRAPH_PORT" -u "$GRAPH_USER" -p "$GRAPH_PASSWORD"
}

run_ngql() {
  printf '%s\n' "$1" | console
}

months() {
  local y="${MONTH_START:0:4}" m="${MONTH_START:4:2}"
  local end_y="${MONTH_END:0:4}" end_m="${MONTH_END:4:2}"
  while (( y < end_y || (y == end_y && 10#$m <= 10#$end_m) )); do
    printf '%04d%02d\n' "$y" "$((10#$m))"
    m="$(printf '%02d' $((10#$m + 1)))"
    if [[ "$m" == "13" ]]; then
      m="01"
      y=$((y + 1))
    fi
  done
}

wait_no_space() {
  local i output
  for i in $(seq 1 120); do
    output="$(run_ngql 'SHOW SPACES;' || true)"
    if ! grep -q "\"$SPACE\"" <<<"$output"; then
      return 0
    fi
    sleep 5
  done
  log "Timed out waiting for space $SPACE to disappear"
  return 1
}

wait_space_ready() {
  local i output
  for i in $(seq 1 120); do
    output="$(run_ngql "$(printf 'USE %s;\nSHOW TAGS;\nSHOW EDGES;' "$SPACE")" || true)"
    if grep -q '"tx"' <<<"$output" && grep -q '"input_to_tx"' <<<"$output" && grep -q '"tx_to_output"' <<<"$output"; then
      return 0
    fi
    sleep 5
  done
  log "Timed out waiting for schema in $SPACE"
  return 1
}

detect_space_id() {
  local ids id i
  for i in $(seq 1 120); do
    ids="$(
      {
        for storage_id in $(seq 0 $((STORAGE_COUNT - 1))); do
          docker exec "${STORAGE_CONTAINER_PREFIX}${storage_id}${STORAGE_CONTAINER_SUFFIX}" \
            sh -lc "find /data/storage/nebula -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null" || true
        done
      } | awk '/^[0-9]+$/ && $0 != "0"' | sort -n | uniq || true
    )"
    id="$(tail -n 1 <<<"$ids")"
    if [[ -n "$id" ]]; then
      printf '%s\n' "$id"
      return 0
    fi
    sleep 5
  done
  log "Could not detect a Nebula space id under storage data paths"
  return 1
}

extract_job_id() {
  awk -F'|' '/^[|][[:space:]]*[0-9]+[[:space:]]*[|]/ {gsub(/[[:space:]]/, "", $2); print $2; exit}'
}

submit_job() {
  local command="$1" output job_id
  output="$(run_ngql "$(printf 'USE %s;\nSUBMIT JOB %s;\nSHOW JOBS;' "$SPACE" "$command")")"
  printf '%s\n' "$output" >> "$LOG_DIR/jobs.log"
  job_id="$(extract_job_id <<<"$output")"
  if [[ -z "$job_id" ]]; then
    log "Could not parse job id for SUBMIT JOB $command"
    printf '%s\n' "$output"
    return 1
  fi
  printf '%s\n' "$job_id"
}

wait_job() {
  local job_id="$1" label="$2" i output
  for i in $(seq 1 720); do
    output="$(run_ngql "$(printf 'USE %s;\nSHOW JOB %s;' "$SPACE" "$job_id")")"
    printf '%s\n' "$output" >> "$LOG_DIR/job_${job_id}_${label}.log"
    if grep -q '"FAILED"\|FAILED' <<<"$output" || grep -q '"STOPPED"\|STOPPED' <<<"$output"; then
      log "$label job $job_id failed or stopped"
      printf '%s\n' "$output"
      return 1
    fi
    if grep -q '"FINISHED"' <<<"$output" && grep -q '"Succeeded:3"' <<<"$output" && grep -q '"Failed:0"' <<<"$output"; then
      log "$label job $job_id finished successfully"
      return 0
    fi
    sleep 10
  done
  log "Timed out waiting for $label job $job_id"
  return 1
}

create_clean_space() {
  log "Dropping space $SPACE"
  run_ngql "DROP SPACE IF EXISTS $SPACE;" | tee "$LOG_DIR/drop_space.log"
  wait_no_space

  log "Creating space $SPACE"
  run_ngql "CREATE SPACE $SPACE(partition_num=256, replica_factor=2, vid_type=FIXED_STRING(100));" | tee "$LOG_DIR/create_space.log"
  sleep 30

  log "Creating tag and edge schema"
  run_ngql "USE $SPACE;
CREATE TAG tx(txid string, hash string, block_hash string, block_height int64, block_time int64);
CREATE EDGE input_to_tx(txid string, input_index int64, utxo_txid string, utxo_vout int64, value double);
CREATE EDGE tx_to_output(utxo_txid string, utxo_vout int64, value double);" | tee "$LOG_DIR/create_schema.log"
  wait_space_ready
}

make_month_conf() {
  local month="$1"
  local month_root="$REMOTE_ROOT/$month"
  local conf="$RUN_CONF_DIR/clickhouse_sst_${month}.conf"
  local graph_literal meta_literal
  if [[ ! -f "$BASE_CONF" ]]; then
    log "Base Exchange config not found: $BASE_CONF"
    return 1
  fi
  mkdir -p "$month_root/sst_output" "$month_root/errors"
  graph_literal="$(printf '%s' "$GRAPH_CONF_ADDRS" | awk -F, '{for(i=1;i<=NF;i++){printf "%s\"%s\"", (i==1?"":", "), $i}}')"
  meta_literal="$(printf '%s' "$META_ADDRS" | awk -F, '{for(i=1;i<=NF;i++){printf "%s\"%s\"", (i==1?"":", "), $i}}')"
  MONTH="$month" MONTH_ROOT="$month_root" GRAPH_LITERAL="$graph_literal" META_LITERAL="$meta_literal" perl -0pe '
    s/202601/$ENV{MONTH}/g;
    s#/home/btc/nebula_sst_clickhouse(?:_[0-9]{6})?/sst_output#$ENV{MONTH_ROOT}/sst_output#g;
    s#/home/btc/nebula_sst_clickhouse(?:_[0-9]{6})?/errors#$ENV{MONTH_ROOT}/errors#g;
    s#graph:\s*\[[^\]]+\]#graph: [$ENV{GRAPH_LITERAL}]#s;
    s#meta:\s*\[[^\]]+\]#meta: [$ENV{META_LITERAL}]#s;
  ' "$BASE_CONF" > "$conf"
  printf '%s\n' "$conf"
}

spark_submit_cmd() {
  if [[ -n "$SPARK_DOCKER_IMAGE" ]]; then
    docker run --rm --user 0 \
      --network "$SPARK_DOCKER_NETWORK" \
      -v "$SPARK_DOCKER_SPARK_DIR:/opt/spark:ro" \
      -v "/opt/nebula-exchange:/opt/nebula-exchange:ro" \
      -v "/home/btc:/home/btc" \
      -w "$SPARK_DOCKER_WORKDIR" \
      "$SPARK_DOCKER_IMAGE" \
      "$SPARK_SUBMIT" "$@"
  else
    "$SPARK_SUBMIT" "$@"
  fi
}

run_exchange() {
  local month="$1" conf="$2" month_root="$REMOTE_ROOT/$month"
  log "Generating SST for $month"
  rm -rf "$month_root/sst_output" "$month_root/errors"
  mkdir -p "$month_root/sst_output" "$month_root/errors"
  spark_submit_cmd \
    --master "$SPARK_MASTER" \
    --driver-memory "$SPARK_DRIVER_MEMORY" \
    --conf "spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS" \
    --conf "spark.task.maxFailures=$SPARK_TASK_MAX_FAILURES" \
    --class com.vesoft.nebula.exchange.Exchange \
    "$EXCHANGE_JAR" \
    -c "$conf" 2>&1 | tee "$LOG_DIR/exchange_${month}.log"

  local sst_count error_count size
  sst_count="$(find "$month_root/sst_output" -type f -name '*.sst' | wc -l)"
  error_count="$(find "$month_root/errors" -type f | wc -l)"
  size="$(du -sh "$month_root/sst_output" | awk '{print $1}')"
  log "$month SST files=$sst_count size=$size error_files=$error_count"
  if grep -qiE 'MalformedChunkCodingException|Job aborted due to stage failure|Task [0-9]+ in stage [0-9.]+ failed|ERROR TaskSetManager' "$LOG_DIR/exchange_${month}.log"; then
    log "$month Exchange log contains Spark/ClickHouse failures"
    return 1
  fi
  if [[ "$sst_count" -ne "$EXPECTED_SST_FILES" || "$error_count" -ne 0 ]]; then
    log "$month has invalid SST output"
    return 1
  fi
}

stage_sst() {
  local month="$1" space_id="$2" month_root="$REMOTE_ROOT/$month"
  local storage_id container count
  for storage_id in $(seq 0 $((STORAGE_COUNT - 1))); do
    container="${STORAGE_CONTAINER_PREFIX}${storage_id}${STORAGE_CONTAINER_SUFFIX}"
    log "Staging $month SST to $container space_id=$space_id"
    docker exec "$container" sh -lc "rm -rf /data/storage/nebula/$space_id/download && mkdir -p /data/storage/nebula/$space_id/download"
    docker cp "$month_root/sst_output/." "$container:/data/storage/nebula/$space_id/download/"
    count="$(docker exec "$container" sh -lc "find /data/storage/nebula/$space_id/download -type f -name '*.sst' | wc -l")"
    log "$container staged $count SST files"
  done
}

cleanup_downloads() {
  local space_id="$1" storage_id container
  for storage_id in $(seq 0 $((STORAGE_COUNT - 1))); do
    container="${STORAGE_CONTAINER_PREFIX}${storage_id}${STORAGE_CONTAINER_SUFFIX}"
    docker exec "$container" sh -lc "rm -rf /data/storage/nebula/$space_id/download"
  done
}

main() {
  log "Starting UTC monthly SST rebuild for $SPACE from $MONTH_START to $MONTH_END"
  if [[ "$RESET_SPACE" == "1" ]]; then
    create_clean_space
  else
    log "RESET_SPACE=0, using existing $SPACE schema"
    wait_space_ready
  fi
  local space_id
  space_id="$(detect_space_id)"
  log "Detected space id $space_id for $SPACE"

  local month conf ingest_job
  for month in $(months); do
    log "===== Month $month begin ====="
    conf="$(make_month_conf "$month")"
    run_exchange "$month" "$conf"
    stage_sst "$month" "$space_id"
    ingest_job="$(submit_job INGEST)"
    log "$month ingest job id $ingest_job"
    wait_job "$ingest_job" "ingest_${month}"
    cleanup_downloads "$space_id"
    if [[ "$KEEP_SST" != "1" ]]; then
      rm -rf "$REMOTE_ROOT/$month/sst_output"
    fi
    log "===== Month $month complete ====="
  done

  local stats_job
  stats_job="$(submit_job STATS)"
  log "stats job id $stats_job"
  wait_job "$stats_job" "stats"
  run_ngql "$(printf 'USE %s;\nSHOW STATS;\nSHOW JOBS;' "$SPACE")" | tee "$LOG_DIR/final_stats.log"
  log "Completed UTC monthly SST rebuild for $SPACE"
}

main "$@"
