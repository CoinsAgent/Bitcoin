#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONF="${1:-$ROOT_DIR/conf/clickhouse_sst_host.conf}"
SPARK_SUBMIT="${SPARK_SUBMIT:-/usr/local/spark/bin/spark-submit}"
EXCHANGE_JAR="${EXCHANGE_JAR:-/usr/local/spark/jars/nebula-exchange-3.8.0.jar}"

"$SPARK_SUBMIT" \
  --master local[4] \
  --driver-memory 8g \
  --conf spark.sql.shuffle.partitions=256 \
  --class com.vesoft.nebula.exchange.Exchange \
  "$EXCHANGE_JAR" \
  -c "$CONF"
