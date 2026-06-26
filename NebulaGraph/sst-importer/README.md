# Nebula Exchange SST Import

This directory contains the code and configuration for the SST-to-storage import path:

```text
ClickHouse -> Spark/Exchange -> SST -> Console INGEST -> Nebula Storage
```

`graphd` is still used for schema creation and job submission. The bulk data load bypasses `graphd` and is performed by `storaged` through RocksDB SST ingest.

## Directory Layout

```text
sst-import/
  conf/
    clickhouse_sst_host.conf   # Host-local Spark/Exchange config used by this compose setup
    clickhouse_sst_hdfs.conf   # HDFS-oriented Exchange config template
  ngql/
    schema.ngql                # Space, tag, and edge schema for the SST files
  scripts/
    create_schema.sh
    run_exchange_local.sh
    stage_local_sst_to_storage.sh
    submit_ingest.sh
    show_job.sh
    submit_stats.sh
    show_stats.sh
```

## 1. Start NebulaGraph

From the repository root:

```bash
docker compose -f NebulaGraph/docker-compose/docker-compose.yaml up -d
```

## 2. Create Schema

```bash
NebulaGraph/sst-import/scripts/create_schema.sh
```

The schema is stored in `ngql/schema.ngql`.

## 3. Generate SST Files From ClickHouse

Edit `conf/clickhouse_sst_host.conf` before running if you need to change:

- ClickHouse JDBC URL.
- `202601` partition filters.
- Nebula graph/meta addresses.
- SST output path.

Then run:

```bash
NebulaGraph/sst-import/scripts/run_exchange_local.sh
```

By default, this uses:

```text
/usr/local/spark/bin/spark-submit
/usr/local/spark/jars/nebula-exchange-3.8.0.jar
/home/btc/nebula_sst_clickhouse/sst_output
```

You can override the binaries:

```bash
SPARK_SUBMIT=/path/to/spark-submit \
EXCHANGE_JAR=/path/to/nebula-exchange-3.8.0.jar \
NebulaGraph/sst-import/scripts/run_exchange_local.sh
```

## 4. Stage Local SST Files To Storage

For this Docker Compose setup, there is no HDFS service. Stage the local SST output into each storage container's `download` directory:

```bash
NebulaGraph/sst-import/scripts/stage_local_sst_to_storage.sh
```

Optional overrides:

```bash
SPACE_ID=1 COMPOSE_PROJECT=docker-compose \
NebulaGraph/sst-import/scripts/stage_local_sst_to_storage.sh /home/btc/nebula_sst_clickhouse/sst_output
```

The script copies the partition folders into:

```text
/data/storage/nebula/<space_id>/download
```

inside each `storaged` container.

## 5. Submit Ingest

```bash
NebulaGraph/sst-import/scripts/submit_ingest.sh
NebulaGraph/sst-import/scripts/show_job.sh <ingest_job_id>
```

The successful run on 2026-06-26 used ingest job `2`:

```text
INGEST FINISHED / SUCCEEDED
Succeeded: 3, Failed: 0
```

## 6. Verify Counts

```bash
NebulaGraph/sst-import/scripts/submit_stats.sh
NebulaGraph/sst-import/scripts/show_job.sh <stats_job_id>
NebulaGraph/sst-import/scripts/show_stats.sh
```

The successful `202601` import produced:

```text
tx vertices:        12,117,179
input_to_tx edges:  29,608,529
tx_to_output edges: 28,053,168
total vertices:     12,117,179
total edges:        57,661,697
```

## HDFS Variant

Use `conf/clickhouse_sst_hdfs.conf` when Spark/Exchange writes to HDFS.

After Exchange finishes, run in Nebula Console:

```ngql
USE bitcoin;
SUBMIT JOB DOWNLOAD HDFS "hdfs://name_node:9000/bitcoin/sst/202601";
SHOW JOB <download_job_id>;
SUBMIT JOB INGEST;
SHOW JOB <ingest_job_id>;
```

## Notes

- Exchange uses the ClickHouse HTTP/JDBC port `8123`, not native port `9000`.
- If schema changes, regenerate SST files. SST encodes Nebula space, tag, and edge IDs.
- Do not reuse old SST files after rebuilding the space or changing tag/edge definitions.
- Generated SST output and Docker storage data are intentionally not included in this directory.
