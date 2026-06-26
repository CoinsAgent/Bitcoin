# Nebula Importer v3 Graphd Import

This directory contains the code and configuration for the graphd-based import path:

```text
ClickHouse -> CSV files -> Nebula Importer v3 -> graphd -> Nebula Storage
```

Unlike the SST flow, Nebula Importer v3 writes through `graphd`. It is easier to operate and debug, but slower for very large loads because every write goes through the graph service.

## Directory Layout

```text
importer-graphd/
  conf/
    docker-compose.bitcoin-importer.profile.yaml     # reference compose service for importer
    nebula_importer_vid_only_address_partition.yaml # static importer template for one partition
    bitcoin_import_202601.example.yaml              # example generated importer config
  ngql/
    import_schema.ngql                              # minimal schema for import
    full_schema_with_indexes.ngql                   # documented schema and optional indexes
  scripts/
    create_schema.sh
    export_partition_csv.sh
    export_partition_range_csv.sh
    run_importer_partition.sh
    run_importer_range.sh
    submit_stats.sh
    show_job.sh
    show_stats.sh
  src/
    export_clickhouse_to_nebula_csv_by_partition.py # ClickHouse -> CSV exporter
```

## 1. Install Python Dependencies

From the repository root:

```bash
python3 -m pip install -r NebulaGraph/importer-graphd/requirements.txt
```

The exporter uses the ClickHouse native protocol through `clickhouse-driver`.

## 2. Start NebulaGraph

```bash
docker compose -f NebulaGraph/docker-compose/docker-compose.yaml up -d
```

The compose file already contains a `bitcoin-importer` tools profile using `vesoft/nebula-importer:latest`.

## 3. Create Schema

```bash
NebulaGraph/importer-graphd/scripts/create_schema.sh
```

This runs `ngql/import_schema.ngql`.

Use `ngql/full_schema_with_indexes.ngql` as the fuller documented schema if you also want optional indexes. For large initial imports, create or rebuild indexes after the data load.

## 4. Export CSV From ClickHouse

Export one partition:

```bash
NebulaGraph/importer-graphd/scripts/export_partition_csv.sh 202601
```

Export a range:

```bash
NebulaGraph/importer-graphd/scripts/export_partition_range_csv.sh 202501 202512
```

Default output:

```text
/home/btc/nebula/<YYYYMM>/
  tx_vertices.csv
  input_to_tx_edges.csv
  tx_to_output_edges.csv
  bitcoin_import.yaml
```

That default matches the current Docker Compose mount:

```text
host /home/btc/nebula -> container /importer
```

Common overrides:

```bash
OUTPUT_DIR=/home/btc/nebula \
NebulaGraph/importer-graphd/scripts/export_partition_csv.sh 202601 \
  --ch-host 192.168.2.241 \
  --ch-port 9000 \
  --ch-database bitcoin
```

## 5. Run Nebula Importer v3

Import one partition:

```bash
NebulaGraph/importer-graphd/scripts/run_importer_partition.sh 202601
```

Import a range:

```bash
NebulaGraph/importer-graphd/scripts/run_importer_range.sh 202501 202512
```

Under the hood this runs:

```bash
BITCOIN_IMPORT_PARTITION=202601 \
docker compose -f NebulaGraph/docker-compose/docker-compose.yaml --profile tools run --rm bitcoin-importer
```

The importer reads:

```text
/importer/<YYYYMM>/bitcoin_import.yaml
```

inside the container.

## 6. Verify

After import:

```bash
NebulaGraph/importer-graphd/scripts/submit_stats.sh
NebulaGraph/importer-graphd/scripts/show_job.sh <stats_job_id>
NebulaGraph/importer-graphd/scripts/show_stats.sh
```

## Notes

- This path writes through `graphd`; use `sst-import/` for the storage-bypass SST path.
- CSV files are generated under `/home/btc/nebula` by default and are intentionally not included in this directory.
- The importer config is generated per partition by the exporter, because file paths include the `YYYYMM` partition.
- The static YAML in `conf/` is a template/example for the `202601` layout.
