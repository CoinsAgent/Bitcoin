#!/usr/bin/env python3
"""
Export ClickHouse Bitcoin tables to CSV files for the VID-only NebulaGraph schema.

Generated files under one directory per UTC YYYYMM partition:
  - <output-dir>/<YYYYMM>/tx_vertices.csv
  - <output-dir>/<YYYYMM>/input_to_tx_edges.csv
  - <output-dir>/<YYYYMM>/tx_to_output_edges.csv

NebulaGraph model:
  - tx vertex only
  - input_to_tx edge: Address VID -> Tx VID
  - tx_to_output edge: Tx VID -> Address VID
  - no address tag and no address vertex CSV

Partition arguments are inclusive YYYYMM values. The script exports each
partition separately. Transaction vertices are read from
bitcoin.transactions.transaction_month. Edges are read from
bitcoin.addresses.address_month. All month derivation in the source schema uses
UTC, and block_time is exported as UTC Unix seconds.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
import textwrap
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable


DEFAULT_CH_HOST = "192.168.2.241"
DEFAULT_CH_PORT = 9000
DEFAULT_OUTPUT_DIR = "nebula_csv"
DEFAULT_NG_SPACE = "bitcoin"
DEFAULT_IMPORTER_CONFIG_NAME = "bitcoin_import.yaml"
DEFAULT_IMPORTER_MOUNT_ROOT = "/importer"
PROGRESS_EVERY_ROWS = 1_000_000


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate one NebulaGraph CSV file set per ClickHouse UTC YYYYMM partition."
    )
    p.add_argument("--ch-host", default=DEFAULT_CH_HOST, help="ClickHouse host")
    p.add_argument("--ch-port", type=int, default=DEFAULT_CH_PORT, help="ClickHouse native port")
    p.add_argument("--ch-user", default="default", help="ClickHouse user")
    p.add_argument("--ch-password", default="", help="ClickHouse password")
    p.add_argument("--ch-database", default="bitcoin", help="ClickHouse database")
    p.add_argument(
        "--from-partition",
        type=int,
        required=True,
        help="Inclusive UTC YYYYMM partition to start from, for example 202401.",
    )
    p.add_argument(
        "--to-partition",
        type=int,
        required=True,
        help="Inclusive UTC YYYYMM partition to end at, for example 202403.",
    )
    p.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Base directory for generated per-partition CSV files (default: {DEFAULT_OUTPUT_DIR}).",
    )
    p.add_argument(
        "--ng-space",
        default=DEFAULT_NG_SPACE,
        help=f"NebulaGraph space name for generated importer configs (default: {DEFAULT_NG_SPACE}).",
    )
    p.add_argument(
        "--importer-config-name",
        default=DEFAULT_IMPORTER_CONFIG_NAME,
        help=(
            "Importer config filename to write inside each partition directory "
            f"(default: {DEFAULT_IMPORTER_CONFIG_NAME})."
        ),
    )
    p.add_argument(
        "--importer-mount-root",
        default=DEFAULT_IMPORTER_MOUNT_ROOT,
        help=(
            "Container path where output-dir is mounted for generated importer configs "
            f"(default: {DEFAULT_IMPORTER_MOUNT_ROOT})."
        ),
    )
    p.add_argument(
        "--importer-batch",
        type=int,
        default=10_000,
        help="Nebula importer batch size for generated configs (default: 10000).",
    )
    p.add_argument(
        "--importer-reader-concurrency",
        type=int,
        default=2,
        help="Nebula importer reader concurrency for generated configs (default: 2).",
    )
    p.add_argument(
        "--importer-concurrency",
        type=int,
        default=8,
        help="Nebula importer write concurrency for generated configs (default: 8).",
    )
    p.add_argument(
        "--no-final",
        action="store_true",
        help="Do not add FINAL to ClickHouse reads. Faster, but may include replaced rows.",
    )
    p.add_argument(
        "--max-block-size",
        type=int,
        default=100_000,
        help="ClickHouse max_block_size setting for streamed reads (default: 100000).",
    )
    return p.parse_args()


def validate_partition_range(from_partition: int, to_partition: int) -> None:
    for name, value in (("from-partition", from_partition), ("to-partition", to_partition)):
        text = str(value)
        if len(text) != 6 or not text.isdigit():
            raise ValueError(f"{name} must be a YYYYMM integer, got {value!r}")
        month = int(text[4:6])
        if month < 1 or month > 12:
            raise ValueError(f"{name} has an invalid month, got {value!r}")
    if from_partition > to_partition:
        raise ValueError("--from-partition must be <= --to-partition")


def iter_month_partitions(from_partition: int, to_partition: int) -> Iterable[int]:
    year = from_partition // 100
    month = from_partition % 100
    while True:
        partition = year * 100 + month
        if partition > to_partition:
            break
        yield partition
        month += 1
        if month == 13:
            year += 1
            month = 1


def table_expr(database: str, table: str, use_final: bool) -> str:
    suffix = " FINAL" if use_final else ""
    return f"{database}.{table}{suffix}"


def tx_vid(txid: Any) -> str:
    return f"tx:{txid}"


def addr_vid(address: Any) -> str:
    return f"addr:{address}"


def csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return int(value.timestamp())
    if isinstance(value, dt.date):
        midnight = dt.datetime.combine(value, dt.time.min, tzinfo=dt.timezone.utc)
        return int(midnight.timestamp())
    return value


def iter_clickhouse_rows(ch_client: Any, query: str, max_block_size: int) -> Iterable[tuple[Any, ...]]:
    settings = {"max_block_size": int(max_block_size)}
    if hasattr(ch_client, "execute_iter"):
        yield from ch_client.execute_iter(query, settings=settings)
        return
    yield from ch_client.execute(query, settings=settings)


def print_progress(label: str, count: int) -> None:
    if count and count % PROGRESS_EVERY_ROWS == 0:
        print(f"  {label}: wrote {count:,} rows", flush=True)


def write_tx_vertices_csv(
    ch_client: Any,
    output_path: Path,
    *,
    database: str,
    partition: int,
    use_final: bool,
    max_block_size: int,
) -> int:
    query = f"""
        SELECT
            txid,
            `hash`,
            block_hash,
            toInt64(block_height) AS block_height,
            toInt64(block_time) AS block_time
        FROM {table_expr(database, "transactions", use_final)}
        WHERE transaction_month = {int(partition)}
    """

    count = 0
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([":VID", "txid", "hash", "block_hash", "block_height", "block_time"])
        for txid, hash_val, block_hash, block_height, block_time in iter_clickhouse_rows(
            ch_client, query, max_block_size
        ):
            if not txid:
                continue
            writer.writerow(
                [
                    tx_vid(txid),
                    csv_value(txid),
                    csv_value(hash_val),
                    csv_value(block_hash),
                    csv_value(block_height),
                    csv_value(block_time),
                ]
            )
            count += 1
            print_progress("tx_vertices", count)
    return count


def write_input_edges_csv(
    ch_client: Any,
    output_path: Path,
    *,
    database: str,
    partition: int,
    use_final: bool,
    max_block_size: int,
) -> int:
    query = f"""
        SELECT
            address,
            txid,
            toInt64(source_index) AS input_index,
            utxo_txid,
            toInt64(utxo_vout) AS utxo_vout,
            value
        FROM {table_expr(database, "addresses", use_final)}
        WHERE address_month = {int(partition)}
          AND direction = 'input'
          AND address != ''
          AND txid != ''
          AND utxo_txid != ''
    """

    count = 0
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([":SRC", ":DST", ":RANK", "txid", "input_index", "utxo_txid", "utxo_vout", "value"])
        for address, txid, input_index, utxo_txid, utxo_vout, value in iter_clickhouse_rows(
            ch_client, query, max_block_size
        ):
            if not address or not txid:
                continue
            writer.writerow(
                [
                    addr_vid(address),
                    tx_vid(txid),
                    csv_value(input_index),
                    csv_value(txid),
                    csv_value(input_index),
                    csv_value(utxo_txid),
                    csv_value(utxo_vout),
                    csv_value(value),
                ]
            )
            count += 1
            print_progress("input_to_tx_edges", count)
    return count


def write_output_edges_csv(
    ch_client: Any,
    output_path: Path,
    *,
    database: str,
    partition: int,
    use_final: bool,
    max_block_size: int,
) -> int:
    query = f"""
        SELECT
            address,
            txid,
            utxo_txid,
            toInt64(utxo_vout) AS utxo_vout,
            value
        FROM {table_expr(database, "addresses", use_final)}
        WHERE address_month = {int(partition)}
          AND direction = 'output'
          AND address != ''
          AND txid != ''
          AND utxo_txid != ''
    """

    count = 0
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([":SRC", ":DST", ":RANK", "utxo_txid", "utxo_vout", "value"])
        for address, txid, utxo_txid, utxo_vout, value in iter_clickhouse_rows(
            ch_client, query, max_block_size
        ):
            if not address or not txid:
                continue
            writer.writerow(
                [
                    tx_vid(txid),
                    addr_vid(address),
                    csv_value(utxo_vout),
                    csv_value(utxo_txid),
                    csv_value(utxo_vout),
                    csv_value(value),
                ]
            )
            count += 1
            print_progress("tx_to_output_edges", count)
    return count


def importer_path(mount_root: str, partition: int, filename: str) -> str:
    return f"{mount_root.rstrip('/')}/{partition}/{filename}"


def write_importer_config(
    output_path: Path,
    *,
    partition: int,
    mount_root: str,
    ng_space: str,
    batch: int,
    reader_concurrency: int,
    importer_concurrency: int,
) -> None:
    config = f"""\
    client:
      version: v3
      address: graphd:9669
      user: root
      password: nebula
      retry: 3
      concurrencyPerAddress: 4
      reconnectInitialInterval: 1s
      retryInitialInterval: 1s

    manager:
      spaceName: {ng_space}
      batch: {int(batch)}
      readerConcurrency: {int(reader_concurrency)}
      importerConcurrency: {int(importer_concurrency)}
      statsInterval: 10s

    log:
      level: INFO
      console: true
      files:
        - {importer_path(mount_root, partition, "nebula-importer.log")}

    sources:
      - path: {importer_path(mount_root, partition, "tx_vertices.csv")}
        batch: {int(batch)}
        csv:
          withHeader: true
        tags:
          - name: tx
            id:
              type: STRING
              index: 0
            props:
              - name: txid
                type: STRING
                index: 1
              - name: hash
                type: STRING
                index: 2
              - name: block_hash
                type: STRING
                index: 3
              - name: block_height
                type: INT
                index: 4
              - name: block_time
                type: INT
                index: 5

      - path: {importer_path(mount_root, partition, "input_to_tx_edges.csv")}
        batch: {int(batch)}
        csv:
          withHeader: true
        edges:
          - name: input_to_tx
            src:
              id:
                type: STRING
                index: 0
            dst:
              id:
                type: STRING
                index: 1
            rank:
              index: 2
            props:
              - name: txid
                type: STRING
                index: 3
              - name: input_index
                type: INT
                index: 4
              - name: utxo_txid
                type: STRING
                index: 5
              - name: utxo_vout
                type: INT
                index: 6
              - name: value
                type: DOUBLE
                index: 7

      - path: {importer_path(mount_root, partition, "tx_to_output_edges.csv")}
        batch: {int(batch)}
        csv:
          withHeader: true
        edges:
          - name: tx_to_output
            src:
              id:
                type: STRING
                index: 0
            dst:
              id:
                type: STRING
                index: 1
            rank:
              index: 2
            props:
              - name: utxo_txid
                type: STRING
                index: 3
              - name: utxo_vout
                type: INT
                index: 4
              - name: value
                type: DOUBLE
                index: 5
    """
    output_path.write_text(textwrap.dedent(config), encoding="utf-8")


def main() -> int:
    args = parse_args()
    try:
        validate_partition_range(args.from_partition, args.to_partition)
    except ValueError as exc:
        print(f"Invalid arguments: {exc}", file=sys.stderr)
        return 2

    try:
        from clickhouse_driver import Client as CHClient
    except ImportError:
        print("Missing dependency: pip install clickhouse-driver", file=sys.stderr)
        return 1

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ch_client = CHClient(
        host=args.ch_host,
        port=args.ch_port,
        user=args.ch_user,
        password=args.ch_password,
        database=args.ch_database,
    )

    use_final = not args.no_final
    partitions = list(iter_month_partitions(args.from_partition, args.to_partition))
    print(
        "Exporting UTC partitions one by one: "
        f"{args.from_partition}..{args.to_partition} from ClickHouse database {args.ch_database}"
    )
    print(f"Output directory: {output_dir}")

    total_tx_count = 0
    total_input_count = 0
    total_output_count = 0

    for partition in partitions:
        partition_dir = output_dir / str(partition)
        partition_dir.mkdir(parents=True, exist_ok=True)
        print(f"Partition {partition}: writing {partition_dir}")

        tx_count = write_tx_vertices_csv(
            ch_client,
            partition_dir / "tx_vertices.csv",
            database=args.ch_database,
            partition=partition,
            use_final=use_final,
            max_block_size=args.max_block_size,
        )
        input_count = write_input_edges_csv(
            ch_client,
            partition_dir / "input_to_tx_edges.csv",
            database=args.ch_database,
            partition=partition,
            use_final=use_final,
            max_block_size=args.max_block_size,
        )
        output_count = write_output_edges_csv(
            ch_client,
            partition_dir / "tx_to_output_edges.csv",
            database=args.ch_database,
            partition=partition,
            use_final=use_final,
            max_block_size=args.max_block_size,
        )
        write_importer_config(
            partition_dir / args.importer_config_name,
            partition=partition,
            mount_root=args.importer_mount_root,
            ng_space=args.ng_space,
            batch=args.importer_batch,
            reader_concurrency=args.importer_reader_concurrency,
            importer_concurrency=args.importer_concurrency,
        )

        total_tx_count += tx_count
        total_input_count += input_count
        total_output_count += output_count
        print(
            f"Partition {partition}: "
            f"tx_vertices={tx_count}, input_to_tx_edges={input_count}, "
            f"tx_to_output_edges={output_count}"
        )
        print(f"Partition {partition}: importer config={partition_dir / args.importer_config_name}")

    print(
        "Total rows: "
        f"tx_vertices={total_tx_count}, input_to_tx_edges={total_input_count}, "
        f"tx_to_output_edges={total_output_count}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
