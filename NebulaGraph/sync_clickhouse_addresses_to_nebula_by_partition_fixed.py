#!/usr/bin/env python3
"""
Sync ClickHouse bitcoin.addresses to NebulaGraph block by block.

This fixed version focuses on correctness and observability:

1. It reads from ClickHouse only. It does NOT write back to ClickHouse.
2. It writes vertices and edges to NebulaGraph with valid nGQL tuple syntax.
3. It checks every NebulaGraph execution result with result.is_succeeded().
4. It fails fast by default, so Nebula errors are not silently ignored.
5. It supports --dry-run and --debug-ngql for troubleshooting.

Expected NebulaGraph schema:

CREATE TAG IF NOT EXISTS address(address string);
CREATE TAG IF NOT EXISTS tx(
    txid string,
    hash string,
    block_hash string,
    block_height int64,
    block_time int64
);

CREATE EDGE IF NOT EXISTS input_to_tx(
    direction string,
    txid string,
    hash string,
    block_hash string,
    block_height int64,
    block_time int64,
    utxo_txid string,
    utxo_vout int64,
    source_index int64,
    value double,
    value_delta double,
    revision int64
);

CREATE EDGE IF NOT EXISTS tx_to_output(
    direction string,
    txid string,
    hash string,
    block_hash string,
    block_height int64,
    block_time int64,
    utxo_txid string,
    utxo_vout int64,
    source_index int64,
    value double,
    value_delta double,
    revision int64
);
"""

from __future__ import annotations

import argparse
import datetime as dt
import math
import sys
from dataclasses import dataclass
from typing import Any, Iterable, Optional


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync ClickHouse bitcoin.addresses to NebulaGraph by partition and block"
    )

    p.add_argument("--ch-host", default="192.168.2.241", help="ClickHouse host")
    p.add_argument("--ch-port", type=int, default=9000, help="ClickHouse native port")
    p.add_argument("--ch-user", default="default", help="ClickHouse user")
    p.add_argument("--ch-password", default="", help="ClickHouse password")

    p.add_argument(
        "--start-partition",
        required=True,
        help="Start partition YYYYMM. The script auto-discovers all partitions >= this value.",
    )
    p.add_argument(
        "--end-partition",
        default=None,
        help="Optional end partition YYYYMM. Useful for testing a small range.",
    )
    p.add_argument(
        "--only-block-height",
        type=int,
        default=None,
        help="Optional single block height to sync for debugging.",
    )
    p.add_argument(
        "--max-blocks",
        type=int,
        default=None,
        help="Optional maximum number of blocks to process.",
    )

    p.add_argument("--ng-host", default="192.168.2.65", help="NebulaGraph host")
    p.add_argument("--ng-port", type=int, default=9669, help="NebulaGraph graphd port")
    p.add_argument("--ng-user", default="root", help="NebulaGraph user")
    p.add_argument("--ng-password", default="nebula", help="NebulaGraph password")
    p.add_argument("--ng-space", default="bitcoin", help="NebulaGraph space name")

    p.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Log NebulaGraph insert errors and continue. Default is fail-fast.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print generated nGQL instead of executing it.",
    )
    p.add_argument(
        "--debug-ngql",
        action="store_true",
        help="Print every generated nGQL statement before executing it.",
    )
    p.add_argument(
        "--verify-after-use",
        action="store_true",
        help="Run SHOW TAGS and SHOW EDGES after USE space to verify schema visibility.",
    )

    return p.parse_args()


@dataclass(frozen=True)
class AddressRow:
    address: str
    direction: str
    txid: str
    hash_val: str
    block_hash: str
    block_height: int
    block_time: Any
    utxo_txid: str
    utxo_vout: int
    source_index: int
    value: float
    value_delta: float
    revision: int


# -----------------------------
# ClickHouse read functions
# -----------------------------


def discover_partitions(ch_client: Any, start_partition: int, end_partition: Optional[int]) -> list[str]:
    where_end = "" if end_partition is None else f"AND block_month <= {int(end_partition)}"
    query = f"""
        SELECT DISTINCT block_month
        FROM bitcoin.blocks
        WHERE block_month >= {int(start_partition)}
          {where_end}
        ORDER BY block_month
    """
    result = ch_client.execute(query)
    return [str(row[0]) for row in result]


def get_blocks_for_partition(
    ch_client: Any,
    partition: int,
    only_block_height: Optional[int] = None,
) -> list[tuple[int, int, str]]:
    where_block = "" if only_block_height is None else f"AND height = {int(only_block_height)}"
    query = f"""
        SELECT
            block_month,
            height,
            hash
        FROM bitcoin.blocks
        WHERE block_month = {int(partition)}
          {where_block}
        ORDER BY height
    """
    return ch_client.execute(query)


def get_addresses_for_block(ch_client: Any, partition: int, block_height: int) -> list[AddressRow]:
    query = f"""
        SELECT
            address,
            direction,
            txid,
            hash,
            block_hash,
            block_height,
            block_time,
            utxo_txid,
            utxo_vout,
            source_index,
            value,
            value_delta,
            revision
        FROM bitcoin.addresses
        WHERE address_month = {int(partition)}
          AND block_height = {int(block_height)}
        ORDER BY address, txid, direction, source_index, utxo_vout
    """
    rows = ch_client.execute(query)
    return [AddressRow(*row) for row in rows]


# -----------------------------
# nGQL value formatting helpers
# -----------------------------


def escape_string(value: Any) -> str:
    """Return a raw escaped string without surrounding quotes."""
    if value is None:
        return ""
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def q(value: Any) -> str:
    """Return a NebulaGraph quoted string literal."""
    return f'"{escape_string(value)}"'


def q_vid(prefix: str, value: Any) -> str:
    """Return a quoted NebulaGraph VID with a stable prefix."""
    return q(f"{prefix}:{value}")


def ng_int(value: Any, default: int = 0) -> str:
    if value is None or value == "":
        return str(default)
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(default)


def ng_float(value: Any, default: float = 0.0) -> str:
    if value is None or value == "":
        return str(default)
    try:
        number = float(value)
        if not math.isfinite(number):
            return str(default)
        return repr(number)
    except (TypeError, ValueError):
        return str(default)


def to_unix_timestamp(value: Any, default: int = 0) -> int:
    """
    Convert ClickHouse block_time values to the NebulaGraph schema's int64 timestamp.

    bitcoin.addresses.block_time is UInt64 in ClickHouse. Some drivers can return
    datetime/date-like values when queries change, so keep those cases safe too.
    """
    if value is None or value == "":
        return default

    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return int(value.timestamp())

    if isinstance(value, dt.date):
        midnight = dt.datetime.combine(value, dt.time.min, tzinfo=dt.timezone.utc)
        return int(midnight.timestamp())

    try:
        return int(value)
    except (TypeError, ValueError):
        pass

    text = str(value).strip()
    if text.isdigit():
        return int(text)

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = dt.datetime.strptime(text.split(".", 1)[0].rstrip("Z"), fmt)
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            continue

    return default


def ng_block_time(value: Any) -> str:
    return ng_int(to_unix_timestamp(value))


def safe_rank(value: Any) -> int:
    """
    Nebula edge rank must be an integer.
    Use 0 when the source value is NULL/empty.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


# -----------------------------
# NebulaGraph execution helpers
# -----------------------------


def result_error_message(result: Any) -> str:
    for attr in ("error_msg", "error_message"):
        fn = getattr(result, attr, None)
        if callable(fn):
            try:
                return str(fn())
            except Exception:
                pass
    return str(result)


def execute_ngql_or_raise(
    ng_session: Any,
    ngql: str,
    context: str = "",
    *,
    dry_run: bool = False,
    debug_ngql: bool = False,
) -> Any:
    ngql = "\n".join(line.rstrip() for line in ngql.strip().splitlines())

    if debug_ngql or dry_run:
        print("\n--- nGQL", f"[{context}]" if context else "", "---")
        print(ngql)

    if dry_run:
        return None

    result = ng_session.execute(ngql)
    is_succeeded = getattr(result, "is_succeeded", None)

    if callable(is_succeeded) and not result.is_succeeded():
        raise RuntimeError(
            f"NebulaGraph query failed: {context}\n"
            f"Error: {result_error_message(result)}\n"
            f"nGQL:\n{ngql}"
        )

    return result


def execute_ngql(
    ng_session: Any,
    ngql: str,
    context: str,
    *,
    dry_run: bool = False,
    debug_ngql: bool = False,
    continue_on_error: bool = False,
) -> bool:
    try:
        execute_ngql_or_raise(
            ng_session,
            ngql,
            context,
            dry_run=dry_run,
            debug_ngql=debug_ngql,
        )
        return True
    except Exception as exc:
        if continue_on_error:
            print(f"WARNING: {exc}", file=sys.stderr)
            return False
        raise


# -----------------------------
# NebulaGraph insert builders
# -----------------------------


def build_address_vertex_ngql(addr: str) -> str:
    # Correct nGQL syntax: VALUES "vid":("property value"), not JSON object.
    return f"""
        INSERT VERTEX address(address)
        VALUES {q_vid("addr", addr)}:({q(addr)});
    """


def build_tx_vertex_ngql(txid: str, hash_val: str, block_hash: str, block_height: int, block_time: Any) -> str:
    return f"""
        INSERT VERTEX tx(txid, hash, block_hash, block_height, block_time)
        VALUES {q_vid("tx", txid)}:(
            {q(txid)},
            {q(hash_val)},
            {q(block_hash)},
            {ng_int(block_height)},
            {ng_block_time(block_time)}
        );
    """


def edge_values(row: AddressRow) -> str:
    return f"""
        {q(row.direction)},
        {q(row.txid)},
        {q(row.hash_val)},
        {q(row.block_hash)},
        {ng_int(row.block_height)},
        {ng_block_time(row.block_time)},
        {q(row.utxo_txid)},
        {ng_int(row.utxo_vout)},
        {ng_int(row.source_index)},
        {ng_float(row.value)},
        {ng_float(row.value_delta)},
        {ng_int(row.revision)}
    """


def build_input_edge_ngql(row: AddressRow) -> str:
    # input edge: address -> tx, rank = source_index
    return f"""
        INSERT EDGE input_to_tx(
            direction,
            txid,
            hash,
            block_hash,
            block_height,
            block_time,
            utxo_txid,
            utxo_vout,
            source_index,
            value,
            value_delta,
            revision
        )
        VALUES {q_vid("addr", row.address)}->{q_vid("tx", row.txid)}@{safe_rank(row.source_index)}:(
            {edge_values(row)}
        );
    """


def build_output_edge_ngql(row: AddressRow) -> str:
    # output edge: tx -> address, rank = utxo_vout/output index
    return f"""
        INSERT EDGE tx_to_output(
            direction,
            txid,
            hash,
            block_hash,
            block_height,
            block_time,
            utxo_txid,
            utxo_vout,
            source_index,
            value,
            value_delta,
            revision
        )
        VALUES {q_vid("tx", row.txid)}->{q_vid("addr", row.address)}@{safe_rank(row.utxo_vout)}:(
            {edge_values(row)}
        );
    """


def insert_vertices_and_edges(
    ng_session: Any,
    addresses_data: list[AddressRow],
    *,
    dry_run: bool = False,
    debug_ngql: bool = False,
    continue_on_error: bool = False,
) -> dict[str, int]:
    """Insert address vertices, tx vertices, and input/output edges into NebulaGraph."""
    stats = {
        "address_vertices": 0,
        "tx_vertices": 0,
        "input_edges": 0,
        "output_edges": 0,
        "failed": 0,
    }

    if not addresses_data:
        return stats

    unique_addresses = sorted({row.address for row in addresses_data if row.address})

    transactions: dict[str, tuple[str, str, int, Any]] = {}
    input_edges: list[AddressRow] = []
    output_edges: list[AddressRow] = []

    for row in addresses_data:
        if not row.address or not row.txid:
            print(f"WARNING: skip row with empty address or txid: {row}", file=sys.stderr)
            stats["failed"] += 1
            continue

        transactions.setdefault(
            row.txid,
            (row.hash_val, row.block_hash, row.block_height, row.block_time),
        )

        if str(row.direction).lower() == "input":
            input_edges.append(row)
        elif str(row.direction).lower() == "output":
            output_edges.append(row)
        else:
            print(f"WARNING: skip row with unknown direction {row.direction!r}: {row}", file=sys.stderr)
            stats["failed"] += 1

    for addr in unique_addresses:
        ok = execute_ngql(
            ng_session,
            build_address_vertex_ngql(addr),
            f"insert address vertex {addr}",
            dry_run=dry_run,
            debug_ngql=debug_ngql,
            continue_on_error=continue_on_error,
        )
        stats["address_vertices" if ok else "failed"] += 1

    for txid, (hash_val, block_hash, block_height, block_time) in transactions.items():
        ok = execute_ngql(
            ng_session,
            build_tx_vertex_ngql(txid, hash_val, block_hash, block_height, block_time),
            f"insert tx vertex {txid}",
            dry_run=dry_run,
            debug_ngql=debug_ngql,
            continue_on_error=continue_on_error,
        )
        stats["tx_vertices" if ok else "failed"] += 1

    for row in input_edges:
        ok = execute_ngql(
            ng_session,
            build_input_edge_ngql(row),
            f"insert input_to_tx edge {row.address}->{row.txid}@{row.source_index}",
            dry_run=dry_run,
            debug_ngql=debug_ngql,
            continue_on_error=continue_on_error,
        )
        stats["input_edges" if ok else "failed"] += 1

    for row in output_edges:
        ok = execute_ngql(
            ng_session,
            build_output_edge_ngql(row),
            f"insert tx_to_output edge {row.txid}->{row.address}@{row.utxo_vout}",
            dry_run=dry_run,
            debug_ngql=debug_ngql,
            continue_on_error=continue_on_error,
        )
        stats["output_edges" if ok else "failed"] += 1

    return stats


# -----------------------------
# Verification helpers
# -----------------------------


def verify_nebula_space(ng_session: Any, ng_space: str, *, dry_run: bool = False, debug_ngql: bool = False) -> None:
    execute_ngql_or_raise(
        ng_session,
        f"USE {ng_space};",
        f"USE space {ng_space}",
        dry_run=dry_run,
        debug_ngql=debug_ngql,
    )


def print_schema_overview(ng_session: Any) -> None:
    for statement in ("SHOW TAGS;", "SHOW EDGES;"):
        result = execute_ngql_or_raise(ng_session, statement, statement)
        print(f"\n{statement}")
        print(result)


def print_final_counts(ng_session: Any) -> None:
    checks = [
        ("address vertices", "MATCH (v:address) RETURN count(v);"),
        ("tx vertices", "MATCH (v:tx) RETURN count(v);"),
        ("input_to_tx edges", "MATCH ()-[e:input_to_tx]->() RETURN count(e);"),
        ("tx_to_output edges", "MATCH ()-[e:tx_to_output]->() RETURN count(e);"),
    ]
    print("\nNebulaGraph verification counts:")
    for label, statement in checks:
        try:
            result = execute_ngql_or_raise(ng_session, statement, label)
            print(f"  {label}: {result}")
        except Exception as exc:
            print(f"  WARNING: failed to count {label}: {exc}", file=sys.stderr)


# -----------------------------
# Main workflow
# -----------------------------


def main() -> None:
    args = parse_args()

    try:
        from clickhouse_driver import Client as CHClient
    except ImportError:
        print("Missing dependency: pip install clickhouse-driver", file=sys.stderr)
        sys.exit(2)

    try:
        from nebula3.Config import Config
        from nebula3.gclient.net import ConnectionPool
    except ImportError:
        print("Missing dependency: pip install nebula3-python", file=sys.stderr)
        sys.exit(2)

    ch_client = CHClient(
        host=args.ch_host,
        port=args.ch_port,
        user=args.ch_user,
        password=args.ch_password,
    )

    config = Config()
    config.max_connection_pool_size = 10
    connection_pool = ConnectionPool()

    if not connection_pool.init([(args.ng_host, args.ng_port)], config):
        raise RuntimeError(f"Failed to initialize NebulaGraph connection pool: {args.ng_host}:{args.ng_port}")

    ng_session = connection_pool.get_session(args.ng_user, args.ng_password)

    try:
        verify_nebula_space(
            ng_session,
            args.ng_space,
            dry_run=args.dry_run,
            debug_ngql=args.debug_ngql,
        )

        if args.verify_after_use and not args.dry_run:
            print_schema_overview(ng_session)

        print(f"Discovering partitions from ClickHouse starting from {args.start_partition}...")
        partitions = discover_partitions(
            ch_client,
            int(args.start_partition),
            int(args.end_partition) if args.end_partition else None,
        )

        if not partitions:
            print(f"No partitions found >= {args.start_partition}")
            return

        print(f"Found {len(partitions)} partition(s): {', '.join(partitions)}")

        total_blocks_seen = 0
        total_blocks_with_addresses = 0
        total_address_rows = 0
        total_stats = {
            "address_vertices": 0,
            "tx_vertices": 0,
            "input_edges": 0,
            "output_edges": 0,
            "failed": 0,
        }

        stop = False
        for partition in partitions:
            print(f"\nProcessing partition {partition}...")
            blocks = get_blocks_for_partition(ch_client, int(partition), args.only_block_height)
            print(f"  Found {len(blocks)} block(s)")

            for _block_month, block_height, block_hash in blocks:
                if args.max_blocks is not None and total_blocks_seen >= args.max_blocks:
                    stop = True
                    break

                total_blocks_seen += 1
                print(f"  Syncing block {block_height} ({str(block_hash)[:16]}...)")

                rows = get_addresses_for_block(ch_client, int(partition), int(block_height))
                if not rows:
                    print("    No address rows found; skip Nebula insert.")
                    continue

                total_blocks_with_addresses += 1
                total_address_rows += len(rows)
                print(f"    Found {len(rows)} address row(s)")

                stats = insert_vertices_and_edges(
                    ng_session,
                    rows,
                    dry_run=args.dry_run,
                    debug_ngql=args.debug_ngql,
                    continue_on_error=args.continue_on_error,
                )

                for key, value in stats.items():
                    total_stats[key] += value

                print(
                    "    Insert stats: "
                    f"address_vertices={stats['address_vertices']}, "
                    f"tx_vertices={stats['tx_vertices']}, "
                    f"input_edges={stats['input_edges']}, "
                    f"output_edges={stats['output_edges']}, "
                    f"failed={stats['failed']}"
                )

            if stop:
                break

        print("\nSync complete.")
        print(f"  Blocks scanned: {total_blocks_seen}")
        print(f"  Blocks with address rows: {total_blocks_with_addresses}")
        print(f"  ClickHouse address rows read: {total_address_rows}")
        print(f"  Nebula address vertices attempted: {total_stats['address_vertices']}")
        print(f"  Nebula tx vertices attempted: {total_stats['tx_vertices']}")
        print(f"  Nebula input_to_tx edges attempted: {total_stats['input_edges']}")
        print(f"  Nebula tx_to_output edges attempted: {total_stats['output_edges']}")
        print(f"  Failed/skipped rows/statements: {total_stats['failed']}")

        if not args.dry_run:
            print_final_counts(ng_session)

    finally:
        try:
            ng_session.release()
        finally:
            connection_pool.close()


if __name__ == "__main__":
    main()
