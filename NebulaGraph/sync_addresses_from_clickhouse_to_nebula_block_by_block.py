#!/usr/bin/env python3
"""
Sync ClickHouse bitcoin.addresses to NebulaGraph block by block.

This fixed version focuses on correctness and observability:

1. It reads from ClickHouse only. It does NOT write back to ClickHouse.
2. It writes vertices and edges to NebulaGraph with valid nGQL tuple syntax.
3. It checks every NebulaGraph execution result with result.is_succeeded().
4. It fails fast by default, so Nebula errors are not silently ignored.
5. It supports --debug-ngql for troubleshooting.

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
import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional


SLEEP_WHEN_CAUGHT_UP = 120


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync ClickHouse bitcoin.addresses to NebulaGraph by partition and block"
    )

    p.add_argument("--ch-host", default="192.168.2.241", help="ClickHouse host")
    p.add_argument("--ch-port", type=int, default=9000, help="ClickHouse native port")
    p.add_argument("--ch-user", default="default", help="ClickHouse user")
    p.add_argument("--ch-password", default="", help="ClickHouse password")

    p.add_argument(
        "--poll-interval",
        type=int,
        default=SLEEP_WHEN_CAUGHT_UP,
        help="Seconds to sleep when NebulaGraph is caught up with ClickHouse (default: 120).",
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


def get_clickhouse_max_block_height(ch_client: Any) -> Optional[int]:
    result = ch_client.execute(
        """
        SELECT if(count() = 0, -1, toInt64(max(height)))
        FROM bitcoin.blocks
        """
    )
    height = int(result[0][0])
    return None if height < 0 else height


def get_transaction_month_for_block(ch_client: Any, block_height: int) -> Optional[int]:
    result = ch_client.execute(
        f"""
        SELECT toYYYYMM(toDateTime(time)) AS transaction_month
        FROM bitcoin.blocks FINAL
        WHERE height = {int(block_height)}
        ORDER BY revision DESC
        LIMIT 1
        """
    )
    if not result:
        return None
    return int(result[0][0])


def get_clickhouse_txids_for_block(ch_client: Any, block_height: int) -> list[str]:
    transaction_month = get_transaction_month_for_block(ch_client, block_height)
    if transaction_month is None:
        return []

    result = ch_client.execute(
        f"""
        SELECT DISTINCT txid
        FROM bitcoin.transactions FINAL
        WHERE block_height = {int(block_height)}
          AND transaction_month = {int(transaction_month)}
        ORDER BY txid
        """
    )
    return [str(row[0]) for row in result]


def get_block_by_height(ch_client: Any, block_height: int) -> Optional[tuple[int, int, str]]:
    query = f"""
        SELECT
            toYYYYMM(toDateTime(time)) AS block_month,
            height,
            hash
        FROM bitcoin.blocks
        WHERE height = {int(block_height)}
        ORDER BY revision DESC
        LIMIT 1
    """
    result = ch_client.execute(query)
    return result[0] if result else None


def get_addresses_for_block(ch_client: Any, partition: Optional[int], block_height: int) -> list[AddressRow]:
    partition_filter = "" if partition is None else f"AND address_month = {int(partition)}"
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
        WHERE block_height = {int(block_height)}
          {partition_filter}
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


def chunked(items: list[Any], size: int) -> Iterable[list[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


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
    debug_ngql: bool = False,
) -> Any:
    ngql = "\n".join(line.rstrip() for line in ngql.strip().splitlines())

    if debug_ngql:
        print("\n--- nGQL", f"[{context}]" if context else "", "---")
        print(ngql)

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
    debug_ngql: bool = False,
    continue_on_error: bool = False,
) -> bool:
    try:
        execute_ngql_or_raise(
            ng_session,
            ngql,
            context,
            debug_ngql=debug_ngql,
        )
        return True
    except Exception as exc:
        if continue_on_error:
            print(f"WARNING: {exc}", file=sys.stderr)
            return False
        raise


def nebula_value_to_python(value: Any) -> Any:
    if value is None:
        return None
    for method in ("as_int", "as_string", "as_bool", "as_double"):
        fn = getattr(value, method, None)
        if callable(fn):
            try:
                return fn()
            except Exception:
                pass
    for attr in ("value", "_value"):
        if hasattr(value, attr):
            try:
                raw = getattr(value, attr)
                return raw() if callable(raw) else raw
            except Exception:
                pass
    text = str(value)
    if text.upper() in {"NULL", "__NULL__"}:
        return None
    return text.strip('"')


def nebula_result_rows(result: Any) -> list[list[Any]]:
    if result is None:
        return []

    as_primitive = getattr(result, "as_primitive", None)
    if callable(as_primitive):
        try:
            primitive = as_primitive()
            if primitive is None:
                return []
            if isinstance(primitive, dict):
                return [list(primitive.values())]
            if isinstance(primitive, list):
                if not primitive:
                    return []
                if isinstance(primitive[0], dict):
                    return [list(item.values()) for item in primitive]
                if isinstance(primitive[0], list):
                    return primitive
                return [[item] for item in primitive]
        except Exception:
            pass

    rows_fn = getattr(result, "rows", None)
    if callable(rows_fn):
        try:
            out = []
            for row in rows_fn():
                values = getattr(row, "values", None)
                values = values() if callable(values) else values
                if values is None:
                    out.append([nebula_value_to_python(row)])
                else:
                    out.append([nebula_value_to_python(value) for value in values])
            return out
        except Exception:
            pass

    return []


def nebula_scalar(ng_session: Any, ngql: str, context: str) -> Any:
    result = execute_ngql_or_raise(ng_session, ngql, context)
    rows = nebula_result_rows(result)
    if not rows or not rows[0]:
        return None
    return rows[0][0]


def get_nebula_max_tx_block_height(ng_session: Any) -> Optional[int]:
    value = nebula_scalar(
        ng_session,
        """
        LOOKUP ON tx YIELD tx.block_height AS block_height
        | ORDER BY $-.block_height DESC
        | LIMIT 1;
        """,
        "get max tx block height from NebulaGraph",
    )
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def get_existing_nebula_txids(ng_session: Any, txids: list[str]) -> set[str]:
    existing: set[str] = set()
    chunk_size = 500
    for i in range(0, len(txids), chunk_size):
        chunk = txids[i : i + chunk_size]
        if not chunk:
            continue
        txid_values = ", ".join(q(txid) for txid in chunk)
        result = execute_ngql_or_raise(
            ng_session,
            f"""
            LOOKUP ON tx
            WHERE tx.txid IN [{txid_values}]
            YIELD tx.txid AS txid;
            """,
            "lookup existing tx vertices from NebulaGraph by tx_txid_index",
        )
        for row in nebula_result_rows(result):
            if row and row[0]:
                existing.add(str(row[0]))
    return existing


def count_missing_transactions_for_block(
    ch_client: Any,
    ng_session: Any,
    block_height: int,
) -> tuple[int, int, int]:
    expected_txids = get_clickhouse_txids_for_block(ch_client, block_height)
    existing_txids = get_existing_nebula_txids(ng_session, expected_txids)
    expected = len(expected_txids)
    existing = len(existing_txids)
    missing = expected - existing
    return expected, existing, missing


def is_block_transactions_complete(ch_client: Any, ng_session: Any, block_height: int) -> bool:
    expected, existing, missing = count_missing_transactions_for_block(ch_client, ng_session, block_height)
    complete = expected > 0 and missing == 0 and existing >= expected
    print(
        "Nebula tx completeness: "
        f"block_height={block_height} expected={expected} existing={existing} missing={missing} complete={complete}"
    )
    return complete


def determine_resume_height(ch_client: Any, ng_session: Any) -> int:
    max_nebula_height = get_nebula_max_tx_block_height(ng_session)
    if max_nebula_height is None:
        print("No tx data found in NebulaGraph bitcoin space; starting from block 0")
        return 0

    print(f"Max tx block height in NebulaGraph bitcoin space: {max_nebula_height}")
    if is_block_transactions_complete(ch_client, ng_session, max_nebula_height):
        return max_nebula_height + 1

    print(
        f"Block {max_nebula_height} exists in NebulaGraph but tx vertices are incomplete; "
        "resyncing from this block"
    )
    return max_nebula_height


# -----------------------------
# NebulaGraph insert builders
# -----------------------------


def build_address_vertices_ngql(addresses: list[str]) -> str:
    values = [f"{q_vid('addr', addr)}:({q(addr)})" for addr in addresses]
    return f"""
        INSERT VERTEX address(address)
        VALUES {", ".join(values)};
    """


def build_tx_vertices_ngql(transactions: dict[str, tuple[str, str, int, Any]]) -> str:
    values = []
    for txid, (hash_val, block_hash, block_height, block_time) in transactions.items():
        values.append(
            f"{q_vid('tx', txid)}:("
            f"{q(txid)}, "
            f"{q(hash_val)}, "
            f"{q(block_hash)}, "
            f"{ng_int(block_height)}, "
            f"{ng_block_time(block_time)}"
            ")"
        )
    return f"""
        INSERT VERTEX tx(txid, hash, block_hash, block_height, block_time)
        VALUES {", ".join(values)};
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


def build_input_edges_ngql(rows: list[AddressRow]) -> str:
    values = [
        f"{q_vid('addr', row.address)}->{q_vid('tx', row.txid)}@{safe_rank(row.source_index)}:({edge_values(row)})"
        for row in rows
    ]
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
        VALUES {", ".join(values)};
    """


def build_output_edges_ngql(rows: list[AddressRow]) -> str:
    values = [
        f"{q_vid('tx', row.txid)}->{q_vid('addr', row.address)}@{safe_rank(row.utxo_vout)}:({edge_values(row)})"
        for row in rows
    ]
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
        VALUES {", ".join(values)};
    """


def insert_vertices_and_edges(
    ng_session: Any,
    addresses_data: list[AddressRow],
    *,
    debug_ngql: bool = False,
    continue_on_error: bool = False,
) -> tuple[int, int, int, int, int]:
    """Insert address vertices, tx vertices, and input/output edges into NebulaGraph."""
    batch_size = 1000
    address_vertices = 0
    tx_vertices = 0
    input_edge_count = 0
    output_edge_count = 0
    failed = 0

    if not addresses_data:
        return address_vertices, tx_vertices, input_edge_count, output_edge_count, failed

    unique_addresses = sorted({row.address for row in addresses_data if row.address})

    transactions: dict[str, tuple[str, str, int, Any]] = {}
    input_edges: list[AddressRow] = []
    output_edges: list[AddressRow] = []

    for row in addresses_data:
        if not row.address or not row.txid:
            print(f"WARNING: skip row with empty address or txid: {row}", file=sys.stderr)
            failed += 1
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
            failed += 1

    for batch in chunked(unique_addresses, batch_size):
        ok = execute_ngql(
            ng_session,
            build_address_vertices_ngql(batch),
            f"insert {len(batch)} address vertices",
            debug_ngql=debug_ngql,
            continue_on_error=continue_on_error,
        )
        if ok:
            address_vertices += len(batch)
        else:
            failed += len(batch)

    transaction_items = list(transactions.items())
    for batch in chunked(transaction_items, batch_size):
        batch_transactions = dict(batch)
        ok = execute_ngql(
            ng_session,
            build_tx_vertices_ngql(batch_transactions),
            f"insert {len(batch_transactions)} tx vertices",
            debug_ngql=debug_ngql,
            continue_on_error=continue_on_error,
        )
        if ok:
            tx_vertices += len(batch_transactions)
        else:
            failed += len(batch_transactions)

    for batch in chunked(input_edges, batch_size):
        ok = execute_ngql(
            ng_session,
            build_input_edges_ngql(batch),
            f"insert {len(batch)} input_to_tx edges",
            debug_ngql=debug_ngql,
            continue_on_error=continue_on_error,
        )
        if ok:
            input_edge_count += len(batch)
        else:
            failed += len(batch)

    for batch in chunked(output_edges, batch_size):
        ok = execute_ngql(
            ng_session,
            build_output_edges_ngql(batch),
            f"insert {len(batch)} tx_to_output edges",
            debug_ngql=debug_ngql,
            continue_on_error=continue_on_error,
        )
        if ok:
            output_edge_count += len(batch)
        else:
            failed += len(batch)

    return address_vertices, tx_vertices, input_edge_count, output_edge_count, failed


# -----------------------------
# Verification helpers
# -----------------------------


def verify_nebula_space(ng_session: Any, ng_space: str, *, debug_ngql: bool = False) -> None:
    execute_ngql_or_raise(
        ng_session,
        f"USE {ng_space};",
        f"USE space {ng_space}",
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


def sync_range(
    ch_client: Any,
    ng_session: Any,
    next_start: int,
    clickhouse_tip: int,
    *,
    debug_ngql: bool,
    continue_on_error: bool,
) -> Optional[int]:
    """Sync data from ClickHouse to NebulaGraph from next_start to clickhouse_tip."""
    block_heights = list(range(int(next_start), int(clickhouse_tip) + 1))
    last_processed_height = None
    print(f"Generated {len(block_heights)} block height(s) to sync from {next_start} to {clickhouse_tip}")

    for height in block_heights:
        last_processed_height = int(height)

        block = get_block_by_height(
            ch_client,
            int(height),
        )
        if block is None:
            print(f"  Block {height} not found in ClickHouse.")
            continue

        block_month, block_height, block_hash = block
        print(f"  Syncing block {block_height} ({str(block_hash)[:16]}...) with block_month={block_month}")

        rows = get_addresses_for_block(ch_client, int(block_month), int(block_height))
        if not rows:
            print("    No address rows found; skip Nebula insert.")
            continue

        print(f"    Found {len(rows)} address row(s)")

        address_vertices, tx_vertices, input_edges, output_edges, failed = insert_vertices_and_edges(
            ng_session,
            rows,
            debug_ngql=debug_ngql,
            continue_on_error=continue_on_error,
        )

        print(
            "    Inserted: "
            f"address_vertices={address_vertices}, "
            f"tx_vertices={tx_vertices}, "
            f"input_edges={input_edges}, "
            f"output_edges={output_edges}, "
            f"failed={failed}"
        )

    return failed


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
            debug_ngql=args.debug_ngql,
        )

        if args.verify_after_use:
            print_schema_overview(ng_session)

        next_start = determine_resume_height(ch_client, ng_session)

        while True:
            clickhouse_tip = get_clickhouse_max_block_height(ch_client)
            if clickhouse_tip is None:
                print("No data found in ClickHouse bitcoin.blocks.")
                print(f"Sleeping {args.poll_interval} seconds before checking ClickHouse again.")
                time.sleep(args.poll_interval)
                continue

            print(f"ClickHouse bitcoin.blocks max height: {clickhouse_tip}")
            if next_start > clickhouse_tip:
                print(
                    "NebulaGraph is caught up with ClickHouse. "
                    f"Next start={next_start}, ClickHouse tip={clickhouse_tip}"
                )
                print(f"Sleeping {args.poll_interval} seconds before checking ClickHouse again.")
                time.sleep(args.poll_interval)
                continue

            failed = sync_range(
                ch_client,
                ng_session,
                next_start,
                clickhouse_tip,
                debug_ngql=args.debug_ngql,
                continue_on_error=args.continue_on_error,
            )

            if failed is not None and failed > 0:
                print(f"WARNING: {failed} insert(s) failed in this sync range. Check logs for details.", file=sys.stderr)
                break

            next_start = clickhouse_tip + 1
            
    finally:
        try:
            ng_session.release()
        finally:
            connection_pool.close()


if __name__ == "__main__":
    main()
