#!/usr/bin/env python3
"""
sync_clickhouse_addresses_to_nebula_opencypher.py

ETL script to map ClickHouse bitcoin.addresses into a Cypher property graph:

    (:address)-[:input_to_tx]->(:tx)-[:tx_to_output]->(:address)

This script intentionally generates standard openCypher-style write statements
using MERGE/MATCH/SET instead of Nebula nGQL INSERT statements.

IMPORTANT NEBULAGRAPH NOTE
--------------------------
NebulaGraph supports an openCypher-like MATCH query style, but depending on the
NebulaGraph version, openCypher DML clauses such as CREATE/MERGE may not be
supported. If your NebulaGraph server rejects MERGE/CREATE, use this script in
--dry-run mode to export Cypher, or use the nGQL ETL script instead.

The schema must already exist. For NebulaGraph, schema creation is still normally
performed with nGQL DDL, for example the previously generated:

    create_bitcoin_addr_graph.ngql

Python dependencies:

    pip install clickhouse-connect nebula3-python

Example dry run:

    python sync_clickhouse_addresses_to_nebula_opencypher.py \
      --address-month 202506 \
      --max-rows 1000 \
      --dry-run \
      --cypher-output /tmp/bitcoin_addresses.cypher

Example execution:

    python sync_clickhouse_addresses_to_nebula_opencypher.py \
      --ch-host 127.0.0.1 \
      --ch-port 8123 \
      --ch-user default \
      --ch-password "" \
      --ch-database bitcoin \
      --nebula-hosts 127.0.0.1:9669 \
      --nebula-user root \
      --nebula-password nebula \
      --nebula-space bitcoin_addr_graph \
      --address-month 202506 \
      --fetch-limit 10000 \
      --cypher-batch-size 100
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import clickhouse_connect
from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool


# -----------------------------------------------------------------------------
# Direction mapping
# -----------------------------------------------------------------------------

INPUT_ALIASES = {
    "input",
    "inputs",
    "vin",
    "spend",
    "spent",
    "spent_in",
    "in_spend",
    "debit",
    "out",
}

OUTPUT_ALIASES = {
    "output",
    "outputs",
    "vout",
    "receive",
    "received",
    "paid_to",
    "credit",
    "in",
}


def normalize_direction(direction: str) -> str:
    d = str(direction).lower().strip()
    if d in INPUT_ALIASES:
        return "input"
    if d in OUTPUT_ALIASES:
        return "output"
    raise ValueError(f"Unknown direction value: {direction!r}")


# -----------------------------------------------------------------------------
# Cypher literal helpers
# -----------------------------------------------------------------------------


def cypher_string(value: Any) -> str:
    if value is None:
        return "null"
    s = str(value)
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def cypher_int(value: Any) -> str:
    if value is None or value == "":
        return "0"
    return str(int(value))


def cypher_float(value: Any) -> str:
    if value is None or value == "":
        return "0.0"
    if isinstance(value, Decimal):
        return str(float(value))
    return str(float(value))


def address_vid(address: str) -> str:
    return f"addr:{address}"


def tx_vid(txid: str) -> str:
    return f"tx:{txid}"


# -----------------------------------------------------------------------------
# Query builders
# -----------------------------------------------------------------------------

EDGE_PROPERTY_KEYS = [
    "direction",
    "txid",
    "hash",
    "block_hash",
    "block_height",
    "block_time",
    "utxo_txid",
    "utxo_vout",
    "source_index",
    "value",
    "value_delta",
    "revision",
]


def row_edge_props(row: Dict[str, Any], normalized_direction: str) -> Dict[str, Any]:
    return {
        "direction": normalized_direction,
        "txid": row.get("txid", ""),
        "hash": row.get("hash") or row.get("txid", ""),
        "block_hash": row.get("block_hash", ""),
        "block_height": int(row.get("block_height") or 0),
        "block_time": int(row.get("block_time") or 0),
        "utxo_txid": row.get("utxo_txid", ""),
        "utxo_vout": int(row.get("utxo_vout") or 0),
        "source_index": int(row.get("source_index") or 0),
        "value": float(row.get("value") or 0),
        "value_delta": float(row.get("value_delta") or 0),
        "revision": int(row.get("revision") or 0),
    }


def cypher_props(props: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key, value in props.items():
        if isinstance(value, str):
            parts.append(f"{key}: {cypher_string(value)}")
        elif isinstance(value, bool):
            parts.append(f"{key}: {'true' if value else 'false'}")
        elif isinstance(value, int):
            parts.append(f"{key}: {value}")
        elif isinstance(value, float):
            parts.append(f"{key}: {value}")
        elif value is None:
            parts.append(f"{key}: null")
        else:
            parts.append(f"{key}: {cypher_string(value)}")
    return "{ " + ", ".join(parts) + " }"


def build_vertex_merge_cypher(rows: Sequence[Dict[str, Any]]) -> List[str]:
    """
    Build standard Cypher MERGE statements for address and tx vertices.

    We store a stable explicit `vid` property because NebulaGraph has its own
    internal VID model. In pure Cypher engines, this property acts as the stable
    application ID.
    """
    addresses: Dict[str, Dict[str, Any]] = {}
    txs: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        address = str(row["address"])
        txid = str(row["txid"])

        addresses[address] = {
            "vid": address_vid(address),
            "address": address,
        }

        txs[txid] = {
            "vid": tx_vid(txid),
            "txid": txid,
            "hash": row.get("hash") or txid,
            "block_hash": row.get("block_hash", ""),
            "block_height": int(row.get("block_height") or 0),
            "block_time": int(row.get("block_time") or 0),
        }

    stmts: List[str] = []

    for props in addresses.values():
        stmts.append(
            "MERGE (a:address {vid: " + cypher_string(props["vid"]) + "})\n"
            "SET a.address = " + cypher_string(props["address"]) + ";"
        )

    for props in txs.values():
        stmts.append(
            "MERGE (t:tx {vid: " + cypher_string(props["vid"]) + "})\n"
            "SET "
            f"t.txid = {cypher_string(props['txid'])}, "
            f"t.hash = {cypher_string(props['hash'])}, "
            f"t.block_hash = {cypher_string(props['block_hash'])}, "
            f"t.block_height = {cypher_int(props['block_height'])}, "
            f"t.block_time = {cypher_int(props['block_time'])};"
        )

    return stmts


def build_edge_merge_cypher(rows: Sequence[Dict[str, Any]]) -> List[str]:
    stmts: List[str] = []

    for row in rows:
        direction = normalize_direction(row["direction"])
        address = str(row["address"])
        txid = str(row["txid"])
        addr_id = address_vid(address)
        tx_id = tx_vid(txid)
        props = row_edge_props(row, direction)
        rank = int(row.get("source_index") or 0)

        # Relationship identity. In Bitcoin the same address can appear multiple
        # times in the same transaction, so direction + source_index is part of
        # the edge identity. `rank` is stored as a normal property for Cypher.
        identity = {
            "txid": txid,
            "source_index": rank,
            "direction": direction,
        }

        set_props = dict(props)
        set_props["rank"] = rank

        if direction == "input":
            stmts.append(
                "MATCH (a:address {vid: " + cypher_string(addr_id) + "})\n"
                "MATCH (t:tx {vid: " + cypher_string(tx_id) + "})\n"
                "MERGE (a)-[e:input_to_tx " + cypher_props(identity) + "]->(t)\n"
                "SET e += " + cypher_props(set_props) + ";"
            )
        else:
            stmts.append(
                "MATCH (t:tx {vid: " + cypher_string(tx_id) + "})\n"
                "MATCH (a:address {vid: " + cypher_string(addr_id) + "})\n"
                "MERGE (t)-[e:tx_to_output " + cypher_props(identity) + "]->(a)\n"
                "SET e += " + cypher_props(set_props) + ";"
            )

    return stmts


# -----------------------------------------------------------------------------
# Database helpers
# -----------------------------------------------------------------------------


def parse_hosts(hosts: str) -> List[Tuple[str, int]]:
    parsed: List[Tuple[str, int]] = []
    for item in hosts.split(","):
        item = item.strip()
        if not item:
            continue
        host, port_s = item.rsplit(":", 1)
        parsed.append((host, int(port_s)))
    if not parsed:
        raise ValueError("No NebulaGraph hosts were provided")
    return parsed


class NebulaSession:
    def __init__(self, hosts: List[Tuple[str, int]], user: str, password: str, space: str):
        self.hosts = hosts
        self.user = user
        self.password = password
        self.space = space
        self.pool: Optional[ConnectionPool] = None
        self.session = None

    def __enter__(self):
        config = Config()
        config.max_connection_pool_size = 10
        self.pool = ConnectionPool()
        if not self.pool.init(self.hosts, config):
            raise RuntimeError("Failed to initialize NebulaGraph connection pool")
        self.session = self.pool.get_session(self.user, self.password)
        self.execute(f"USE {self.space};")
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.session is not None:
            self.session.release()
        if self.pool is not None:
            self.pool.close()

    def execute(self, stmt: str) -> None:
        assert self.session is not None
        result = self.session.execute(stmt)
        if not result.is_succeeded():
            raise RuntimeError(
                "Cypher execution failed.\n"
                f"Statement:\n{stmt}\n\n"
                f"Error: {result.error_msg()}"
            )


def chunked(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_clickhouse_rows(
    ch,
    table: str,
    address_month: Optional[int],
    block_height_start: Optional[int],
    block_height_end: Optional[int],
    fetch_limit: int,
    offset: int,
    final: bool,
) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    where_parts = ["1 = 1"]

    if address_month is not None:
        where_parts.append(f"address_month = {int(address_month)}")
    if block_height_start is not None:
        where_parts.append(f"block_height >= {int(block_height_start)}")
    if block_height_end is not None:
        where_parts.append(f"block_height <= {int(block_height_end)}")

    where_sql = " AND ".join(where_parts)
    final_sql = " FINAL" if final else ""

    sql = f"""
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
    FROM {table}{final_sql}
    WHERE {where_sql}
    ORDER BY block_height, txid, direction, source_index
    LIMIT {int(fetch_limit)}
    OFFSET {int(offset)}
    """

    result = ch.query(sql)
    return result.column_names, result.result_rows


# -----------------------------------------------------------------------------
# Main ETL
# -----------------------------------------------------------------------------


def run_etl(args: argparse.Namespace) -> None:
    ch = clickhouse_connect.get_client(
        host=args.ch_host,
        port=args.ch_port,
        username=args.ch_user,
        password=args.ch_password,
        database=args.ch_database,
    )

    cypher_file = None
    if args.cypher_output:
        cypher_file = Path(args.cypher_output)
        cypher_file.parent.mkdir(parents=True, exist_ok=True)
        cypher_file.write_text("", encoding="utf-8")

    nebula_ctx = None
    if not args.dry_run:
        nebula_ctx = NebulaSession(
            hosts=parse_hosts(args.nebula_hosts),
            user=args.nebula_user,
            password=args.nebula_password,
            space=args.nebula_space,
        )

    total_rows = 0
    offset = 0

    try:
        executor = nebula_ctx.__enter__() if nebula_ctx else None

        while True:
            remaining_limit = None
            if args.max_rows is not None:
                remaining_limit = max(args.max_rows - total_rows, 0)
                if remaining_limit <= 0:
                    break

            current_fetch_limit = args.fetch_limit
            if remaining_limit is not None:
                current_fetch_limit = min(current_fetch_limit, remaining_limit)

            columns, raw_rows = fetch_clickhouse_rows(
                ch=ch,
                table=args.ch_table,
                address_month=args.address_month,
                block_height_start=args.block_height_start,
                block_height_end=args.block_height_end,
                fetch_limit=current_fetch_limit,
                offset=offset,
                final=not args.no_final,
            )

            if not raw_rows:
                break

            rows = [dict(zip(columns, r)) for r in raw_rows]

            for batch in chunked(rows, args.cypher_batch_size):
                batch_rows = list(batch)
                statements = build_vertex_merge_cypher(batch_rows) + build_edge_merge_cypher(batch_rows)

                if cypher_file:
                    with cypher_file.open("a", encoding="utf-8") as f:
                        for stmt in statements:
                            f.write(stmt.strip())
                            f.write("\n\n")

                if args.dry_run:
                    if args.print_cypher:
                        for stmt in statements:
                            print(stmt)
                    continue

                assert executor is not None
                for stmt in statements:
                    executor.execute(stmt)

            total_rows += len(rows)
            offset += len(rows)

            print(
                f"Processed rows={total_rows}, "
                f"last_batch={len(rows)}, "
                f"offset={offset}",
                flush=True,
            )

    finally:
        if nebula_ctx:
            nebula_ctx.__exit__(*sys.exc_info())
        ch.close()

    print(f"Done. Total rows processed: {total_rows}")
    if cypher_file:
        print(f"Cypher output written to: {cypher_file}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Sync ClickHouse bitcoin.addresses to NebulaGraph using openCypher-style writes."
    )

    p.add_argument("--ch-host", default="127.0.0.1")
    p.add_argument("--ch-port", type=int, default=8123)
    p.add_argument("--ch-user", default="default")
    p.add_argument("--ch-password", default="")
    p.add_argument("--ch-database", default="bitcoin")
    p.add_argument("--ch-table", default="bitcoin.addresses")
    p.add_argument("--no-final", action="store_true", help="Do not use FINAL when reading ClickHouse ReplacingMergeTree data.")

    p.add_argument("--nebula-hosts", default="127.0.0.1:9669", help="Comma-separated graphd endpoints, e.g. 127.0.0.1:9669,127.0.0.2:9669")
    p.add_argument("--nebula-user", default="root")
    p.add_argument("--nebula-password", default="nebula")
    p.add_argument("--nebula-space", default="bitcoin_addr_graph")

    p.add_argument("--address-month", type=int, default=None, help="Filter by address_month, e.g. 202506")
    p.add_argument("--block-height-start", type=int, default=None)
    p.add_argument("--block-height-end", type=int, default=None)
    p.add_argument("--fetch-limit", type=int, default=10_000)
    p.add_argument("--cypher-batch-size", type=int, default=100)
    p.add_argument("--max-rows", type=int, default=None)

    p.add_argument("--dry-run", action="store_true", help="Generate Cypher but do not execute it.")
    p.add_argument("--print-cypher", action="store_true", help="Print generated Cypher to stdout. Best with --dry-run and small --max-rows.")
    p.add_argument("--cypher-output", default=None, help="Optional path to write generated Cypher statements.")

    return p


if __name__ == "__main__":
    parser = build_arg_parser()
    run_etl(parser.parse_args())
