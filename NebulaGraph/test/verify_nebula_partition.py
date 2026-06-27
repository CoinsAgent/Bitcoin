#!/usr/bin/env python3
"""
Verify one monthly Bitcoin partition in NebulaGraph against ClickHouse.

The graph model under test is:

    addr:<address> --input_to_tx--> tx:<txid> --tx_to_output--> addr:<address>

This script treats ClickHouse as the source of truth and Nebula as the graph
projection. It checks transaction vertex properties, input/output edge
neighborhoods, and exact ranked UTXO-chain edges.
"""

from __future__ import annotations

import argparse
import json
import math
import shlex
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple


def die(message: str, code: int = 1) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(code)


def parse_partition(value: str) -> str:
    if len(value) != 6 or not value.isdigit():
        raise argparse.ArgumentTypeError("partition must be YYYYMM, for example 202401")
    month = int(value[4:6])
    if month < 1 or month > 12:
        raise argparse.ArgumentTypeError("partition month must be 01..12")
    return value


def normalize_value(value: str) -> Any:
    value = value.strip()
    if value == "" or value.upper() == "NULL" or value == "__NULL__":
        return None
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def parse_nebula_tables(output: str) -> List[List[Dict[str, Any]]]:
    tables: List[List[Dict[str, Any]]] = []
    current_header: List[str] | None = None
    current_rows: List[Dict[str, Any]] = []

    for raw_line in output.splitlines():
        line = raw_line.strip()
        if line.startswith("+") and line.endswith("+"):
            continue
        if not (line.startswith("|") and line.endswith("|")):
            if current_header is not None and current_rows:
                tables.append(current_rows)
            current_header = None
            current_rows = []
            continue

        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if not cells:
            continue

        if current_header is None:
            current_header = cells
            current_rows = []
            continue

        if len(cells) != len(current_header):
            continue

        row = {current_header[i]: normalize_value(cells[i]) for i in range(len(cells))}
        current_rows.append(row)

    if current_header is not None and current_rows:
        tables.append(current_rows)
    return tables


def last_table(output: str) -> List[Dict[str, Any]]:
    tables = parse_nebula_tables(output)
    return tables[-1] if tables else []


@dataclass
class Result:
    passed: int = 0
    failed: int = 0
    skipped: int = 0

    def ok(self, label: str, detail: str = "") -> None:
        self.passed += 1
        suffix = f" {detail}" if detail else ""
        print(f"PASS {label}{suffix}")

    def fail(self, label: str, detail: str = "") -> None:
        self.failed += 1
        suffix = f" {detail}" if detail else ""
        print(f"FAIL {label}{suffix}")

    def skip(self, label: str, detail: str = "") -> None:
        self.skipped += 1
        suffix = f" {detail}" if detail else ""
        print(f"SKIP {label}{suffix}")


class Verifier:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.result = Result()

    def info(self, message: str) -> None:
        if self.args.verbose:
            print(f"INFO {message}", flush=True)

    def section(self, title: str) -> None:
        print(f"\n===== {title} =====", flush=True)

    def detail(self, message: str) -> None:
        if self.args.verbose:
            print(f"  {message}", flush=True)

    def clickhouse(self, sql: str) -> List[Dict[str, Any]]:
        query = sql.rstrip().rstrip(";") + "\nFORMAT JSONEachRow\n"
        data = query.encode("utf-8")
        req = urllib.request.Request(self.args.clickhouse_url, data=data, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.args.clickhouse_timeout) as response:
                text = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ClickHouse HTTP {exc.code}: {body}") from exc
        rows = []
        for line in text.splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
        return rows

    def nebula(self, ngql: str) -> List[Dict[str, Any]]:
        script = f"USE {self.args.space};\n{ngql.rstrip()}\n"
        remote = (
            "cat <<'NGQL' | "
            f"docker exec -i {shlex.quote(self.args.console_container)} "
            "nebula-console "
            f"-addr {shlex.quote(self.args.graph_addr)} "
            f"-port {int(self.args.graph_port)} "
            f"-u {shlex.quote(self.args.graph_user)} "
            f"-p {shlex.quote(self.args.graph_password)}\n"
            f"{script}"
            "NGQL\n"
        )
        cmd = [
            "ssh",
            "-i",
            self.args.ssh_key,
            "-o",
            "IdentitiesOnly=yes",
            f"{self.args.ssh_user}@{self.args.nebula_host}",
            remote,
        ]
        completed = subprocess.run(
            cmd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=self.args.nebula_timeout,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"nebula-console failed with code {completed.returncode}\n"
                f"STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
            )
        if "[ERROR" in completed.stdout:
            raise RuntimeError(completed.stdout)
        return last_table(completed.stdout)

    def tx_vid(self, txid: str) -> str:
        return "tx:" + txid

    def addr_vid(self, address: str) -> str:
        return "addr:" + address

    def q(self, value: str) -> str:
        return json.dumps(value)

    def close_float(self, left: Any, right: Any) -> bool:
        try:
            return math.isclose(float(left), float(right), rel_tol=0, abs_tol=self.args.value_tolerance)
        except (TypeError, ValueError):
            return False

    def source_counts(self) -> Dict[str, int]:
        partition = self.args.partition
        self.info(f"ClickHouse count check for partition={partition}")
        rows = self.clickhouse(
            f"""
            SELECT
              (SELECT count()
               FROM bitcoin.transactions FINAL
               WHERE transaction_month = {partition}
                 AND txid != '') AS tx_count,
              (SELECT count()
               FROM bitcoin.addresses FINAL
               WHERE address_month = {partition}
                 AND direction = 'input'
                 AND address != ''
                 AND txid != ''
                 AND utxo_txid != '') AS input_count,
              (SELECT count()
               FROM bitcoin.addresses FINAL
               WHERE address_month = {partition}
                 AND direction = 'output'
                 AND address != ''
                 AND txid != '') AS output_count
            """
        )
        return {key: int(rows[0][key]) for key in ("tx_count", "input_count", "output_count")}

    def sample_transactions(self) -> List[Dict[str, Any]]:
        self.info(f"Sampling {self.args.tx_samples} transactions from ClickHouse partition={self.args.partition}")
        return self.clickhouse(
            f"""
            SELECT
              txid,
              hash,
              block_hash,
              toInt64(block_height) AS block_height,
              toInt64(block_time) AS block_time
            FROM bitcoin.transactions FINAL
            WHERE transaction_month = {self.args.partition}
              AND txid != ''
            ORDER BY cityHash64(txid, '{self.args.seed}')
            LIMIT {int(self.args.tx_samples)}
            """
        )

    def tx_inputs(self, txid: str) -> List[Dict[str, Any]]:
        self.info(f"ClickHouse input rows for tx={txid}")
        return self.clickhouse(
            f"""
            SELECT
              concat('addr:', address) AS src,
              txid,
              toInt64(source_index) AS input_index,
              utxo_txid,
              toInt64(utxo_vout) AS utxo_vout,
              toFloat64(value) AS value
            FROM bitcoin.addresses FINAL
            WHERE address_month = {self.args.partition}
              AND direction = 'input'
              AND txid = {self.sql_string(txid)}
              AND address != ''
              AND txid != ''
              AND utxo_txid != ''
            ORDER BY input_index, src, utxo_txid, utxo_vout
            """
        )

    def tx_outputs(self, txid: str) -> List[Dict[str, Any]]:
        self.info(f"ClickHouse output rows for tx={txid}")
        return self.clickhouse(
            f"""
            SELECT
              concat('addr:', address) AS dst,
              txid AS utxo_txid,
              toInt64(utxo_vout) AS utxo_vout,
              toFloat64(value) AS value
            FROM bitcoin.addresses FINAL
            WHERE address_month = {self.args.partition}
              AND direction = 'output'
              AND txid = {self.sql_string(txid)}
              AND address != ''
              AND txid != ''
            ORDER BY utxo_vout, dst
            """
        )

    def sample_chains(self) -> List[Dict[str, Any]]:
        # The subquery samples spending inputs in this partition. The join keeps
        # only chains where the previous output is also inside the imported range.
        self.info(
            "Sampling UTXO chains from ClickHouse: "
            "output(txid=P, vout=N, address=A) "
            "JOIN input(utxo_txid=P, utxo_vout=N, address=A)"
        )
        return self.clickhouse(
            f"""
            SELECT
              i.address AS address,
              concat('addr:', i.address) AS address_vid,
              i.txid AS spending_txid,
              concat('tx:', i.txid) AS spending_tx_vid,
              toInt64(i.source_index) AS input_index,
              i.utxo_txid AS prev_txid,
              concat('tx:', i.utxo_txid) AS prev_tx_vid,
              toInt64(i.utxo_vout) AS prev_vout,
              toFloat64(i.value) AS input_value,
              toFloat64(o.value) AS output_value,
              toUInt32(o.address_month) AS output_month
            FROM
            (
              SELECT *
              FROM bitcoin.addresses FINAL
              WHERE address_month = {self.args.partition}
                AND direction = 'input'
                AND address != ''
                AND txid != ''
                AND utxo_txid != ''
              ORDER BY cityHash64(txid, address, utxo_txid, '{self.args.seed}')
              LIMIT {int(self.args.chain_candidates)}
            ) AS i
            INNER JOIN
            (
              SELECT *
              FROM bitcoin.addresses FINAL
              WHERE direction = 'output'
                AND address != ''
                AND txid != ''
                AND address_month BETWEEN {self.args.import_start} AND {self.args.partition}
            ) AS o
              ON o.direction = 'output'
             AND o.txid = i.utxo_txid
             AND o.utxo_vout = i.utxo_vout
             AND o.address = i.address
            ORDER BY cityHash64(i.txid, i.address, i.utxo_txid, '{self.args.seed}')
            LIMIT {int(self.args.chain_samples)}
            """
        )

    def sql_string(self, value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def compare_tx_vertex(self, tx: Dict[str, Any]) -> None:
        txid = tx["txid"]
        self.section(f"Tx Vertex: tx:{txid}")
        self.detail(
            "expected tx props from ClickHouse: "
            f"block_height={tx['block_height']} block_time={tx['block_time']} block_hash={tx['block_hash']}"
        )
        rows = self.nebula(
            "FETCH PROP ON tx "
            f"{self.q(self.tx_vid(txid))} "
            "YIELD "
            "properties(vertex).txid AS txid, "
            "properties(vertex).hash AS hash, "
            "properties(vertex).block_hash AS block_hash, "
            "properties(vertex).block_height AS block_height, "
            "properties(vertex).block_time AS block_time;"
        )
        label = f"tx_vertex txid={txid}"
        if len(rows) != 1:
            self.result.fail(label, f"expected 1 row got {len(rows)}")
            return
        row = rows[0]
        self.detail(f"Nebula tx props: {row}")
        mismatches = []
        for field in ("txid", "hash", "block_hash", "block_height", "block_time"):
            if field in ("block_height", "block_time"):
                same = int(row.get(field)) == int(tx[field])
            else:
                same = row.get(field) == tx[field]
            if not same:
                mismatches.append(f"{field}: nebula={row.get(field)!r} clickhouse={tx[field]!r}")
        if mismatches:
            self.result.fail(label, "; ".join(mismatches))
        else:
            self.result.ok(label)

    def compare_tx_inputs(self, txid: str) -> None:
        expected = self.tx_inputs(txid)
        self.section(f"Input Edges: addr:* --input_to_tx--> tx:{txid}")
        rows = self.nebula(
            f"GO FROM {self.q(self.tx_vid(txid))} "
            "OVER input_to_tx REVERSELY "
            "YIELD "
            "src(edge) AS src, "
            "properties(edge).txid AS txid, "
            "properties(edge).input_index AS input_index, "
            "properties(edge).utxo_txid AS utxo_txid, "
            "properties(edge).utxo_vout AS utxo_vout, "
            "properties(edge).value AS value;"
        )
        print(f"CHECK tx_inputs txid={txid} clickhouse={len(expected)} nebula={len(rows)}", flush=True)
        if expected:
            self.detail(f"sample expected input: {expected[0]}")
        if rows:
            self.detail(f"sample Nebula input edge: {rows[0]}")
        self.compare_edge_sets(f"tx_inputs txid={txid}", expected, rows, ["src", "txid", "input_index", "utxo_txid", "utxo_vout", "value"])

    def compare_tx_outputs(self, txid: str) -> None:
        expected = self.tx_outputs(txid)
        self.section(f"Output Edges: tx:{txid} --tx_to_output--> addr:*")
        rows = self.nebula(
            f"GO FROM {self.q(self.tx_vid(txid))} "
            "OVER tx_to_output "
            "YIELD "
            "dst(edge) AS dst, "
            "properties(edge).utxo_txid AS utxo_txid, "
            "properties(edge).utxo_vout AS utxo_vout, "
            "properties(edge).value AS value;"
        )
        print(f"CHECK tx_outputs txid={txid} clickhouse={len(expected)} nebula={len(rows)}", flush=True)
        if expected:
            self.detail(f"sample expected output: {expected[0]}")
        if rows:
            self.detail(f"sample Nebula output edge: {rows[0]}")
        self.compare_edge_sets(f"tx_outputs txid={txid}", expected, rows, ["dst", "utxo_txid", "utxo_vout", "value"])

    def compare_edge_sets(self, label: str, expected: Sequence[Dict[str, Any]], actual: Sequence[Dict[str, Any]], fields: Sequence[str]) -> None:
        if len(expected) != len(actual):
            self.result.fail(label, f"count mismatch nebula={len(actual)} clickhouse={len(expected)}")
            return

        def key(row: Dict[str, Any]) -> Tuple[Any, ...]:
            normalized = []
            for field in fields:
                value = row.get(field)
                if field == "value":
                    normalized.append(round(float(value), 8))
                elif field in ("input_index", "utxo_vout"):
                    normalized.append(int(value))
                else:
                    normalized.append(value)
            return tuple(normalized)

        expected_keys = sorted(key(row) for row in expected)
        actual_keys = sorted(key(row) for row in actual)
        if expected_keys != actual_keys:
            self.result.fail(label, f"set mismatch sample_expected={expected_keys[:3]} sample_actual={actual_keys[:3]}")
        else:
            self.result.ok(label, f"edges={len(expected)}")

    def compare_chain(self, chain: Dict[str, Any]) -> None:
        prev_tx_vid = chain["prev_tx_vid"]
        address_vid = chain["address_vid"]
        spending_tx_vid = chain["spending_tx_vid"]
        prev_vout = int(chain["prev_vout"])
        input_index = int(chain["input_index"])

        self.section(
            "UTXO Chain: "
            f"tx:{chain['prev_txid']} --tx_to_output@{prev_vout}--> "
            f"addr:{chain['address']} --input_to_tx@{input_index}--> tx:{chain['spending_txid']}"
        )
        self.detail(f"ClickHouse previous output: tx:{chain['prev_txid']} --tx_to_output@{prev_vout}--> addr:{chain['address']}")
        self.detail(f"ClickHouse spending input:  addr:{chain['address']} --input_to_tx@{input_index}--> tx:{chain['spending_txid']}")
        self.detail(
            "Expected value link: "
            f"output_value={chain['output_value']} input_value={chain['input_value']} output_month={chain['output_month']}"
        )
        self.detail(f"Nebula FETCH tx_to_output {prev_tx_vid} -> {address_vid}@{prev_vout}")
        out_rows = self.nebula(
            "FETCH PROP ON tx_to_output "
            f"{self.q(prev_tx_vid)} -> {self.q(address_vid)}@{prev_vout} "
            "YIELD "
            "properties(edge).utxo_txid AS utxo_txid, "
            "properties(edge).utxo_vout AS utxo_vout, "
            "properties(edge).value AS value;"
        )
        self.detail(f"Nebula FETCH input_to_tx {address_vid} -> {spending_tx_vid}@{input_index}")
        in_rows = self.nebula(
            "FETCH PROP ON input_to_tx "
            f"{self.q(address_vid)} -> {self.q(spending_tx_vid)}@{input_index} "
            "YIELD "
            "properties(edge).txid AS txid, "
            "properties(edge).input_index AS input_index, "
            "properties(edge).utxo_txid AS utxo_txid, "
            "properties(edge).utxo_vout AS utxo_vout, "
            "properties(edge).value AS value;"
        )

        label = (
            "utxo_chain "
            f"{chain['prev_txid']}:{prev_vout}->{chain['address']}->{chain['spending_txid']}[{input_index}]"
        )
        if len(out_rows) != 1:
            self.result.fail(label, f"missing previous output edge rows={len(out_rows)}")
            return
        if len(in_rows) != 1:
            self.result.fail(label, f"missing spending input edge rows={len(in_rows)}")
            return

        out_row = out_rows[0]
        in_row = in_rows[0]
        self.detail(f"Nebula previous output edge: {out_row}")
        self.detail(f"Nebula spending input edge:  {in_row}")
        checks = [
            out_row.get("utxo_txid") == chain["prev_txid"],
            out_row.get("utxo_vout") == prev_vout,
            self.close_float(out_row.get("value"), chain["output_value"]),
            in_row.get("txid") == chain["spending_txid"],
            in_row.get("input_index") == input_index,
            in_row.get("utxo_txid") == chain["prev_txid"],
            in_row.get("utxo_vout") == prev_vout,
            self.close_float(in_row.get("value"), chain["input_value"]),
            self.close_float(chain["input_value"], chain["output_value"]),
        ]
        if all(checks):
            self.result.ok(label)
        else:
            self.result.fail(
                label,
                f"out={out_row} in={in_row} source_input_value={chain['input_value']} source_output_value={chain['output_value']}",
            )

    def run(self) -> int:
        print(f"partition={self.args.partition} space={self.args.space} nebula={self.args.nebula_host}", flush=True)
        print(
            "Verify model: tx:P --tx_to_output@vout--> addr:A "
            "--input_to_tx@input_index--> tx:S",
            flush=True,
        )
        if not self.args.verbose:
            print("Use --verbose to print ClickHouse rows and exact Nebula FETCH details.", flush=True)
        counts = self.source_counts()
        print(
            "source_counts "
            f"tx={counts['tx_count']} input_to_tx={counts['input_count']} tx_to_output={counts['output_count']}"
            ,
            flush=True,
        )

        txs = self.sample_transactions()
        self.info(f"Sampled transactions={len(txs)}")
        if not txs:
            self.result.fail("sample_transactions", "no transaction samples found")
        for index, tx in enumerate(txs, start=1):
            self.section(f"Transaction Sample {index}/{len(txs)}")
            print(f"txid={tx['txid']}", flush=True)
            self.compare_tx_vertex(tx)
            self.compare_tx_inputs(tx["txid"])
            self.compare_tx_outputs(tx["txid"])

        chains = self.sample_chains()
        self.info(f"Sampled UTXO chains={len(chains)}")
        if not chains:
            message = (
                "no in-range chained UTXO samples found; this is common for early partitions "
                "such as 200901 where sampled transactions may be coinbase or outputs may not "
                "be spent inside the imported range"
            )
            if self.args.require_chain_samples:
                self.result.fail("sample_chains", message)
            else:
                self.result.skip("sample_chains", message)
        for index, chain in enumerate(chains, start=1):
            self.section(f"UTXO Chain Sample {index}/{len(chains)}")
            self.compare_chain(chain)

        print(
            f"SUMMARY passed={self.result.passed} failed={self.result.failed} skipped={self.result.skipped}",
            flush=True,
        )
        return 0 if self.result.failed == 0 else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify one ClickHouse monthly partition against the NebulaGraph bitcoin space."
    )
    parser.add_argument("--partition", required=True, type=parse_partition, help="Month partition to verify, e.g. 202401")
    parser.add_argument("--import-start", default="200901", type=parse_partition, help="First imported month for in-range UTXO chains")
    parser.add_argument("--space", default="bitcoin", help="Nebula space name")
    parser.add_argument("--tx-samples", type=int, default=5, help="Number of sampled transactions to verify")
    parser.add_argument("--chain-samples", type=int, default=5, help="Number of exact UTXO chains to verify")
    parser.add_argument("--chain-candidates", type=int, default=5000, help="Input rows sampled before joining to previous outputs")
    parser.add_argument("--seed", default="openclaw", help="Deterministic sampling seed")
    parser.add_argument("--value-tolerance", type=float, default=1e-8, help="Allowed BTC value difference")
    parser.add_argument("--clickhouse-url", default="http://192.168.2.241:8123/?database=bitcoin", help="ClickHouse HTTP URL")
    parser.add_argument("--clickhouse-timeout", type=int, default=300, help="ClickHouse HTTP timeout seconds")
    parser.add_argument("--nebula-host", default="192.168.2.65", help="SSH host where nebula-console container is running")
    parser.add_argument("--ssh-user", default="btc", help="SSH user for Nebula host")
    parser.add_argument("--ssh-key", default="/home/btc/.ssh/id_ed25519_nebula_192_168_2_65", help="SSH private key")
    parser.add_argument("--console-container", default="nebula-console-1", help="Nebula console Docker container")
    parser.add_argument("--graph-addr", default="graphd", help="Graphd address from console container")
    parser.add_argument("--graph-port", type=int, default=9669, help="Graphd port")
    parser.add_argument("--graph-user", default="root", help="Nebula user")
    parser.add_argument("--graph-password", default="nebula", help="Nebula password")
    parser.add_argument("--nebula-timeout", type=int, default=120, help="Per Nebula query timeout seconds")
    parser.add_argument("--verbose", action="store_true", help="Print detailed ClickHouse rows and Nebula edge fetches")
    parser.add_argument(
        "--require-chain-samples",
        action="store_true",
        help="Fail if no ClickHouse UTXO chain samples are found; by default this is reported as SKIP",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if int(args.import_start) > int(args.partition):
        die("--import-start cannot be greater than --partition")
    verifier = Verifier(args)
    return verifier.run()


if __name__ == "__main__":
    raise SystemExit(main())
