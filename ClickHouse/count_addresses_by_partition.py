#!/usr/bin/env python3
"""Count addresses in ClickHouse `bitcoin.addresses` table grouped by partition.

Usage examples:
  python3 ClickHouse/count_addresses_by_partition.py --host localhost --port 9000
  python3 ClickHouse/count_addresses_by_partition.py --host ch.example --user readonly --password secret --format csv

Connects using `clickhouse_driver.Client`. If the package is missing, the script prints install instructions.
"""
import argparse
import json
import sys


def parse_args():
    p = argparse.ArgumentParser(description="Count rows and distinct addresses per partition in bitcoin.addresses")
    p.add_argument("--host", default="192.168.2.241", help="ClickHouse host")
    p.add_argument("--port", type=int, default=9000, help="ClickHouse native port")
    p.add_argument("--user", default="default", help="ClickHouse user")
    p.add_argument("--password", default="", help="ClickHouse password")
    p.add_argument("--table", default="bitcoin.addresses", help="Fully-qualified table name (default: bitcoin.addresses)")
    p.add_argument(
        "--partition",
        action="append",
        help=(
            "Partition(s) to filter by (YYYYMM). Can be provided multiple times or as a comma-separated list, "
            "e.g. --partition 202601 --partition 202602 or --partition 202601,202602"
        ),
    )
    p.add_argument("--format", choices=["table", "csv", "json"], default="table", help="Output format")
    p.add_argument("--per-block", action="store_true", help="Group counts per block within each partition (adds block_height and block_hash)")
    p.add_argument("--distinct-only", action="store_true", help="Only print distinct address counts per partition (no row counts)")
    return p.parse_args()


def build_query(table, partition=None, block_height=None, per_block=False):
    """Build ClickHouse query for address counts.
    
    Args:
        table: Table name (usually bitcoin.addresses)
        partition: Single partition (YYYYMM) for filtering
        block_height: Single block height for filtering (used when per_block=True)
        per_block: If True, query by specific block_height; if False, query by partition
    """
    if per_block and block_height is not None:
        # Query addresses for a specific partition and block_height
        where = f"WHERE address_month = {partition} AND block_height = {block_height}"
        return f"""
SELECT
    address_month AS partition,
    block_height,
    block_hash,
    count() AS rows,
    countDistinct(address) AS unique_addresses
FROM {table}
{where}
GROUP BY address_month, block_height, block_hash
"""

    # Aggregate by partition only
    where = ""
    if partition:
        where = f"WHERE address_month = {partition}"

    return f"""
SELECT
    address_month AS partition,
    count() AS rows,
    countDistinct(address) AS unique_addresses
FROM {table}
{where}
GROUP BY partition
"""


def get_blocks_for_partition(client, partition):
    """Retrieve all blocks in a partition from bitcoin.blocks table.
    Returns list of tuples: (partition, block_height, block_hash)
    """
    query = f"""
SELECT
    block_month,
    height,
    hash
FROM bitcoin.blocks
WHERE block_month = {partition}
ORDER BY height
"""
    return client.execute(query)


def main():
    args = parse_args()

    try:
        from clickhouse_driver import Client
    except Exception:
        print("Missing dependency: install with: pip install clickhouse-driver", file=sys.stderr)
        sys.exit(2)

    client = Client(host=args.host, port=args.port, user=args.user, password=args.password)

    # normalize partition argument(s)
    partitions = None
    if args.partition:
        parts = []
        for item in args.partition:
            for p in str(item).split(','):
                s = p.strip()
                if s:
                    parts.append(s)
        if parts:
            partitions = parts

    # Print header
    if args.format == "json":
        json_out = []
    elif args.format == "csv":
        if args.per_block:
            if args.distinct_only:
                print("partition,block_height,block_hash,unique_addresses")
            else:
                print("partition,block_height,block_hash,rows,unique_addresses")
        else:
            if args.distinct_only:
                print("partition,unique_addresses")
            else:
                print("partition,rows,unique_addresses")
    else:
        # table format header
        if args.per_block:
            if args.distinct_only:
                print(f"{'partition':>10}  {'block_height':>12}  {'block_hash':>66}  {'unique_addresses':>18}")
                print('-' * 112)
            else:
                print(f"{'partition':>10}  {'block_height':>12}  {'block_hash':>66}  {'rows':>12}  {'unique_addresses':>18}")
                print('-' * 134)
        else:
            if args.distinct_only:
                print(f"{'partition':>10}  {'unique_addresses':>18}")
                print('-' * 32)
            else:
                print(f"{'partition':>10}  {'rows':>12}  {'unique_addresses':>18}")
                print('-' * 46)
    
    if args.per_block and partitions:
        # For per-block mode: fetch blocks first, then query addresses for each block
        for partition in partitions:
            blocks = get_blocks_for_partition(client, int(partition))
            print(f"Processing partition {partition} with {len(blocks)} blocks...")
            for block_partition, block_height, block_hash in blocks:
                query = build_query(args.table, partition=int(partition), block_height=block_height, per_block=True)
                block_rows = client.execute(query)
                if block_rows:
                    for partition_val, block_h, block_c, row_count, unique_count in block_rows:
                        if args.format == "json":
                            json_out.append({
                                "partition": partition_val,
                                "block_height": block_h,
                                "block_hash": block_c,
                                "rows": row_count,
                                "unique_addresses": unique_count,
                            })
                        elif args.format == "csv":
                            if args.distinct_only:
                                print(f"{partition_val},{block_h},{block_c},{unique_count}")
                            else:
                                print(f"{partition_val},{block_h},{block_c},{row_count},{unique_count}")
                        else:
                            # table format
                            if args.distinct_only:
                                print(f"{partition_val:>10}  {block_h:12}  {block_c:66}  {unique_count:18}")
                            else:
                                print(f"{partition_val:>10}  {block_h:12}  {block_c:66}  {row_count:12}  {unique_count:18}")
    else:
        # Aggregate by partition only
        if partitions:
            for partition in partitions:
                query = build_query(args.table, partition=int(partition), per_block=False)
                partition_rows = client.execute(query)
                for partition_val, row_count, unique_count in partition_rows:
                    if args.format == "json":
                        json_out.append({"partition": partition_val, "rows": row_count, "unique_addresses": unique_count})
                    elif args.format == "csv":
                        if args.distinct_only:
                            print(f"{partition_val},{unique_count}")
                        else:
                            print(f"{partition_val},{row_count},{unique_count}")
                    else:
                        # table format
                        if args.distinct_only:
                            print(f"{partition_val:>10}  {unique_count:18}")
                        else:
                            print(f"{partition_val:>10}  {row_count:12}  {unique_count:18}")
    
    # Print JSON at the end if JSON format
    if args.format == "json":
        print(json.dumps(json_out, indent=2))


if __name__ == '__main__':
    main()
