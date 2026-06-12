#!/usr/bin/env python3
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import time

config = Config()
config.max_connection_pool_size = 2
config.timeout = 0
config.idle_time = 0

pool = ConnectionPool()
pool.init([('192.168.2.65', 9669)], config)

session = pool.get_session('root', 'nebula')

CHUNK = 50

session.execute('USE bitcoin')

# Step 0: Get max block height in NebulaGraph
max_block_result = session.execute(
    """
    LOOKUP ON tx YIELD tx.block_height AS block_height
    | ORDER BY $-.block_height DESC
    | LIMIT 1;
    """
)
max_block_rows = max_block_result.rows()
if not max_block_rows:
    print("No tx block height found in NebulaGraph.", flush=True)
    session.release()
    pool.close()
    raise SystemExit(0)

BLOCK = int(max_block_rows[0].values[0].value)
print(f"Max block height in NebulaGraph: {BLOCK}", flush=True)

# Step 1: Get tx VIDs
result = session.execute(
    f'LOOKUP ON tx WHERE tx.block_height == {BLOCK} YIELD id(vertex) AS tx_vid'
)
tx_vids = [row.values[0].value for row in result.rows()]
tx_strs = [v.decode() if isinstance(v, bytes) else str(v) for v in tx_vids]
print(f"Txns in block {BLOCK}: {len(tx_strs)}", flush=True)

# Step 2: Input addresses (REVERSELY → use src(edge))
all_inputs = set()
for i in range(0, len(tx_strs), CHUNK):
    chunk = tx_strs[i:i+CHUNK]
    vids = ','.join(f'"{v}"' for v in chunk)
    q = f'USE bitcoin; GO FROM {vids} OVER input_to_tx REVERSELY YIELD src(edge) AS addr_vid'
    r = session.execute(q)
    for row in r.rows():
        all_inputs.add(row.values[0].value)
print(f"Input addrs: {len(all_inputs)}", flush=True)

# Step 3: Output addresses (normal direction → use dst(edge))
all_outputs = set()
for i in range(0, len(tx_strs), CHUNK):
    chunk = tx_strs[i:i+CHUNK]
    vids = ','.join(f'"{v}"' for v in chunk)
    q = f'USE bitcoin; GO FROM {vids} OVER tx_to_output YIELD dst(edge) AS addr_vid'
    r = session.execute(q)
    for row in r.rows():
        all_outputs.add(row.values[0].value)
print(f"Output addrs: {len(all_outputs)}", flush=True)

# Combine
both = all_inputs & all_outputs
all_unique = all_inputs | all_outputs

print(f"\n{'='*50}")
print(f"Block {BLOCK} — Address Summary")
print(f"{'='*50}")
print(f"Transaction count:        {len(tx_strs)}")
print(f"Input addresses (unique): {len(all_inputs)}")
print(f"Output addresses (unique):{len(all_outputs)}")
print(f"Both input & output:      {len(both)}")
print(f"Total unique addresses:   {len(all_unique)}")

session.release()
pool.close()
