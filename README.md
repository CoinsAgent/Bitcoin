# Bitcoin AI Agent Database

This repository builds a Bitcoin AI agent data layer with ClickHouse and NebulaGraph.

The main idea is to keep two complementary views of Bitcoin data:

- ClickHouse stores rich block, transaction, input, output, and address ledger tables for fast analytical queries.
- NebulaGraph stores address-to-transaction-to-address relationships for graph traversal, path finding, and hop analysis.

Together, they give an AI agent both SQL-style factual memory and graph-style relationship reasoning.

## Repository Layout

- `ClickHouse/clickhouse_bitcoin_rich_block_schema.sql` defines the Bitcoin analytical schema.
- `ClickHouse/sync_blocks.py` syncs rich Bitcoin Core block data into ClickHouse.
- `NebulaGraph/bitcoin_nebula_graph.ngql` defines the Bitcoin graph schema.
- `NebulaGraph/sync_addresses_from_clickhouse_to_nebula.py` syncs ClickHouse address rows into NebulaGraph.
- `NebulaGraph/cron_block_height.py` and related scripts provide quick graph checks.

## ClickHouse Schema

The ClickHouse database is named `bitcoin`.

It starts from one rich source table:

- `bitcoin.blocks`: stores Bitcoin Core block data with nested rich transaction objects from `getblock` verbosity 3.

Materialized views unfold the nested block data into query-friendly tables:

- `bitcoin.transactions`: one row per transaction, with block context.
- `bitcoin.inputs`: one row per transaction input.
- `bitcoin.outputs`: one row per transaction output.
- `bitcoin.addresses`: an address ledger built from both outputs and spent inputs.

The core pipeline is:

```text
blocks -> transactions -> inputs  -> addresses
                      -> outputs -> addresses
```

The `bitcoin.addresses` table is the key analytical ledger. It records:

- `address`
- `direction`: `output` for received value, `input` for spent value
- `txid`
- `block_height`
- `utxo_txid`
- `utxo_vout`
- `value`
- `value_delta`

This makes ClickHouse useful for questions like:

- What is an address balance?
- Which UTXOs are still unspent?
- How many unique addresses appeared in a block?
- Which blocks, months, or transactions contain high value movement?
- Which transactions or addresses need to be loaded into the graph?

ClickHouse is the agent's fast structured memory for exact numbers, filters, partitions, and large scans.

## NebulaGraph Schema

The NebulaGraph space is named `bitcoin`.

The graph model is:

```text
(:address)-[:input_to_tx]->(:tx)-[:tx_to_output]->(:address)
```

Vertex tags:

- `address`: stores one Bitcoin address.
- `tx`: stores transaction metadata such as `txid`, `block_hash`, `block_height`, and `block_time`.

Edge types:

- `input_to_tx`: connects an address that appears on the input side of a transaction.
- `tx_to_output`: connects a transaction to an output address.

This graph is designed for traversal, not accounting. It shows transaction-level connections between addresses and transactions. It does not prove exact ownership or attribution inside multi-input and multi-output transactions.

NebulaGraph is useful for questions like:

- What addresses are one hop from this address?
- What transaction path connects two addresses?
- Which addresses are connected through a transaction cluster?
- What are the input and output neighborhoods around a suspicious transaction?
- How far apart are two addresses in the transaction graph?

NebulaGraph is the agent's relationship memory for paths, hops, neighborhoods, and graph exploration.

## AI Agent Usage

An AI agent becomes more powerful when it can choose the right database for the question.

Use ClickHouse when the task needs:

- exact counts
- balances
- UTXO checks
- time or block range filtering
- partition-based scans
- transaction, input, output, or address ledger facts

Use NebulaGraph when the task needs:

- path finding
- address-hop analysis
- transaction graph exploration
- relationship discovery
- neighborhood expansion around an address or transaction

Best agent workflow:

1. Use ClickHouse to narrow the search space.
2. Sync or select the relevant address and transaction rows.
3. Use NebulaGraph to traverse relationships.
4. Return to ClickHouse for exact values, timestamps, and balances.
5. Let the AI agent summarize findings in human language.

Example agent tasks:

- "Find all addresses connected to this address within 3 hops, then rank them by total received BTC."
- "Explain what happened in block 850000 using transaction counts, unique address counts, and graph neighborhoods."
- "Trace possible fund movement from address A to address B and list the transaction path."
- "Find high-value output addresses in ClickHouse, then expand their graph neighborhoods in NebulaGraph."
- "Monitor the latest synced block and summarize new address activity."

## Why This Design

Bitcoin data is both tabular and graph-shaped.

ClickHouse is strong for large-scale analytical facts. NebulaGraph is strong for connected relationships. The AI agent should not force every question into one database. Instead, it can combine both:

- ClickHouse answers "what, how many, how much, and when?"
- NebulaGraph answers "who is connected to whom, and through which transactions?"

That combination gives the agent a practical foundation for Bitcoin analytics, investigation, monitoring, and explanation.
