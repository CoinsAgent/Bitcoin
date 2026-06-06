-- ClickHouse Bitcoin rich block schema - fixed version
-- Pipeline:
--   bitcoin.blocks       -> bitcoin.transactions
--   bitcoin.transactions -> bitcoin.inputs
--   bitcoin.transactions -> bitcoin.outputs
--   bitcoin.inputs       -> bitcoin.addresses
--   bitcoin.outputs      -> bitcoin.addresses
--
-- Main fixes in this version:
-- 1. All materialized views use explicit table aliases to avoid ClickHouse name-resolution bugs,
--    especially blocks.hash vs tx_item.hash.
-- 2. bitcoin.blocks.previousblockhash and nextblockhash are Nullable because Bitcoin Core can omit them
--    for genesis block / chain tip.
-- 3. Derived date/month columns in downstream MV target tables are ordinary columns populated explicitly
--    by the MVs. This avoids relying on target-table MATERIALIZED expressions during MV insert.
-- 4. Existing materialized views are dropped before being recreated, so an old wrong MV definition is replaced.
--
-- Important:
-- - This script does not DROP existing data tables. If you already inserted bad data, recreate or truncate
--   downstream tables and reload from bitcoin.blocks after applying the fixed MVs.
-- - ReplacingMergeTree deduplication is based on ORDER BY, not PRIMARY KEY.

CREATE DATABASE IF NOT EXISTS bitcoin;

-- Drop materialized views first so they can be safely recreated with corrected logic.
DROP VIEW IF EXISTS bitcoin.mv_inputs_to_addresses;
DROP VIEW IF EXISTS bitcoin.mv_outputs_to_addresses;
DROP VIEW IF EXISTS bitcoin.mv_transactions_to_outputs;
DROP VIEW IF EXISTS bitcoin.mv_transactions_to_inputs;
DROP VIEW IF EXISTS bitcoin.mv_blocks_to_transactions;

-- If these tables already exist with MATERIALIZED derived columns, CREATE TABLE IF NOT EXISTS will not modify them.
-- For a clean rebuild, drop downstream tables before running this file, or run ALTER MODIFY COLUMN manually.
-- Recommended clean rebuild during development:
-- DROP TABLE IF EXISTS bitcoin.addresses;
-- DROP TABLE IF EXISTS bitcoin.outputs;
-- DROP TABLE IF EXISTS bitcoin.inputs;
-- DROP TABLE IF EXISTS bitcoin.transactions;
-- DROP TABLE IF EXISTS bitcoin.blocks;

-- ============================================================
-- 1. Rich blocks table
-- ============================================================

CREATE TABLE IF NOT EXISTS bitcoin.blocks
(
    `hash` String,
    `confirmations` UInt64,
    `height` UInt64,
    `version` Int32,
    `versionHex` String,
    `merkleroot` String,
    `time` UInt64,
    `mediantime` UInt64,
    `nonce` UInt64,
    `bits` String,
    `target` String,
    `difficulty` Float64,
    `chainwork` String,
    `nTx` UInt64,
    `previousblockhash` Nullable(String),
    `nextblockhash` Nullable(String),
    `strippedsize` UInt64,
    `size` UInt64,
    `weight` UInt64,

    -- Rich transaction objects returned inside the block.
    `tx` Array(Tuple(
        txid String,
        hash String,
        version Int32,
        size UInt64,
        vsize UInt64,
        weight UInt64,
        locktime UInt64,
        vin Array(Tuple(
            coinbase Nullable(String),
            txid Nullable(String),
            vout Nullable(UInt32),
            scriptSig Tuple(
                asm Nullable(String),
                hex Nullable(String)
            ),
            txinwitness Array(String),
            prevout Tuple(
                generated Nullable(Bool),
                height Nullable(UInt64),
                value Nullable(Decimal(20, 8)),
                scriptPubKey Tuple(
                    asm Nullable(String),
                    desc Nullable(String),
                    hex Nullable(String),
                    address Nullable(String),
                    type Nullable(String)
                )
            ),
            sequence UInt64
        )),
        vout Array(Tuple(
            value Decimal(20, 8),
            n UInt32,
            scriptPubKey Tuple(
                asm Nullable(String),
                desc Nullable(String),
                hex Nullable(String),
                address Nullable(String),
                type Nullable(String)
            )
        )),
        fee Nullable(Decimal(20, 8)),
        hex String
    )),

    -- Derived attributes
    `block_datetime` DateTime('UTC') MATERIALIZED toDateTime(`time`, 'UTC'),
    `block_date` Date MATERIALIZED toDate(`block_datetime`),
    `block_month` UInt32 MATERIALIZED toYYYYMM(`block_datetime`),

    -- Operational attribute
    `revision` UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY block_month
PRIMARY KEY (`hash`)
ORDER BY (`hash`);


-- ============================================================
-- 2. Transactions table unfolded from bitcoin.blocks.tx
-- ============================================================

CREATE TABLE IF NOT EXISTS bitcoin.transactions
(
    -- Block context
    `block_hash` String,
    `block_height` UInt64,
    `block_time` UInt64,
    `block_mediantime` UInt64,

    -- Transaction source attributes
    `txid` String,
    `hash` String,
    `version` Int32,
    `size` UInt64,
    `vsize` UInt64,
    `weight` UInt64,
    `locktime` UInt64,

    `vin` Array(Tuple(
        coinbase Nullable(String),
        txid Nullable(String),
        vout Nullable(UInt32),
        scriptSig Tuple(
            asm Nullable(String),
            hex Nullable(String)
        ),
        txinwitness Array(String),
        prevout Tuple(
            generated Nullable(Bool),
            height Nullable(UInt64),
            value Nullable(Decimal(20, 8)),
            scriptPubKey Tuple(
                asm Nullable(String),
                desc Nullable(String),
                hex Nullable(String),
                address Nullable(String),
                type Nullable(String)
            )
        ),
        sequence UInt64
    )),

    `vout` Array(Tuple(
        value Decimal(20, 8),
        n UInt32,
        scriptPubKey Tuple(
            asm Nullable(String),
            desc Nullable(String),
            hex Nullable(String),
            address Nullable(String),
            type Nullable(String)
        )
    )),

    `fee` Nullable(Decimal(20, 8)),
    `hex` String,

    -- Derived attributes.
    -- These are populated explicitly by mv_blocks_to_transactions.
    `transaction_datetime` DateTime('UTC'),
    `transaction_date` Date,
    `transaction_month` UInt32,

    -- Operational attribute
    `revision` UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY transaction_month
PRIMARY KEY (`txid`)
ORDER BY (`txid`);


-- ============================================================
-- 3. Inputs table unfolded from bitcoin.transactions.vin
-- ============================================================

CREATE TABLE IF NOT EXISTS bitcoin.inputs
(
    -- Block context
    `block_hash` String,
    `block_height` UInt64,
    `block_time` UInt64,

    -- Current transaction context
    `txid` String,
    `hash` String,

    -- Input position
    `vin_index` UInt32,

    -- vin source attributes
    `coinbase` Nullable(String),
    `vin_txid` Nullable(String),
    `vin_vout` Nullable(UInt32),
    `scriptSig_asm` Nullable(String),
    `scriptSig_hex` Nullable(String),
    `txinwitness` Array(String),
    `sequence` UInt64,

    -- prevout source attributes
    `prevout_generated` Nullable(Bool),
    `prevout_height` Nullable(UInt64),
    `prevout_value` Nullable(Decimal(20, 8)),
    `prevout_scriptPubKey_asm` Nullable(String),
    `prevout_scriptPubKey_desc` Nullable(String),
    `prevout_scriptPubKey_hex` Nullable(String),
    `prevout_scriptPubKey_address` Nullable(String),
    `prevout_scriptPubKey_type` Nullable(String),

    -- Derived attributes.
    -- These are populated explicitly by mv_transactions_to_inputs.
    `input_datetime` DateTime('UTC'),
    `input_date` Date,
    `input_month` UInt32,

    -- Operational attribute
    `revision` UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY input_month
PRIMARY KEY (`txid`, `vin_index`)
ORDER BY (`txid`, `vin_index`);


-- ============================================================
-- 4. Outputs table unfolded from bitcoin.transactions.vout
-- ============================================================

CREATE TABLE IF NOT EXISTS bitcoin.outputs
(
    -- Block context
    `block_hash` String,
    `block_height` UInt64,
    `block_time` UInt64,

    -- Current transaction context
    `txid` String,
    `hash` String,

    -- Output position
    `vout_index` UInt32,

    -- vout source attributes
    `value` Decimal(20, 8),
    `n` UInt32,

    -- scriptPubKey source attributes
    `scriptPubKey_asm` Nullable(String),
    `scriptPubKey_desc` Nullable(String),
    `scriptPubKey_hex` Nullable(String),
    `scriptPubKey_address` Nullable(String),
    `scriptPubKey_type` Nullable(String),

    -- Derived attributes.
    -- These are populated explicitly by mv_transactions_to_outputs.
    `output_datetime` DateTime('UTC'),
    `output_date` Date,
    `output_month` UInt32,

    -- Operational attribute
    `revision` UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY output_month
PRIMARY KEY (`txid`, `vout_index`)
ORDER BY (`txid`, `vout_index`);


-- ============================================================
-- 5. Addresses ledger table unfolded from outputs and inputs.prevout
-- ============================================================

CREATE TABLE IF NOT EXISTS bitcoin.addresses
(
    `address` String,
    `direction` LowCardinality(String),

    -- Current transaction context
    `txid` String,
    `hash` String,

    -- Block context
    `block_hash` String,
    `block_height` UInt64,
    `block_time` UInt64,

    -- UTXO identity
    `utxo_txid` String,
    `utxo_vout` UInt32,

    -- vout_index for outputs, vin_index for inputs
    `source_index` UInt32,

    -- Value
    `value` Decimal(20, 8),
    `value_delta` Decimal(20, 8),

    -- Derived attributes.
    -- These are populated explicitly by mv_outputs_to_addresses and mv_inputs_to_addresses.
    `address_datetime` DateTime('UTC'),
    `address_date` Date,
    `address_month` UInt32,

    -- Operational attribute
    `revision` UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY address_month
PRIMARY KEY (`address`, `direction`, `txid`, `source_index`)
ORDER BY (`address`, `direction`, `txid`, `source_index`);


-- ============================================================
-- Materialized View 1:
-- bitcoin.blocks -> bitcoin.transactions
-- Critical fix: use b.hash for block_hash and tx_item.hash for transaction hash.
-- ============================================================

CREATE MATERIALIZED VIEW bitcoin.mv_blocks_to_transactions
TO bitcoin.transactions
AS
SELECT
    b.`hash` AS block_hash,
    b.`height` AS block_height,
    b.`time` AS block_time,
    b.`mediantime` AS block_mediantime,

    tx_item.txid AS txid,
    tx_item.`hash` AS `hash`,
    tx_item.version AS version,
    tx_item.size AS size,
    tx_item.vsize AS vsize,
    tx_item.weight AS weight,
    tx_item.locktime AS locktime,
    tx_item.vin AS vin,
    tx_item.vout AS vout,
    tx_item.fee AS fee,
    tx_item.hex AS hex,

    toDateTime(b.`time`, 'UTC') AS transaction_datetime,
    toDate(toDateTime(b.`time`, 'UTC')) AS transaction_date,
    toYYYYMM(toDateTime(b.`time`, 'UTC')) AS transaction_month,

    b.revision AS revision
FROM bitcoin.blocks AS b
ARRAY JOIN b.tx AS tx_item;


-- ============================================================
-- Materialized View 2:
-- bitcoin.transactions -> bitcoin.inputs
-- ============================================================

CREATE MATERIALIZED VIEW bitcoin.mv_transactions_to_inputs
TO bitcoin.inputs
AS
SELECT
    t.block_hash AS block_hash,
    t.block_height AS block_height,
    t.block_time AS block_time,

    t.txid AS txid,
    t.`hash` AS `hash`,

    toUInt32(vin_index_raw - 1) AS vin_index,

    vin_item.coinbase AS coinbase,
    vin_item.txid AS vin_txid,
    vin_item.vout AS vin_vout,
    vin_item.scriptSig.asm AS scriptSig_asm,
    vin_item.scriptSig.hex AS scriptSig_hex,
    vin_item.txinwitness AS txinwitness,
    vin_item.sequence AS sequence,

    vin_item.prevout.generated AS prevout_generated,
    vin_item.prevout.height AS prevout_height,
    vin_item.prevout.value AS prevout_value,
    vin_item.prevout.scriptPubKey.asm AS prevout_scriptPubKey_asm,
    vin_item.prevout.scriptPubKey.desc AS prevout_scriptPubKey_desc,
    vin_item.prevout.scriptPubKey.hex AS prevout_scriptPubKey_hex,
    vin_item.prevout.scriptPubKey.address AS prevout_scriptPubKey_address,
    vin_item.prevout.scriptPubKey.type AS prevout_scriptPubKey_type,

    toDateTime(t.block_time, 'UTC') AS input_datetime,
    toDate(toDateTime(t.block_time, 'UTC')) AS input_date,
    toYYYYMM(toDateTime(t.block_time, 'UTC')) AS input_month,

    t.revision AS revision
FROM bitcoin.transactions AS t
ARRAY JOIN
    arrayEnumerate(t.vin) AS vin_index_raw,
    t.vin AS vin_item;


-- ============================================================
-- Materialized View 3:
-- bitcoin.transactions -> bitcoin.outputs
-- ============================================================

CREATE MATERIALIZED VIEW bitcoin.mv_transactions_to_outputs
TO bitcoin.outputs
AS
SELECT
    t.block_hash AS block_hash,
    t.block_height AS block_height,
    t.block_time AS block_time,

    t.txid AS txid,
    t.`hash` AS `hash`,

    toUInt32(vout_index_raw - 1) AS vout_index,

    vout_item.value AS value,
    vout_item.n AS n,

    vout_item.scriptPubKey.asm AS scriptPubKey_asm,
    vout_item.scriptPubKey.desc AS scriptPubKey_desc,
    vout_item.scriptPubKey.hex AS scriptPubKey_hex,
    vout_item.scriptPubKey.address AS scriptPubKey_address,
    vout_item.scriptPubKey.type AS scriptPubKey_type,

    toDateTime(t.block_time, 'UTC') AS output_datetime,
    toDate(toDateTime(t.block_time, 'UTC')) AS output_date,
    toYYYYMM(toDateTime(t.block_time, 'UTC')) AS output_month,

    t.revision AS revision
FROM bitcoin.transactions AS t
ARRAY JOIN
    arrayEnumerate(t.vout) AS vout_index_raw,
    t.vout AS vout_item;


-- ============================================================
-- Materialized View 4:
-- bitcoin.outputs -> bitcoin.addresses
-- Positive rows: UTXO created
-- ============================================================

CREATE MATERIALIZED VIEW bitcoin.mv_outputs_to_addresses
TO bitcoin.addresses
AS
SELECT
    assumeNotNull(o.scriptPubKey_address) AS address,
    'output' AS direction,

    o.txid AS txid,
    o.`hash` AS `hash`,

    o.block_hash AS block_hash,
    o.block_height AS block_height,
    o.block_time AS block_time,

    o.txid AS utxo_txid,
    o.vout_index AS utxo_vout,

    o.vout_index AS source_index,

    o.value AS value,
    o.value AS value_delta,

    toDateTime(o.block_time, 'UTC') AS address_datetime,
    toDate(toDateTime(o.block_time, 'UTC')) AS address_date,
    toYYYYMM(toDateTime(o.block_time, 'UTC')) AS address_month,

    o.revision AS revision
FROM bitcoin.outputs AS o
WHERE isNotNull(o.scriptPubKey_address)
  AND o.scriptPubKey_address != '';


-- ============================================================
-- Materialized View 5:
-- bitcoin.inputs -> bitcoin.addresses
-- Negative rows: previous UTXO spent
-- ============================================================

CREATE MATERIALIZED VIEW bitcoin.mv_inputs_to_addresses
TO bitcoin.addresses
AS
SELECT
    assumeNotNull(i.prevout_scriptPubKey_address) AS address,
    'input' AS direction,

    i.txid AS txid,
    i.`hash` AS `hash`,

    i.block_hash AS block_hash,
    i.block_height AS block_height,
    i.block_time AS block_time,

    assumeNotNull(i.vin_txid) AS utxo_txid,
    assumeNotNull(i.vin_vout) AS utxo_vout,

    i.vin_index AS source_index,

    assumeNotNull(i.prevout_value) AS value,
    -assumeNotNull(i.prevout_value) AS value_delta,

    toDateTime(i.block_time, 'UTC') AS address_datetime,
    toDate(toDateTime(i.block_time, 'UTC')) AS address_date,
    toYYYYMM(toDateTime(i.block_time, 'UTC')) AS address_month,

    i.revision AS revision
FROM bitcoin.inputs AS i
WHERE isNotNull(i.prevout_scriptPubKey_address)
  AND i.prevout_scriptPubKey_address != ''
  AND isNotNull(i.vin_txid)
  AND isNotNull(i.vin_vout)
  AND isNotNull(i.prevout_value);


-- ============================================================
-- Optional repair / reload helpers
-- ============================================================

-- If bad rows already exist, changing the MV is not enough. Rebuild downstream tables.
-- Choose one of the following strategies carefully.

-- Full downstream rebuild from existing bitcoin.blocks:
-- TRUNCATE TABLE bitcoin.addresses;
-- TRUNCATE TABLE bitcoin.outputs;
-- TRUNCATE TABLE bitcoin.inputs;
-- TRUNCATE TABLE bitcoin.transactions;
-- INSERT INTO bitcoin.transactions
-- SELECT
--     b.`hash` AS block_hash,
--     b.`height` AS block_height,
--     b.`time` AS block_time,
--     b.`mediantime` AS block_mediantime,
--     tx_item.txid AS txid,
--     tx_item.`hash` AS `hash`,
--     tx_item.version AS version,
--     tx_item.size AS size,
--     tx_item.vsize AS vsize,
--     tx_item.weight AS weight,
--     tx_item.locktime AS locktime,
--     tx_item.vin AS vin,
--     tx_item.vout AS vout,
--     tx_item.fee AS fee,
--     tx_item.hex AS hex,
--     toDateTime(b.`time`, 'UTC') AS transaction_datetime,
--     toDate(toDateTime(b.`time`, 'UTC')) AS transaction_date,
--     toYYYYMM(toDateTime(b.`time`, 'UTC')) AS transaction_month,
--     b.revision AS revision
-- FROM bitcoin.blocks AS b
-- ARRAY JOIN b.tx AS tx_item;
--
-- Important: Materialized views on bitcoin.transactions will populate inputs/outputs,
-- and materialized views on inputs/outputs will populate addresses.


-- ============================================================
-- Data validation queries
-- ============================================================

-- 1. txid/hash may be equal for non-SegWit transactions, but block_hash must not equal tx hash.
-- SELECT count() AS bad_block_hash_rows
-- FROM bitcoin.transactions
-- WHERE block_hash = `hash` OR block_hash = txid;

-- 2. Every transaction block_hash should join back to bitcoin.blocks.hash.
-- SELECT count() AS orphan_transaction_block_hash_rows
-- FROM bitcoin.transactions AS t
-- LEFT JOIN bitcoin.blocks AS b ON t.block_hash = b.`hash`
-- WHERE b.`hash` IS NULL;

-- 3. Check address rows inherited correct block hashes.
-- SELECT count() AS bad_address_block_hash_rows
-- FROM bitcoin.addresses
-- WHERE block_hash = `hash` OR block_hash = txid;


-- ============================================================
-- Example queries
-- ============================================================

-- Address balance:
-- SELECT
--     address,
--     sum(value_delta) AS balance
-- FROM bitcoin.addresses FINAL
-- WHERE address = '1811f7UUQAkAejj11dU5cVtKUSTfoSVzdm'
-- GROUP BY address;

-- UTXOs for one address:
-- SELECT
--     o.address,
--     o.utxo_txid,
--     o.utxo_vout,
--     o.value
-- FROM
-- (
--     SELECT address, utxo_txid, utxo_vout, value
--     FROM bitcoin.addresses FINAL
--     WHERE address = '1811f7UUQAkAejj11dU5cVtKUSTfoSVzdm'
--       AND direction = 'output'
-- ) AS o
-- LEFT ANTI JOIN
-- (
--     SELECT address, utxo_txid, utxo_vout
--     FROM bitcoin.addresses FINAL
--     WHERE address = '1811f7UUQAkAejj11dU5cVtKUSTfoSVzdm'
--       AND direction = 'input'
-- ) AS i
-- ON  o.address = i.address
-- AND o.utxo_txid = i.utxo_txid
-- AND o.utxo_vout = i.utxo_vout;
