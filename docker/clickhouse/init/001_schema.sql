CREATE DATABASE IF NOT EXISTS onlydoge;

CREATE TABLE IF NOT EXISTS onlydoge.applied_blocks
(
  network_id UInt64,
  block_height UInt64,
  block_hash String
)
ENGINE = MergeTree
ORDER BY (network_id, block_height, block_hash);

CREATE TABLE IF NOT EXISTS onlydoge.utxo_outputs
(
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  vout UInt64,
  output_key String,
  address String,
  script_type String,
  value_base String,
  is_coinbase UInt8,
  is_spendable UInt8,
  spent_by_txid Nullable(String),
  spent_in_block Nullable(UInt64),
  spent_input_index Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY (network_id, output_key);

CREATE TABLE IF NOT EXISTS onlydoge.address_movements
(
  movement_id String,
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  entry_index UInt64,
  address String,
  asset_address String,
  direction String,
  amount_base String,
  output_key Nullable(String),
  derivation_method String
)
ENGINE = MergeTree
ORDER BY (network_id, movement_id);

CREATE TABLE IF NOT EXISTS onlydoge.transfers
(
  transfer_id String,
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  transfer_index UInt64,
  asset_address String,
  from_address String,
  to_address String,
  amount_base String,
  derivation_method String,
  confidence Float64,
  is_change UInt8,
  input_address_count UInt64,
  output_address_count UInt64
)
ENGINE = MergeTree
ORDER BY (network_id, transfer_id);

CREATE TABLE IF NOT EXISTS onlydoge.balances
(
  network_id UInt64,
  address String,
  asset_address String,
  balance String,
  as_of_block_height UInt64
)
ENGINE = MergeTree
ORDER BY (network_id, address, asset_address);

CREATE TABLE IF NOT EXISTS onlydoge.direct_links
(
  network_id UInt64,
  from_address String,
  to_address String,
  asset_address String,
  transfer_count UInt64,
  total_amount_base String,
  first_seen_block_height UInt64,
  last_seen_block_height UInt64
)
ENGINE = MergeTree
ORDER BY (network_id, from_address, to_address, asset_address);

CREATE TABLE IF NOT EXISTS onlydoge.source_links
(
  network_id UInt64,
  source_address_id UInt64,
  source_address String,
  to_address String,
  hop_count UInt64,
  path_transfer_count UInt64,
  path_addresses Array(String),
  first_seen_block_height UInt64,
  last_seen_block_height UInt64
)
ENGINE = MergeTree
ORDER BY (network_id, source_address_id, to_address);

CREATE TABLE IF NOT EXISTS onlydoge.applied_blocks_v2
(
  network_id UInt64,
  block_height UInt64,
  block_hash String
)
ENGINE = MergeTree
ORDER BY (network_id, block_height, block_hash);

CREATE TABLE IF NOT EXISTS onlydoge.utxo_outputs_v2
(
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  vout UInt64,
  output_key String,
  address String,
  script_type String,
  value_base String,
  is_coinbase UInt8,
  is_spendable UInt8,
  spent_by_txid Nullable(String),
  spent_in_block Nullable(UInt64),
  spent_input_index Nullable(UInt64),
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (network_id, output_key);

CREATE TABLE IF NOT EXISTS onlydoge.address_movements_v2
(
  movement_id String,
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  entry_index UInt64,
  address String,
  asset_address String,
  direction String,
  amount_base String,
  output_key Nullable(String),
  derivation_method String
)
ENGINE = MergeTree
ORDER BY (network_id, movement_id);

CREATE TABLE IF NOT EXISTS onlydoge.transfers_v2
(
  transfer_id String,
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  transfer_index UInt64,
  asset_address String,
  from_address String,
  to_address String,
  amount_base String,
  derivation_method String,
  confidence Float64,
  is_change UInt8,
  input_address_count UInt64,
  output_address_count UInt64
)
ENGINE = MergeTree
ORDER BY (network_id, transfer_id);

CREATE TABLE IF NOT EXISTS onlydoge.balances_v2
(
  network_id UInt64,
  address String,
  asset_address String,
  balance String,
  as_of_block_height UInt64,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (network_id, address, asset_address);

CREATE TABLE IF NOT EXISTS onlydoge.direct_links_v2
(
  network_id UInt64,
  from_address String,
  to_address String,
  asset_address String,
  transfer_count UInt64,
  total_amount_base String,
  first_seen_block_height UInt64,
  last_seen_block_height UInt64,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (network_id, from_address, to_address, asset_address);
