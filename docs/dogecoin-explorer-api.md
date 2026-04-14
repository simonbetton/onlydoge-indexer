# Dogecoin Explorer API

## Current Goal

Add a public Dogecoin block-explorer read surface without breaking the existing authenticated investigation API.

The explorer should:

1. read only from indexed warehouse facts and stored raw block snapshots,
2. stay inside the modular-monolith boundaries already used by the repo,
3. expose a public `/v1/explorer/*` route group,
4. preserve entity, tag, and source-path overlays where they add value.

## Explorer Module Shape

Keep explorer reads in a dedicated `explorer-query` module.

- `domain`
  - explorer response models
- `application`
  - block, transaction, address, and search orchestration
- `contracts`
  - active-network reader, warehouse reader, raw-block reader, overlay reader
- `infrastructure`
  - public Elysia HTTP routes

This keeps the split clear:

- `investigation-query` stays analyst-oriented and authenticated,
- `explorer-query` stays public, read-only, and Dogecoin-focused.

## Route Surface

Public routes:

- `GET /v1/explorer/networks`
  - active Dogecoin networks plus indexing status
- `GET /v1/explorer/search?q=...&network=...`
  - numeric query => block height
  - otherwise try txid, then block hash, then address
- `GET /v1/explorer/blocks`
  - recent indexed blocks
- `GET /v1/explorer/blocks/:ref`
  - block by height or block hash
- `GET /v1/explorer/transactions/:txid`
  - transaction summary, inputs, outputs, transfer projection, label refs
- `GET /v1/explorer/addresses/:address`
  - balance summary plus the full investigation overlay
- `GET /v1/explorer/addresses/:address/transactions`
  - reverse-chronological address history
- `GET /v1/explorer/addresses/:address/utxos`
  - current spendable outputs

Network selection:

- if one active Dogecoin network exists, it becomes the implicit default
- if more than one exists, explorer reads require `?network=...`

## Data Sources

The explorer reads from existing storage instead of live RPC.

### Raw block snapshots

Used for:

- recent block lists
- block detail
- transaction output shapes and ordering

The raw block storage remains keyed by `networkId + blockHeight + part`.

### Warehouse read models

Used for:

- block ref lookup via `applied_blocks_v2`
- tx lookup via `utxo_outputs_v2`
- address history via `address_movements_v2`
- current UTXOs via `utxo_outputs_current_v2`
- current balances via `balances_v2`

### Metadata and overlay

Used for:

- active network catalog
- labeled address to entity joins
- tag joins and risk level derivation
- address detail overlay via the existing investigation read path

## Auth And Cache Policy

Explorer routes are intentionally public.

- `/up`, `/v1/heartbeat`, `/openapi`, and `/v1/explorer/*` bypass API token enforcement
- all existing admin and investigation routes stay protected

Cache policy:

- `/v1/explorer/search`: `public, max-age=5, stale-while-revalidate=15`
- `/v1/explorer/addresses*`: `public, max-age=15, stale-while-revalidate=60`
- `/v1/explorer/blocks*`, `/v1/explorer/transactions*`, `/v1/explorer/networks`: `public, max-age=30, stale-while-revalidate=120`

## Test Coverage

The explorer work should stay covered by the same layers used elsewhere:

- API integration tests for public access, block/tx/address reads, and OpenAPI exposure
- warehouse contract tests for block refs, tx refs, address summaries, history, and UTXOs
- existing indexer integration tests continue proving the underlying Dogecoin projection data
