# OnlyDoge

OnlyDoge is a Bun-based blockchain investigation backend focused on Dogecoin-first analysis with EVM support. It ingests chain data, labels entities and addresses, exposes investigation APIs, and runs an indexing pipeline behind a single modular-monolith codebase.

This repository is organized around business modules with Clean/Hexagonal boundaries:

- `access-control`
- `network-catalog`
- `entity-labeling`
- `explorer-query`
- `investigation-query`
- `indexing-pipeline`

The runtime stack uses TypeScript, Bun, Elysia, OpenAPI, Biome, and Vitest.

## Business Objectives

- Provide a programmable investigation API for entities, tags, addresses, networks, tokens, stats, and heartbeat checks.
- Provide a Dogecoin explorer API for networks, search, blocks, transactions, addresses, address history, and UTXOs.
- Support Dogecoin as the UTXO family and EVM as the account-based family.
- Separate domain logic from transport, storage, and RPC concerns.
- Run the system as a modular monolith while preserving clear domain boundaries and testability.
- Support three runtime modes from one entrypoint: `http`, `indexer`, and `both`.

## Current Status

The repository is production-shaped, but not full upstream feature parity yet.

- The HTTP contract, modular architecture, metadata storage, raw block snapshotting, and auth flows are implemented.
- OpenAPI is enabled at `/openapi` and `/openapi/json`.
- The current indexer persists raw snapshots and coordinator progress.
- The current warehouse layer is still scaffold-level. The DuckDB path is JSON-backed and the ClickHouse adapter is intentionally minimal.
- Full transfer, balance, token, and link materialization logic still needs to be completed for complete feature parity.

## Architecture

### Bounded Contexts

```mermaid
flowchart LR
  CLI["CLI Runtime"] --> API["API Mode"]
  CLI --> IDX["Indexer Mode"]
  API --> AC["Access Control"]
  API --> NC["Network Catalog"]
  API --> EL["Entity Labeling"]
  API --> EQ["Explorer Query"]
  API --> IQ["Investigation Query"]
  IDX --> IP["Indexing Pipeline"]
  NC --> RPC["Blockchain RPC Gateway"]
  IQ --> META["Metadata Store"]
  IQ --> WH["Warehouse"]
  EL --> META
  AC --> META
  IP --> META
  IP --> RAW["Raw Block Storage"]
  IP --> WH
  IP --> RPC
```

### Clean / Hexagonal Shape

```mermaid
flowchart TB
  subgraph Module["Each Business Module"]
    D["domain"]
    A["application"]
    C["contracts"]
    I["infrastructure"]
  end
  D --> A
  A --> C
  I --> C
  API["Elysia HTTP"] --> A
  DB["Metadata / Warehouse / Storage / RPC"] --> I
```

### Production Deployment Topology

```mermaid
flowchart LR
  User["Client / Analyst"] --> LB["Ingress / Reverse Proxy"]
  LB --> API["onlydoge-api container"]
  API --> PG["PostgreSQL"]
  API --> CH["ClickHouse"]
  API --> S3["MinIO / S3"]
  IDX["onlydoge-indexer container"] --> PG
  IDX --> CH
  IDX --> S3
  IDX --> RPC["Dogecoin / EVM RPC Endpoints"]
```

## Repository Layout

```text
apps/
  api/          Elysia app assembly
  indexer/      indexer process entrypoint
  onlydoge/     unified CLI entrypoint

packages/
  shared-kernel/  shared errors, IDs, value objects
  platform/       runtime wiring and infrastructure adapters
  modules/
    access-control/
    network-catalog/
    entity-labeling/
    explorer-query/
    investigation-query/
    indexing-pipeline/

tests/
  unit/
  integration/
```

## DX Commands

### Host-Native

```bash
bun install
bun run typecheck
bun run ci
bun run dev:both
bun run dev:http
bun run dev:indexer
```

### Docker Local

1. Copy the local environment template.

```bash
cp .env.local.example .env.local
```

2. Start the local stack.

```bash
bun run docker:local:up
```

3. Stop it.

```bash
bun run docker:local:down
```

4. Reset the local stack and volumes.

```bash
bun run docker:local:reset
```

5. Follow logs.

```bash
bun run docker:local:logs
```

The local stack includes:

- `onlydoge` running in `both` mode with Bun watch mode
- `postgres` for metadata
- `minio` for S3-compatible raw block storage
- `clickhouse` for warehouse queries

Service endpoints in local Docker:

- API: `http://localhost:2277`
- OpenAPI UI: `http://localhost:2277/openapi`
- OpenAPI JSON: `http://localhost:2277/openapi/json`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- ClickHouse HTTP: `http://localhost:8123`
- PostgreSQL: `localhost:5432`

## Environment Model

The app reads the following configuration surface:

- `ONLYDOGE_DATABASE`
- `ONLYDOGE_DATABASE_HOST`
- `ONLYDOGE_DATABASE_PORT`
- `ONLYDOGE_DATABASE_NAME`
- `ONLYDOGE_DATABASE_USER`
- `ONLYDOGE_DATABASE_PASSWORD`
- `ONLYDOGE_STORAGE`
- `ONLYDOGE_WAREHOUSE`
- `ONLYDOGE_S3_ACCESS_KEY_ID`
- `ONLYDOGE_S3_SECRET_ACCESS_KEY`
- `ONLYDOGE_WAREHOUSE_USER`
- `ONLYDOGE_WAREHOUSE_PASSWORD`
- `ONLYDOGE_INDEXER_LEASE_HEARTBEAT_INTERVAL_MS`
- `ONLYDOGE_INDEXER_NETWORK_CONCURRENCY`
- `ONLYDOGE_INDEXER_DOGECOIN_TRANSFER_MAX_INPUT_ADDRESSES`
- `ONLYDOGE_INDEXER_DOGECOIN_TRANSFER_MAX_EDGES`
- `ONLYDOGE_INDEXER_SYNC_BACKLOG_HIGH_WATERMARK`
- `ONLYDOGE_INDEXER_SYNC_BACKLOG_LOW_WATERMARK`
- `ONLYDOGE_INDEXER_SYNC_WINDOW`
- `ONLYDOGE_INDEXER_SYNC_WINDOW_MIN`
- `ONLYDOGE_INDEXER_SYNC_WINDOW_MAX`
- `ONLYDOGE_INDEXER_SYNC_CONCURRENCY`
- `ONLYDOGE_INDEXER_SYNC_TARGET_MS`
- `ONLYDOGE_INDEXER_SYNC_TIMEOUT_MS`
- `ONLYDOGE_INDEXER_BOOTSTRAP_TIMEOUT_MS`
- `ONLYDOGE_INDEXER_FACT_WINDOW`
- `ONLYDOGE_INDEXER_FACT_TIMEOUT_MS`
- `ONLYDOGE_CORE_SYNC_COMPLETE_DISTANCE`
- `ONLYDOGE_CORE_PROCESS_WINDOW`
- `ONLYDOGE_CORE_ONLINE_TIP_DISTANCE`
- `ONLYDOGE_INDEXER_PROJECT_WINDOW`
- `ONLYDOGE_INDEXER_PROJECT_WINDOW_MIN`
- `ONLYDOGE_INDEXER_PROJECT_WINDOW_MAX`
- `ONLYDOGE_INDEXER_PROJECT_TARGET_MS`
- `ONLYDOGE_INDEXER_PROJECT_TIMEOUT_MS`
- `ONLYDOGE_INDEXER_RELINK_BACKLOG_THRESHOLD`
- `ONLYDOGE_INDEXER_RELINK_TIP_DISTANCE`
- `ONLYDOGE_INDEXER_RELINK_BATCH_SIZE`
- `ONLYDOGE_INDEXER_RELINK_CONCURRENCY`
- `ONLYDOGE_INDEXER_RELINK_FRONTIER_BATCH`
- `ONLYDOGE_INDEXER_RELINK_TIMEOUT_MS`
- `ONLYDOGE_MODE`
- `ONLYDOGE_IP`
- `ONLYDOGE_PORT`

Docker Compose generates the main app settings from the `.env.local` or `.env.production` file.

For PostgreSQL, you can either supply a full DSN in `ONLYDOGE_DATABASE` or let OnlyDoge build it from `ONLYDOGE_DATABASE_HOST`, `ONLYDOGE_DATABASE_PORT`, `ONLYDOGE_DATABASE_NAME`, `ONLYDOGE_DATABASE_USER`, and `ONLYDOGE_DATABASE_PASSWORD`.

In the production image, startup fails fast if database config, `ONLYDOGE_STORAGE`, or `ONLYDOGE_WAREHOUSE` are missing. It will no longer silently fall back to local SQLite, file storage, or DuckDB when `NODE_ENV=production`.

## ClickHouse Memory Posture

The default ClickHouse profile is tuned for stability over peak throughput.

- address-heavy explorer reads use address-oriented read tables instead of scanning the write-optimized facts directly
- current-state UTXO lookups avoid `FINAL` and use bounded `LIMIT 1 BY output_key` reads
- the ClickHouse server memory ratio is capped below total RAM to leave headroom for merges, the page cache, and the OS
- default query and insert thread counts are capped to reduce memory spikes
- external sort and group-by thresholds spill earlier instead of holding large intermediates in RAM

The app also boots ClickHouse with runtime-safe warehouse migrations. Existing deployments automatically create and backfill the address-oriented read tables on startup before serving explorer traffic.

## Local Docker Workflow

The local setup intentionally favors developer feedback over immutability.

- The source tree is bind-mounted into the app container.
- The app runs with `bun run --watch`.
- Infrastructure is persisted in Docker volumes.
- A MinIO bootstrap job creates the S3 bucket automatically.
- ClickHouse is initialized with the Dogecoin warehouse schema, including the address-oriented explorer read models.
- `/v1/heartbeat` stays open.
- `POST /v1/keys` stays open only until the first API key is created.
- Every other `/v1` route requires `x-api-token`.

If you change dependency manifests, restart the local app container or rerun `bun run docker:local:up`.
If you change ClickHouse credentials, run `bun run docker:local:reset` before bringing the stack up again so the ClickHouse volume is recreated with the new user setup.

The local and production Compose stacks now mount the checked-in ClickHouse tuning files:

- `docker/clickhouse/config.d/onlydoge-memory.xml`
- `docker/clickhouse/users.d/onlydoge-memory.xml`

Those files codify the current memory profile used to keep projection queries off the old implicit limits.

## Dogecoin Runsheet

Use the built-in runsheet helper to create the first API key if needed, register a Dogecoin network, and verify that stats can be read.

```bash
bun run runsheet:dogecoin -- \
  --base-url http://127.0.0.1:2277 \
  --rpc-endpoint 'http://USER:PASS=@110.124.0.2:22555/' \
  --name 'Dogecoin Mainnet' \
  --chain-id 0 \
  --block-time 60 \
  --rps 25
```

If the API already has keys configured, pass an existing token:

```bash
bun run runsheet:dogecoin -- \
  --base-url http://127.0.0.1:2277 \
  --api-token 'sk_...' \
  --rpc-endpoint 'http://USER:PASS=@110.124.0.2:22555/'
```

What this does:

- checks `GET /v1/heartbeat/`
- creates a bootstrap API key with `POST /v1/keys/` when no keys exist yet
- creates the Dogecoin network with `POST /v1/networks/`
- reads `GET /v1/stats/`
- prints the equivalent `curl` commands for later use

OnlyDoge starts indexing automatically after the network is created, as long as the runtime is running in `--mode=both` or `--mode=indexer`.

## Production Docker Workflow

1. Copy the production environment template.

```bash
cp .env.production.example .env.production
```

2. Replace every placeholder secret before deployment.

3. Build and start the production-style stack.

```bash
bun run docker:prod:up
```

4. Stop it.

```bash
bun run docker:prod:down
```

5. Tail logs.

```bash
bun run docker:prod:logs
```

The production compose file splits runtime responsibilities across two containers:

- `onlydoge-api` runs `--mode=http`
- `onlydoge-indexer` runs `--mode=indexer`

This is the preferred operational shape because the API and the long-running worker have different scaling and restart characteristics.

## Published Images

Production images are published to GitHub Container Registry:

- `ghcr.io/simonbetton/onlydoge-indexer:latest`
- `ghcr.io/simonbetton/onlydoge-indexer:vX.Y.Z`

To push a fresh multi-arch `latest` image from this repo:

```bash
npm run image:push
```

The script bootstraps and uses a `docker-container` Buildx builder named `onlydoge-multiarch`, which avoids the multi-platform limitation of the default Docker driver on tools like OrbStack.

If you only want to initialize that builder first:

```bash
npm run image:builder:init
```

You still need to be authenticated to `ghcr.io` before pushing.

The production image defaults to `--mode=both`, listens on port `80`, and exposes `/up` as an unauthenticated health endpoint so it can be installed by ONCE-compatible runtimes.

## Deploying This Project

### Recommended Deployment Shape

Deploy this project as containers on a platform that supports long-running processes.

- A single host with Docker Compose is the smallest viable deployment.
- ECS, Nomad, Kubernetes, or Fly Machines are better targets once you need stronger scheduling and secret management.
- Reverse-proxy the API container through Nginx, Caddy, Traefik, or a cloud load balancer.

### Minimum Production Components

- `onlydoge-api`
- `onlydoge-indexer`
- PostgreSQL
- S3-compatible object storage
- ClickHouse
- Dogecoin and EVM RPC providers

### ONCE First-Time Deployment

ONCE is a good fit if you want to run OnlyDoge as a single production image in `both` mode while pointing it at external PostgreSQL, S3-compatible storage, ClickHouse, and RPC providers.

1. Install ONCE on the target machine.

```bash
curl https://get.once.com | sh
```

2. Make sure your chosen hostname already resolves to the target machine.

3. In ONCE, install a custom image and use:

```text
ghcr.io/simonbetton/onlydoge-indexer:latest
```

4. Configure the application environment in ONCE with at least:

- `ONLYDOGE_DATABASE`
- or `ONLYDOGE_DATABASE_HOST` with the optional `ONLYDOGE_DATABASE_PORT`, `ONLYDOGE_DATABASE_NAME`, `ONLYDOGE_DATABASE_USER`, and `ONLYDOGE_DATABASE_PASSWORD`
- `ONLYDOGE_STORAGE`
- `ONLYDOGE_WAREHOUSE`
- `ONLYDOGE_S3_ACCESS_KEY_ID`
- `ONLYDOGE_S3_SECRET_ACCESS_KEY`
- `ONLYDOGE_WAREHOUSE_USER`
- `ONLYDOGE_WAREHOUSE_PASSWORD`
- `ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS`
- `ONLYDOGE_INDEXER_LEASE_HEARTBEAT_INTERVAL_MS=5000`
- `ONLYDOGE_INDEXER_BOOTSTRAP_TIMEOUT_MS=60000`
- `ONLYDOGE_INDEXER_NETWORK_CONCURRENCY=2`
- `ONLYDOGE_INDEXER_DOGECOIN_TRANSFER_MAX_INPUT_ADDRESSES=64`
- `ONLYDOGE_INDEXER_DOGECOIN_TRANSFER_MAX_EDGES=1024`
- `ONLYDOGE_INDEXER_SYNC_CONCURRENCY=4`
- `ONLYDOGE_INDEXER_FACT_WINDOW=64`
- `ONLYDOGE_INDEXER_FACT_TIMEOUT_MS=300000`
- `ONLYDOGE_CORE_SYNC_COMPLETE_DISTANCE=6`
- `ONLYDOGE_CORE_PROCESS_WINDOW=128`
- `ONLYDOGE_CORE_ONLINE_TIP_DISTANCE=6`
- `ONLYDOGE_INDEXER_PROJECT_WINDOW=4`
- `ONLYDOGE_INDEXER_PROJECT_WINDOW_MIN=2`
- `ONLYDOGE_INDEXER_PROJECT_WINDOW_MAX=16`
- `ONLYDOGE_INDEXER_PROJECT_TARGET_MS=30000`
- `ONLYDOGE_INDEXER_PROJECT_TIMEOUT_MS=300000`
- `ONLYDOGE_INDEXER_SYNC_BACKLOG_HIGH_WATERMARK=2048`
- `ONLYDOGE_INDEXER_SYNC_BACKLOG_LOW_WATERMARK=512`
- `ONLYDOGE_INDEXER_SYNC_WINDOW=32`
- `ONLYDOGE_INDEXER_SYNC_WINDOW_MIN=32`
- `ONLYDOGE_INDEXER_SYNC_WINDOW_MAX=256`
- `ONLYDOGE_INDEXER_SYNC_TARGET_MS=15000`
- `ONLYDOGE_INDEXER_SYNC_TIMEOUT_MS=120000`
- `ONLYDOGE_INDEXER_RELINK_BACKLOG_THRESHOLD=256`
- `ONLYDOGE_INDEXER_RELINK_TIP_DISTANCE=512`
- `ONLYDOGE_INDEXER_RELINK_BATCH_SIZE=16`
- `ONLYDOGE_INDEXER_RELINK_CONCURRENCY=2`
- `ONLYDOGE_INDEXER_RELINK_FRONTIER_BATCH=32`
- `ONLYDOGE_INDEXER_RELINK_TIMEOUT_MS=120000`
- `ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS=30000`

5. Leave the image command at its default unless you intentionally want to override the bundled `--mode=both --ip=0.0.0.0 --port=80`.

6. After ONCE finishes booting the container, verify:

- `https://<your-hostname>/up`
- `https://<your-hostname>/v1/heartbeat`
- `https://<your-hostname>/openapi`

If your ClickHouse instance is self-hosted rather than managed, also install the checked-in memory tuning files on that host before starting OnlyDoge:

```bash
scp docker/clickhouse/config.d/onlydoge-memory.xml root@<clickhouse-host>:/etc/clickhouse-server/config.d/onlydoge-memory.xml
scp docker/clickhouse/users.d/onlydoge-memory.xml root@<clickhouse-host>:/etc/clickhouse-server/users.d/onlydoge-memory.xml
ssh root@<clickhouse-host> 'systemctl restart clickhouse-server'
```

The ONCE-specific assumptions come from ONCE's compatibility model: Docker image, HTTP on port `80`, a successful `/up` healthcheck, and persistent storage mounted at `/storage` if your app uses local disk state. OnlyDoge can run on ONCE without local state when you provide external production services.

### Releases

This repository includes a GitHub Actions workflow at `.github/workflows/publish-image.yml` that publishes the production image to GHCR.

For a new release:

1. Commit and push the desired production state to `main`.
2. Create and push a version tag.

```bash
git tag v0.1.0
git push origin v0.1.0
```

3. GitHub Actions will publish:

- `ghcr.io/simonbetton/onlydoge-indexer:v0.1.0`
- `ghcr.io/simonbetton/onlydoge-indexer:0.1.0`
- `ghcr.io/simonbetton/onlydoge-indexer:0.1`
- `ghcr.io/simonbetton/onlydoge-indexer:0`
- `ghcr.io/simonbetton/onlydoge-indexer:latest`

4. In ONCE, update the installed image tag from `latest` to the version you want to pin, or switch from one version tag to the next and let ONCE restart the app.

### Checked-In ONCE Deploy Script

Use the checked-in deploy script when you want the repo, not the host UI, to be the source of truth for the ONCE image and app env set.

1. Create a local ONCE env file.

```bash
cp .env.once.example .env.once
```

2. Fill in the real production secrets and infrastructure values.

3. Deploy with the checked-in script.

```bash
npm run deploy:once
```

The script:

- reads `.env.once`
- resolves `ghcr.io/simonbetton/onlydoge-indexer:latest` to an immutable digest
- connects to the ONCE host over SSH
- uses `once deploy` if the app does not exist yet
- uses `once update` if it already exists
- pushes the full app env set atomically instead of patching individual values
- waits for `/up` and `/v1/heartbeat` to succeed

Useful overrides:

```bash
npm run deploy:once -- --envFile .env.once --image ghcr.io/simonbetton/onlydoge-indexer:latest
npm run deploy:once -- --dryRun
npm run deploy:once -- --host platform.onlydoge.io --sshJump root@164.90.159.127 --sshTarget root@10.124.0.3
```

By default the script expects:

- `ONCE_APP_HOST=platform.onlydoge.io`
- `ONCE_SSH_JUMP=root@164.90.159.127`
- `ONCE_SSH_TARGET=root@10.124.0.3`

If your Postgres DSN uses `sslrootcert=/storage/do-ca.pem`, make sure that CA file already exists in the ONCE app volume before the first deploy. For the current production host that file is already present.

### Deployment Steps

```bash
cp .env.production.example .env.production
# edit secrets and infrastructure values
bun run docker:prod:up
```

After the stack is running:

- verify `GET /v1/heartbeat` returns `204`
- open `/openapi` to inspect the live contract
- create an API key via `POST /v1/keys`
- register at least one network before expecting indexing activity

For the bundled Compose production stack, ClickHouse and the indexer tuning are already codified through:

- `docker/clickhouse/config.d/onlydoge-memory.xml`
- `docker/clickhouse/users.d/onlydoge-memory.xml`
- `ONLYDOGE_INDEXER_LEASE_HEARTBEAT_INTERVAL_MS=5000`
- `ONLYDOGE_INDEXER_BOOTSTRAP_TIMEOUT_MS=60000`
- `ONLYDOGE_INDEXER_NETWORK_CONCURRENCY=2`
- `ONLYDOGE_INDEXER_DOGECOIN_TRANSFER_MAX_INPUT_ADDRESSES=64`
- `ONLYDOGE_INDEXER_DOGECOIN_TRANSFER_MAX_EDGES=1024`
- `ONLYDOGE_INDEXER_FACT_WINDOW=64`
- `ONLYDOGE_INDEXER_FACT_TIMEOUT_MS=300000`
- `ONLYDOGE_CORE_SYNC_COMPLETE_DISTANCE=6`
- `ONLYDOGE_CORE_PROCESS_WINDOW=128`
- `ONLYDOGE_CORE_ONLINE_TIP_DISTANCE=6`
- `ONLYDOGE_INDEXER_PROJECT_WINDOW=4`
- `ONLYDOGE_INDEXER_PROJECT_WINDOW_MIN=2`
- `ONLYDOGE_INDEXER_PROJECT_WINDOW_MAX=16`
- `ONLYDOGE_INDEXER_PROJECT_TARGET_MS=30000`
- `ONLYDOGE_INDEXER_PROJECT_TIMEOUT_MS=300000`
- `ONLYDOGE_INDEXER_SYNC_BACKLOG_HIGH_WATERMARK=2048`
- `ONLYDOGE_INDEXER_SYNC_BACKLOG_LOW_WATERMARK=512`
- `ONLYDOGE_INDEXER_SYNC_WINDOW=32`
- `ONLYDOGE_INDEXER_SYNC_WINDOW_MIN=32`
- `ONLYDOGE_INDEXER_SYNC_WINDOW_MAX=256`
- `ONLYDOGE_INDEXER_SYNC_CONCURRENCY=4`
- `ONLYDOGE_INDEXER_SYNC_TARGET_MS=15000`
- `ONLYDOGE_INDEXER_SYNC_TIMEOUT_MS=120000`
- `ONLYDOGE_INDEXER_RELINK_BACKLOG_THRESHOLD=256`
- `ONLYDOGE_INDEXER_RELINK_TIP_DISTANCE=512`
- `ONLYDOGE_INDEXER_RELINK_BATCH_SIZE=16`
- `ONLYDOGE_INDEXER_RELINK_CONCURRENCY=2`
- `ONLYDOGE_INDEXER_RELINK_FRONTIER_BATCH=32`
- `ONLYDOGE_INDEXER_RELINK_TIMEOUT_MS=120000`
- `ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS=30000`

### Reverse Proxy Example Concerns

- terminate TLS at the proxy or load balancer
- forward `x-api-token` headers unchanged
- keep API timeouts higher than your longest request path
- do not expose PostgreSQL, MinIO, or ClickHouse directly to the public internet unless intentionally required

## What Not To Deploy On

The full system is not a good fit for Vercel-style request-only serverless deployment because the indexer is a long-running worker. The API alone could be adapted to a function model, but the combined `both` or `indexer` modes should be deployed on a container or VM platform instead.

## API Surface

Current `/v1` route groups:

- `/v1/heartbeat`
- `/v1/explorer`
- `/v1/stats`
- `/v1/info`
- `/v1/keys`
- `/v1/networks`
- `/v1/entities`
- `/v1/addresses`
- `/v1/tokens`
- `/v1/tags`

Error payload shape:

```json
{"error":"..."}
```

Public explorer reads:

- `GET /v1/explorer/networks`
- `GET /v1/explorer/search?q=...`
- `GET /v1/explorer/blocks`
- `GET /v1/explorer/blocks/:ref`
- `GET /v1/explorer/transactions/:txid`
- `GET /v1/explorer/addresses/:address`
- `GET /v1/explorer/addresses/:address/transactions`
- `GET /v1/explorer/addresses/:address/utxos`

Explorer routes are public and read only indexed data plus stored raw block snapshots. Analyst and admin routes stay behind `x-api-token`.

## Quality Gates

```bash
bun run lint
bun run typecheck
bun run test
bun run ci
```

Vitest covers:

- shared-kernel value objects and settings
- API auth and contract smoke tests
- OpenAPI snapshot generation
- indexer progress and raw block snapshot persistence

## Notes for Operators

- Keep passwords URL-safe if you inject them directly into `ONLYDOGE_DATABASE`. If you need special characters, URL-encode them.
- The current ClickHouse schema is intentionally minimal and exists to keep the current query surface bootable.
- The local and production Compose stacks assume the warehouse database name is `onlydoge`.
- Raw block storage is written to S3-compatible object storage in Dockerized environments.
- The current checked-in ClickHouse memory profile assumes a warehouse node in roughly the `16 GB RAM` class. If you run a materially smaller box, lower the profile values before deployment.
- The current checked-in indexer defaults are intentionally conservative for production backfill: `ONLYDOGE_CORE_SYNC_COMPLETE_DISTANCE=6`, `ONLYDOGE_CORE_PROCESS_WINDOW=128`, `ONLYDOGE_CORE_ONLINE_TIP_DISTANCE=6`, `ONLYDOGE_INDEXER_BOOTSTRAP_TIMEOUT_MS=60000`, `ONLYDOGE_INDEXER_FACT_WINDOW=64`, `ONLYDOGE_INDEXER_FACT_TIMEOUT_MS=300000`, `ONLYDOGE_INDEXER_PROJECT_WINDOW=4`, `ONLYDOGE_INDEXER_PROJECT_TIMEOUT_MS=300000`, `ONLYDOGE_INDEXER_DOGECOIN_TRANSFER_MAX_INPUT_ADDRESSES=64`, `ONLYDOGE_INDEXER_DOGECOIN_TRANSFER_MAX_EDGES=1024`, `ONLYDOGE_INDEXER_SYNC_BACKLOG_HIGH_WATERMARK=2048`, `ONLYDOGE_INDEXER_SYNC_BACKLOG_LOW_WATERMARK=512`, `ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS=30000`, and relink deferral near the values in `.env.production.example`.
