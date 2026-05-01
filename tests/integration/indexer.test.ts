import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

import {
  CoreDogecoinIndexerService,
  configKeyIndexerProcessTail,
  configKeyIndexerStage,
  configKeyIndexerSyncTail,
  type IndexingPipelineSettings,
} from '@onlydoge/indexing-pipeline';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { dogecoinFixture } from '../fixtures/dogecoin';
import { createDogecoinTestNetwork, createTestApp, installRpcMock } from '../helpers';

describe('core dogecoin indexer integration', () => {
  let restoreFetch: ReturnType<typeof installRpcMock>;

  beforeEach(() => {
    restoreFetch = installRpcMock();
  });

  afterEach(() => {
    restoreFetch.mockRestore();
  });

  it('syncs raw blocks first, then processes deterministic core UTXO state', async () => {
    const ctx = await createTestApp('indexer');
    const network = await createDogecoinTestNetwork(ctx.runtime);

    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
    const internalNetwork = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
    expect(internalNetwork?.networkId).toBeDefined();
    const networkId = internalNetwork?.networkId ?? 0;

    await expect(
      ctx.runtime.metadata.getJsonValue<string>(configKeyIndexerStage(networkId)),
    ).resolves.toBe('sync_backfill');
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerSyncTail(networkId)),
    ).resolves.toBe(2);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail(networkId)),
    ).resolves.toBe(-1);

    const snapshotPath = join(ctx.tempRoot, 'storage', String(networkId), '0', 'block.json.gz');
    const snapshot = await readFile(snapshotPath);
    expect(snapshot.byteLength).toBeGreaterThan(0);

    await runUntilProcessed(ctx, networkId, 2);

    await expect(
      ctx.runtime.metadata.getJsonValue<string>(configKeyIndexerStage(networkId)),
    ).resolves.toBe('process_backfill');
    await expect(
      ctx.runtime.metadata.getCurrentAddressSummary(networkId, dogecoinFixture.sourceAddress),
    ).resolves.toEqual({
      balance: '5900000000',
      utxoCount: 1,
    });
    await expect(
      ctx.runtime.metadata.getCurrentAddressSummary(networkId, dogecoinFixture.intermediaryAddress),
    ).resolves.toEqual({
      balance: '1400000000',
      utxoCount: 1,
    });
    await expect(
      ctx.runtime.metadata.getCurrentAddressSummary(networkId, dogecoinFixture.targetAddress),
    ).resolves.toEqual({
      balance: '2500000000',
      utxoCount: 1,
    });

    await expect(
      ctx.runtime.explorerQuery.listAddressUtxos(dogecoinFixture.targetAddress, network.id),
    ).resolves.toMatchObject({
      utxos: [
        expect.objectContaining({
          address: dogecoinFixture.targetAddress,
          outputKey: 'doge-tx-2:0',
          valueBase: '2500000000',
        }),
      ],
    });

    await ctx.cleanup();
  });

  it('reclaims a stale primary lease and resumes raw sync', async () => {
    const ctx = await createTestApp('indexer');
    await createDogecoinTestNetwork(ctx.runtime);
    await ctx.runtime.metadata.setJsonValue('primary', 'stale-instance-id');

    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);

    const network = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
    const networkId = network?.networkId ?? 0;
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerSyncTail(networkId)),
    ).resolves.toBe(2);

    await ctx.cleanup();
  });

  it('does not start core processing until raw sync reaches the tip window', async () => {
    const ctx = await createTestApp('indexer');
    await createDogecoinTestNetwork(ctx.runtime);
    ctx.runtime.settings.indexer.coreSyncCompleteDistance = 0;
    ctx.runtime.settings.indexer.syncWindow = 1;

    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
    const network = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
    const networkId = network?.networkId ?? 0;

    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerSyncTail(networkId)),
    ).resolves.toBe(0);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail(networkId)),
    ).resolves.toBe(-1);
    await expect(
      ctx.runtime.metadata.getJsonValue<string>(configKeyIndexerStage(networkId)),
    ).resolves.toBe('sync_backfill');

    await ctx.cleanup();
  });

  it('advances through already processed blocks idempotently', async () => {
    const ctx = await createTestApp('indexer');
    await createDogecoinTestNetwork(ctx.runtime);
    const network = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
    const networkId = network?.networkId ?? 0;

    await runUntilProcessed(ctx, networkId, 2);
    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail(networkId)),
    ).resolves.toBe(2);

    await ctx.cleanup();
  });

  it('fails fast when a core block step exceeds the deadline', async () => {
    const values = new Map<string, unknown>();
    const errors: string[] = [];
    const service = new CoreDogecoinIndexerService(
      {
        async compareAndSwapJsonValue(key, expectedValue, nextValue) {
          if ((values.get(key) ?? null) !== expectedValue) {
            return false;
          }
          values.set(key, nextValue);
          return true;
        },
        async deleteByPrefix() {},
        async getJsonValue(key) {
          return (values.get(key) as never) ?? null;
        },
        async setJsonValue(key, value) {
          values.set(key, value);
        },
      },
      {
        async listActiveNetworks() {
          return [
            {
              architecture: 'dogecoin',
              blockTime: 60,
              id: 'net_doge',
              networkId: 7,
              rpcEndpoint: 'https://doge.example/rpc',
              rps: 10,
            },
          ];
        },
      },
      {
        async getPart() {
          return new Promise<null>(() => {});
        },
        async putPart() {},
      },
      {
        async getBlockHeight() {
          return 0;
        },
        async getBlockSnapshot() {
          return {};
        },
      },
      {
        async applyCoreDogecoinBlock() {
          return { applied: true, processTail: 0 };
        },
        async getCoreIndexerState() {
          return {
            lastError: null,
            networkId: 7,
            onlineTip: 0,
            processTail: -1,
            stage: 'process_backfill',
            syncTail: 0,
            updatedAt: new Date().toISOString(),
          };
        },
        async getCoreUtxoOutputs() {
          return new Map();
        },
        async setCoreIndexerError(_networkId, error) {
          errors.push(error ?? '');
        },
        async setCoreIndexerStage() {},
        async upsertCoreBlock() {},
        async upsertCoreIndexerState(input) {
          return {
            lastError: input.lastError ?? null,
            networkId: input.networkId,
            onlineTip: input.onlineTip ?? 0,
            processTail: input.processTail ?? -1,
            stage: input.stage ?? 'process_backfill',
            syncTail: input.syncTail ?? 0,
            updatedAt: new Date().toISOString(),
          };
        },
      },
      {
        ...testIndexerSettings(),
        coreBlockTimeoutMs: 1,
      },
      {
        exitProcess(code): never {
          throw new Error(`exit ${code}`);
        },
      },
    );

    await expect(service.runOnce()).rejects.toThrow('exit 1');
    expect(errors.at(-1)).toContain('height=0');
    expect(errors.at(-1)).toContain('active_step=load_raw');
  });
});

async function runUntilProcessed(
  ctx: Awaited<ReturnType<typeof createTestApp>>,
  networkId: number,
  targetTail: number,
): Promise<void> {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    await ctx.runtime.indexingPipeline.runOnce();
    const processTail =
      (await ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail(networkId))) ??
      -1;
    if (processTail >= targetTail) {
      return;
    }
  }

  throw new Error(`core process tail did not reach ${targetTail}`);
}

function testIndexerSettings(): IndexingPipelineSettings {
  return {
    bootstrapTimeoutMs: 60_000,
    coreBlockTimeoutMs: 120_000,
    coreDbStatementTimeoutMs: 30_000,
    coreOnlineTipDistance: 6,
    coreProcessWindow: 128,
    coreProgressWatchdogMs: 180_000,
    coreRawStorageTimeoutMs: 30_000,
    coreSyncCompleteDistance: 6,
    dogecoinTransferMaxEdges: 1024,
    dogecoinTransferMaxInputAddresses: 64,
    factTimeoutMs: 300_000,
    factWindow: 64,
    leaseHeartbeatIntervalMs: 5_000,
    networkConcurrency: 2,
    projectTargetMs: 30_000,
    projectTimeoutMs: 120_000,
    projectWindow: 4,
    projectWindowMax: 16,
    projectWindowMin: 2,
    relinkBacklogThreshold: 256,
    relinkBatchSize: 16,
    relinkConcurrency: 2,
    relinkFrontierBatch: 32,
    relinkTipDistance: 512,
    relinkTimeoutMs: 120_000,
    syncBacklogHighWatermark: 2048,
    syncBacklogLowWatermark: 512,
    syncConcurrency: 4,
    syncTargetMs: 15_000,
    syncTimeoutMs: 120_000,
    syncWindow: 32,
    syncWindowMax: 256,
    syncWindowMin: 32,
  };
}
