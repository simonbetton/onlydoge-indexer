import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

import {
  configKeyIndexerProcessTail,
  configKeyProjectionBootstrapCursorBalance,
  configKeyProjectionBootstrapCursorUtxo,
  configKeyProjectionBootstrapPhase,
  configKeyProjectionBootstrapStartedAt,
  configKeyProjectionBootstrapTail,
  configKeyProjectionBootstrapTargetTail,
} from '@onlydoge/indexing-pipeline';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { dogecoinFixture } from '../fixtures/dogecoin';
import {
  createDogecoinAddressBook,
  createDogecoinTestNetwork,
  createTestApp,
  expectDogecoinBalances,
  installRpcMock,
  readObjectArray,
  requireNumber,
  requireObject,
  requireString,
} from '../helpers';

describe('indexer integration', () => {
  let restoreFetch: ReturnType<typeof installRpcMock>;

  beforeEach(() => {
    restoreFetch = installRpcMock();
  });

  afterEach(() => {
    restoreFetch.mockRestore();
  });

  it('derives dogecoin balances and source paths from raw snapshots', async () => {
    const ctx = await createTestApp('indexer');

    const network = await createDogecoinTestNetwork(ctx.runtime);

    const { sourceEntity } = await createDogecoinAddressBook(ctx.runtime, network.id);

    await ctx.runtime.indexingPipeline.runOnce();

    const internalNetwork = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
    expect(internalNetwork?.networkId).toBeDefined();

    const blockHeight = await ctx.runtime.metadata.getJsonValue<number>(
      `block_height_n${internalNetwork?.networkId}`,
    );
    const syncTail = await ctx.runtime.metadata.getJsonValue<number>(
      `indexer_sync_tail_n${internalNetwork?.networkId}`,
    );
    const processTail = await ctx.runtime.metadata.getJsonValue<number>(
      `indexer_process_tail_n${internalNetwork?.networkId}`,
    );
    expect(blockHeight).toBe(2);
    expect(syncTail).toBe(2);
    expect(processTail).toBe(2);

    const snapshotPath = join(
      ctx.tempRoot,
      'storage',
      String(internalNetwork?.networkId),
      '0',
      'block.json.gz',
    );
    const snapshot = await readFile(snapshotPath);
    expect(snapshot.byteLength).toBeGreaterThan(0);

    const warehouse = parseWarehouseState(
      await readFile(join(ctx.tempRoot, 'warehouse.json'), 'utf8'),
    );
    expectDogecoinBalances(warehouse.balances);
    expect(warehouse.directLinks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          fromAddress: dogecoinFixture.sourceAddress,
          toAddress: dogecoinFixture.intermediaryAddress,
          transferCount: 1,
        }),
        expect.objectContaining({
          fromAddress: dogecoinFixture.intermediaryAddress,
          toAddress: dogecoinFixture.targetAddress,
          transferCount: 1,
        }),
      ]),
    );

    const sourceAddressRecord = (
      await ctx.runtime.metadata.listAddressesByValues([dogecoinFixture.sourceAddress])
    )[0];
    expect(sourceAddressRecord?.addressId).toBeDefined();
    expect(warehouse.sourceLinks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          sourceAddress: dogecoinFixture.sourceAddress,
          sourceAddressId: sourceAddressRecord?.addressId ?? 0,
          toAddress: dogecoinFixture.targetAddress,
          hopCount: 2,
        }),
      ]),
    );

    const info = await ctx.runtime.investigationQuery.info(dogecoinFixture.targetAddress);
    expect(info.assets).toEqual(
      expect.arrayContaining([
        {
          network: network.id,
          balance: '2500000000',
        },
      ]),
    );
    expect(info.risk.reasons).toContain('source');
    expect(info.sources).toEqual(
      expect.arrayContaining([
        {
          network: network.id,
          entity: sourceEntity.entity.id,
          from: dogecoinFixture.sourceAddress,
          to: dogecoinFixture.targetAddress,
          hops: 2,
        },
      ]),
    );

    await ctx.cleanup();
  });

  it('reclaims a stale primary lease and resumes syncing', async () => {
    const ctx = await createTestApp('indexer');

    await createDogecoinTestNetwork(ctx.runtime);

    await ctx.runtime.metadata.setJsonValue('primary', 'stale-instance-id');

    const didWork = await ctx.runtime.indexingPipeline.runOnce();
    expect(didWork).toBe(true);

    const network = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
    const syncTail = await ctx.runtime.metadata.getJsonValue<number>(
      `indexer_sync_tail_n${network?.networkId}`,
    );
    expect(syncTail).toBe(2);

    await ctx.cleanup();
  });

  it('replays an already-applied dogecoin block when the process tail lags', async () => {
    const ctx = await createTestApp('indexer');

    await createDogecoinTestNetwork(ctx.runtime);

    await ctx.runtime.indexingPipeline.runOnce();
    await ctx.runtime.indexingPipeline.runOnce();

    const network = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
    expect(network?.networkId).toBeDefined();

    await ctx.runtime.metadata.setJsonValue(`indexer_process_tail_n${network?.networkId}`, 0);

    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(`indexer_process_tail_n${network?.networkId}`),
    ).resolves.toBe(2);

    const warehouse = parseWarehouseState(
      await readFile(join(ctx.tempRoot, 'warehouse.json'), 'utf8'),
    );
    expectDogecoinBalances(warehouse.balances, ['source', 'intermediary']);

    await ctx.cleanup();
  });

  it('times out a hung projection phase instead of stalling the indexer loop', async () => {
    const ctx = await createTestApp('indexer');

    await createDogecoinTestNetwork(ctx.runtime);

    const pipeline = ctx.runtime.indexingPipeline as unknown as {
      settings: {
        leaseHeartbeatIntervalMs: number;
        projectTimeoutMs: number;
      };
      warehouse: {
        applyProjectionWindow: (batches: unknown[]) => Promise<void>;
      };
    };
    pipeline.settings.leaseHeartbeatIntervalMs = 5;
    pipeline.settings.projectTimeoutMs = 25;

    const originalApplyProjectionWindow = pipeline.warehouse.applyProjectionWindow.bind(
      pipeline.warehouse,
    );
    pipeline.warehouse.applyProjectionWindow = () => new Promise<void>(() => {});

    const startedAt = Date.now();
    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
    expect(Date.now() - startedAt).toBeLessThan(1000);

    const network = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
    expect(network?.networkId).toBeDefined();
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(`indexer_sync_tail_n${network?.networkId}`),
    ).resolves.toBe(2);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(`indexer_process_tail_n${network?.networkId}`),
    ).resolves.toBeNull();

    pipeline.warehouse.applyProjectionWindow = originalApplyProjectionWindow;

    const secondStartedAt = Date.now();
    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
    expect(Date.now() - secondStartedAt).toBeLessThan(1000);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(`indexer_process_tail_n${network?.networkId}`),
    ).resolves.toBe(2);

    await ctx.cleanup();
  });

  it('advances processed state even when fact persistence times out', async () => {
    const ctx = await createTestApp('indexer');

    const network = await createDogecoinTestNetwork(ctx.runtime);

    const pipeline = ctx.runtime.indexingPipeline as unknown as {
      factWarehouse: {
        applyProjectionFacts: (window: unknown) => Promise<void>;
      };
      settings: {
        factTimeoutMs: number;
      };
    };
    pipeline.settings.factTimeoutMs = 25;

    const originalApplyProjectionFacts = pipeline.factWarehouse.applyProjectionFacts.bind(
      pipeline.factWarehouse,
    );
    pipeline.factWarehouse.applyProjectionFacts = () => new Promise<void>(() => {});

    const internalNetwork = await runIndexerOnceAndGetDogecoinNetwork(ctx);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(
        `indexer_process_tail_n${internalNetwork?.networkId}`,
      ),
    ).resolves.toBe(2);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(`indexer_fact_tail_n${internalNetwork?.networkId}`),
    ).resolves.toBe(-1);

    await expect(
      ctx.runtime.explorerQuery.getAddress(dogecoinFixture.targetAddress, network.id),
    ).resolves.toMatchObject({
      address: {
        balance: '2500000000',
        utxoCount: 1,
      },
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

    pipeline.factWarehouse.applyProjectionFacts = originalApplyProjectionFacts;
    await ctx.cleanup();
  });

  it('bootstraps the fact tail from existing warehouse state', async () => {
    const ctx = await createTestApp('indexer');

    await createDogecoinTestNetwork(ctx.runtime);

    const internalNetwork = await runIndexerOnceAndGetDogecoinNetwork(ctx);

    await ctx.runtime.metadata.deleteByPrefix(`indexer_fact_tail_n${internalNetwork?.networkId}`);
    await ctx.runtime.metadata.deleteByPrefix(
      `indexer_fact_progress_n${internalNetwork?.networkId}`,
    );

    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(false);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(`indexer_fact_tail_n${internalNetwork?.networkId}`),
    ).resolves.toBe(2);

    await ctx.cleanup();
  });

  it('bootstraps metadata state before resuming strict project-state', async () => {
    const ctx = await createTestApp('indexer');

    const networkId = await prepareBootstrapRetryScenario(ctx);

    const pipeline = ctx.runtime.indexingPipeline as unknown as {
      factWarehouse: {
        getUtxoOutputs: (networkId: number, outputKeys: string[]) => Promise<Map<string, unknown>>;
        listCurrentUtxoOutputsPage: (
          networkId: number,
          cursorOutputKey: string | null,
          limit: number,
          context?: {
            abortSignal?: AbortSignal;
            timeoutMs?: number;
          },
        ) => Promise<unknown>;
      };
    };
    const fallbackSpy = vi.spyOn(pipeline.factWarehouse, 'getUtxoOutputs');
    const bootstrapSpy = vi.spyOn(pipeline.factWarehouse, 'listCurrentUtxoOutputsPage');

    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail(networkId)),
    ).resolves.toBe(1);
    await expect(
      ctx.runtime.metadata.getJsonValue<string>(configKeyProjectionBootstrapPhase(networkId)),
    ).resolves.toBe('balances');
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyProjectionBootstrapTail(networkId)),
    ).resolves.toBeNull();
    expect(bootstrapSpy).toHaveBeenCalled();
    expect(fallbackSpy).not.toHaveBeenCalled();

    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyProjectionBootstrapTail(networkId)),
    ).resolves.toSatisfy((value) => typeof value === 'number' && value >= 1);
    await expect(
      ctx.runtime.metadata.listAddressUtxos(networkId, dogecoinFixture.intermediaryAddress),
    ).resolves.toEqual([
      expect.objectContaining({
        txid: 'doge-tx-2',
      }),
    ]);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail(networkId)),
    ).resolves.toBe(1);

    await ctx.cleanup();
  });

  it('retries timed-out bootstrap exports and ignores late completions from stale attempts', async () => {
    const ctx = await createTestApp('indexer');

    const networkId = await prepareBootstrapRetryScenario(ctx);

    const pipeline = ctx.runtime.indexingPipeline as unknown as {
      factWarehouse: {
        listCurrentUtxoOutputsPage: (
          networkId: number,
          cursorOutputKey: string | null,
          limit: number,
          context?: {
            abortSignal?: AbortSignal;
            timeoutMs?: number;
          },
        ) => Promise<{
          nextCursor: string | null;
          rows: Array<Record<string, unknown>>;
        }>;
      };
      settings: {
        bootstrapTimeoutMs: number;
      };
    };
    pipeline.settings.bootstrapTimeoutMs = 25;

    const originalListCurrentUtxoOutputsPage =
      pipeline.factWarehouse.listCurrentUtxoOutputsPage.bind(pipeline.factWarehouse);

    const firstAttempt = {
      resolve: null as
        | ((value: { nextCursor: string | null; rows: Array<Record<string, unknown>> }) => void)
        | null,
    };
    let firstAttemptPending = true;
    const listCurrentUtxoOutputsPage = vi.fn(
      (
        currentNetworkId: number,
        cursorOutputKey: string | null,
        limit: number,
        context?: {
          abortSignal?: AbortSignal;
          timeoutMs?: number;
        },
      ) => {
        if (firstAttemptPending) {
          firstAttemptPending = false;
          return new Promise<{ nextCursor: string | null; rows: Array<Record<string, unknown>> }>(
            (resolve) => {
              firstAttempt.resolve = resolve;
            },
          );
        }

        return originalListCurrentUtxoOutputsPage(
          currentNetworkId,
          cursorOutputKey,
          limit,
          context,
        );
      },
    );
    pipeline.factWarehouse.listCurrentUtxoOutputsPage = listCurrentUtxoOutputsPage;

    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(false);
    expect(listCurrentUtxoOutputsPage).toHaveBeenCalledTimes(1);

    await expect(
      ctx.runtime.metadata.getJsonValue<string>(configKeyProjectionBootstrapCursorUtxo(networkId)),
    ).resolves.toBeNull();

    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
    expect(listCurrentUtxoOutputsPage).toHaveBeenCalledTimes(2);
    await expect(
      ctx.runtime.metadata.getJsonValue<string>(configKeyProjectionBootstrapPhase(networkId)),
    ).resolves.toBe('balances');

    if (firstAttempt.resolve) {
      firstAttempt.resolve({
        nextCursor: 'stale-cursor',
        rows: [
          {
            networkId,
            blockHeight: 2,
            blockHash: 'block-2',
            blockTime: 1_700_000_002,
            txid: 'stale-tx',
            txIndex: 0,
            vout: 0,
            outputKey: 'stale-tx:0',
            address: 'DStaleAddress123',
            scriptType: 'pubkeyhash',
            valueBase: '123',
            isCoinbase: false,
            isSpendable: true,
            spentByTxid: null,
            spentInBlock: null,
            spentInputIndex: null,
          },
        ],
      });
    }
    await new Promise((resolve) => setTimeout(resolve, 0));

    await expect(
      ctx.runtime.metadata.getJsonValue<string>(configKeyProjectionBootstrapCursorUtxo(networkId)),
    ).resolves.toBeNull();
    await expect(ctx.runtime.metadata.getUtxoOutputs(networkId, ['stale-tx:0'])).resolves.toEqual(
      new Map(),
    );

    pipeline.factWarehouse.listCurrentUtxoOutputsPage = originalListCurrentUtxoOutputsPage;
    await ctx.cleanup();
  });
});

function parseWarehouseState(value: string): {
  balances: Array<{ address: string; balance: string }>;
  directLinks: Array<{ fromAddress: string; toAddress: string; transferCount: number }>;
  sourceLinks: Array<{
    hopCount: number;
    sourceAddress: string;
    sourceAddressId: number;
    toAddress: string;
  }>;
} {
  const parsed = requireObject(JSON.parse(value), 'warehouse');
  return {
    balances: readObjectArray(parsed, 'balances').map((item) => ({
      address: requireString(item, 'balances.address'),
      balance: requireString(item, 'balances.balance'),
    })),
    directLinks: readObjectArray(parsed, 'directLinks').map((item) => ({
      fromAddress: requireString(item, 'directLinks.fromAddress'),
      toAddress: requireString(item, 'directLinks.toAddress'),
      transferCount: requireNumber(item, 'directLinks.transferCount'),
    })),
    sourceLinks: readObjectArray(parsed, 'sourceLinks').map((item) => ({
      hopCount: requireNumber(item, 'sourceLinks.hopCount'),
      sourceAddress: requireString(item, 'sourceLinks.sourceAddress'),
      sourceAddressId: requireNumber(item, 'sourceLinks.sourceAddressId'),
      toAddress: requireString(item, 'sourceLinks.toAddress'),
    })),
  };
}

async function clearProjectionBootstrapKeys(
  ctx: Awaited<ReturnType<typeof createTestApp>>,
  networkId: number,
): Promise<void> {
  await Promise.all([
    ctx.runtime.metadata.deleteByPrefix(configKeyProjectionBootstrapTail(networkId)),
    ctx.runtime.metadata.deleteByPrefix(configKeyProjectionBootstrapTargetTail(networkId)),
    ctx.runtime.metadata.deleteByPrefix(configKeyProjectionBootstrapPhase(networkId)),
    ctx.runtime.metadata.deleteByPrefix(configKeyProjectionBootstrapCursorUtxo(networkId)),
    ctx.runtime.metadata.deleteByPrefix(configKeyProjectionBootstrapCursorBalance(networkId)),
    ctx.runtime.metadata.deleteByPrefix(configKeyProjectionBootstrapStartedAt(networkId)),
  ]);
}

async function prepareBootstrapRetryScenario(
  ctx: Awaited<ReturnType<typeof createTestApp>>,
): Promise<number> {
  await createDogecoinTestNetwork(ctx.runtime);
  await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);

  const network = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
  if (network === null) {
    throw new TypeError('missing Dogecoin Mainnet network');
  }

  await ctx.runtime.metadata.clearProjectionBootstrapState(network.networkId);
  await clearProjectionBootstrapKeys(ctx, network.networkId);
  await ctx.runtime.metadata.setJsonValue(configKeyIndexerProcessTail(network.networkId), 1);
  return network.networkId;
}

async function runIndexerOnceAndGetDogecoinNetwork(ctx: Awaited<ReturnType<typeof createTestApp>>) {
  await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
  const network = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
  expect(network?.networkId).toBeDefined();
  return network;
}
