import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

import {
  configKeyIndexerProcessTail,
  configKeyIndexerStage,
  configKeyIndexerSyncTail,
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
