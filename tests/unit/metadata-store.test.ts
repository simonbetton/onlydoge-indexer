import type {
  BlockProjectionBatch,
  ProjectionDirectLinkBatch,
  ProjectionStateStorePort,
} from '@onlydoge/indexing-pipeline';
import { describe, expect, it, vi } from 'vitest';

import { createTestApp } from '../helpers';

describe('relational metadata store', () => {
  it('rolls back projection state writes when a bulk upsert fails', async () => {
    const ctx = await createTestApp('indexer');
    const store = ctx.runtime.metadata as unknown as ProjectionStateStorePort & {
      upsertProjectionBalances: (
        balances: unknown[],
        timestamp: string,
        executor: unknown,
      ) => Promise<void>;
    };

    const original = store.upsertProjectionBalances.bind(store);
    store.upsertProjectionBalances = vi.fn(async () => {
      throw new Error('boom');
    });

    const batch: BlockProjectionBatch = {
      networkId: 7,
      blockHeight: 1,
      blockHash: 'block-1',
      blockTime: 1_700_000_001,
      utxoCreates: [
        {
          networkId: 7,
          blockHeight: 1,
          blockHash: 'block-1',
          blockTime: 1_700_000_001,
          txid: 'tx-1',
          txIndex: 0,
          vout: 0,
          outputKey: 'tx-1:0',
          address: 'DTestAddress123',
          scriptType: 'pubkeyhash',
          valueBase: '100000000',
          isCoinbase: false,
          isSpendable: true,
          spentByTxid: null,
          spentInBlock: null,
          spentInputIndex: null,
        },
      ],
      utxoSpends: [],
      addressMovements: [
        {
          movementId: 'tx-1:vout:0',
          networkId: 7,
          blockHeight: 1,
          blockHash: 'block-1',
          blockTime: 1_700_000_001,
          txid: 'tx-1',
          txIndex: 0,
          entryIndex: 0,
          address: 'DTestAddress123',
          assetAddress: '',
          direction: 'credit',
          amountBase: '100000000',
          outputKey: 'tx-1:0',
          derivationMethod: 'test',
        },
      ],
      transfers: [],
      directLinkDeltas: [],
    };

    await expect(store.applyProjectionWindow([batch])).rejects.toThrow('boom');
    await expect(store.getUtxoOutputs(7, ['tx-1:0'])).resolves.toEqual(new Map());
    await expect(store.hasAppliedBlock(7, 1, 'block-1')).resolves.toBe(false);

    store.upsertProjectionBalances = original;
    await ctx.cleanup();
  });

  it('applies delayed direct-link batches idempotently', async () => {
    const ctx = await createTestApp('indexer');
    const store = ctx.runtime.metadata as unknown as ProjectionStateStorePort;

    const batch: ProjectionDirectLinkBatch = {
      networkId: 7,
      blockHeight: 10,
      blockHash: 'block-10',
      directLinkDeltas: [
        {
          networkId: 7,
          fromAddress: 'DFromAddress123',
          toAddress: 'DToAddress456',
          assetAddress: '',
          transferCount: 1,
          totalAmountBase: '2500000000',
          firstSeenBlockHeight: 10,
          lastSeenBlockHeight: 10,
        },
      ],
    };

    await store.applyDirectLinkDeltasWindow([batch]);
    await store.applyDirectLinkDeltasWindow([batch]);

    const snapshots = await store.getDirectLinkSnapshots(7, [
      {
        fromAddress: 'DFromAddress123',
        toAddress: 'DToAddress456',
        assetAddress: '',
      },
    ]);

    expect(snapshots.get('DFromAddress123:DToAddress456:')).toMatchObject({
      transferCount: 1,
      totalAmountBase: '2500000000',
      lastSeenBlockHeight: 10,
    });

    await ctx.cleanup();
  });
});
