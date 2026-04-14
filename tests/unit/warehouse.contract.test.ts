import {
  DogecoinBlockProjector,
  SourceLinkProjector,
  type TrackedAddress,
} from '@onlydoge/indexing-pipeline';
import { InMemoryWarehouseAdapter } from '@onlydoge/platform';
import { describe, expect, it } from 'vitest';

import { dogecoinFixture } from '../fixtures/dogecoin';

describe('projection warehouse contract', () => {
  it('materializes balances, direct links, and source links from projected blocks', async () => {
    const warehouse = new InMemoryWarehouseAdapter();
    const blockProjector = new DogecoinBlockProjector(warehouse);
    const sourceLinkProjector = new SourceLinkProjector(warehouse);
    const seed: TrackedAddress = {
      addressId: 101,
      address: dogecoinFixture.sourceAddress,
    };

    for (const block of Object.values(dogecoinFixture.blocksByHeight)) {
      const batch = await blockProjector.project(7, { block });
      await warehouse.applyBlockProjection(batch);
    }
    await sourceLinkProjector.rebuild(7, seed);

    await expect(warehouse.getUtxoOutput(7, 'doge-tx-1:0')).resolves.toMatchObject({
      address: dogecoinFixture.intermediaryAddress,
      spentByTxid: 'doge-tx-2',
    });
    await expect(
      warehouse.getBalancesByAddresses([
        dogecoinFixture.sourceAddress,
        dogecoinFixture.intermediaryAddress,
        dogecoinFixture.targetAddress,
      ]),
    ).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          address: dogecoinFixture.sourceAddress,
          balance: '5900000000',
        }),
        expect.objectContaining({
          address: dogecoinFixture.intermediaryAddress,
          balance: '1400000000',
        }),
        expect.objectContaining({
          address: dogecoinFixture.targetAddress,
          balance: '2500000000',
        }),
      ]),
    );
    await expect(
      warehouse.listDirectLinksFromAddresses(7, [
        dogecoinFixture.sourceAddress,
        dogecoinFixture.intermediaryAddress,
      ]),
    ).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          fromAddress: dogecoinFixture.sourceAddress,
          toAddress: dogecoinFixture.intermediaryAddress,
        }),
        expect.objectContaining({
          fromAddress: dogecoinFixture.intermediaryAddress,
          toAddress: dogecoinFixture.targetAddress,
        }),
      ]),
    );
    await expect(
      warehouse.getDistinctLinksByAddresses([dogecoinFixture.targetAddress]),
    ).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          fromAddress: dogecoinFixture.sourceAddress,
          toAddress: dogecoinFixture.targetAddress,
          transferCount: 2,
        }),
      ]),
    );
    await expect(warehouse.listAppliedBlocks(7)).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          blockHeight: 2,
          blockHash: 'doge-block-2',
        }),
      ]),
    );
    await expect(warehouse.getAppliedBlockByHash(7, 'doge-block-1')).resolves.toEqual(
      expect.objectContaining({
        blockHeight: 1,
        blockHash: 'doge-block-1',
      }),
    );
    await expect(warehouse.getTransactionRef(7, 'doge-tx-2')).resolves.toEqual(
      expect.objectContaining({
        blockHeight: 2,
        txIndex: 0,
      }),
    );
    await expect(warehouse.getAddressSummary(7, dogecoinFixture.targetAddress)).resolves.toEqual(
      expect.objectContaining({
        balance: '2500000000',
        receivedBase: '2500000000',
        sentBase: '0',
        txCount: 1,
        utxoCount: 1,
      }),
    );
    await expect(
      warehouse.listAddressTransactions(7, dogecoinFixture.targetAddress),
    ).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          txid: 'doge-tx-2',
          receivedBase: '2500000000',
          sentBase: '0',
        }),
      ]),
    );
    await expect(warehouse.listAddressUtxos(7, dogecoinFixture.targetAddress)).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          outputKey: 'doge-tx-2:0',
          address: dogecoinFixture.targetAddress,
        }),
      ]),
    );
  });
});
