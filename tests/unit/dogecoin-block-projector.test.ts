import { DogecoinBlockProjector } from '@onlydoge/indexing-pipeline';
import { InMemoryWarehouseAdapter } from '@onlydoge/platform';
import { describe, expect, it } from 'vitest';

describe('dogecoin block projector', () => {
  it('resolves same-block spends and emits deterministic transfer legs', async () => {
    const warehouse = new InMemoryWarehouseAdapter();
    const projector = new DogecoinBlockProjector(warehouse);

    const batch = await projector.project(1, {
      block: {
        hash: 'block-10',
        height: 10,
        time: 1_700_000_000,
        tx: [
          {
            txid: 'tx-coinbase',
            vin: [{ coinbase: 'coinbase' }],
            vout: [
              {
                n: 0,
                value: '50.00000000',
                scriptPubKey: {
                  type: 'pubkeyhash',
                  addresses: ['DAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
                },
              },
            ],
          },
          {
            txid: 'tx-spend',
            vin: [{ txid: 'tx-coinbase', vout: 0 }],
            vout: [
              {
                n: 0,
                value: '30.00000000',
                scriptPubKey: {
                  type: 'pubkeyhash',
                  addresses: ['DBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB'],
                },
              },
              {
                n: 1,
                value: '19.00000000',
                scriptPubKey: {
                  type: 'pubkeyhash',
                  addresses: ['DAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
                },
              },
            ],
          },
        ],
      },
    });

    expect(batch.utxoSpends).toEqual([]);
    expect(batch.utxoCreates[0]).toMatchObject({
      outputKey: 'tx-coinbase:0',
      spentByTxid: 'tx-spend',
      spentInBlock: 10,
      spentInputIndex: 0,
    });
    expect(batch.addressMovements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          movementId: 'tx-spend:vin:0',
          direction: 'debit',
          amountBase: '5000000000',
          address: 'DAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
        }),
        expect.objectContaining({
          movementId: 'tx-spend:vout:0',
          direction: 'credit',
          amountBase: '3000000000',
          address: 'DBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB',
        }),
      ]),
    );
    expect(batch.transfers).toEqual([
      expect.objectContaining({
        fromAddress: 'DAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
        toAddress: 'DBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB',
        amountBase: '3000000000',
        isChange: false,
      }),
      expect.objectContaining({
        fromAddress: 'DAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
        toAddress: 'DAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
        amountBase: '1900000000',
        isChange: true,
      }),
    ]);
    expect(batch.directLinkDeltas).toEqual([
      expect.objectContaining({
        fromAddress: 'DAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
        toAddress: 'DBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB',
        totalAmountBase: '3000000000',
        transferCount: 1,
      }),
    ]);
  });

  it('treats a later spend in persisted state as unspent for historical replay', async () => {
    const warehouse = new InMemoryWarehouseAdapter();
    const projector = new DogecoinBlockProjector(warehouse);

    const batch = await projector.project(
      1,
      {
        block: {
          hash: 'block-10',
          height: 10,
          time: 1_700_000_000,
          tx: [
            {
              txid: 'tx-spend',
              vin: [{ txid: 'tx-prev', vout: 1 }],
              vout: [
                {
                  n: 0,
                  value: '30.00000000',
                  scriptPubKey: {
                    type: 'pubkeyhash',
                    addresses: ['DBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB'],
                  },
                },
              ],
            },
          ],
        },
      },
      {
        persistedOutputs: new Map([
          [
            'tx-prev:1',
            {
              networkId: 1,
              blockHeight: 5,
              blockHash: 'block-5',
              blockTime: 1_699_999_000,
              txid: 'tx-prev',
              txIndex: 0,
              vout: 1,
              outputKey: 'tx-prev:1',
              address: 'DAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
              scriptType: 'pubkeyhash',
              valueBase: '3000000000',
              isCoinbase: false,
              isSpendable: true,
              spentByTxid: 'tx-later',
              spentInBlock: 25,
              spentInputIndex: 0,
            },
          ],
        ]),
      },
    );

    expect(batch.utxoSpends).toEqual([
      expect.objectContaining({
        outputKey: 'tx-prev:1',
        spentByTxid: 'tx-spend',
        spentInBlock: 10,
      }),
    ]);
    expect(batch.transfers).toEqual([
      expect.objectContaining({
        fromAddress: 'DAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
        toAddress: 'DBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB',
        amountBase: '3000000000',
      }),
    ]);
  });
});
