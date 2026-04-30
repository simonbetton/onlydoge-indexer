import { DogecoinBlockProjector, type ProjectionUtxoOutput } from '@onlydoge/indexing-pipeline';
import { InMemoryWarehouseAdapter } from '@onlydoge/platform';
import { describe, expect, it } from 'vitest';

describe('dogecoin block projector', () => {
  it('resolves same-block spends and emits deterministic transfer legs', async () => {
    const projector = createProjector();

    const batch = await projector.project(1, {
      block: {
        hash: 'block-10',
        height: 10,
        time: 1_700_000_000,
        tx: [
          {
            txid: 'tx-coinbase',
            vin: [{ coinbase: 'coinbase' }],
            vout: [dogecoinVout(0, '50.00000000', sourceAddress)],
          },
          {
            txid: 'tx-spend',
            vin: [{ txid: 'tx-coinbase', vout: 0 }],
            vout: [
              dogecoinVout(0, '30.00000000', targetAddress),
              dogecoinVout(1, '19.00000000', sourceAddress),
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
          address: sourceAddress,
        }),
        expect.objectContaining({
          movementId: 'tx-spend:vout:0',
          direction: 'credit',
          amountBase: '3000000000',
          address: targetAddress,
        }),
      ]),
    );
    expect(batch.transfers).toEqual([
      expect.objectContaining({
        fromAddress: sourceAddress,
        toAddress: targetAddress,
        amountBase: '3000000000',
        isChange: false,
      }),
      expect.objectContaining({
        fromAddress: sourceAddress,
        toAddress: sourceAddress,
        amountBase: '1900000000',
        isChange: true,
      }),
    ]);
    expect(batch.directLinkDeltas).toEqual([
      expect.objectContaining({
        fromAddress: sourceAddress,
        toAddress: targetAddress,
        totalAmountBase: '3000000000',
        transferCount: 1,
      }),
    ]);
  });

  it('treats a later spend in persisted state as unspent for historical replay', async () => {
    const batch = await projectStandardSpend({
      persistedOutput: persistedPrevOutput({
        spentByTxid: 'tx-later',
        spentInBlock: 25,
        spentInputIndex: 0,
      }),
    });

    expect(batch.utxoSpends).toEqual([
      expect.objectContaining({
        outputKey: 'tx-prev:1',
        spentByTxid: 'tx-spend',
        spentInBlock: 10,
      }),
    ]);
    expect(batch.transfers).toEqual([
      expect.objectContaining({
        fromAddress: sourceAddress,
        toAddress: targetAddress,
        amountBase: '3000000000',
      }),
    ]);
  });

  it('treats the same persisted spend as idempotent during replay', async () => {
    const batch = await projectStandardSpend({
      persistedOutput: persistedPrevOutput({
        spentByTxid: 'tx-spend',
        spentInBlock: 10,
        spentInputIndex: 0,
      }),
    });

    expect(batch.utxoSpends).toEqual([
      expect.objectContaining({
        outputKey: 'tx-prev:1',
        spentByTxid: 'tx-spend',
        spentInBlock: 10,
        spentInputIndex: 0,
      }),
    ]);
    expect(batch.transfers).toEqual([
      expect.objectContaining({
        fromAddress: sourceAddress,
        toAddress: targetAddress,
        amountBase: '3000000000',
      }),
    ]);
  });

  it('can derive state without transfer and link facts', async () => {
    const batch = await projectStandardSpend({
      options: {
        includeTransfers: false,
        includeDirectLinkDeltas: false,
      },
    });

    expect(batch.utxoSpends).toEqual([
      expect.objectContaining({
        outputKey: 'tx-prev:1',
        spentByTxid: 'tx-spend',
      }),
    ]);
    expect(batch.addressMovements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          direction: 'debit',
          address: sourceAddress,
        }),
        expect.objectContaining({
          direction: 'credit',
          address: targetAddress,
        }),
      ]),
    );
    expect(batch.transfers).toEqual([]);
    expect(batch.directLinkDeltas).toEqual([]);
  });

  it('skips pathological transfer derivation when guardrails are exceeded', async () => {
    const projector = createProjector();

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
              vin: [
                { txid: 'tx-prev-a', vout: 0 },
                { txid: 'tx-prev-b', vout: 0 },
              ],
              vout: [
                dogecoinVout(0, '20.00000000', 'DCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC'),
                dogecoinVout(1, '20.00000000', 'DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD'),
              ],
            },
          ],
        },
      },
      {
        persistedOutputs: new Map([
          [
            'tx-prev-a:0',
            persistedPrevOutput({
              txid: 'tx-prev-a',
              vout: 0,
              outputKey: 'tx-prev-a:0',
              valueBase: '2000000000',
            }),
          ],
          [
            'tx-prev-b:0',
            persistedPrevOutput({
              txid: 'tx-prev-b',
              txIndex: 1,
              vout: 0,
              outputKey: 'tx-prev-b:0',
              address: targetAddress,
              valueBase: '2000000000',
            }),
          ],
        ]),
      },
      {
        maxTransferEdges: 1,
      },
    );

    expect(batch.addressMovements).toHaveLength(4);
    expect(batch.transfers).toEqual([]);
    expect(batch.directLinkDeltas).toEqual([]);
  });
});

const sourceAddress = 'DAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
const targetAddress = 'DBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB';

type ProjectOptions = Parameters<DogecoinBlockProjector['project']>[3];

function createProjector(): DogecoinBlockProjector {
  return new DogecoinBlockProjector(new InMemoryWarehouseAdapter());
}

async function projectStandardSpend(
  input: { options?: ProjectOptions; persistedOutput?: ProjectionUtxoOutput } = {},
) {
  const persistedOutput = input.persistedOutput ?? persistedPrevOutput();
  return createProjector().project(
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
            vout: [dogecoinVout(0, '30.00000000', targetAddress)],
          },
        ],
      },
    },
    {
      persistedOutputs: new Map([[persistedOutput.outputKey, persistedOutput]]),
    },
    input.options,
  );
}

function dogecoinVout(n: number, value: string, address: string) {
  return {
    n,
    value,
    scriptPubKey: {
      type: 'pubkeyhash',
      addresses: [address],
    },
  };
}

function persistedPrevOutput(overrides: Partial<ProjectionUtxoOutput> = {}): ProjectionUtxoOutput {
  return {
    networkId: 1,
    blockHeight: 5,
    blockHash: 'block-5',
    blockTime: 1_699_999_000,
    txid: 'tx-prev',
    txIndex: 0,
    vout: 1,
    outputKey: 'tx-prev:1',
    address: sourceAddress,
    scriptType: 'pubkeyhash',
    valueBase: '3000000000',
    isCoinbase: false,
    isSpendable: true,
    spentByTxid: null,
    spentInBlock: null,
    spentInputIndex: null,
    ...overrides,
  };
}
