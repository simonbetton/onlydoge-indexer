import { ClickHouseWarehouseAdapter } from '@onlydoge/platform';
import { describe, expect, it, vi } from 'vitest';

describe('clickhouse warehouse adapter', () => {
  it('chunks oversized output-key queries instead of sending one huge request', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(
      async ({
        query: _query,
        query_params,
      }: {
        query: string;
        query_params: { outputKeys: string[] };
      }) => ({
        json: async () =>
          query_params.outputKeys.map((outputKey) => ({
            networkId: 7,
            blockHeight: 1,
            blockHash: 'hash',
            blockTime: 1,
            txid: 'txid',
            txIndex: 0,
            vout: 0,
            outputKey,
            address: 'DTestAddress',
            scriptType: 'pubkeyhash',
            valueBase: '1',
            isCoinbase: false,
            isSpendable: true,
            spentByTxid: null,
            spentInBlock: null,
            spentInputIndex: null,
          })),
      }),
    );

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

    const outputKeys = Array.from(
      { length: 400 },
      (_, index) => `doge-${'x'.repeat(64)}-${index.toString().padStart(4, '0')}`,
    );

    const rows = await adapter.getUtxoOutputs(7, outputKeys);

    expect(rows.size).toBe(outputKeys.length);
    expect(query.mock.calls.length).toBeGreaterThan(1);
    expect(
      query.mock.calls.every(
        ([parameters]) =>
          Array.isArray(parameters.query_params.outputKeys) &&
          parameters.query_params.outputKeys.length < outputKeys.length,
      ),
    ).toBe(true);
    expect(
      query.mock.calls.every(
        ([parameters]) =>
          typeof parameters.query === 'string' &&
          parameters.query.includes('FROM utxo_outputs_current_v2 FINAL') &&
          !parameters.query.includes('LIMIT 1 BY output_key') &&
          !parameters.query.includes('argMax('),
      ),
    ).toBe(true);
  });

  it('uses exact tuple filters for projection state lookups instead of argMax aggregates', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM applied_blocks_v2')) {
        return { json: async () => [] };
      }

      if (statement.includes('FROM utxo_outputs_current_v2')) {
        return {
          json: async () => [
            {
              networkId: 7,
              blockHeight: 1,
              blockHash: 'prev-hash',
              blockTime: 1,
              txid: 'prev-txid',
              txIndex: 0,
              vout: 1,
              outputKey: 'prev-txid:1',
              address: 'DInputAddress',
              scriptType: 'pubkeyhash',
              valueBase: '10',
              isCoinbase: false,
              isSpendable: true,
              spentByTxid: null,
              spentInBlock: null,
              spentInputIndex: null,
            },
          ],
        };
      }

      if (statement.includes('FROM balances_v2')) {
        return {
          json: async () => [
            {
              networkId: 7,
              address: 'DInputAddress',
              assetAddress: 'DOGE',
              balance: '10',
              asOfBlockHeight: 1,
              version: 1,
            },
          ],
        };
      }

      return { json: async () => [] };
    });
    const insert = vi.fn(async () => undefined);

    (adapter as unknown as { client: { insert: typeof insert; query: typeof query } }).client = {
      query,
      insert,
    };

    await adapter.applyProjectionWindow([
      {
        networkId: 7,
        blockHeight: 2,
        blockHash: 'block-hash',
        blockTime: 2,
        utxoCreates: [],
        utxoSpends: [
          {
            outputKey: 'prev-txid:1',
            spentByTxid: 'next-txid',
            spentInBlock: 2,
            spentInputIndex: 0,
          },
        ],
        addressMovements: [
          {
            movementId: 'movement-1',
            networkId: 7,
            blockHeight: 2,
            blockHash: 'block-hash',
            blockTime: 2,
            txid: 'next-txid',
            txIndex: 0,
            entryIndex: 0,
            address: 'DInputAddress',
            assetAddress: 'DOGE',
            direction: 'debit',
            amountBase: '1',
            outputKey: 'prev-txid:1',
            derivationMethod: 'utxo',
          },
          {
            movementId: 'movement-2',
            networkId: 7,
            blockHeight: 2,
            blockHash: 'block-hash',
            blockTime: 2,
            txid: 'next-txid',
            txIndex: 0,
            entryIndex: 1,
            address: 'DOutputAddress',
            assetAddress: 'DOGE',
            direction: 'credit',
            amountBase: '1',
            outputKey: 'next-txid:0',
            derivationMethod: 'utxo',
          },
        ],
        transfers: [],
        directLinkDeltas: [
          {
            networkId: 7,
            fromAddress: 'DInputAddress',
            toAddress: 'DOutputAddress',
            assetAddress: 'DOGE',
            transferCount: 1,
            totalAmountBase: '1',
            firstSeenBlockHeight: 2,
            lastSeenBlockHeight: 2,
          },
        ],
      },
    ]);

    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(
      statements.find((statement) => statement.includes('FROM utxo_outputs_current_v2')),
    ).toContain('FROM utxo_outputs_current_v2 FINAL');
    expect(statements.find((statement) => statement.includes('FROM balances_v2'))).toContain(
      "(address, asset_address) IN (('DInputAddress', 'DOGE'), ('DOutputAddress', 'DOGE'))",
    );
    expect(statements.find((statement) => statement.includes('FROM direct_links_v2'))).toContain(
      "(from_address, to_address, asset_address) IN (('DInputAddress', 'DOutputAddress', 'DOGE'))",
    );
    expect(statements.some((statement) => statement.includes('argMax('))).toBe(false);
    const insertedTables = insert.mock.calls.map(
      (call) => (call as Array<{ table: string }>).at(0)?.table ?? '<missing-table>',
    );

    expect(insertedTables).toContain('utxo_outputs_current_v2');
  });

  it('falls back to the versioned UTXO table when the current-state table misses rows', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM utxo_outputs_current_v2')) {
        return { json: async () => [] };
      }

      if (statement.includes('FROM utxo_outputs_v2')) {
        return {
          json: async () => [
            {
              networkId: 7,
              blockHeight: 123,
              blockHash: 'prev-hash',
              blockTime: 456,
              txid: 'prev-txid',
              txIndex: 0,
              vout: 1,
              outputKey: 'prev-txid:1',
              address: 'DInputAddress',
              scriptType: 'pubkeyhash',
              valueBase: '10',
              isCoinbase: false,
              isSpendable: true,
              spentByTxid: 'next-txid',
              spentInBlock: 124,
              spentInputIndex: 0,
            },
          ],
        };
      }

      return { json: async () => [] };
    });
    const insert = vi.fn(async () => undefined);

    (adapter as unknown as { client: { insert: typeof insert; query: typeof query } }).client = {
      query,
      insert,
    };

    const rows = await adapter.getUtxoOutputs(7, ['prev-txid:1']);

    expect(rows.get('prev-txid:1')).toMatchObject({
      outputKey: 'prev-txid:1',
      spentByTxid: 'next-txid',
      spentInBlock: 124,
    });
    expect(
      query.mock.calls.some(([parameters]) =>
        parameters.query.includes('FROM utxo_outputs_current_v2 FINAL'),
      ),
    ).toBe(true);
    expect(
      query.mock.calls.some(([parameters]) => parameters.query.includes('FROM utxo_outputs_v2')),
    ).toBe(true);
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'utxo_outputs_current_v2',
        values: [
          expect.objectContaining({
            output_key: 'prev-txid:1',
            version: 124,
          }),
        ],
      }),
    );
  });
});
