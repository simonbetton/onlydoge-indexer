import { ClickHouseWarehouseAdapter } from '@onlydoge/platform';
import { InfrastructureError } from '@onlydoge/shared-kernel';
import { describe, expect, it, vi } from 'vitest';

describe('clickhouse warehouse adapter', () => {
  it('surfaces warehouse connection failures as infrastructure errors', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async () => {
      const error = new Error('connect ECONNREFUSED 10.124.0.6:8123');
      Object.assign(error, { code: 'ECONNREFUSED' });
      throw error;
    });

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

    await expect(adapter.listAppliedBlocks(7)).rejects.toEqual(
      new InfrastructureError('warehouse unavailable', {
        cause: expect.any(Error),
      }),
    );
  });

  it('surfaces warehouse memory-limit failures as infrastructure errors', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async () => {
      const error = new Error('MEMORY_LIMIT_EXCEEDED: User memory limit exceeded');
      Object.assign(error, { code: '241' });
      throw error;
    });

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

    await expect(adapter.listAppliedBlocks(7)).rejects.toEqual(
      new InfrastructureError('warehouse query exceeded memory limit', {
        cause: expect.any(Error),
      }),
    );
  });

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
          parameters.query.includes('FROM utxo_outputs_current_v2') &&
          parameters.query.includes('LIMIT 1 BY output_key') &&
          !parameters.query.includes('FINAL') &&
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
    ).toContain('LIMIT 1 BY output_key');
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

  it('lists address UTXOs from a deduplicated key page before loading full rows', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM utxo_outputs_current_by_address_v2')) {
        return {
          json: async () => [
            {
              outputKey: 'prev-txid:1',
              blockHeight: 123,
              txIndex: 4,
              vout: 1,
            },
          ],
        };
      }

      if (
        statement.includes('FROM utxo_outputs_current_v2') &&
        statement.includes('AND output_key IN')
      ) {
        return {
          json: async () => [
            {
              networkId: 7,
              blockHeight: 123,
              blockHash: 'prev-hash',
              blockTime: 456,
              txid: 'prev-txid',
              txIndex: 4,
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

      return { json: async () => [] };
    });

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

    const rows = await adapter.listAddressUtxos(7, 'DInputAddress', 0, 50);
    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      outputKey: 'prev-txid:1',
      address: 'DInputAddress',
    });
    expect(
      statements.some(
        (statement) =>
          statement.includes('LIMIT 1 BY output_key') &&
          statement.includes('FROM utxo_outputs_current_by_address_v2') &&
          !statement.includes('FINAL'),
      ),
    ).toBe(true);
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
      query.mock.calls.some(
        ([parameters]) =>
          parameters.query.includes('FROM utxo_outputs_current_v2') &&
          parameters.query.includes('LIMIT 1 BY output_key') &&
          !parameters.query.includes('FINAL'),
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

  it('boots clickhouse with address-oriented read models and backfills them once', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const command = vi.fn(async () => undefined);
    const query = vi.fn(async () => ({ json: async () => [] }));

    (
      adapter as unknown as {
        client: {
          command: typeof command;
          query: typeof query;
        };
      }
    ).client = {
      command,
      query,
    };

    await adapter.boot();

    const statements = command.mock.calls.map(
      (call) => (call as Array<{ query: string }>).at(0)?.query ?? '',
    );

    expect(
      statements.some((statement) =>
        statement.includes('CREATE TABLE IF NOT EXISTS utxo_outputs_current_by_address_v2'),
      ),
    ).toBe(true);
    expect(
      statements.some((statement) =>
        statement.includes(
          'CREATE MATERIALIZED VIEW IF NOT EXISTS utxo_outputs_current_by_address_v2_mv',
        ),
      ),
    ).toBe(true);
    expect(
      statements.some((statement) =>
        statement.includes('CREATE TABLE IF NOT EXISTS address_movements_by_address_v2'),
      ),
    ).toBe(true);
    expect(
      statements.some((statement) =>
        statement.includes(
          'CREATE MATERIALIZED VIEW IF NOT EXISTS address_movements_by_address_v2_mv',
        ),
      ),
    ).toBe(true);
    expect(
      statements.some((statement) =>
        statement.includes('INSERT INTO utxo_outputs_current_by_address_v2'),
      ),
    ).toBe(true);
    expect(
      statements.some((statement) =>
        statement.includes('INSERT INTO address_movements_by_address_v2'),
      ),
    ).toBe(true);
  });

  it('uses address-oriented movement reads with a precomputed integer amount column', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM address_movements_by_address_v2')) {
        return {
          json: async () => [
            {
              receivedBase: '11',
              sentBase: '7',
              txCount: 2,
            },
          ],
        };
      }

      if (statement.includes('FROM balances_v2')) {
        return {
          json: async () => [
            {
              balance: '4',
            },
          ],
        };
      }

      if (statement.includes('FROM utxo_outputs_current_by_address_v2')) {
        return {
          json: async () => [
            {
              utxoCount: 1,
            },
          ],
        };
      }

      return { json: async () => [] };
    });

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

    const summary = await adapter.getAddressSummary(7, 'DInputAddress');
    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(summary).toMatchObject({
      balance: '4',
      receivedBase: '11',
      sentBase: '7',
      txCount: 2,
      utxoCount: 1,
    });
    expect(
      statements.some(
        (statement) =>
          statement.includes('FROM address_movements_by_address_v2') &&
          statement.includes('sumIf(amount_base_i256'),
      ),
    ).toBe(true);
  });
});
