import { ClickHouseWarehouseAdapter } from '@onlydoge/platform';
import { describe, expect, it, vi } from 'vitest';

describe('clickhouse warehouse adapter', () => {
  it('chunks oversized output-key queries instead of sending one huge request', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query_params }: { query_params: { outputKeys: string[] } }) => ({
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
    }));

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
  });
});
