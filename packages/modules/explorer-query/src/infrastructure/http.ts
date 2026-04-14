import { Elysia, t } from 'elysia';

import type { ExplorerQueryService } from '../application/explorer-query-service';

function parsePagination(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }

  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

export function buildExplorerQueryHttp(service: ExplorerQueryService) {
  return new Elysia({ prefix: '/v1/explorer' })
    .get('/networks', () => service.listNetworks(), {
      detail: {
        description: 'Lists public Dogecoin explorer networks and their indexing status.',
      },
    })
    .get('/search', ({ query }) => service.search(query.q, query.network), {
      detail: {
        description:
          'Searches the indexed Dogecoin explorer by block height, block hash, txid, or address.',
      },
      query: t.Object({
        q: t.Optional(t.String()),
        network: t.Optional(t.String()),
      }),
    })
    .get(
      '/blocks',
      ({ query }) =>
        service.listBlocks(
          query.network,
          parsePagination(query.offset),
          parsePagination(query.limit),
        ),
      {
        detail: {
          description: 'Lists recent indexed Dogecoin blocks.',
        },
        query: t.Object({
          network: t.Optional(t.String()),
          offset: t.Optional(t.String()),
          limit: t.Optional(t.String()),
        }),
      },
    )
    .get('/blocks/:ref', ({ params, query }) => service.getBlock(params.ref, query.network), {
      detail: {
        description: 'Returns a block by indexed height or block hash.',
      },
      params: t.Object({
        ref: t.String(),
      }),
      query: t.Object({
        network: t.Optional(t.String()),
      }),
    })
    .get(
      '/transactions/:txid',
      ({ params, query }) => service.getTransaction(params.txid, query.network),
      {
        detail: {
          description: 'Returns a Dogecoin transaction with inputs, outputs, and label overlays.',
        },
        params: t.Object({
          txid: t.String(),
        }),
        query: t.Object({
          network: t.Optional(t.String()),
        }),
      },
    )
    .get(
      '/addresses/:address',
      ({ params, query }) => service.getAddress(params.address, query.network),
      {
        detail: {
          description: 'Returns an address summary with public investigation overlay data.',
        },
        params: t.Object({
          address: t.String(),
        }),
        query: t.Object({
          network: t.Optional(t.String()),
        }),
      },
    )
    .get(
      '/addresses/:address/transactions',
      ({ params, query }) =>
        service.listAddressTransactions(
          params.address,
          query.network,
          parsePagination(query.offset),
          parsePagination(query.limit),
        ),
      {
        detail: {
          description: 'Returns reverse-chronological transaction history for a Dogecoin address.',
        },
        params: t.Object({
          address: t.String(),
        }),
        query: t.Object({
          network: t.Optional(t.String()),
          offset: t.Optional(t.String()),
          limit: t.Optional(t.String()),
        }),
      },
    )
    .get(
      '/addresses/:address/utxos',
      ({ params, query }) =>
        service.listAddressUtxos(
          params.address,
          query.network,
          parsePagination(query.offset),
          parsePagination(query.limit),
        ),
      {
        detail: {
          description: 'Returns current spendable UTXOs for a Dogecoin address.',
        },
        params: t.Object({
          address: t.String(),
        }),
        query: t.Object({
          network: t.Optional(t.String()),
          offset: t.Optional(t.String()),
          limit: t.Optional(t.String()),
        }),
      },
    );
}
