import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { buildApiApp } from '@onlydoge/api';
import { createRuntime } from '@onlydoge/platform';
import { vi } from 'vitest';

import { dogecoinBlocksByHash, dogecoinFixture, dogecoinHashesByHeight } from './fixtures/dogecoin';

type EnvKey = 'ONLYDOGE_DATABASE' | 'ONLYDOGE_STORAGE' | 'ONLYDOGE_WAREHOUSE' | 'ONLYDOGE_MODE';

const ENV_KEYS: readonly EnvKey[] = [
  'ONLYDOGE_DATABASE',
  'ONLYDOGE_STORAGE',
  'ONLYDOGE_WAREHOUSE',
  'ONLYDOGE_MODE',
];

export async function createTestApp(mode: 'both' | 'http' | 'indexer' = 'both') {
  const tempRoot = await mkdtemp(join(tmpdir(), 'onlydoge-'));
  const previousEnv = new Map<string, string | undefined>();

  for (const key of ENV_KEYS) {
    previousEnv.set(key, process.env[key]);
  }

  process.env.ONLYDOGE_DATABASE = `sqlite://${tempRoot}/onlydoge.sqlite.db`;
  process.env.ONLYDOGE_STORAGE = `file://${tempRoot}/storage`;
  process.env.ONLYDOGE_WAREHOUSE = `${tempRoot}/warehouse.json`;
  process.env.ONLYDOGE_MODE = mode;

  const runtime = await createRuntime({ mode, ip: '127.0.0.1', port: 2277 });
  const app = buildApiApp(runtime);

  return {
    app,
    runtime,
    tempRoot,
    async cleanup() {
      for (const [key, value] of previousEnv.entries()) {
        if (value === undefined) {
          delete process.env[key];
        } else {
          process.env[key] = value;
        }
      }

      await rm(tempRoot, { recursive: true, force: true });
    },
  };
}

export async function request(
  app: ReturnType<typeof buildApiApp>,
  path: string,
  init?: {
    body?: unknown;
    headers?: Record<string, string>;
    method?: string;
  },
) {
  const response = await app.handle(
    new Request(`http://localhost${path}`, {
      method: init?.method ?? 'GET',
      headers: {
        ...(init?.body ? { 'content-type': 'application/json' } : {}),
        ...(init?.headers ?? {}),
      },
      ...(init?.body ? { body: JSON.stringify(init.body) } : {}),
    }),
  );

  return response;
}

export function installRpcMock() {
  return vi
    .spyOn(globalThis, 'fetch')
    .mockImplementation(async (_input: RequestInfo | URL, init?: RequestInit) => {
      const body = parseRpcRequestBody(String(init?.body ?? '{}'));

      switch (body.method) {
        case 'getblockcount':
          return Response.json({ result: dogecoinFixture.latestBlockHeight, error: null });
        case 'getblockhash':
          return Response.json({
            result: dogecoinHashesByHeight.get(Number(body.params?.[0] ?? -1)) ?? null,
            error: null,
          });
        case 'getblock':
          return Response.json({
            result: dogecoinBlocksByHash.get(String(body.params?.[0] ?? '')) ?? null,
            error: null,
          });
        case 'eth_blockNumber':
          return Response.json({ result: '0x3' });
        case 'eth_getBlockByNumber':
          return Response.json({
            result: {
              number: '0x1',
              hash: '0xabc',
              timestamp: '0x1',
              transactions: [],
            },
          });
        case 'eth_getTransactionReceipt':
          return Response.json({
            result: {
              transactionHash: String(body.params?.[0] ?? '0x0'),
              logs: [],
            },
          });
        default:
          return Response.json({ result: 1, error: null });
      }
    });
}

function parseRpcRequestBody(value: string): { method?: string; params?: unknown[] } {
  const parsed = JSON.parse(value);
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return {};
  }

  const method = typeof parsed.method === 'string' ? parsed.method : undefined;
  const params = Array.isArray(parsed.params) ? [...parsed.params] : undefined;

  return {
    ...(method ? { method } : {}),
    ...(params ? { params } : {}),
  };
}
