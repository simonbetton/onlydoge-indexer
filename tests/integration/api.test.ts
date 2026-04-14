import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { dogecoinFixture } from '../fixtures/dogecoin';
import { createTestApp, installRpcMock, request } from '../helpers';

describe('api integration', () => {
  let restoreFetch: ReturnType<typeof installRpcMock>;

  beforeEach(() => {
    restoreFetch = installRpcMock();
  });

  afterEach(() => {
    restoreFetch.mockRestore();
  });

  it('serves heartbeat and openapi', async () => {
    const ctx = await createTestApp();

    const up = await request(ctx.app, '/up');
    expect(up.status).toBe(200);
    expect(up.headers.get('cache-control')).toBe('no-store');

    const heartbeat = await request(ctx.app, '/v1/heartbeat');
    expect(heartbeat.status).toBe(204);
    expect(heartbeat.headers.get('cache-control')).toBe('no-store');

    const openapi = await request(ctx.app, '/openapi/json');
    expect(openapi.status).toBe(200);
    expect(openapi.headers.get('cache-control')).toBe(
      'public, max-age=300, stale-while-revalidate=3600',
    );
    const document = await openapi.json();
    expect(document).toMatchSnapshot();

    const scalar = await request(ctx.app, '/openapi');
    expect(scalar.status).toBe(200);
    expect(scalar.headers.get('content-type')).toContain('text/html');

    await ctx.cleanup();
  });

  it('logs the requested route for missing paths and returns a 404 envelope', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    const ctx = await createTestApp();

    const response = await request(ctx.app, '/missing-route');

    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({
      error: 'not found',
    });
    expect(consoleError).toHaveBeenCalledWith('[onlydoge] not found route=GET /missing-route');

    consoleError.mockRestore();
    await ctx.cleanup();
  });

  it('enforces auth after the first key is created and hides the secret after use', async () => {
    const ctx = await createTestApp();

    const deniedBeforeBootstrap = await request(ctx.app, '/v1/networks');
    expect(deniedBeforeBootstrap.status).toBe(401);

    const deniedBeforeBootstrapWithSlash = await request(ctx.app, '/v1/networks/');
    expect(deniedBeforeBootstrapWithSlash.status).toBe(401);

    const deniedKeyListBeforeBootstrap = await request(ctx.app, '/v1/keys');
    expect(deniedKeyListBeforeBootstrap.status).toBe(401);

    const created = await request(ctx.app, '/v1/keys/', {
      method: 'POST',
      body: {},
    });
    expect(created.status).toBe(200);
    const key = await readJsonObject(created);
    const keyId = requireStringField(key, 'id');
    const apiToken = requireStringField(key, 'key');
    expect(apiToken).toMatch(/^sk_/u);

    const denied = await request(ctx.app, '/v1/networks');
    expect(denied.status).toBe(401);

    const deniedWithSlash = await request(ctx.app, '/v1/networks/');
    expect(deniedWithSlash.status).toBe(401);

    const deniedInfo = await request(ctx.app, '/v1/info?q=test');
    expect(deniedInfo.status).toBe(401);

    const heartbeat = await request(ctx.app, '/v1/heartbeat');
    expect(heartbeat.status).toBe(204);

    const allowed = await request(ctx.app, '/v1/networks', {
      headers: {
        'x-api-token': apiToken,
      },
    });
    expect(allowed.status).toBe(200);

    const fetched = await request(ctx.app, `/v1/keys/${keyId}`, {
      headers: {
        'x-api-token': apiToken,
      },
    });
    const fetchedBody = await readJsonObject(fetched);
    const fetchedKey = readObjectField(fetchedBody, 'key');
    expect(requireStringField(fetchedKey, 'id')).toBe(keyId);
    expect(fetchedKey.key).toBeUndefined();

    await ctx.cleanup();
  });

  it('creates networks, tags, entities, addresses, and resolves info queries', async () => {
    const ctx = await createTestApp();
    const created = await request(ctx.app, '/v1/keys/', {
      method: 'POST',
      body: {},
    });
    const key = await readJsonObject(created);
    const apiToken = requireStringField(key, 'key');
    const headers = {
      'x-api-token': apiToken,
    };

    const networkResponse = await request(ctx.app, '/v1/networks', {
      method: 'POST',
      headers,
      body: {
        name: 'Dogecoin Mainnet',
        architecture: 'dogecoin',
        chainId: 0,
        blockTime: 60,
        rpcEndpoint: 'https://user:pass@doge.example/rpc',
      },
    });
    const network = await readJsonObject(networkResponse);
    const networkId = requireStringField(network, 'id');
    expect(requireStringField(network, 'rpcEndpoint')).toContain('***');

    const tagResponse = await request(ctx.app, '/v1/tags', {
      method: 'POST',
      headers,
      body: {
        name: 'Sanctions',
        riskLevel: 'high',
      },
    });
    const tag = await readJsonObject(tagResponse);
    const tagId = requireStringField(tag, 'id');

    const entityResponse = await request(ctx.app, '/v1/entities', {
      method: 'POST',
      headers,
      body: {
        name: 'Example Entity',
        description: 'Tracked counterparty',
        tags: [tagId],
      },
    });
    const entityPayload = await readJsonObject(entityResponse);
    const entity = readObjectField(entityPayload, 'entity');
    const entityId = requireStringField(entity, 'id');

    const addressResponse = await request(ctx.app, '/v1/addresses', {
      method: 'POST',
      headers,
      body: {
        entity: entityId,
        network: networkId,
        addresses: [
          {
            address: 'DTestAddress123',
            description: 'Main wallet',
          },
        ],
      },
    });
    expect(addressResponse.status).toBe(200);

    const fetchedEntity = await request(ctx.app, `/v1/entities/${entityId}`, {
      headers,
    });
    const fetchedEntityBody = await readJsonObject(fetchedEntity);
    const fetchedEntityRecord = readObjectField(fetchedEntityBody, 'entity');
    expect(readStringArrayField(fetchedEntityRecord, 'tags')).toEqual([tagId]);
    expect(readStringArrayField(fetchedEntityRecord, 'addresses')).toHaveLength(1);

    const infoResponse = await request(ctx.app, '/v1/info?q=DTestAddress123', {
      headers,
    });
    expect(infoResponse.headers.get('cache-control')).toBe(
      'private, max-age=15, stale-while-revalidate=30',
    );
    expect(infoResponse.headers.get('vary')).toBe('x-api-token');
    const info = await readJsonObject(infoResponse);
    expect(readStringArrayField(info, 'addresses')).toEqual(['DTestAddress123']);
    const [infoEntity] = readObjectArrayField(info, 'entities');
    const [infoTag] = readObjectArrayField(info, 'tags');
    const risk = readObjectField(info, 'risk');
    if (!infoEntity || !infoTag) {
      throw new TypeError('missing info relationship records');
    }
    expect(requireStringField(infoEntity, 'id')).toBe(entityId);
    expect(requireStringField(infoTag, 'id')).toBe(tagId);
    expect(requireStringField(risk, 'level')).toBe('high');
    expect(readStringArrayField(risk, 'reasons')).toContain('entity');

    await ctx.cleanup();
  });

  it('returns short-lived private cache headers for authenticated collection reads', async () => {
    const ctx = await createTestApp();
    const created = await request(ctx.app, '/v1/keys/', {
      method: 'POST',
      body: {},
    });
    const key = await readJsonObject(created);
    const apiToken = requireStringField(key, 'key');

    const response = await request(ctx.app, '/v1/networks', {
      headers: {
        'x-api-token': apiToken,
      },
    });

    expect(response.status).toBe(200);
    expect(response.headers.get('cache-control')).toBe(
      'private, max-age=30, stale-while-revalidate=60',
    );
    expect(response.headers.get('vary')).toBe('x-api-token');

    await ctx.cleanup();
  });

  it('serves public explorer endpoints from indexed dogecoin data', async () => {
    const ctx = await createTestApp();
    const network = await ctx.runtime.networkCatalog.createNetwork({
      name: 'Dogecoin Mainnet',
      architecture: 'dogecoin',
      chainId: 0,
      blockTime: 60,
      rpcEndpoint: 'https://doge.example/rpc',
    });
    const highRiskTag = await ctx.runtime.entityLabeling.createTag({
      name: 'High Risk Source',
      riskLevel: 'high',
    });
    const sourceEntity = await ctx.runtime.entityLabeling.createEntity({
      name: 'Source Entity',
      description: 'Known source',
      tags: [highRiskTag.id],
    });
    const targetEntity = await ctx.runtime.entityLabeling.createEntity({
      name: 'Target Entity',
      description: 'Known target',
    });
    await ctx.runtime.entityLabeling.createAddresses({
      entity: sourceEntity.entity.id,
      network: network.id,
      addresses: [
        {
          address: dogecoinFixture.sourceAddress,
          description: 'Source wallet',
        },
      ],
    });
    const [targetAddressRecord] = await ctx.runtime.entityLabeling.createAddresses({
      entity: targetEntity.entity.id,
      network: network.id,
      addresses: [
        {
          address: dogecoinFixture.targetAddress,
          description: 'Target wallet',
        },
      ],
    });

    expect(targetAddressRecord?.address).toBe(dogecoinFixture.targetAddress);

    await ctx.runtime.indexingPipeline.runOnce();

    const networks = await request(ctx.app, '/v1/explorer/networks');
    expect(networks.status).toBe(200);
    expect(networks.headers.get('cache-control')).toBe(
      'public, max-age=30, stale-while-revalidate=120',
    );
    const networksBody = await readJsonObject(networks);
    const [networkSummary] = readObjectArrayField(networksBody, 'networks');
    expect(requireStringField(networkSummary ?? {}, 'id')).toBe(network.id);

    const searchByHeight = await request(ctx.app, '/v1/explorer/search?q=2');
    expect(searchByHeight.status).toBe(200);
    expect(searchByHeight.headers.get('cache-control')).toBe(
      'public, max-age=5, stale-while-revalidate=15',
    );
    const heightMatch = readObjectArrayField(await readJsonObject(searchByHeight), 'matches')[0];
    expect(requireStringField(heightMatch ?? {}, 'type')).toBe('block');

    const searchByTx = await request(ctx.app, '/v1/explorer/search?q=doge-tx-2');
    const txMatch = readObjectArrayField(await readJsonObject(searchByTx), 'matches')[0];
    expect(requireStringField(txMatch ?? {}, 'txid')).toBe('doge-tx-2');

    const searchByAddress = await request(
      ctx.app,
      `/v1/explorer/search?q=${dogecoinFixture.targetAddress}`,
    );
    const addressMatch = readObjectArrayField(await readJsonObject(searchByAddress), 'matches')[0];
    expect(requireStringField(addressMatch ?? {}, 'address')).toBe(dogecoinFixture.targetAddress);

    const blocks = await request(ctx.app, '/v1/explorer/blocks');
    const [block] = readObjectArrayField(await readJsonObject(blocks), 'blocks');
    expect(requireNumberField(block ?? {}, 'height')).toBe(2);

    const blockDetail = await request(ctx.app, '/v1/explorer/blocks/2');
    const blockDetailBody = await readJsonObject(blockDetail);
    expect(requireNumberField(readObjectField(blockDetailBody, 'block'), 'height')).toBe(2);
    const [blockTx] = readObjectArrayField(blockDetailBody, 'transactions');
    expect(requireStringField(blockTx ?? {}, 'txid')).toBe('doge-tx-2');

    const transaction = await request(ctx.app, '/v1/explorer/transactions/doge-tx-2');
    const transactionBody = await readJsonObject(transaction);
    expect(requireStringField(readObjectField(transactionBody, 'transaction'), 'txid')).toBe(
      'doge-tx-2',
    );
    const [transactionInput] = readObjectArrayField(transactionBody, 'inputs');
    const [transactionOutput] = readObjectArrayField(transactionBody, 'outputs');
    expect(requireStringField(transactionInput ?? {}, 'address')).toBe(
      dogecoinFixture.intermediaryAddress,
    );
    expect(requireStringField(transactionOutput ?? {}, 'address')).toBe(
      dogecoinFixture.targetAddress,
    );

    const address = await request(
      ctx.app,
      `/v1/explorer/addresses/${dogecoinFixture.targetAddress}`,
    );
    expect(address.status).toBe(200);
    expect(address.headers.get('cache-control')).toBe(
      'public, max-age=15, stale-while-revalidate=60',
    );
    const addressBody = await readJsonObject(address);
    const addressSummary = readObjectField(addressBody, 'address');
    expect(requireStringField(addressSummary, 'balance')).toBe('2500000000');
    const overlay = readObjectField(addressBody, 'overlay');
    const risk = readObjectField(overlay, 'risk');
    expect(readStringArrayField(risk, 'reasons')).toContain('source');

    const history = await request(
      ctx.app,
      `/v1/explorer/addresses/${dogecoinFixture.targetAddress}/transactions`,
    );
    const [historyItem] = readObjectArrayField(await readJsonObject(history), 'transactions');
    expect(requireStringField(readObjectField(historyItem ?? {}, 'transaction'), 'txid')).toBe(
      'doge-tx-2',
    );

    const utxos = await request(
      ctx.app,
      `/v1/explorer/addresses/${dogecoinFixture.targetAddress}/utxos`,
    );
    const [utxo] = readObjectArrayField(await readJsonObject(utxos), 'utxos');
    expect(requireStringField(utxo ?? {}, 'outputKey')).toBe('doge-tx-2:0');

    const deniedInfo = await request(ctx.app, `/v1/info?q=${dogecoinFixture.targetAddress}`);
    expect(deniedInfo.status).toBe(401);

    await ctx.cleanup();
  });

  it('returns a clean validation error when info is requested without q', async () => {
    const ctx = await createTestApp();
    const created = await request(ctx.app, '/v1/keys/', {
      method: 'POST',
      body: {},
    });
    const key = await readJsonObject(created);
    const apiToken = requireStringField(key, 'key');

    const response = await request(ctx.app, '/v1/info/', {
      headers: {
        'x-api-token': apiToken,
      },
    });

    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({
      error: 'missing input params',
    });

    await ctx.cleanup();
  });

  it('returns a connection error when rpc health checks fail during network creation', async () => {
    restoreFetch.mockRejectedValue(new Error('socket hang up'));
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    const ctx = await createTestApp();
    const created = await request(ctx.app, '/v1/keys/', {
      method: 'POST',
      body: {},
    });
    const key = await readJsonObject(created);
    const apiToken = requireStringField(key, 'key');

    const response = await request(ctx.app, '/v1/networks', {
      method: 'POST',
      headers: {
        'x-api-token': apiToken,
      },
      body: {
        name: 'Dogecoin Mainnet',
        architecture: 'dogecoin',
        chainId: 0,
        blockTime: 60,
        rpcEndpoint: 'https://doge.example/rpc',
      },
    });

    expect(response.status).toBe(500);
    expect(await response.json()).toEqual({
      error: 'could not connect to `https://doge.example/rpc`',
    });
    expect(consoleError).toHaveBeenCalledWith('[onlydoge] infrastructure error', {
      route: 'POST /v1/networks',
      code: 'UNKNOWN',
      message: 'could not connect to `https://doge.example/rpc`',
      cause: 'Error: socket hang up',
    });

    consoleError.mockRestore();
    await ctx.cleanup();
  });
});

async function readJsonObject(response: Response): Promise<Record<string, unknown>> {
  return requireObject(await response.json(), 'response');
}

function readObjectArrayField(
  record: Record<string, unknown>,
  field: string,
): Array<Record<string, unknown>> {
  const value = record[field];
  if (!Array.isArray(value)) {
    throw new TypeError(`expected array for ${field}`);
  }

  return value.map((item, index) => requireObject(item, `${field}[${index}]`));
}

function readObjectField(record: Record<string, unknown>, field: string): Record<string, unknown> {
  return requireObject(record[field], field);
}

function readStringArrayField(record: Record<string, unknown>, field: string): string[] {
  const value = record[field];
  if (!Array.isArray(value) || value.some((item) => typeof item !== 'string')) {
    throw new TypeError(`expected string array for ${field}`);
  }

  return [...value];
}

function requireObject(value: unknown, field: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`expected object for ${field}`);
  }

  return Object.fromEntries(Object.entries(value));
}

function requireStringField(record: Record<string, unknown>, field: string): string {
  const value = record[field];
  if (typeof value !== 'string') {
    throw new TypeError(`expected string for ${field}`);
  }

  return value;
}

function requireNumberField(record: Record<string, unknown>, field: string): number {
  const value = record[field];
  if (typeof value !== 'number') {
    throw new TypeError(`expected number for ${field}`);
  }

  return value;
}
