import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

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
