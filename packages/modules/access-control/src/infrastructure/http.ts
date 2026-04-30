import { OnlyDogeError, UnauthorizedError } from '@onlydoge/shared-kernel';
import { Elysia, t } from 'elysia';

import type { AccessControlService } from '../application/access-control-service';

function parsePagination(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new OnlyDogeError(`invalid parameter for \`${value}\`: ${value}`);
  }

  return parsed;
}

export const apiTokenSecuritySchemeName = 'ApiTokenAuth';
const apiTokenSecurityRequirement: Record<typeof apiTokenSecuritySchemeName, []> = {
  [apiTokenSecuritySchemeName]: [],
};
export const protectedOperationDetail = {
  security: [apiTokenSecurityRequirement],
};

export function buildAccessControlHttp(service: AccessControlService) {
  return new Elysia({ prefix: '/v1/keys' })
    .post('/', async ({ body }) => service.createKey(body), {
      detail: {
        description:
          'Creates the first API key without authentication. After the first key exists, this route also requires x-api-token.',
      },
      body: t.Object({
        id: t.Optional(t.String()),
      }),
    })
    .get(
      '/',
      async ({ query }) => {
        const offset = parsePagination(query.offset);
        const limit = parsePagination(query.limit);

        return service.listKeys(offset, limit);
      },
      {
        detail: protectedOperationDetail,
      },
    )
    .get('/:id', async ({ params }) => service.getKey(params.id), {
      detail: protectedOperationDetail,
    })
    .put(
      '/:id',
      async ({ params, body }) => {
        await service.updateKey(params.id, body);
        return new Response(null, { status: 204 });
      },
      {
        detail: protectedOperationDetail,
        body: t.Object({
          isActive: t.Optional(t.Boolean()),
        }),
      },
    )
    .delete(
      '/',
      async ({ body }) => {
        await service.deleteKeys([...body.keys]);
        return new Response(null, { status: 204 });
      },
      {
        detail: protectedOperationDetail,
        body: t.Object({
          keys: t.Array(t.String()),
        }),
      },
    );
}

export async function enforceApiTokenAuth(
  service: AccessControlService,
  method: string,
  path: string,
  apiTokenHeader: string | null,
): Promise<void> {
  const normalizedPath = normalizeAuthPath(path);

  if (isPublicRoute(normalizedPath)) {
    return;
  }

  const hasConfiguredKeys = await service.hasConfiguredKeys();
  if (isBootstrapKeyRoute(method, normalizedPath, hasConfiguredKeys)) {
    return;
  }

  if (!hasConfiguredKeys) {
    throw new UnauthorizedError();
  }

  await authenticateOrThrow(service, apiTokenHeader);
}

function normalizeAuthPath(path: string): string {
  return path !== '/' && path.endsWith('/') ? path.slice(0, -1) : path;
}

function isPublicRoute(path: string): boolean {
  return path === '/up' || path.startsWith('/v1/heartbeat') || path.startsWith('/openapi');
}

function isBootstrapKeyRoute(method: string, path: string, hasConfiguredKeys: boolean): boolean {
  return method.toUpperCase() === 'POST' && path === '/v1/keys' && !hasConfiguredKeys;
}

async function authenticateOrThrow(
  service: AccessControlService,
  apiTokenHeader: string | null,
): Promise<void> {
  try {
    await service.authenticate(apiTokenHeader);
  } catch (_error) {
    throw new UnauthorizedError();
  }
}
