import { openapi } from '@elysiajs/openapi';
import {
  apiTokenSecuritySchemeName,
  buildAccessControlHttp,
  enforceApiTokenAuth,
} from '@onlydoge/access-control';
import { buildEntityLabelingHttp } from '@onlydoge/entity-labeling';
import { buildExplorerQueryHttp } from '@onlydoge/explorer-query';
import { buildInvestigationQueryHttp } from '@onlydoge/investigation-query';
import { buildNetworkCatalogHttp } from '@onlydoge/network-catalog';
import type { Runtime } from '@onlydoge/platform';
import { InfrastructureError, OnlyDogeError, RpcEndpoint } from '@onlydoge/shared-kernel';
import { Elysia } from 'elysia';

export function buildApiApp(runtime: Runtime) {
  return new Elysia()
    .get('/up', async () => {
      await runtime.investigationQuery.heartbeat();
      return new Response('ok', {
        headers: {
          'cache-control': 'no-store',
          'content-type': 'text/plain; charset=utf-8',
        },
      });
    })
    .onBeforeHandle(async ({ request }) => {
      await enforceApiTokenAuth(
        runtime.accessControl,
        request.method,
        new URL(request.url).pathname,
        request.headers.get('x-api-token'),
      );
    })
    .onAfterHandle(({ path, request, response, set }) => {
      const status =
        response instanceof Response
          ? response.status
          : typeof set.status === 'number'
            ? set.status
            : 200;
      const policy = resolveCachePolicy(
        request.method,
        path || new URL(request.url).pathname,
        status,
      );

      if (!policy) {
        return;
      }

      return applyCachePolicy(path || new URL(request.url).pathname, response, status, policy, set);
    })
    .onError(({ code, error, path, request, set }) => {
      const route = `${request.method} ${path || new URL(request.url).pathname}`;

      if (error instanceof InfrastructureError) {
        console.error('[onlydoge] infrastructure error', {
          route,
          code,
          message: maskLoggedErrorMessage(error.message),
          ...(error.cause ? { cause: describeErrorCause(error.cause) } : {}),
        });
        set.status = error.statusCode;
        return {
          error: error.message,
        };
      }

      if (error instanceof OnlyDogeError) {
        set.status = error.statusCode;
        return {
          error: error.message,
        };
      }

      if (code === 'VALIDATION') {
        set.status = typeof error.status === 'number' ? error.status : 422;
        return {
          error: describeValidationError(error),
        };
      }

      if (code === 'NOT_FOUND') {
        console.error(`[onlydoge] not found route=${route}`);
        set.status = 404;
        return {
          error: 'not found',
        };
      }

      console.error(`[onlydoge] unhandled error route=${route} code=${code}`, error);
      set.status = 500;
      return {
        error: 'rekt',
      };
    })
    .use(buildAccessControlHttp(runtime.accessControl))
    .use(buildNetworkCatalogHttp(runtime.networkCatalog))
    .use(buildEntityLabelingHttp(runtime.entityLabeling))
    .use(buildExplorerQueryHttp(runtime.explorerQuery))
    .use(buildInvestigationQueryHttp(runtime.investigationQuery))
    .use(
      openapi({
        path: '/openapi',
        specPath: '/openapi/json',
        provider: 'scalar',
        exclude: {
          paths: ['/up'],
        },
        documentation: {
          info: {
            title: 'OnlyDoge API',
            version: '0.1.0',
          },
          components: {
            securitySchemes: {
              [apiTokenSecuritySchemeName]: {
                type: 'apiKey',
                in: 'header',
                name: 'x-api-token',
                description: 'OnlyDoge API token.',
              },
            },
          },
        },
      }),
    );
}

export async function startApiServer(runtime: Runtime) {
  const app = buildApiApp(runtime);
  return app.listen({
    hostname: runtime.settings.ip,
    port: runtime.settings.port,
  });
}

function maskLoggedErrorMessage(message: string): string {
  return message.replace(/`(https?:\/\/[^`]+)`/gu, (_match, endpoint) => {
    try {
      return `\`${RpcEndpoint.parse(endpoint).maskAuth()}\``;
    } catch {
      return `\`${endpoint}\``;
    }
  });
}

function describeErrorCause(cause: unknown): string {
  if (cause instanceof Error) {
    return `${cause.name}: ${cause.message}`;
  }

  return String(cause);
}

function describeValidationError(error: unknown): string {
  if (!error || typeof error !== 'object') {
    return 'invalid request';
  }

  const details = error as Record<string, unknown>;
  const summary = details.summary;
  if (typeof summary === 'string' && summary.trim()) {
    return summary;
  }

  const message = details.message;
  if (typeof message === 'string' && message.trim()) {
    return message;
  }

  return 'invalid request';
}

function resolveCachePolicy(
  method: string,
  path: string,
  status: number,
): { cacheControl: string; vary?: string } | null {
  if (status >= 400) {
    return {
      cacheControl: 'no-store',
    };
  }

  const normalizedPath = path !== '/' && path.endsWith('/') ? path.slice(0, -1) : path;
  const normalizedMethod = method.toUpperCase();

  if (normalizedMethod !== 'GET' && normalizedMethod !== 'HEAD') {
    return {
      cacheControl: 'no-store',
    };
  }

  if (normalizedPath === '/up' || normalizedPath.startsWith('/v1/heartbeat')) {
    return {
      cacheControl: 'no-store',
    };
  }

  if (normalizedPath.startsWith('/v1/explorer/search')) {
    return {
      cacheControl: 'public, max-age=5, stale-while-revalidate=15',
    };
  }

  if (normalizedPath.startsWith('/v1/explorer/addresses')) {
    return {
      cacheControl: 'public, max-age=15, stale-while-revalidate=60',
    };
  }

  if (
    normalizedPath.startsWith('/v1/explorer/blocks') ||
    normalizedPath.startsWith('/v1/explorer/transactions') ||
    normalizedPath.startsWith('/v1/explorer/networks')
  ) {
    return {
      cacheControl: 'public, max-age=30, stale-while-revalidate=120',
    };
  }

  if (normalizedPath.startsWith('/openapi')) {
    return {
      cacheControl: 'public, max-age=300, stale-while-revalidate=3600',
    };
  }

  if (normalizedPath === '/v1/keys' || normalizedPath.startsWith('/v1/keys/')) {
    return {
      cacheControl: 'no-store',
      vary: 'x-api-token',
    };
  }

  if (normalizedPath.startsWith('/v1/stats')) {
    return {
      cacheControl: 'private, max-age=5, stale-while-revalidate=15',
      vary: 'x-api-token',
    };
  }

  if (normalizedPath.startsWith('/v1/info')) {
    return {
      cacheControl: 'private, max-age=15, stale-while-revalidate=30',
      vary: 'x-api-token',
    };
  }

  if (
    normalizedPath.startsWith('/v1/networks') ||
    normalizedPath.startsWith('/v1/tokens') ||
    normalizedPath.startsWith('/v1/entities') ||
    normalizedPath.startsWith('/v1/addresses') ||
    normalizedPath.startsWith('/v1/tags')
  ) {
    return {
      cacheControl: 'private, max-age=30, stale-while-revalidate=60',
      vary: 'x-api-token',
    };
  }

  return {
    cacheControl: 'no-store',
  };
}

function applyCachePolicy(
  path: string,
  response: unknown,
  status: number,
  policy: { cacheControl: string; vary?: string },
  set: { headers: Record<string, string | number> },
): unknown {
  const headers = new Headers(response instanceof Response ? response.headers : undefined);
  headers.set('cache-control', policy.cacheControl);

  if (policy.vary) {
    headers.set('vary', policy.vary);
  }

  if (response instanceof Response) {
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  }

  if (typeof response === 'string') {
    headers.set(
      'content-type',
      path.startsWith('/openapi') && !path.endsWith('/json')
        ? 'text/html; charset=utf-8'
        : 'text/plain; charset=utf-8',
    );
    return new Response(response, {
      status,
      headers,
    });
  }

  if (response === null) {
    return new Response(null, {
      status,
      headers,
    });
  }

  if (!isPlainJsonBody(response)) {
    set.headers['cache-control'] = policy.cacheControl;
    if (policy.vary) {
      set.headers.vary = policy.vary;
    }
    return response;
  }

  return Response.json(response, {
    status,
    headers,
  });
}

function isPlainJsonBody(value: unknown): value is Record<string, unknown> | unknown[] {
  if (Array.isArray(value)) {
    return true;
  }

  if (!value || typeof value !== 'object') {
    return false;
  }

  return Object.getPrototypeOf(value) === Object.prototype;
}
