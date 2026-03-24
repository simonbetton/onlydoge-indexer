import { openapi } from '@elysiajs/openapi';
import {
  apiTokenSecuritySchemeName,
  buildAccessControlHttp,
  enforceApiTokenAuth,
} from '@onlydoge/access-control';
import { buildEntityLabelingHttp } from '@onlydoge/entity-labeling';
import { buildInvestigationQueryHttp } from '@onlydoge/investigation-query';
import { buildNetworkCatalogHttp } from '@onlydoge/network-catalog';
import type { Runtime } from '@onlydoge/platform';
import { InfrastructureError, OnlyDogeError, RpcEndpoint } from '@onlydoge/shared-kernel';
import { Elysia } from 'elysia';

export function buildApiApp(runtime: Runtime) {
  return new Elysia()
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
    )
    .get('/up', async () => {
      await runtime.investigationQuery.heartbeat();
      return 'ok';
    })
    .onBeforeHandle(async ({ request }) => {
      await enforceApiTokenAuth(
        runtime.accessControl,
        request.method,
        new URL(request.url).pathname,
        request.headers.get('x-api-token'),
      );
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
    .use(buildInvestigationQueryHttp(runtime.investigationQuery));
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
