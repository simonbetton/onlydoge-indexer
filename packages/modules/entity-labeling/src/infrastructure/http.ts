import { protectedOperationDetail } from '@onlydoge/access-control';
import { Elysia, t } from 'elysia';

import type { EntityLabelingService } from '../application/entity-labeling-service';

function parsePagination(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }

  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

const dataSchema = t.Optional(t.Record(t.String(), t.Any()));

export function buildEntityLabelingHttp(service: EntityLabelingService) {
  return new Elysia()
    .use(
      new Elysia({ prefix: '/v1/entities' })
        .post('/', async ({ body }) => service.createEntity(body), {
          detail: protectedOperationDetail,
          body: t.Object({
            id: t.Optional(t.String()),
            name: t.Optional(t.Nullable(t.String())),
            description: t.String(),
            data: dataSchema,
            tags: t.Optional(t.Array(t.String())),
          }),
        })
        .get(
          '/',
          async ({ query }) =>
            service.listEntities(parsePagination(query.offset), parsePagination(query.limit)),
          {
            detail: protectedOperationDetail,
          },
        )
        .get('/:id', async ({ params }) => service.getEntity(params.id), {
          detail: protectedOperationDetail,
        })
        .put(
          '/:id',
          async ({ params, body }) => {
            await service.updateEntity(params.id, body);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedOperationDetail,
            body: t.Object({
              name: t.Optional(t.Nullable(t.String())),
              description: t.Optional(t.String()),
              data: dataSchema,
              tags: t.Optional(t.Array(t.String())),
            }),
          },
        )
        .delete(
          '/',
          async ({ body }) => {
            await service.deleteEntities(body.entities);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedOperationDetail,
            body: t.Object({
              entities: t.Array(t.String()),
            }),
          },
        ),
    )
    .use(
      new Elysia({ prefix: '/v1/addresses' })
        .post('/', async ({ body }) => service.createAddresses(body), {
          detail: protectedOperationDetail,
          body: t.Object({
            entity: t.String(),
            network: t.String(),
            addresses: t.Array(
              t.Object({
                address: t.String(),
                description: t.String(),
                data: dataSchema,
              }),
            ),
          }),
        })
        .get(
          '/',
          async ({ query }) =>
            service.listAddresses(parsePagination(query.offset), parsePagination(query.limit)),
          {
            detail: protectedOperationDetail,
          },
        )
        .get('/:id', async ({ params }) => service.getAddress(params.id), {
          detail: protectedOperationDetail,
        })
        .delete(
          '/',
          async ({ body }) => {
            await service.deleteAddresses(body.addresses);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedOperationDetail,
            body: t.Object({
              addresses: t.Array(t.String()),
            }),
          },
        ),
    )
    .use(
      new Elysia({ prefix: '/v1/tags' })
        .post('/', async ({ body }) => service.createTag(body), {
          detail: protectedOperationDetail,
          body: t.Object({
            id: t.Optional(t.String()),
            name: t.String(),
            riskLevel: t.Union([t.Literal('low'), t.Literal('high')]),
          }),
        })
        .get(
          '/',
          async ({ query }) =>
            service.listTags(parsePagination(query.offset), parsePagination(query.limit)),
          {
            detail: protectedOperationDetail,
          },
        )
        .get('/:id', async ({ params }) => service.getTag(params.id), {
          detail: protectedOperationDetail,
        })
        .put(
          '/:id',
          async ({ params, body }) => {
            await service.updateTag(params.id, body);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedOperationDetail,
            body: t.Object({
              name: t.Optional(t.String()),
              riskLevel: t.Optional(t.Union([t.Literal('low'), t.Literal('high')])),
            }),
          },
        )
        .delete(
          '/',
          async ({ body }) => {
            await service.deleteTags(body.tags);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedOperationDetail,
            body: t.Object({
              tags: t.Array(t.String()),
            }),
          },
        ),
    );
}
