import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { createClient as createLibsqlClient, type Client as LibsqlClient } from '@libsql/client';
import type { ApiKeyRecord, ApiKeyRepository } from '@onlydoge/access-control';
import type {
  AddressRecord,
  AddressRepository,
  ConfigMutationPort,
  EntityRecord,
  EntityRepository,
  EntityTagRepository,
  NetworkReader,
  TagRecord,
  TagRepository,
} from '@onlydoge/entity-labeling';
import {
  type CoordinatorConfigPort,
  configKeyNewlyAddedAddress,
  type IndexedNetworkPort,
  type ProjectionLinkSeedPort,
} from '@onlydoge/indexing-pipeline';
import type { ConfigReader, InvestigationMetadataPort } from '@onlydoge/investigation-query';
import type {
  NetworkRecord,
  NetworkRepository,
  TokenRecord,
  TokenRepository,
} from '@onlydoge/network-catalog';
import {
  nowIsoString,
  type PrimaryId,
  parseChainFamily,
  parseRiskLevel,
  safeJsonParse,
} from '@onlydoge/shared-kernel';
import { drizzle as drizzleLibsql } from 'drizzle-orm/libsql';
import { drizzle as drizzleMysql } from 'drizzle-orm/mysql2';
import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres';
import mysql from 'mysql2/promise';
import { Pool } from 'pg';

import type { DatabaseSettings } from './settings';

type SupportedClient =
  | { kind: 'sqlite'; raw: LibsqlClient }
  | { kind: 'postgres'; raw: Pool }
  | { kind: 'mysql'; raw: mysql.Pool };

type SqlValue = boolean | number | string | null;

type DatabaseRow = Record<string, SqlValue>;

function compileQuery(kind: SupportedClient['kind'], query: string): string {
  if (kind !== 'postgres') {
    return query;
  }

  let index = 0;
  return query.replaceAll('?', () => {
    index += 1;
    return `$${index}`;
  });
}

function toBoolean(value: unknown): boolean {
  return value === true || value === 1 || value === '1';
}

export class RelationalMetadataStore
  implements
    ApiKeyRepository,
    NetworkRepository,
    TokenRepository,
    EntityRepository,
    AddressRepository,
    TagRepository,
    EntityTagRepository,
    ConfigReader,
    CoordinatorConfigPort,
    ConfigMutationPort,
    InvestigationMetadataPort,
    IndexedNetworkPort,
    ProjectionLinkSeedPort,
    NetworkReader
{
  private constructor(private readonly client: SupportedClient) {}

  public static async connect(settings: DatabaseSettings): Promise<RelationalMetadataStore> {
    if (settings.driver === 'sqlite') {
      const path = settings.location.replace(/^file:/u, '');
      await mkdir(dirname(path), { recursive: true });
      const raw = createLibsqlClient({ url: settings.location });
      drizzleLibsql(raw);

      const store = new RelationalMetadataStore({ kind: 'sqlite', raw });
      await store.migrate();
      return store;
    }

    if (settings.driver === 'postgres') {
      const raw = new Pool({
        connectionString: settings.location,
        ...(settings.ssl ? { ssl: settings.ssl } : {}),
      });
      drizzlePg(raw);

      const store = new RelationalMetadataStore({ kind: 'postgres', raw });
      await store.migrate();
      return store;
    }

    const raw = mysql.createPool(settings.location);
    drizzleMysql(raw);

    const store = new RelationalMetadataStore({ kind: 'mysql', raw });
    await store.migrate();
    return store;
  }

  public async countApiKeys(): Promise<number> {
    const row = await this.one<{ count: number | string }>(
      'SELECT COUNT(*) AS count FROM api_keys',
    );
    return Number(row?.count ?? 0);
  }

  public async createApiKey(record: {
    createdAt: string;
    id: string;
    isActive: boolean;
    secretKeyHash: string;
    secretKeyPlaintext: string | null;
    updatedAt: string | null;
  }) {
    await this.execute(
      `
        INSERT INTO api_keys (id, secret_key, secret_key_hash, is_active, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
      `,
      [
        record.id,
        record.secretKeyPlaintext,
        record.secretKeyHash,
        this.booleanValue(record.isActive),
        record.updatedAt,
        record.createdAt,
      ],
    );

    return this.getApiKeyById(record.id).then(assertFound);
  }

  public async getApiKeyByHash(secretKeyHash: string) {
    const row = await this.one<DatabaseRow>(
      'SELECT * FROM api_keys WHERE secret_key_hash = ? LIMIT 1',
      [secretKeyHash],
    );
    return row ? this.mapApiKey(row) : null;
  }

  public async getApiKeyById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM api_keys WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapApiKey(row) : null;
  }

  public async listApiKeys(offset?: number, limit?: number) {
    const rows = await this.query<DatabaseRow>(
      `
        SELECT * FROM api_keys
        ORDER BY api_key_id ASC
        ${limit !== undefined ? 'LIMIT ?' : ''}
        ${offset !== undefined ? 'OFFSET ?' : ''}
      `,
      [...(limit !== undefined ? [limit] : []), ...(offset !== undefined ? [offset] : [])],
    );

    return rows.map((row) => this.mapApiKey(row));
  }

  public async updateApiKey(record: {
    id: string;
    isActive: boolean;
    secretKeyHash: string;
    secretKeyPlaintext: string | null;
    updatedAt: string | null;
  }): Promise<void> {
    await this.execute(
      `
        UPDATE api_keys
        SET secret_key = ?, secret_key_hash = ?, is_active = ?, updated_at = ?
        WHERE id = ?
      `,
      [
        record.secretKeyPlaintext,
        record.secretKeyHash,
        this.booleanValue(record.isActive),
        record.updatedAt ?? nowIsoString(),
        record.id,
      ],
    );
  }

  public async deleteApiKeys(ids: string[]): Promise<void> {
    if (ids.length === 0) {
      return;
    }

    await this.execute(`DELETE FROM api_keys WHERE id IN (${placeholders(ids.length)})`, ids);
  }

  public async createNetwork(record: NetworkRecord) {
    await this.execute(
      `
        INSERT INTO networks (id, name, architecture, chain_id, block_time, rpc_endpoint, rps, is_deleted, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        record.id,
        record.name,
        record.architecture,
        record.chainId,
        record.blockTime,
        record.rpcEndpoint,
        record.rps,
        record.isDeleted ? 1 : 0,
        record.updatedAt,
        record.createdAt,
      ],
    );

    return this.getNetworkById(record.id).then(assertFound);
  }

  public async getNetworkByArchitectureAndChainId(
    architecture: string,
    chainId: number,
    includeDeleted = false,
  ) {
    const row = await this.one<DatabaseRow>(
      `
        SELECT * FROM networks
        WHERE architecture = ? AND chain_id = ?
        ${includeDeleted ? '' : `AND ${this.booleanCondition('is_deleted', false)}`}
        LIMIT 1
      `,
      [architecture, chainId],
    );
    return row ? this.mapNetwork(row) : null;
  }

  public async getNetworkById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM networks WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapNetwork(row) : null;
  }

  public async getNetworkByInternalId(id: PrimaryId) {
    const row = await this.one<DatabaseRow>('SELECT * FROM networks WHERE network_id = ? LIMIT 1', [
      id,
    ]);
    return row ? this.mapNetwork(row) : null;
  }

  public async getNetworkByName(name: string, includeDeleted = false) {
    const row = await this.one<DatabaseRow>(
      `
        SELECT * FROM networks
        WHERE LOWER(name) = LOWER(?)
        ${includeDeleted ? '' : `AND ${this.booleanCondition('is_deleted', false)}`}
        LIMIT 1
      `,
      [name],
    );
    return row ? this.mapNetwork(row) : null;
  }

  public async listNetworks(offset?: number, limit?: number) {
    const rows = await this.query<DatabaseRow>(
      `
        SELECT * FROM networks
        ORDER BY network_id ASC
        ${limit !== undefined ? 'LIMIT ?' : ''}
        ${offset !== undefined ? 'OFFSET ?' : ''}
      `,
      [...(limit !== undefined ? [limit] : []), ...(offset !== undefined ? [offset] : [])],
    );

    return rows.map((row) => this.mapNetwork(row));
  }

  public async updateNetworkRecord(record: NetworkRecord): Promise<void> {
    await this.execute(
      `
        UPDATE networks
        SET name = ?, architecture = ?, chain_id = ?, block_time = ?, rpc_endpoint = ?, rps = ?, is_deleted = ?, updated_at = ?
        WHERE id = ?
      `,
      [
        record.name,
        record.architecture,
        record.chainId,
        record.blockTime,
        record.rpcEndpoint,
        record.rps,
        this.booleanValue(record.isDeleted),
        record.updatedAt ?? nowIsoString(),
        record.id,
      ],
    );
  }

  public async softDeleteNetworks(ids: string[]) {
    if (ids.length === 0) {
      return [];
    }

    const networks = await Promise.all(ids.map((id) => this.getNetworkById(id)));
    const existing = networks.filter((network): network is NonNullable<typeof network> =>
      Boolean(network),
    );
    if (existing.length === 0) {
      return [];
    }

    await this.execute(
      `UPDATE networks SET is_deleted = ${this.booleanLiteral(true)}, updated_at = ? WHERE id IN (${placeholders(
        existing.length,
      )})`,
      [nowIsoString(), ...existing.map((network) => network.id)],
    );

    return Promise.all(existing.map((network) => this.getNetworkById(network.id))).then((updated) =>
      updated.filter((network): network is NonNullable<typeof network> => Boolean(network)),
    );
  }

  public async createToken(record: TokenRecord) {
    await this.execute(
      `
        INSERT INTO tokens (network_id, id, name, symbol, address, decimals, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        record.networkId,
        record.id,
        record.name,
        record.symbol,
        record.address,
        record.decimals,
        record.updatedAt,
        record.createdAt,
      ],
    );

    return this.getTokenById(record.id).then(assertFound);
  }

  public async getTokenById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM tokens WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapToken(row) : null;
  }

  public async listTokens(offset?: number, limit?: number) {
    const rows = await this.query<DatabaseRow>(
      `
        SELECT * FROM tokens
        ORDER BY token_id ASC
        ${limit !== undefined ? 'LIMIT ?' : ''}
        ${offset !== undefined ? 'OFFSET ?' : ''}
      `,
      [...(limit !== undefined ? [limit] : []), ...(offset !== undefined ? [offset] : [])],
    );

    return rows.map((row) => this.mapToken(row));
  }

  public async listTokensByNetworkIds(networkIds: PrimaryId[]) {
    if (networkIds.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `SELECT * FROM tokens WHERE network_id IN (${placeholders(networkIds.length)})`,
      networkIds,
    );
    return rows.map((row) => this.mapToken(row));
  }

  public async getTokenByNetworkAndAddress(networkId: PrimaryId, address: string) {
    const row = await this.one<DatabaseRow>(
      'SELECT * FROM tokens WHERE network_id = ? AND address = ? LIMIT 1',
      [networkId, address],
    );
    return row ? this.mapToken(row) : null;
  }

  public async deleteTokens(ids: string[]): Promise<void> {
    if (ids.length === 0) {
      return;
    }

    await this.execute(`DELETE FROM tokens WHERE id IN (${placeholders(ids.length)})`, ids);
  }

  public async createEntity(record: EntityRecord) {
    await this.execute(
      `
        INSERT INTO entities (id, name, description, data, is_deleted, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `,
      [
        record.id,
        record.name,
        record.description,
        JSON.stringify(record.data),
        this.booleanValue(record.isDeleted),
        record.updatedAt,
        record.createdAt,
      ],
    );

    return this.getEntityById(record.id).then(assertFound);
  }

  public async getEntityById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM entities WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapEntity(row) : null;
  }

  public async getEntityByInternalId(entityId: PrimaryId) {
    const row = await this.one<DatabaseRow>('SELECT * FROM entities WHERE entity_id = ? LIMIT 1', [
      entityId,
    ]);
    return row ? this.mapEntity(row) : null;
  }

  public async getEntityByName(name: string, includeDeleted = false) {
    const row = await this.one<DatabaseRow>(
      `
        SELECT * FROM entities
        WHERE LOWER(name) = LOWER(?)
        ${includeDeleted ? '' : `AND ${this.booleanCondition('is_deleted', false)}`}
        LIMIT 1
      `,
      [name],
    );
    return row ? this.mapEntity(row) : null;
  }

  public async listEntities(offset?: number, limit?: number) {
    const rows = await this.query<DatabaseRow>(
      `
        SELECT * FROM entities
        ORDER BY entity_id ASC
        ${limit !== undefined ? 'LIMIT ?' : ''}
        ${offset !== undefined ? 'OFFSET ?' : ''}
      `,
      [...(limit !== undefined ? [limit] : []), ...(offset !== undefined ? [offset] : [])],
    );

    return rows.map((row) => this.mapEntity(row));
  }

  public async listEntitiesByIds(entityIds: PrimaryId[]) {
    if (entityIds.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `SELECT * FROM entities WHERE entity_id IN (${placeholders(entityIds.length)})`,
      entityIds,
    );
    return rows.map((row) => this.mapEntity(row));
  }

  public async listEntitiesByTagIds(tagIds: PrimaryId[]) {
    if (tagIds.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT e.*, et.tag_id
        FROM entities e
        JOIN entity_tags et ON et.entity_id = e.entity_id
        WHERE et.tag_id IN (${placeholders(tagIds.length)})
      `,
      tagIds,
    );

    return rows.map((row) => ({
      entity: this.mapEntity(row),
      tagId: Number(row.tag_id),
    }));
  }

  public async updateEntityRecord(record: EntityRecord): Promise<void> {
    await this.execute(
      `
        UPDATE entities
        SET name = ?, description = ?, data = ?, is_deleted = ?, updated_at = ?
        WHERE id = ?
      `,
      [
        record.name,
        record.description,
        JSON.stringify(record.data),
        this.booleanValue(record.isDeleted),
        record.updatedAt ?? nowIsoString(),
        record.id,
      ],
    );
  }

  public async softDeleteEntities(ids: string[]) {
    if (ids.length === 0) {
      return [];
    }

    const entities = await Promise.all(ids.map((id) => this.getEntityById(id)));
    const existing = entities.filter((entity): entity is NonNullable<typeof entity> =>
      Boolean(entity),
    );
    if (existing.length === 0) {
      return [];
    }

    await this.execute(
      `UPDATE entities SET is_deleted = ${this.booleanLiteral(true)}, updated_at = ? WHERE id IN (${placeholders(existing.length)})`,
      [nowIsoString(), ...existing.map((entity) => entity.id)],
    );

    return Promise.all(existing.map((entity) => this.getEntityById(entity.id))).then((updated) =>
      updated.filter((entity): entity is NonNullable<typeof entity> => Boolean(entity)),
    );
  }

  public async createAddresses(records: AddressRecord[]) {
    for (const record of records) {
      await this.execute(
        `
          INSERT INTO addresses (entity_id, network_id, network, id, address, description, data, is_deleted, updated_at, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          record.entityId,
          record.networkId,
          record.network,
          record.id,
          record.address,
          record.description,
          JSON.stringify(record.data),
          this.booleanValue(record.isDeleted),
          record.updatedAt,
          record.createdAt,
        ],
      );
    }

    if (records.length === 0) {
      return [];
    }

    const [firstRecord] = records;
    if (!firstRecord) {
      return [];
    }

    return this.findAddressesByEntityNetworkAndAddresses(
      firstRecord.entityId,
      firstRecord.networkId,
      records.map((record) => record.address),
      false,
    );
  }

  public async getAddressById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM addresses WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapAddress(row) : null;
  }

  public async getAddressByInternalId(addressId: PrimaryId) {
    const row = await this.one<DatabaseRow>(
      'SELECT * FROM addresses WHERE address_id = ? LIMIT 1',
      [addressId],
    );
    return row ? this.mapAddress(row) : null;
  }

  public async listAddresses(offset?: number, limit?: number) {
    const rows = await this.query<DatabaseRow>(
      `
        SELECT * FROM addresses
        ORDER BY address_id ASC
        ${limit !== undefined ? 'LIMIT ?' : ''}
        ${offset !== undefined ? 'OFFSET ?' : ''}
      `,
      [...(limit !== undefined ? [limit] : []), ...(offset !== undefined ? [offset] : [])],
    );

    return rows.map((row) => this.mapAddress(row));
  }

  public async listAddressesByEntityIds(entityIds: PrimaryId[]) {
    if (entityIds.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `SELECT * FROM addresses WHERE entity_id IN (${placeholders(entityIds.length)})`,
      entityIds,
    );
    return rows.map((row) => this.mapAddress(row));
  }

  public async listAddressesByNetworkIds(networkIds: PrimaryId[]) {
    if (networkIds.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `SELECT * FROM addresses WHERE network_id IN (${placeholders(networkIds.length)})`,
      networkIds,
    );
    return rows.map((row) => this.mapAddress(row));
  }

  public async listTrackedAddresses(networkId: PrimaryId) {
    const rows = await this.query<DatabaseRow>(
      `
        SELECT address_id, address
        FROM addresses
        WHERE network_id = ? AND ${this.booleanCondition('is_deleted', false)}
      `,
      [networkId],
    );
    return rows.map((row) => ({
      addressId: Number(row.address_id),
      address: String(row.address),
    }));
  }

  public async listTrackedAddressesByValues(networkId: PrimaryId, addresses: string[]) {
    if (addresses.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT address_id, address
        FROM addresses
        WHERE
          network_id = ?
          AND address IN (${placeholders(addresses.length)})
          AND ${this.booleanCondition('is_deleted', false)}
      `,
      [networkId, ...addresses],
    );

    return rows.map((row) => ({
      addressId: Number(row.address_id),
      address: String(row.address),
    }));
  }

  public async listAddressesByValues(addresses: string[], includeDeleted = false) {
    if (addresses.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT * FROM addresses
        WHERE address IN (${placeholders(addresses.length)})
        ${includeDeleted ? '' : `AND ${this.booleanCondition('is_deleted', false)}`}
      `,
      addresses,
    );
    return rows.map((row) => this.mapAddress(row));
  }

  public async findAddressesByEntityNetworkAndAddresses(
    entityId: PrimaryId,
    networkId: PrimaryId,
    addresses: string[],
    includeDeleted = false,
  ) {
    if (addresses.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT * FROM addresses
        WHERE entity_id = ? AND network_id = ? AND address IN (${placeholders(addresses.length)})
        ${includeDeleted ? '' : `AND ${this.booleanCondition('is_deleted', false)}`}
      `,
      [entityId, networkId, ...addresses],
    );
    return rows.map((row) => this.mapAddress(row));
  }

  public async softDeleteAddresses(ids: string[]) {
    if (ids.length === 0) {
      return [];
    }

    const existing = await Promise.all(ids.map((id) => this.getAddressById(id)));
    const addresses = existing.filter((address): address is NonNullable<typeof address> =>
      Boolean(address),
    );
    if (addresses.length === 0) {
      return [];
    }

    await this.execute(
      `UPDATE addresses SET is_deleted = ${this.booleanLiteral(true)}, updated_at = ? WHERE id IN (${placeholders(addresses.length)})`,
      [nowIsoString(), ...addresses.map((address) => address.id)],
    );

    return Promise.all(addresses.map((address) => this.getAddressById(address.id))).then(
      (updated) =>
        updated.filter((address): address is NonNullable<typeof address> => Boolean(address)),
    );
  }

  public async softDeleteAddressesByEntityIds(entityIds: PrimaryId[]): Promise<void> {
    if (entityIds.length === 0) {
      return;
    }

    await this.execute(
      `UPDATE addresses SET is_deleted = ${this.booleanLiteral(true)}, updated_at = ? WHERE entity_id IN (${placeholders(
        entityIds.length,
      )})`,
      [nowIsoString(), ...entityIds],
    );
  }

  public async softDeleteAddressesByNetworkIds(networkIds: PrimaryId[]): Promise<void> {
    if (networkIds.length === 0) {
      return;
    }

    await this.execute(
      `UPDATE addresses SET is_deleted = ${this.booleanLiteral(true)}, updated_at = ? WHERE network_id IN (${placeholders(
        networkIds.length,
      )})`,
      [nowIsoString(), ...networkIds],
    );
  }

  public async createTag(record: TagRecord) {
    await this.execute(
      `
        INSERT INTO tags (id, name, risk_level, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?)
      `,
      [record.id, record.name, record.riskLevel, record.updatedAt, record.createdAt],
    );

    return this.getTagById(record.id).then(assertFound);
  }

  public async getTagById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM tags WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapTag(row) : null;
  }

  public async getTagByName(name: string) {
    const row = await this.one<DatabaseRow>(
      'SELECT * FROM tags WHERE LOWER(name) = LOWER(?) LIMIT 1',
      [name],
    );
    return row ? this.mapTag(row) : null;
  }

  public async listTags(offset?: number, limit?: number) {
    const rows = await this.query<DatabaseRow>(
      `
        SELECT * FROM tags
        ORDER BY tag_id ASC
        ${limit !== undefined ? 'LIMIT ?' : ''}
        ${offset !== undefined ? 'OFFSET ?' : ''}
      `,
      [...(limit !== undefined ? [limit] : []), ...(offset !== undefined ? [offset] : [])],
    );

    return rows.map((row) => this.mapTag(row));
  }

  public async listTagsByIds(tagIds: PrimaryId[]) {
    if (tagIds.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `SELECT * FROM tags WHERE tag_id IN (${placeholders(tagIds.length)})`,
      tagIds,
    );
    return rows.map((row) => this.mapTag(row));
  }

  public async updateTagRecord(record: TagRecord): Promise<void> {
    await this.execute(
      `
        UPDATE tags
        SET name = ?, risk_level = ?, updated_at = ?
        WHERE id = ?
      `,
      [record.name, record.riskLevel, record.updatedAt ?? nowIsoString(), record.id],
    );
  }

  public async deleteTags(ids: string[]): Promise<void> {
    if (ids.length === 0) {
      return;
    }

    await this.execute(`DELETE FROM tags WHERE id IN (${placeholders(ids.length)})`, ids);
  }

  public async listEntityTagMap(entityIds: PrimaryId[]) {
    if (entityIds.length === 0) {
      return new Map<PrimaryId, PrimaryId[]>();
    }

    const rows = await this.query<DatabaseRow>(
      `SELECT entity_id, tag_id FROM entity_tags WHERE entity_id IN (${placeholders(entityIds.length)})`,
      entityIds,
    );

    const result = new Map<PrimaryId, PrimaryId[]>();
    for (const row of rows) {
      const entityId = Number(row.entity_id);
      const tagIds = result.get(entityId) ?? [];
      tagIds.push(Number(row.tag_id));
      result.set(entityId, tagIds);
    }

    return result;
  }

  public async replaceEntityTags(entityId: PrimaryId, tagIds: PrimaryId[]): Promise<void> {
    await this.execute('DELETE FROM entity_tags WHERE entity_id = ?', [entityId]);
    for (const tagId of tagIds) {
      await this.execute(
        'INSERT INTO entity_tags (entity_id, tag_id, created_at) VALUES (?, ?, ?)',
        [entityId, tagId, nowIsoString()],
      );
    }
  }

  public async getJsonValue<T>(key: string): Promise<T | null> {
    const row = await this.one<{ value: string }>(
      'SELECT value FROM configs WHERE key = ? LIMIT 1',
      [key],
    );
    return row ? safeJsonParse<T | null>(row.value, null) : null;
  }

  public async setJsonValue<T>(key: string, value: T): Promise<void> {
    const exists = await this.getJsonValue<T>(key);
    if (exists === null) {
      await this.execute(
        'INSERT INTO configs (key, value, updated_at, created_at) VALUES (?, ?, ?, ?)',
        [key, JSON.stringify(value), nowIsoString(), nowIsoString()],
      );
      return;
    }

    await this.execute('UPDATE configs SET value = ?, updated_at = ? WHERE key = ?', [
      JSON.stringify(value),
      nowIsoString(),
      key,
    ]);
  }

  public async compareAndSwapJsonValue<T>(
    key: string,
    expectedValue: T | null,
    nextValue: T,
  ): Promise<boolean> {
    const current = await this.getJsonValue<T>(key);
    if (JSON.stringify(current) !== JSON.stringify(expectedValue)) {
      return false;
    }

    await this.setJsonValue(key, nextValue);
    return true;
  }

  public async deleteByPrefix(prefix: string): Promise<void> {
    await this.execute('DELETE FROM configs WHERE key LIKE ?', [`${prefix}%`]);
  }

  public async markNewlyAddedAddress(networkId: PrimaryId, addressId: PrimaryId): Promise<void> {
    await this.markPendingRelinkSeed(networkId, addressId);
  }

  public async markPendingRelinkSeed(networkId: PrimaryId, addressId: PrimaryId): Promise<void> {
    await this.setJsonValue(configKeyNewlyAddedAddress(networkId, addressId), addressId);
  }

  public async clearPendingRelinkSeed(networkId: PrimaryId, addressId: PrimaryId): Promise<void> {
    await this.execute('DELETE FROM configs WHERE key = ?', [
      configKeyNewlyAddedAddress(networkId, addressId),
    ]);
  }

  public async getTrackedAddress(networkId: PrimaryId, addressId: PrimaryId) {
    const row = await this.one<DatabaseRow>(
      `
        SELECT address_id, address
        FROM addresses
        WHERE network_id = ? AND address_id = ? AND ${this.booleanCondition('is_deleted', false)}
        LIMIT 1
      `,
      [networkId, addressId],
    );
    return row
      ? {
          addressId: Number(row.address_id),
          address: String(row.address),
        }
      : null;
  }

  public async listPendingRelinkSeeds(networkId: PrimaryId) {
    const rows = await this.query<DatabaseRow>(
      'SELECT key FROM configs WHERE key LIKE ? ORDER BY key ASC',
      [`newly_added_address_n${networkId}_a%`],
    );
    const trackedAddresses = await Promise.all(
      rows
        .map((row) => parsePendingRelinkAddressId(String(row.key), networkId))
        .filter((addressId): addressId is PrimaryId => addressId !== null)
        .map((addressId) => this.getTrackedAddress(networkId, addressId)),
    );

    return trackedAddresses.filter((address): address is NonNullable<typeof address> =>
      Boolean(address),
    );
  }

  public async getActiveNetworkById(id: string) {
    const network = await this.getNetworkById(id);
    return network && !network.isDeleted ? network : null;
  }

  public async getActiveNetworksByInternalIds(networkIds: PrimaryId[]) {
    const networks = await Promise.all(
      networkIds.map((networkId) => this.getNetworkByInternalId(networkId)),
    );
    return networks.filter((network): network is NonNullable<typeof network> =>
      Boolean(network && !network.isDeleted),
    );
  }

  public async listTagsByEntityIds(entityIds: PrimaryId[]) {
    if (entityIds.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT t.id, t.name, t.risk_level, et.entity_id
        FROM tags t
        JOIN entity_tags et ON et.tag_id = t.tag_id
        WHERE et.entity_id IN (${placeholders(entityIds.length)})
      `,
      entityIds,
    );

    return rows.map((row) => ({
      entityId: Number(row.entity_id),
      id: String(row.id),
      name: String(row.name),
      riskLevel: parseRiskLevel(String(row.risk_level)),
    }));
  }

  public async listNetworksByInternalIds(networkIds: PrimaryId[]) {
    if (networkIds.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT network_id, id, name, chain_id
        FROM networks
        WHERE network_id IN (${placeholders(networkIds.length)}) AND ${this.booleanCondition('is_deleted', false)}
      `,
      networkIds,
    );

    return rows.map((row) => ({
      networkId: Number(row.network_id),
      id: String(row.id),
      name: String(row.name),
      chainId: Number(row.chain_id),
    }));
  }

  public async listActiveNetworks() {
    const rows = await this.query<DatabaseRow>(
      `SELECT network_id, id, name, architecture, chain_id, block_time, rpc_endpoint, rps FROM networks WHERE ${this.booleanCondition('is_deleted', false)} ORDER BY network_id ASC`,
    );

    return rows.map((row) => ({
      networkId: Number(row.network_id),
      id: String(row.id),
      name: String(row.name),
      architecture: parseChainFamily(String(row.architecture)),
      chainId: Number(row.chain_id),
      blockTime: Number(row.block_time),
      rpcEndpoint: String(row.rpc_endpoint),
      rps: Number(row.rps),
    }));
  }

  private async query<T extends DatabaseRow>(
    statement: string,
    params: SqlValue[] = [],
  ): Promise<T[]> {
    const compiled = compileQuery(this.client.kind, statement);

    if (this.client.kind === 'sqlite') {
      const result = await this.client.raw.execute({ sql: compiled, args: params });
      return (result.rows ?? []) as unknown as T[];
    }

    if (this.client.kind === 'postgres') {
      const result = await this.client.raw.query(compiled, params);
      return result.rows as T[];
    }

    const [rows] = await this.client.raw.query(compiled, params);
    return rows as T[];
  }

  private async one<T extends DatabaseRow>(
    statement: string,
    params: SqlValue[] = [],
  ): Promise<T | null> {
    const rows = await this.query<T>(statement, params);
    return rows[0] ?? null;
  }

  private async execute(statement: string, params: SqlValue[] = []): Promise<void> {
    const compiled = compileQuery(this.client.kind, statement);

    if (this.client.kind === 'sqlite') {
      await this.client.raw.execute({ sql: compiled, args: params });
      return;
    }

    if (this.client.kind === 'postgres') {
      await this.client.raw.query(compiled, params);
      return;
    }

    await this.client.raw.query(compiled, params);
  }

  private booleanLiteral(value: boolean): string {
    if (this.client.kind === 'sqlite') {
      return value ? '1' : '0';
    }

    return value ? 'TRUE' : 'FALSE';
  }

  private booleanCondition(column: string, value: boolean): string {
    return `${column} = ${this.booleanLiteral(value)}`;
  }

  private booleanValue(value: boolean): SqlValue {
    if (this.client.kind === 'sqlite') {
      return value ? 1 : 0;
    }

    return value;
  }

  private async migrate(): Promise<void> {
    const statements =
      this.client.kind === 'sqlite'
        ? sqliteMigrations
        : this.client.kind === 'postgres'
          ? postgresMigrations
          : mysqlMigrations;

    for (const statement of statements) {
      await this.execute(statement);
    }
  }

  private mapApiKey(row: DatabaseRow): ApiKeyRecord {
    return {
      apiKeyId: Number(row.api_key_id),
      id: String(row.id),
      secretKeyPlaintext: row.secret_key ? String(row.secret_key) : null,
      secretKeyHash: String(row.secret_key_hash),
      isActive: toBoolean(row.is_active),
      updatedAt: row.updated_at ? String(row.updated_at) : null,
      createdAt: String(row.created_at),
    };
  }

  private mapNetwork(row: DatabaseRow): NetworkRecord {
    return {
      networkId: Number(row.network_id),
      id: String(row.id),
      name: String(row.name),
      architecture: parseChainFamily(String(row.architecture)),
      chainId: Number(row.chain_id),
      blockTime: Number(row.block_time),
      rpcEndpoint: String(row.rpc_endpoint),
      rps: Number(row.rps),
      isDeleted: toBoolean(row.is_deleted),
      updatedAt: row.updated_at ? String(row.updated_at) : null,
      createdAt: String(row.created_at),
    };
  }

  private mapToken(row: DatabaseRow): TokenRecord {
    return {
      tokenId: Number(row.token_id),
      networkId: Number(row.network_id),
      id: String(row.id),
      name: String(row.name),
      symbol: String(row.symbol),
      address: String(row.address),
      decimals: Number(row.decimals),
      updatedAt: row.updated_at ? String(row.updated_at) : null,
      createdAt: String(row.created_at),
    };
  }

  private mapEntity(row: DatabaseRow): EntityRecord {
    return {
      entityId: Number(row.entity_id),
      id: String(row.id),
      name: row.name === null ? null : String(row.name),
      description: String(row.description),
      data: safeJsonParse<Record<string, unknown>>(String(row.data ?? '{}'), {}),
      isDeleted: toBoolean(row.is_deleted),
      updatedAt: row.updated_at ? String(row.updated_at) : null,
      createdAt: String(row.created_at),
    };
  }

  private mapAddress(row: DatabaseRow): AddressRecord {
    return {
      addressId: Number(row.address_id),
      entityId: Number(row.entity_id),
      networkId: Number(row.network_id),
      id: String(row.id),
      network: String(row.network),
      address: String(row.address),
      description: String(row.description),
      data: safeJsonParse<Record<string, unknown>>(String(row.data ?? '{}'), {}),
      isDeleted: toBoolean(row.is_deleted),
      updatedAt: row.updated_at ? String(row.updated_at) : null,
      createdAt: String(row.created_at),
    };
  }

  private mapTag(row: DatabaseRow): TagRecord {
    return {
      tagId: Number(row.tag_id),
      id: String(row.id),
      name: String(row.name),
      riskLevel: parseRiskLevel(String(row.risk_level)),
      updatedAt: row.updated_at ? String(row.updated_at) : null,
      createdAt: String(row.created_at),
    };
  }
}

function assertFound<T>(value: T | null): T {
  if (!value) {
    throw new Error('Expected record to exist');
  }

  return value;
}

function placeholders(count: number): string {
  return Array.from({ length: count }, () => '?').join(', ');
}

function parsePendingRelinkAddressId(key: string, networkId: PrimaryId): PrimaryId | null {
  const match = key.match(new RegExp(`^newly_added_address_n${networkId}_a(\\d+)$`, 'u'));
  if (!match?.[1]) {
    return null;
  }

  return Number(match[1]);
}

const sqliteMigrations = [
  `CREATE TABLE IF NOT EXISTS configs (
      config_id INTEGER PRIMARY KEY AUTOINCREMENT,
      key TEXT NOT NULL UNIQUE,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS api_keys (
      api_key_id INTEGER PRIMARY KEY AUTOINCREMENT,
      id TEXT NOT NULL UNIQUE,
      secret_key TEXT NULL,
      secret_key_hash TEXT NOT NULL,
      is_active INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS networks (
      network_id INTEGER PRIMARY KEY AUTOINCREMENT,
      id TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL UNIQUE,
      architecture TEXT NOT NULL,
      chain_id INTEGER NOT NULL,
      block_time INTEGER NOT NULL,
      rpc_endpoint TEXT NOT NULL,
      rps INTEGER NOT NULL,
      is_deleted INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS entities (
      entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
      id TEXT NOT NULL UNIQUE,
      name TEXT NULL UNIQUE,
      description TEXT NOT NULL,
      data TEXT NOT NULL,
      is_deleted INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS addresses (
      address_id INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_id INTEGER NOT NULL,
      network_id INTEGER NOT NULL,
      network TEXT NOT NULL,
      id TEXT NOT NULL UNIQUE,
      address TEXT NOT NULL,
      description TEXT NOT NULL,
      data TEXT NOT NULL,
      is_deleted INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(entity_id, network_id, address)
    )`,
  `CREATE TABLE IF NOT EXISTS tags (
      tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
      id TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL UNIQUE,
      risk_level TEXT NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS entity_tags (
      entity_id INTEGER NOT NULL,
      tag_id INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (entity_id, tag_id)
    )`,
  `CREATE TABLE IF NOT EXISTS tokens (
      token_id INTEGER PRIMARY KEY AUTOINCREMENT,
      network_id INTEGER NOT NULL,
      id TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      symbol TEXT NOT NULL,
      address TEXT NOT NULL,
      decimals INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(network_id, address)
    )`,
];

const postgresMigrations = [
  `CREATE TABLE IF NOT EXISTS configs (
      config_id BIGSERIAL PRIMARY KEY,
      key TEXT NOT NULL UNIQUE,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS api_keys (
      api_key_id BIGSERIAL PRIMARY KEY,
      id TEXT NOT NULL UNIQUE,
      secret_key TEXT NULL,
      secret_key_hash TEXT NOT NULL,
      is_active BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS networks (
      network_id BIGSERIAL PRIMARY KEY,
      id TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL UNIQUE,
      architecture TEXT NOT NULL,
      chain_id BIGINT NOT NULL,
      block_time BIGINT NOT NULL,
      rpc_endpoint TEXT NOT NULL,
      rps INTEGER NOT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS entities (
      entity_id BIGSERIAL PRIMARY KEY,
      id TEXT NOT NULL UNIQUE,
      name TEXT NULL UNIQUE,
      description TEXT NOT NULL,
      data TEXT NOT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS addresses (
      address_id BIGSERIAL PRIMARY KEY,
      entity_id BIGINT NOT NULL,
      network_id BIGINT NOT NULL,
      network TEXT NOT NULL,
      id TEXT NOT NULL UNIQUE,
      address TEXT NOT NULL,
      description TEXT NOT NULL,
      data TEXT NOT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(entity_id, network_id, address)
    )`,
  `CREATE TABLE IF NOT EXISTS tags (
      tag_id BIGSERIAL PRIMARY KEY,
      id TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL UNIQUE,
      risk_level TEXT NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS entity_tags (
      entity_id BIGINT NOT NULL,
      tag_id BIGINT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (entity_id, tag_id)
    )`,
  `CREATE TABLE IF NOT EXISTS tokens (
      token_id BIGSERIAL PRIMARY KEY,
      network_id BIGINT NOT NULL,
      id TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      symbol TEXT NOT NULL,
      address TEXT NOT NULL,
      decimals INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(network_id, address)
    )`,
];

const mysqlMigrations = [
  `CREATE TABLE IF NOT EXISTS configs (
      config_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      key TEXT NOT NULL,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE KEY uq_configs_key (key(255))
    )`,
  `CREATE TABLE IF NOT EXISTS api_keys (
      api_key_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      id VARCHAR(255) NOT NULL UNIQUE,
      secret_key TEXT NULL,
      secret_key_hash TEXT NOT NULL,
      is_active BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS networks (
      network_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      id VARCHAR(255) NOT NULL UNIQUE,
      name VARCHAR(255) NOT NULL UNIQUE,
      architecture VARCHAR(64) NOT NULL,
      chain_id BIGINT NOT NULL,
      block_time BIGINT NOT NULL,
      rpc_endpoint TEXT NOT NULL,
      rps INTEGER NOT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS entities (
      entity_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      id VARCHAR(255) NOT NULL UNIQUE,
      name VARCHAR(255) NULL UNIQUE,
      description TEXT NOT NULL,
      data JSON NOT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS addresses (
      address_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      entity_id BIGINT NOT NULL,
      network_id BIGINT NOT NULL,
      network VARCHAR(255) NOT NULL,
      id VARCHAR(255) NOT NULL UNIQUE,
      address TEXT NOT NULL,
      description TEXT NOT NULL,
      data JSON NOT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE KEY uq_addresses_entity_network_address (entity_id, network_id, address(255))
    )`,
  `CREATE TABLE IF NOT EXISTS tags (
      tag_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      id VARCHAR(255) NOT NULL UNIQUE,
      name VARCHAR(255) NOT NULL UNIQUE,
      risk_level VARCHAR(32) NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS entity_tags (
      entity_id BIGINT NOT NULL,
      tag_id BIGINT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (entity_id, tag_id)
    )`,
  `CREATE TABLE IF NOT EXISTS tokens (
      token_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      network_id BIGINT NOT NULL,
      id VARCHAR(255) NOT NULL UNIQUE,
      name TEXT NOT NULL,
      symbol TEXT NOT NULL,
      address TEXT NOT NULL,
      decimals INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE KEY uq_tokens_network_address (network_id, address(255))
    )`,
];
