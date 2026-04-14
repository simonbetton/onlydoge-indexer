import { URL } from 'node:url';

import {
  type ChainFamily,
  expandHomePath,
  type Mode,
  type PrimaryId,
  parseMode,
} from '@onlydoge/shared-kernel';

export interface DatabaseSettings {
  driver: 'sqlite' | 'postgres' | 'mysql';
  location: string;
  ssl?: {
    ca?: string;
    rejectUnauthorized?: boolean;
  };
}

export interface StorageSettings {
  driver: 'file' | 's3';
  location: string;
  accessKeyId?: string;
  secretAccessKey?: string;
}

export interface WarehouseSettings {
  driver: 'duckdb' | 'clickhouse';
  location: string;
  database?: string;
  user?: string;
  password?: string;
}

export interface IndexerSettings {
  leaseHeartbeatIntervalMs: number;
  projectTimeoutMs: number;
  networkConcurrency: number;
  projectWindow: number;
  relinkBatchSize: number;
  relinkConcurrency: number;
  relinkFrontierBatch: number;
  relinkTimeoutMs: number;
  syncConcurrency: number;
  syncTimeoutMs: number;
  syncWindow: number;
}

export interface AppSettings {
  mode: Mode;
  isIndexer: boolean;
  isHttp: boolean;
  ip: string;
  port: number;
  database: DatabaseSettings;
  indexer: IndexerSettings;
  storage: StorageSettings;
  warehouse: WarehouseSettings;
}

export function loadSettings(input?: {
  env?: NodeJS.ProcessEnv;
  ip?: string;
  mode?: string;
  port?: number;
}): AppSettings {
  const homePlaceholder = '${' + 'HOME}';
  const env = input?.env ?? process.env;
  assertRequiredEnvironment(env);
  const mode = parseMode(input?.mode ?? env.ONLYDOGE_MODE);
  const databaseLocation = expandHomePath(
    resolveDatabaseLocation(env) ?? `sqlite://${homePlaceholder}/.onlydoge/onlydoge.sqlite.db`,
  );
  const storageLocation = expandHomePath(
    env.ONLYDOGE_STORAGE ?? `file://${homePlaceholder}/.onlydoge/storage`,
  );
  const warehouseLocation = expandHomePath(
    env.ONLYDOGE_WAREHOUSE ?? `${homePlaceholder}/.onlydoge/onlydoge.duckdb.db`,
  );

  return {
    mode,
    isIndexer: mode === 'both' || mode === 'indexer',
    isHttp: mode === 'both' || mode === 'http',
    ip: input?.ip ?? env.ONLYDOGE_IP ?? '127.0.0.1',
    port: input?.port ?? Number(env.ONLYDOGE_PORT ?? 2277),
    database: parseDatabaseSettings(databaseLocation, env),
    indexer: parseIndexerSettings(env),
    storage: parseStorageSettings(storageLocation, env),
    warehouse: parseWarehouseSettings(warehouseLocation, env),
  };
}

function assertRequiredEnvironment(env: NodeJS.ProcessEnv): void {
  if (env.NODE_ENV !== 'production') {
    return;
  }

  const missing = [
    ...(hasDatabaseConfiguration(env) ? [] : ['ONLYDOGE_DATABASE']),
    ...(!env.ONLYDOGE_STORAGE ? ['ONLYDOGE_STORAGE'] : []),
    ...(!env.ONLYDOGE_WAREHOUSE ? ['ONLYDOGE_WAREHOUSE'] : []),
  ];
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

function hasDatabaseConfiguration(env: NodeJS.ProcessEnv): boolean {
  return Boolean(resolveDatabaseLocation(env));
}

function resolveDatabaseLocation(env: NodeJS.ProcessEnv): string | undefined {
  if (env.ONLYDOGE_DATABASE) {
    return env.ONLYDOGE_DATABASE;
  }

  if (!env.ONLYDOGE_DATABASE_HOST) {
    return undefined;
  }

  const user = env.ONLYDOGE_DATABASE_USER ?? 'onlydoge';
  const password = env.ONLYDOGE_DATABASE_PASSWORD ?? '';
  const port = env.ONLYDOGE_DATABASE_PORT ?? '5432';
  const database = env.ONLYDOGE_DATABASE_NAME ?? 'onlydoge';
  const credentials = password
    ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}`
    : encodeURIComponent(user);

  return `postgres://${credentials}@${env.ONLYDOGE_DATABASE_HOST}:${port}/${database}`;
}

function parseDatabaseSettings(location: string, env: NodeJS.ProcessEnv): DatabaseSettings {
  if (location.startsWith('sqlite://')) {
    return {
      driver: 'sqlite',
      location: `file:${new URL(location).pathname}`,
    };
  }

  if (location.startsWith('postgres://') || location.startsWith('postgresql://')) {
    const ssl = parseDatabaseSslSettings(env);
    return {
      driver: 'postgres',
      location: ssl ? stripPostgresSslQueryParams(location) : location,
      ...(ssl ? { ssl } : {}),
    };
  }

  if (location.startsWith('mysql://')) {
    return {
      driver: 'mysql',
      location,
    };
  }

  throw new Error(`Unsupported database configuration: ${location}`);
}

function decodeMaybeBase64(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return trimmed;
  }

  if (trimmed.includes('-----BEGIN CERTIFICATE-----')) {
    return trimmed;
  }

  try {
    const decoded = Buffer.from(trimmed, 'base64').toString('utf8');
    if (decoded.includes('-----BEGIN CERTIFICATE-----')) {
      return decoded;
    }
  } catch {
    return trimmed;
  }

  return trimmed;
}

function parseDatabaseSslSettings(env: NodeJS.ProcessEnv): DatabaseSettings['ssl'] | undefined {
  const caValue = env.ONLYDOGE_DATABASE_SSLROOTCERT_PEM ?? env.ONLYDOGE_DATABASE_SSLROOTCERT_BASE64;
  if (!caValue) {
    return undefined;
  }

  return {
    ca: decodeMaybeBase64(caValue),
    rejectUnauthorized: true,
  };
}

function stripPostgresSslQueryParams(location: string): string {
  const url = new URL(location);
  for (const parameter of ['sslcert', 'sslkey', 'sslmode', 'sslrootcert']) {
    url.searchParams.delete(parameter);
  }
  return url.toString();
}

function parseStorageSettings(location: string, env: NodeJS.ProcessEnv): StorageSettings {
  if (location.startsWith('file://')) {
    return {
      driver: 'file',
      location: new URL(location).pathname,
    };
  }

  return {
    driver: 's3',
    location,
    ...(env.ONLYDOGE_S3_ACCESS_KEY_ID ? { accessKeyId: env.ONLYDOGE_S3_ACCESS_KEY_ID } : {}),
    ...(env.ONLYDOGE_S3_SECRET_ACCESS_KEY
      ? { secretAccessKey: env.ONLYDOGE_S3_SECRET_ACCESS_KEY }
      : {}),
  };
}

function parseWarehouseSettings(location: string, env: NodeJS.ProcessEnv): WarehouseSettings {
  if (location.startsWith('http://') || location.startsWith('https://')) {
    const url = new URL(location);
    const database = url.searchParams.get('database') ?? undefined;
    url.searchParams.delete('database');

    return {
      driver: 'clickhouse',
      location: url.toString(),
      ...(database ? { database } : {}),
      ...(env.ONLYDOGE_WAREHOUSE_USER ? { user: env.ONLYDOGE_WAREHOUSE_USER } : {}),
      ...(env.ONLYDOGE_WAREHOUSE_PASSWORD ? { password: env.ONLYDOGE_WAREHOUSE_PASSWORD } : {}),
    };
  }

  return {
    driver: 'duckdb',
    location: location.replace(/^file:/u, ''),
  };
}

function parseIndexerSettings(env: NodeJS.ProcessEnv): IndexerSettings {
  return {
    leaseHeartbeatIntervalMs: parsePositiveInteger(
      env.ONLYDOGE_INDEXER_LEASE_HEARTBEAT_INTERVAL_MS,
      5_000,
    ),
    networkConcurrency: parsePositiveInteger(env.ONLYDOGE_INDEXER_NETWORK_CONCURRENCY, 2),
    syncWindow: parsePositiveInteger(env.ONLYDOGE_INDEXER_SYNC_WINDOW, 32),
    syncConcurrency: parsePositiveInteger(env.ONLYDOGE_INDEXER_SYNC_CONCURRENCY, 4),
    syncTimeoutMs: parsePositiveInteger(env.ONLYDOGE_INDEXER_SYNC_TIMEOUT_MS, 120_000),
    projectWindow: parsePositiveInteger(env.ONLYDOGE_INDEXER_PROJECT_WINDOW, 8),
    projectTimeoutMs: parsePositiveInteger(env.ONLYDOGE_INDEXER_PROJECT_TIMEOUT_MS, 120_000),
    relinkBatchSize: parsePositiveInteger(env.ONLYDOGE_INDEXER_RELINK_BATCH_SIZE, 16),
    relinkConcurrency: parsePositiveInteger(env.ONLYDOGE_INDEXER_RELINK_CONCURRENCY, 2),
    relinkFrontierBatch: parsePositiveInteger(env.ONLYDOGE_INDEXER_RELINK_FRONTIER_BATCH, 32),
    relinkTimeoutMs: parsePositiveInteger(env.ONLYDOGE_INDEXER_RELINK_TIMEOUT_MS, 120_000),
  };
}

function parsePositiveInteger(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid positive integer: ${value}`);
  }

  return parsed;
}

export interface RpcBackedNetwork {
  architecture: ChainFamily;
  blockTime: number;
  id: string;
  networkId: PrimaryId;
  rpcEndpoint: string;
  rps: number;
}
