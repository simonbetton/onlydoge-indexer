import { createHash, randomUUID } from 'node:crypto';
import { homedir } from 'node:os';
import { resolve } from 'node:path';

import { ValidationError } from './errors';

export type PrimaryId = number;

export type IdPrefix = 'net' | 'key' | 'ent' | 'adr' | 'tag' | 'tok';

export type ChainFamily = 'dogecoin' | 'evm';

export type Mode = 'both' | 'indexer' | 'http';

export type RiskLevel = 'low' | 'high';

export type RiskReason = 'entity' | 'source';

const prefixSet = new Set<IdPrefix>(['net', 'key', 'ent', 'adr', 'tag', 'tok']);

function isIdPrefix(value: string | undefined): value is IdPrefix {
  return value !== undefined && prefixSet.has(value as IdPrefix);
}

function isChainFamily(value: string): value is ChainFamily {
  return value === 'dogecoin' || value === 'evm';
}

function isMode(value: string): value is Mode {
  return value === 'both' || value === 'indexer' || value === 'http';
}

function isRiskLevel(value: string): value is RiskLevel {
  return value === 'low' || value === 'high';
}

export class ExternalId {
  public readonly value: string;
  public readonly prefix: IdPrefix;

  private constructor(value: string, prefix: IdPrefix) {
    this.value = value;
    this.prefix = prefix;
  }

  public static create(prefix: IdPrefix, raw?: string): ExternalId {
    const suffix = raw?.trim() ? raw.trim() : randomUUID().replaceAll('-', '').slice(0, 24);
    const value = `${prefix}_${suffix}`;

    return ExternalId.parse(value, prefix);
  }

  public static parse(value: string, expectedPrefix?: IdPrefix): ExternalId {
    const trimmed = value.trim();
    const [prefix, suffix] = trimmed.split('_');

    if (!isIdPrefix(prefix)) {
      throw new ValidationError(`invalid id prefix: ${value}`);
    }

    if (expectedPrefix && prefix !== expectedPrefix) {
      throw new ValidationError(`invalid parameter for \`id\`: ${value}`);
    }

    if (!suffix || suffix.length > 32 || !/^[A-Za-z0-9]+$/u.test(suffix)) {
      throw new ValidationError(`invalid parameter for \`id\`: ${value}`);
    }

    return new ExternalId(trimmed, prefix);
  }

  public toString(): string {
    return this.value;
  }
}

export class RpcEndpoint {
  public readonly value: string;

  private constructor(value: string) {
    this.value = value;
  }

  public static parse(value: string): RpcEndpoint {
    const trimmed = value.trim();

    try {
      const url = new URL(trimmed);
      if (!['http:', 'https:'].includes(url.protocol)) {
        throw new ValidationError(`invalid RPC endpoint: ${value}`);
      }

      return new RpcEndpoint(trimmed);
    } catch (_error) {
      throw new ValidationError(`invalid RPC endpoint: ${value}`);
    }
  }

  public maskAuth(): string {
    const url = new URL(this.value);
    if (url.username) {
      url.username = '***';
    }
    if (url.password) {
      url.password = '***';
    }

    return url.toString();
  }

  public toString(): string {
    return this.value;
  }
}

export class BlockchainAddress {
  public readonly value: string;

  private constructor(value: string) {
    this.value = value;
  }

  public static parse(value: string): BlockchainAddress {
    const trimmed = value.trim();
    if (!trimmed) {
      throw new ValidationError('address cannot be empty');
    }

    return new BlockchainAddress(trimmed);
  }

  public toString(): string {
    return this.value;
  }
}

export class BlockHeight {
  public readonly value: number;

  private constructor(value: number) {
    this.value = value;
  }

  public static parse(value: number): BlockHeight {
    if (!Number.isInteger(value) || value < 0) {
      throw new ValidationError(`invalid block height: ${value}`);
    }

    return new BlockHeight(value);
  }
}

export class BlockTime {
  public readonly value: number;

  private constructor(value: number) {
    this.value = value;
  }

  public static parse(value: number): BlockTime {
    if (!Number.isInteger(value) || value <= 0) {
      throw new ValidationError(`invalid block time: ${value}`);
    }

    return new BlockTime(value);
  }
}

export class ApiSecret {
  public readonly value: string;
  public readonly hash: string;

  private constructor(value: string, hash: string) {
    this.value = value;
    this.hash = hash;
  }

  public static generate(): ApiSecret {
    const raw = createHash('sha256').update(randomUUID()).digest('hex');
    const token = raw.slice(0, 48);

    return new ApiSecret(`sk_${token}`, hashSha256(token));
  }

  public static hashFromToken(apiToken: string): string {
    const token = apiToken.split('_').at(-1) ?? apiToken;

    return hashSha256(token);
  }

  public static hashFromBearer(bearerToken: string): string {
    return ApiSecret.hashFromToken(bearerToken);
  }
}

export function parseMode(input: string | undefined): Mode {
  const value = input?.trim().toLowerCase() ?? 'both';
  if (!isMode(value)) {
    throw new ValidationError(`invalid mode: ${input}`);
  }

  return value;
}

export function parseChainFamily(input: string): ChainFamily {
  const value = input.trim().toLowerCase();
  if (!isChainFamily(value)) {
    throw new ValidationError(`invalid architecture: ${input}`);
  }

  return value;
}

export function parseRiskLevel(input: string): RiskLevel {
  const value = input.trim().toLowerCase();
  if (!isRiskLevel(value)) {
    throw new ValidationError(`invalid risk level: ${input}`);
  }

  return value;
}

export function nowIsoString(): string {
  return new Date().toISOString();
}

export function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolvePromise) => {
    setTimeout(resolvePromise, milliseconds);
  });
}

export function hashSha256(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

export function expandHomePath(value: string): string {
  const homePlaceholder = '${' + 'HOME}';
  if (value.startsWith('~/')) {
    return resolve(homedir(), value.slice(2));
  }

  return value.replaceAll(homePlaceholder, homedir()).replaceAll('$HOME', homedir());
}

export function safeJsonParse<T>(value: string, fallback: T): T {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}
