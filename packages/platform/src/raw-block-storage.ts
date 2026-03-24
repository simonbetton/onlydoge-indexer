import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { gunzipSync, gzipSync } from 'node:zlib';

import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';

import type { RawBlockStoragePort } from '@onlydoge/indexing-pipeline';
import type { PrimaryId } from '@onlydoge/shared-kernel';

import type { StorageSettings } from './settings';

export class FileRawBlockStorageAdapter implements RawBlockStoragePort {
  public constructor(private readonly basePath: string) {}

  public async getPart<T extends Record<string, unknown>>(
    networkId: PrimaryId,
    blockHeight: number,
    part: string,
  ): Promise<T | null> {
    const filePath = join(this.basePath, String(networkId), String(blockHeight), `${part}.json.gz`);
    try {
      const payload = await readFile(filePath);
      const parsed: T = JSON.parse(gunzipSync(payload).toString('utf8'));
      return parsed;
    } catch {
      return null;
    }
  }

  public async putPart(
    networkId: PrimaryId,
    blockHeight: number,
    part: string,
    payload: Record<string, unknown>,
  ): Promise<void> {
    const filePath = join(this.basePath, String(networkId), String(blockHeight), `${part}.json.gz`);
    await mkdir(dirname(filePath), { recursive: true });
    await writeFile(filePath, gzipSync(Buffer.from(JSON.stringify(payload))));
  }
}

export class S3RawBlockStorageAdapter implements RawBlockStoragePort {
  private readonly bucket: string;
  private readonly client: S3Client;
  private readonly prefix: string;

  public constructor(settings: StorageSettings) {
    const url = new URL(settings.location);
    this.bucket = url.pathname.replace(/^\/+/u, '').split('/')[0] ?? 'onlydoge';
    this.prefix = url.pathname.replace(/^\/+/u, '').split('/').slice(1).join('/');
    const credentials =
      settings.accessKeyId && settings.secretAccessKey
        ? {
            accessKeyId: settings.accessKeyId,
            secretAccessKey: settings.secretAccessKey,
          }
        : undefined;
    this.client = new S3Client({
      endpoint: `${url.protocol}//${url.host}`,
      region: 'auto',
      ...(credentials ? { credentials } : {}),
      forcePathStyle: true,
    });
  }

  public async getPart<T extends Record<string, unknown>>(
    networkId: PrimaryId,
    blockHeight: number,
    part: string,
  ): Promise<T | null> {
    const key = [this.prefix, networkId, blockHeight, `${part}.json.gz`].filter(Boolean).join('/');
    try {
      const response = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: key,
        }),
      );
      const body = await toBuffer(response.Body);
      const parsed: T = JSON.parse(gunzipSync(body).toString('utf8'));
      return parsed;
    } catch {
      return null;
    }
  }

  public async putPart(
    networkId: PrimaryId,
    blockHeight: number,
    part: string,
    payload: Record<string, unknown>,
  ): Promise<void> {
    const key = [this.prefix, networkId, blockHeight, `${part}.json.gz`].filter(Boolean).join('/');
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: gzipSync(Buffer.from(JSON.stringify(payload))),
        ContentType: 'application/json',
        ContentEncoding: 'gzip',
      }),
    );
  }
}

export function createRawBlockStorage(settings: StorageSettings): RawBlockStoragePort {
  return settings.driver === 'file'
    ? new FileRawBlockStorageAdapter(settings.location)
    : new S3RawBlockStorageAdapter(settings);
}

async function toBuffer(body: unknown): Promise<Buffer> {
  if (!body) {
    return Buffer.alloc(0);
  }
  if (body instanceof Uint8Array) {
    return Buffer.from(body);
  }
  if (typeof body === 'string') {
    return Buffer.from(body);
  }
  if (
    typeof body === 'object' &&
    body !== null &&
    'transformToByteArray' in body &&
    typeof body.transformToByteArray === 'function'
  ) {
    return Buffer.from(await body.transformToByteArray());
  }
  if (isAsyncIterable(body)) {
    const chunks: Buffer[] = [];
    for await (const chunk of body) {
      chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : Buffer.from(chunk));
    }
    return Buffer.concat(chunks);
  }

  throw new Error('Unsupported S3 response body');
}

function isAsyncIterable(value: unknown): value is AsyncIterable<Buffer | Uint8Array | string> {
  return typeof value === 'object' && value !== null && Symbol.asyncIterator in value;
}
