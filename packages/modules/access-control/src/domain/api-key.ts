import {
  ApiSecret,
  ExternalId,
  NotFoundError,
  type PrimaryId,
  ValidationError,
} from '@onlydoge/shared-kernel';

export interface ApiKeyRecord {
  apiKeyId: PrimaryId;
  id: string;
  secretKeyPlaintext: string | null;
  secretKeyHash: string;
  isActive: boolean;
  updatedAt: string | null;
  createdAt: string;
}

export interface CreateApiKeyInput {
  id?: string;
}

export class ApiKey {
  public readonly record: ApiKeyRecord;

  private constructor(record: ApiKeyRecord) {
    this.record = record;
  }

  public static create(input: CreateApiKeyInput, nextPrimaryId = 0): ApiKey {
    const id = input.id
      ? ExternalId.parse(input.id, 'key').toString()
      : ExternalId.create('key').toString();
    const secret = ApiSecret.generate();

    return new ApiKey({
      apiKeyId: nextPrimaryId,
      id,
      secretKeyPlaintext: secret.value,
      secretKeyHash: secret.hash,
      isActive: true,
      updatedAt: null,
      createdAt: new Date().toISOString(),
    });
  }

  public static rehydrate(record: ApiKeyRecord): ApiKey {
    if (!record.id) {
      throw new ValidationError('invalid api key record');
    }

    return new ApiKey(record);
  }

  public requireActive(): void {
    if (!this.record.isActive) {
      throw new ValidationError('unauthorized');
    }
  }

  public hideSecret(): ApiKeyRecord {
    return {
      ...this.record,
      secretKeyPlaintext: null,
      updatedAt: new Date().toISOString(),
    };
  }

  public setIsActive(isActive: boolean): ApiKeyRecord {
    return {
      ...this.record,
      isActive,
      updatedAt: new Date().toISOString(),
    };
  }

  public toResponse(): {
    createdAt: string;
    id: string;
    isActive: boolean;
    key?: string;
  } {
    return {
      id: this.record.id,
      isActive: this.record.isActive,
      createdAt: this.record.createdAt,
      ...(this.record.secretKeyPlaintext ? { key: this.record.secretKeyPlaintext } : {}),
    };
  }
}

export function requireApiKey(record: ApiKeyRecord | null): ApiKeyRecord {
  if (!record) {
    throw new NotFoundError();
  }

  return record;
}
