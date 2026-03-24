import { ApiSecret, ExternalId, UnauthorizedError, ValidationError } from '@onlydoge/shared-kernel';
import type { ApiKeyRepository } from '../contracts/api-key-repository';
import { ApiKey, type CreateApiKeyInput, requireApiKey } from '../domain/api-key';

export class AccessControlService {
  public constructor(private readonly apiKeys: ApiKeyRepository) {}

  public async hasConfiguredKeys(): Promise<boolean> {
    return (await this.apiKeys.countApiKeys()) > 0;
  }

  public async createKey(input: CreateApiKeyInput): Promise<ReturnType<ApiKey['toResponse']>> {
    if (input.id) {
      ExternalId.parse(input.id, 'key');
      const existing = await this.apiKeys.getApiKeyById(input.id);
      if (existing) {
        throw new ValidationError(`invalid parameter for \`id\`: ${input.id}`);
      }
    }

    const entity = ApiKey.create(input);
    const created = await this.apiKeys.createApiKey(entity.record);

    return ApiKey.rehydrate(created).toResponse();
  }

  public async listKeys(
    offset?: number,
    limit?: number,
  ): Promise<{
    keys: Array<ReturnType<ApiKey['toResponse']>>;
  }> {
    return {
      keys: (await this.apiKeys.listApiKeys(offset, limit)).map((record) =>
        ApiKey.rehydrate(record).toResponse(),
      ),
    };
  }

  public async getKey(id: string): Promise<{ key: ReturnType<ApiKey['toResponse']> }> {
    return {
      key: ApiKey.rehydrate(requireApiKey(await this.apiKeys.getApiKeyById(id))).toResponse(),
    };
  }

  public async updateKey(
    id: string,
    input: {
      isActive?: boolean;
    },
  ): Promise<void> {
    const current = ApiKey.rehydrate(requireApiKey(await this.apiKeys.getApiKeyById(id)));
    const updated =
      typeof input.isActive === 'boolean' ? current.setIsActive(input.isActive) : current.record;

    if (updated !== current.record) {
      await this.apiKeys.updateApiKey(updated);
    }
  }

  public async deleteKeys(ids: string[]): Promise<void> {
    await this.apiKeys.deleteApiKeys(ids);
  }

  public async authenticate(apiToken: string | null | undefined): Promise<void> {
    if (!(await this.hasConfiguredKeys())) {
      return;
    }

    const token = apiToken?.trim();
    if (!token) {
      throw new UnauthorizedError();
    }

    const secretHash = ApiSecret.hashFromToken(token);
    const record = await this.apiKeys.getApiKeyByHash(secretHash);
    if (!record || !record.isActive) {
      throw new UnauthorizedError();
    }

    if (record.secretKeyPlaintext) {
      await this.apiKeys.updateApiKey(ApiKey.rehydrate(record).hideSecret());
    }
  }
}
