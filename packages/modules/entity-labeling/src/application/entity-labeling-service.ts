import {
  ConflictError,
  NotFoundError,
  type PrimaryId,
  type RiskLevel,
  TooEarlyError,
  ValidationError,
} from '@onlydoge/shared-kernel';
import type {
  AddressRepository,
  ConfigMutationPort,
  EntityRepository,
  EntityTagRepository,
  NetworkReader,
  TagRepository,
} from '../contracts/repositories';
import {
  Address,
  type AddressRecord,
  Entity,
  type EntityRecord,
  Tag,
  type TagRecord,
} from '../domain/entity';

function unique(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))];
}

export class EntityLabelingService {
  public constructor(
    private readonly entities: EntityRepository,
    private readonly addresses: AddressRepository,
    private readonly tags: TagRepository,
    private readonly entityTags: EntityTagRepository,
    private readonly networks: NetworkReader,
    private readonly configs: ConfigMutationPort,
  ) {}

  public async createEntity(input: {
    data?: Record<string, unknown>;
    description: string;
    id?: string;
    name?: string | null;
    tags?: string[];
  }) {
    if (input.id && (await this.entities.getEntityById(input.id))) {
      throw new ValidationError(`invalid parameter for \`id\`: ${input.id}`);
    }

    if (input.name) {
      const deleted = await this.entities.getEntityByName(input.name, true);
      if (deleted?.isDeleted) {
        throw new TooEarlyError(`too early: entity hasn't been deleted yet: ${input.name}`);
      }
      const duplicate = await this.entities.getEntityByName(input.name);
      if (duplicate) {
        throw new ConflictError(`duplicate found at \`name\`: ${input.name}`);
      }
    }

    const tagRecords = await this.resolveTags(input.tags ?? []);
    const created = await this.entities.createEntity(Entity.create(input).record);
    await this.entityTags.replaceEntityTags(
      created.entityId,
      tagRecords.map((tag) => tag.tagId),
    );

    return this.getEntity(created.id);
  }

  public async updateEntity(
    id: string,
    input: {
      data?: Record<string, unknown>;
      description?: string;
      name?: string | null;
      tags?: string[];
    },
  ): Promise<void> {
    const entity = await this.entities.getEntityById(id);
    if (!entity || entity.isDeleted) {
      throw new NotFoundError();
    }

    if (input.name) {
      const deleted = await this.entities.getEntityByName(input.name, true);
      if (deleted?.isDeleted) {
        throw new TooEarlyError(`too early: entity hasn't been deleted yet: ${input.name}`);
      }
      const duplicate = await this.entities.getEntityByName(input.name);
      if (duplicate && duplicate.id !== entity.id) {
        throw new ConflictError(`duplicate found at \`name\`: ${input.name}`);
      }
    }

    const updated = Entity.rehydrate(entity).update(input);
    await this.entities.updateEntityRecord(updated);

    if (input.tags) {
      const tags = await this.resolveTags(input.tags);
      await this.entityTags.replaceEntityTags(
        entity.entityId,
        tags.map((tag) => tag.tagId),
      );
    }
  }

  public async deleteEntities(ids: string[]): Promise<void> {
    const deleted = await this.entities.softDeleteEntities(ids);
    await this.addresses.softDeleteAddressesByEntityIds(deleted.map((entity) => entity.entityId));
  }

  public async listEntities(offset?: number, limit?: number) {
    const entities = (await this.entities.listEntities(offset, limit)).filter(
      (entity) => !entity.isDeleted,
    );
    const entityIds = entities.map((entity) => entity.entityId);

    const [tagIdMap, tagRecords, addressRecords] = await Promise.all([
      this.entityTags.listEntityTagMap(entityIds),
      this.loadTagsForEntityIds(entityIds),
      this.addresses.listAddressesByEntityIds(entityIds),
    ]);
    const tagMap = buildEntityTagMap(tagIdMap, tagRecords);

    const networkRecords = await this.networks.getActiveNetworksByInternalIds(
      unique(addressRecords.map((address) => String(address.networkId))).map(Number),
    );

    return {
      entities: entities.map((entity) => this.serializeEntity(entity, tagMap, addressRecords)),
      tags: tagRecords.map((tag) => this.serializeTag(tag)),
      addresses: addressRecords
        .filter((address) => !address.isDeleted)
        .map((address) => this.serializeAddress(address)),
      networks: networkRecords.map((network) => ({
        id: network.id,
        name: network.name,
        chainId: network.chainId,
      })),
    };
  }

  public async getEntity(id: string) {
    const entity = await this.entities.getEntityById(id);
    if (!entity || entity.isDeleted) {
      throw new NotFoundError();
    }

    const [tagIdMap, tagRecords, addressRecords] = await Promise.all([
      this.entityTags.listEntityTagMap([entity.entityId]),
      this.loadTagsForEntityIds([entity.entityId]),
      this.addresses.listAddressesByEntityIds([entity.entityId]),
    ]);
    const tagMap = buildEntityTagMap(tagIdMap, tagRecords);

    const networkRecords = await this.networks.getActiveNetworksByInternalIds(
      addressRecords.map((address) => address.networkId),
    );

    return {
      entity: this.serializeEntity(entity, tagMap, addressRecords),
      tags: tagRecords.map((tag) => this.serializeTag(tag)),
      addresses: addressRecords
        .filter((address) => !address.isDeleted)
        .map((address) => this.serializeAddress(address)),
      networks: networkRecords.map((network) => ({
        id: network.id,
        name: network.name,
        chainId: network.chainId,
      })),
    };
  }

  public async createAddresses(input: {
    addresses: Array<{
      address: string;
      data?: Record<string, unknown>;
      description: string;
    }>;
    entity: string;
    network: string;
  }) {
    const entity = await this.entities.getEntityById(input.entity);
    if (!entity || entity.isDeleted) {
      throw new ValidationError(`invalid parameter for \`entity\`: ${input.entity}`);
    }

    const network = await this.networks.getActiveNetworkById(input.network);
    if (!network) {
      throw new ValidationError(`invalid parameter for \`network\`: ${input.network}`);
    }

    const uniqueAddresses = unique(input.addresses.map((address) => address.address));
    if (uniqueAddresses.length !== input.addresses.length) {
      throw new ValidationError('bad request: request contains duplicate addresses');
    }

    const softDeleted = await this.addresses.findAddressesByEntityNetworkAndAddresses(
      entity.entityId,
      network.networkId,
      uniqueAddresses,
      true,
    );
    if (softDeleted.some((address) => address.isDeleted)) {
      throw new TooEarlyError(
        `too early: addresses haven't been deleted yet: ${softDeleted
          .filter((address) => address.isDeleted)
          .map((address) => address.address)
          .join(', ')}`,
      );
    }

    const duplicates = await this.addresses.findAddressesByEntityNetworkAndAddresses(
      entity.entityId,
      network.networkId,
      uniqueAddresses,
      false,
    );
    if (duplicates.length > 0) {
      throw new ConflictError(
        `duplicates found at \`addresses\`: ${duplicates.map((address) => address.address).join(', ')}`,
      );
    }

    const created = await this.addresses.createAddresses(
      input.addresses.map(
        (address) =>
          Address.create({
            address: address.address,
            ...(address.data ? { data: address.data } : {}),
            description: address.description,
            entityId: entity.entityId,
            network: network.id,
            networkId: network.networkId,
          }).record,
      ),
    );

    await Promise.all(
      created.map((address) =>
        this.configs.markNewlyAddedAddress(address.networkId, address.addressId),
      ),
    );

    return created.map((address) => this.serializeAddress(address));
  }

  public async listAddresses(offset?: number, limit?: number) {
    const addresses = (await this.addresses.listAddresses(offset, limit)).filter(
      (address) => !address.isDeleted,
    );
    const networks = await this.networks.getActiveNetworksByInternalIds(
      addresses.map((address) => address.networkId),
    );

    return {
      addresses: addresses.map((address) => this.serializeAddress(address)),
      networks: networks.map((network) => ({
        id: network.id,
        name: network.name,
        chainId: network.chainId,
      })),
    };
  }

  public async getAddress(id: string) {
    const address = await this.addresses.getAddressById(id);
    if (!address || address.isDeleted) {
      throw new NotFoundError();
    }

    const network = await this.networks.getActiveNetworkById(address.network);
    return {
      address: this.serializeAddress(address),
      networks: network
        ? [
            {
              id: network.id,
              name: network.name,
              chainId: network.chainId,
            },
          ]
        : [],
    };
  }

  public async deleteAddresses(ids: string[]): Promise<void> {
    await this.addresses.softDeleteAddresses(ids);
  }

  public async createTag(input: { id?: string; name: string; riskLevel: RiskLevel }) {
    if (input.id && (await this.tags.getTagById(input.id))) {
      throw new ValidationError(`invalid parameter for \`id\`: ${input.id}`);
    }

    if (await this.tags.getTagByName(input.name)) {
      throw new ConflictError(`duplicate found at \`name\`: ${input.name}`);
    }

    const created = await this.tags.createTag(Tag.create(input).record);
    return this.serializeTag(created);
  }

  public async updateTag(
    id: string,
    input: {
      name?: string;
      riskLevel?: RiskLevel;
    },
  ): Promise<void> {
    const tag = await this.tags.getTagById(id);
    if (!tag) {
      throw new NotFoundError();
    }

    if (input.name) {
      const duplicate = await this.tags.getTagByName(input.name);
      if (duplicate && duplicate.id !== tag.id) {
        throw new ConflictError(`duplicate found at \`name\`: ${input.name}`);
      }
    }

    await this.tags.updateTagRecord(Tag.rehydrate(tag).update(input));
  }

  public async listTags(offset?: number, limit?: number) {
    const tags = await this.tags.listTags(offset, limit);
    const joinedEntities = await this.entities.listEntitiesByTagIds(tags.map((tag) => tag.tagId));
    const entities = joinedEntities.map((joined) => joined.entity);
    const addresses = await this.addresses.listAddressesByEntityIds(
      entities.map((entity) => entity.entityId),
    );
    const networks = await this.networks.getActiveNetworksByInternalIds(
      addresses.map((address) => address.networkId),
    );

    const entitiesByTag = new Map<PrimaryId, string[]>();
    for (const joined of joinedEntities) {
      const current = entitiesByTag.get(joined.tagId) ?? [];
      current.push(joined.entity.id);
      entitiesByTag.set(joined.tagId, current);
    }

    return {
      tags: tags.map((tag) => this.serializeTag(tag, entitiesByTag.get(tag.tagId) ?? [])),
      entities: entities.map((entity) =>
        this.serializeEntity(
          entity,
          new Map(),
          addresses.filter((address) => address.entityId === entity.entityId),
        ),
      ),
      addresses: addresses.map((address) => this.serializeAddress(address)),
      networks: networks.map((network) => ({
        id: network.id,
        name: network.name,
        chainId: network.chainId,
      })),
    };
  }

  public async getTag(id: string) {
    const tag = await this.tags.getTagById(id);
    if (!tag) {
      throw new NotFoundError();
    }

    const joinedEntities = await this.entities.listEntitiesByTagIds([tag.tagId]);
    const entities = joinedEntities.map((joined) => joined.entity);
    const addresses = await this.addresses.listAddressesByEntityIds(
      entities.map((entity) => entity.entityId),
    );
    const networks = await this.networks.getActiveNetworksByInternalIds(
      addresses.map((address) => address.networkId),
    );

    return {
      tag: this.serializeTag(
        tag,
        entities.map((entity) => entity.id),
      ),
      entities: entities.map((entity) =>
        this.serializeEntity(
          entity,
          new Map(),
          addresses.filter((address) => address.entityId === entity.entityId),
        ),
      ),
      addresses: addresses.map((address) => this.serializeAddress(address)),
      networks: networks.map((network) => ({
        id: network.id,
        name: network.name,
        chainId: network.chainId,
      })),
    };
  }

  public async deleteTags(ids: string[]): Promise<void> {
    await this.tags.deleteTags(ids);
  }

  public async softDeleteAddressesByNetworkIds(networkIds: PrimaryId[]): Promise<void> {
    await this.addresses.softDeleteAddressesByNetworkIds(networkIds);
  }

  private async resolveTags(tagIds: string[]) {
    const resolved = await Promise.all(tagIds.map((tagId) => this.tags.getTagById(tagId)));
    if (resolved.some((tag) => !tag)) {
      throw new ValidationError(`invalid value(s) for \`tags\`: ${tagIds.join(', ')}`);
    }

    return resolved.filter((tag): tag is NonNullable<typeof tag> => Boolean(tag));
  }

  private async loadTagsForEntityIds(entityIds: PrimaryId[]) {
    const tagMap = await this.entityTags.listEntityTagMap(entityIds);
    const tagIds = [...new Set([...tagMap.values()].flat())];
    return this.tags.listTagsByIds(tagIds);
  }

  private serializeEntity(
    entity: EntityRecord,
    tagMap: Map<PrimaryId, string[]>,
    addresses: AddressRecord[],
  ) {
    return {
      id: entity.id,
      name: entity.name,
      description: entity.description,
      data: entity.data,
      createdAt: entity.createdAt,
      tags: tagMap.get(entity.entityId) ?? [],
      addresses: addresses
        .filter((address) => address.entityId === entity.entityId)
        .map((address) => address.id),
    };
  }

  private serializeAddress(address: AddressRecord) {
    return {
      id: address.id,
      network: address.network,
      address: address.address,
      description: address.description,
      data: address.data,
      createdAt: address.createdAt,
    };
  }

  private serializeTag(tag: TagRecord, entities: string[] = []) {
    return {
      id: tag.id,
      name: tag.name,
      riskLevel: tag.riskLevel,
      createdAt: tag.createdAt,
      entities,
    };
  }
}

function buildEntityTagMap(
  tagIdMap: Map<PrimaryId, PrimaryId[]>,
  tags: TagRecord[],
): Map<PrimaryId, string[]> {
  const tagLookup = new Map(tags.map((tag) => [tag.tagId, tag.id]));
  const result = new Map<PrimaryId, string[]>();

  for (const [entityId, tagIds] of tagIdMap.entries()) {
    result.set(
      entityId,
      tagIds
        .map((tagId) => tagLookup.get(tagId))
        .filter((tagId): tagId is string => Boolean(tagId)),
    );
  }

  return result;
}
