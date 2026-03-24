import {
  ExternalId,
  type PrimaryId,
  type RiskLevel,
  ValidationError,
} from '@onlydoge/shared-kernel';

export interface EntityRecord {
  entityId: PrimaryId;
  id: string;
  name: string | null;
  description: string;
  data: Record<string, unknown>;
  isDeleted: boolean;
  updatedAt: string | null;
  createdAt: string;
}

export interface AddressRecord {
  addressId: PrimaryId;
  entityId: PrimaryId;
  networkId: PrimaryId;
  id: string;
  network: string;
  address: string;
  description: string;
  data: Record<string, unknown>;
  isDeleted: boolean;
  updatedAt: string | null;
  createdAt: string;
}

export interface TagRecord {
  tagId: PrimaryId;
  id: string;
  name: string;
  riskLevel: RiskLevel;
  updatedAt: string | null;
  createdAt: string;
}

export class Entity {
  public readonly record: EntityRecord;

  private constructor(record: EntityRecord) {
    this.record = record;
  }

  public static create(input: {
    data?: Record<string, unknown>;
    description: string;
    id?: string;
    name?: string | null;
  }): Entity {
    if (!input.description.trim()) {
      throw new ValidationError('invalid parameter for `description`: ');
    }

    return new Entity({
      entityId: 0,
      id: input.id
        ? ExternalId.parse(input.id, 'ent').toString()
        : ExternalId.create('ent').toString(),
      name: input.name?.trim() || null,
      description: input.description.trim(),
      data: input.data ?? {},
      isDeleted: false,
      updatedAt: null,
      createdAt: new Date().toISOString(),
    });
  }

  public static rehydrate(record: EntityRecord): Entity {
    return new Entity(record);
  }

  public update(input: {
    data?: Record<string, unknown>;
    description?: string;
    name?: string | null;
  }): EntityRecord {
    return {
      ...this.record,
      name: input.name === undefined ? this.record.name : input.name?.trim() || null,
      description: input.description?.trim() ?? this.record.description,
      data: input.data ?? this.record.data,
      updatedAt: new Date().toISOString(),
    };
  }
}

export class Address {
  public readonly record: AddressRecord;

  private constructor(record: AddressRecord) {
    this.record = record;
  }

  public static create(input: {
    address: string;
    data?: Record<string, unknown>;
    description: string;
    entityId: PrimaryId;
    network: string;
    networkId: PrimaryId;
  }): Address {
    if (!input.address.trim()) {
      throw new ValidationError('invalid parameter for `address`: ');
    }

    return new Address({
      addressId: 0,
      entityId: input.entityId,
      networkId: input.networkId,
      id: ExternalId.create('adr').toString(),
      network: input.network,
      address: input.address.trim(),
      description: input.description.trim(),
      data: input.data ?? {},
      isDeleted: false,
      updatedAt: null,
      createdAt: new Date().toISOString(),
    });
  }

  public static rehydrate(record: AddressRecord): Address {
    return new Address(record);
  }
}

export class Tag {
  public readonly record: TagRecord;

  private constructor(record: TagRecord) {
    this.record = record;
  }

  public static create(input: { id?: string; name: string; riskLevel: RiskLevel }): Tag {
    if (!input.name.trim()) {
      throw new ValidationError('invalid parameter for `name`: ');
    }

    return new Tag({
      tagId: 0,
      id: input.id
        ? ExternalId.parse(input.id, 'tag').toString()
        : ExternalId.create('tag').toString(),
      name: input.name.trim(),
      riskLevel: input.riskLevel,
      updatedAt: null,
      createdAt: new Date().toISOString(),
    });
  }

  public static rehydrate(record: TagRecord): Tag {
    return new Tag(record);
  }

  public update(input: { name?: string; riskLevel?: RiskLevel }): TagRecord {
    return {
      ...this.record,
      name: input.name?.trim() ?? this.record.name,
      riskLevel: input.riskLevel ?? this.record.riskLevel,
      updatedAt: new Date().toISOString(),
    };
  }
}
