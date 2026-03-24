import {
  BlockTime,
  type ChainFamily,
  ExternalId,
  type PrimaryId,
  RpcEndpoint,
  ValidationError,
} from '@onlydoge/shared-kernel';

export interface NetworkRecord {
  networkId: PrimaryId;
  id: string;
  name: string;
  architecture: ChainFamily;
  chainId: number;
  blockTime: number;
  rpcEndpoint: string;
  rps: number;
  isDeleted: boolean;
  updatedAt: string | null;
  createdAt: string;
}

export interface CreateNetworkInput {
  id?: string;
  name: string;
  architecture: ChainFamily;
  chainId?: number;
  blockTime: number;
  rpcEndpoint: string;
  rps?: number;
}

export class Network {
  public readonly record: NetworkRecord;

  private constructor(record: NetworkRecord) {
    this.record = record;
  }

  public static create(input: CreateNetworkInput, nextPrimaryId = 0): Network {
    if (!input.name.trim()) {
      throw new ValidationError('invalid parameter for `name`: ');
    }

    if (!Number.isInteger(input.chainId ?? 0) || (input.chainId ?? 0) < 0) {
      throw new ValidationError(`invalid parameter for \`chainId\`: ${input.chainId ?? 0}`);
    }

    BlockTime.parse(input.blockTime);
    RpcEndpoint.parse(input.rpcEndpoint);

    return new Network({
      networkId: nextPrimaryId,
      id: input.id
        ? ExternalId.parse(input.id, 'net').toString()
        : ExternalId.create('net').toString(),
      name: input.name.trim(),
      architecture: input.architecture,
      chainId: input.chainId ?? 0,
      blockTime: input.blockTime,
      rpcEndpoint: input.rpcEndpoint.trim(),
      rps: input.rps ?? 100,
      isDeleted: false,
      updatedAt: null,
      createdAt: new Date().toISOString(),
    });
  }

  public static rehydrate(record: NetworkRecord): Network {
    return new Network(record);
  }

  public update(
    input: Partial<
      Pick<NetworkRecord, 'architecture' | 'blockTime' | 'chainId' | 'name' | 'rpcEndpoint' | 'rps'>
    >,
  ): NetworkRecord {
    if (input.blockTime !== undefined) {
      BlockTime.parse(input.blockTime);
    }
    if (input.rpcEndpoint !== undefined) {
      RpcEndpoint.parse(input.rpcEndpoint);
    }

    return {
      ...this.record,
      name: input.name?.trim() ?? this.record.name,
      architecture: input.architecture ?? this.record.architecture,
      chainId: input.chainId ?? this.record.chainId,
      blockTime: input.blockTime ?? this.record.blockTime,
      rpcEndpoint: input.rpcEndpoint?.trim() ?? this.record.rpcEndpoint,
      rps: input.rps ?? this.record.rps,
      updatedAt: new Date().toISOString(),
    };
  }

  public markDeleted(): NetworkRecord {
    return {
      ...this.record,
      isDeleted: true,
      updatedAt: new Date().toISOString(),
    };
  }

  public toResponse(): {
    architecture: ChainFamily;
    blockTime: number;
    chainId: number;
    createdAt: string;
    id: string;
    name: string;
    rpcEndpoint: string;
    rps: number;
  } {
    return {
      id: this.record.id,
      name: this.record.name,
      architecture: this.record.architecture,
      chainId: this.record.chainId,
      blockTime: this.record.blockTime,
      rpcEndpoint: RpcEndpoint.parse(this.record.rpcEndpoint).maskAuth(),
      rps: this.record.rps,
      createdAt: this.record.createdAt,
    };
  }
}
