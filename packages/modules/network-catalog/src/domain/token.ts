import { ExternalId, type PrimaryId, ValidationError } from '@onlydoge/shared-kernel';

export interface TokenRecord {
  tokenId: PrimaryId;
  networkId: PrimaryId;
  id: string;
  name: string;
  symbol: string;
  address: string;
  decimals: number;
  updatedAt: string | null;
  createdAt: string;
}

export interface CreateTokenInput {
  id?: string;
  networkId: PrimaryId;
  name: string;
  symbol: string;
  address?: string;
  decimals: number;
}

export class Token {
  public readonly record: TokenRecord;

  private constructor(record: TokenRecord) {
    this.record = record;
  }

  public static create(input: CreateTokenInput, nextPrimaryId = 0): Token {
    if (!input.name.trim()) {
      throw new ValidationError('invalid parameter for `name`: ');
    }
    if (!input.symbol.trim()) {
      throw new ValidationError('invalid parameter for `symbol`: ');
    }
    if (!Number.isInteger(input.decimals) || input.decimals < 0) {
      throw new ValidationError(`invalid parameter for \`decimals\`: ${input.decimals}`);
    }

    return new Token({
      tokenId: nextPrimaryId,
      networkId: input.networkId,
      id: input.id
        ? ExternalId.parse(input.id, 'tok').toString()
        : ExternalId.create('tok').toString(),
      name: input.name.trim(),
      symbol: input.symbol.trim(),
      address: input.address?.trim() ?? '',
      decimals: input.decimals,
      updatedAt: null,
      createdAt: new Date().toISOString(),
    });
  }

  public static rehydrate(record: TokenRecord): Token {
    return new Token(record);
  }

  public toResponse(): {
    address: string;
    createdAt: string;
    decimals: number;
    id: string;
    name: string;
    symbol: string;
  } {
    return {
      id: this.record.id,
      name: this.record.name,
      symbol: this.record.symbol,
      address: this.record.address,
      decimals: this.record.decimals,
      createdAt: this.record.createdAt,
    };
  }
}
