import type { ProjectionUtxoOutput } from '@onlydoge/indexing-pipeline';
import type { InvestigationMetadataPort } from '@onlydoge/investigation-query';
import type { PrimaryId, RiskLevel } from '@onlydoge/shared-kernel';

export interface ExplorerActiveNetworkPort {
  listActiveNetworks(): Promise<
    Array<{
      architecture: 'dogecoin' | 'evm';
      blockTime: number;
      chainId: number;
      id: string;
      name: string;
      networkId: PrimaryId;
      rpcEndpoint: string;
      rps: number;
    }>
  >;
}

export interface ExplorerConfigPort {
  getJsonValue<T>(key: string): Promise<T | null>;
}

export interface ExplorerMetadataPort extends InvestigationMetadataPort {}

export interface ExplorerRawBlockPort {
  getPart<T extends Record<string, unknown>>(
    networkId: PrimaryId,
    blockHeight: number,
    part: string,
  ): Promise<T | null>;
}

export interface ExplorerWarehousePort {
  getAddressSummary(
    networkId: PrimaryId,
    address: string,
  ): Promise<{
    balance: string;
    receivedBase: string;
    sentBase: string;
    txCount: number;
    utxoCount: number;
  } | null>;
  getAppliedBlockByHash(
    networkId: PrimaryId,
    blockHash: string,
  ): Promise<{
    blockHash: string;
    blockHeight: number;
  } | null>;
  getTransactionRef(
    networkId: PrimaryId,
    txid: string,
  ): Promise<{
    blockHash: string;
    blockHeight: number;
    blockTime: number;
    txIndex: number;
  } | null>;
  listAddressTransactions(
    networkId: PrimaryId,
    address: string,
    offset?: number,
    limit?: number,
  ): Promise<
    Array<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      receivedBase: string;
      sentBase: string;
      txIndex: number;
      txid: string;
    }>
  >;
  listAddressUtxos(
    networkId: PrimaryId,
    address: string,
    offset?: number,
    limit?: number,
  ): Promise<ProjectionUtxoOutput[]>;
  listAppliedBlocks(
    networkId: PrimaryId,
    offset?: number,
    limit?: number,
  ): Promise<
    Array<{
      blockHash: string;
      blockHeight: number;
    }>
  >;
}

export interface ExplorerLabelRef {
  entity: string;
  name: string | null;
  riskLevel: RiskLevel;
  tags: string[];
}
