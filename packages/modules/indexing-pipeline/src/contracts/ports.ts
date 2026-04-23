import type { PrimaryId } from '@onlydoge/shared-kernel';

import type {
  BlockProjectionBatch,
  DirectLinkRecord,
  ProjectionBalanceCursor,
  ProjectionBalanceSnapshot,
  ProjectionCurrentBalancePage,
  ProjectionCurrentUtxoPage,
  ProjectionDirectLinkBatch,
  ProjectionFactWindow,
  ProjectionPageRequestContext,
  ProjectionStateBootstrapSnapshot,
  ProjectionUtxoOutput,
  SourceLinkRecord,
  TrackedAddress,
} from '../domain/projection-models';

export interface CoordinatorConfigPort {
  compareAndSwapJsonValue<T>(key: string, expectedValue: T | null, nextValue: T): Promise<boolean>;
  deleteByPrefix(prefix: string): Promise<void>;
  getJsonValue<T>(key: string): Promise<T | null>;
  setJsonValue<T>(key: string, value: T): Promise<void>;
}

export interface IndexedNetworkPort {
  listActiveNetworks(): Promise<
    Array<{
      architecture: 'dogecoin' | 'evm';
      blockTime: number;
      id: string;
      networkId: PrimaryId;
      rpcEndpoint: string;
      rps: number;
    }>
  >;
}

export interface RawBlockStoragePort {
  getPart<T extends Record<string, unknown>>(
    networkId: PrimaryId,
    blockHeight: number,
    part: string,
  ): Promise<T | null>;
  putPart(
    networkId: PrimaryId,
    blockHeight: number,
    part: string,
    payload: Record<string, unknown>,
  ): Promise<void>;
}

export interface BlockchainRpcPort {
  getBlockHeight(network: {
    architecture: 'dogecoin' | 'evm';
    rpcEndpoint: string;
    rps: number;
  }): Promise<number>;
  getBlockSnapshot(
    network: {
      architecture: 'dogecoin' | 'evm';
      rpcEndpoint: string;
      rps: number;
    },
    blockHeight: number,
  ): Promise<Record<string, unknown>>;
}

export interface ProjectionWarehousePort {
  applyProjectionWindow(batches: BlockProjectionBatch[]): Promise<void>;
  hasAppliedBlock(networkId: PrimaryId, blockHeight: number, blockHash: string): Promise<boolean>;
  listAppliedBlockSet(
    networkId: PrimaryId,
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>>;
  getUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<Map<string, ProjectionUtxoOutput>>;
  listDirectLinksFromAddresses(
    networkId: PrimaryId,
    fromAddresses: string[],
  ): Promise<DirectLinkRecord[]>;
  listSourceSeedIdsReachingAddresses(
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<PrimaryId[]>;
  replaceSourceLinks(
    networkId: PrimaryId,
    sourceAddressId: PrimaryId,
    rows: SourceLinkRecord[],
  ): Promise<void>;
}

export interface ProjectionStateStorePort {
  applyDirectLinkDeltasWindow(batches: ProjectionDirectLinkBatch[]): Promise<void>;
  applyProjectionWindow(batches: BlockProjectionBatch[]): Promise<void>;
  clearProjectionBootstrapState(networkId: PrimaryId): Promise<void>;
  finalizeProjectionBootstrap(networkId: PrimaryId, processTail: number): Promise<void>;
  getCurrentAddressSummary(
    networkId: PrimaryId,
    address: string,
  ): Promise<{
    balance: string;
    utxoCount: number;
  } | null>;
  getBalanceSnapshots(
    networkId: PrimaryId,
    keys: Array<{
      address: string;
      assetAddress: string;
    }>,
  ): Promise<Map<string, ProjectionBalanceSnapshot>>;
  getDirectLinkSnapshots(
    networkId: PrimaryId,
    keys: Array<{
      assetAddress: string;
      fromAddress: string;
      toAddress: string;
    }>,
  ): Promise<Map<string, DirectLinkRecord>>;
  getDistinctLinksByAddresses(addresses: string[]): Promise<
    Array<{
      fromAddress: string;
      networkId: PrimaryId;
      toAddress: string;
      transferCount: number;
    }>
  >;
  getBalancesByAddresses(addresses: string[]): Promise<
    Array<{
      assetAddress: string;
      balance: string;
      networkId: PrimaryId;
    }>
  >;
  getProjectionBootstrapTail(networkId: PrimaryId): Promise<number | null>;
  getUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<Map<string, ProjectionUtxoOutput>>;
  hasAppliedBlock(networkId: PrimaryId, blockHeight: number, blockHash: string): Promise<boolean>;
  hasProjectionState(networkId: PrimaryId): Promise<boolean>;
  importProjectionStateSnapshot(
    networkId: PrimaryId,
    snapshot: ProjectionStateBootstrapSnapshot,
    processTail: number,
  ): Promise<void>;
  listDirectLinksFromAddresses(
    networkId: PrimaryId,
    fromAddresses: string[],
  ): Promise<DirectLinkRecord[]>;
  listAddressUtxos(
    networkId: PrimaryId,
    address: string,
    offset?: number,
    limit?: number,
  ): Promise<ProjectionUtxoOutput[]>;
  listAppliedBlockSet(
    networkId: PrimaryId,
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>>;
  listSourceSeedIdsReachingAddresses(
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<PrimaryId[]>;
  replaceSourceLinks(
    networkId: PrimaryId,
    sourceAddressId: PrimaryId,
    rows: SourceLinkRecord[],
  ): Promise<void>;
  upsertProjectionBootstrapBalances(rows: ProjectionBalanceSnapshot[]): Promise<void>;
  upsertProjectionBootstrapUtxoOutputs(rows: ProjectionUtxoOutput[]): Promise<void>;
}

export interface ProjectionFactWarehousePort {
  applyProjectionFacts(window: ProjectionFactWindow): Promise<void>;
  exportProjectionStateSnapshot(networkId: PrimaryId): Promise<ProjectionStateBootstrapSnapshot>;
  getAppliedBlockTail(networkId: PrimaryId): Promise<number | null>;
  hasAppliedBlock(networkId: PrimaryId, blockHeight: number, blockHash: string): Promise<boolean>;
  listCurrentBalancesPage(
    networkId: PrimaryId,
    cursor: ProjectionBalanceCursor | null,
    limit: number,
    context?: ProjectionPageRequestContext,
  ): Promise<ProjectionCurrentBalancePage>;
  listCurrentUtxoOutputsPage(
    networkId: PrimaryId,
    cursorOutputKey: string | null,
    limit: number,
    context?: ProjectionPageRequestContext,
  ): Promise<ProjectionCurrentUtxoPage>;
  listAppliedBlockSet(
    networkId: PrimaryId,
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>>;
}

export interface ProjectionLinkSeedPort {
  clearPendingRelinkSeed(networkId: PrimaryId, addressId: PrimaryId): Promise<void>;
  getTrackedAddress(networkId: PrimaryId, addressId: PrimaryId): Promise<TrackedAddress | null>;
  listPendingRelinkSeeds(networkId: PrimaryId): Promise<TrackedAddress[]>;
  listTrackedAddresses(networkId: PrimaryId): Promise<TrackedAddress[]>;
  listTrackedAddressesByValues(
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<TrackedAddress[]>;
  markPendingRelinkSeed(networkId: PrimaryId, addressId: PrimaryId): Promise<void>;
}
