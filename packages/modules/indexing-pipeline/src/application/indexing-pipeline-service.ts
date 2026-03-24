import { randomUUID } from 'node:crypto';

import { nowIsoString, sleep } from '@onlydoge/shared-kernel';

import type {
  BlockchainRpcPort,
  CoordinatorConfigPort,
  IndexedNetworkPort,
  ProjectionLinkSeedPort,
  ProjectionWarehousePort,
  RawBlockStoragePort,
} from '../contracts/ports';
import {
  configKeyBlockHeight,
  configKeyIndexerProcessProgress,
  configKeyIndexerProcessTail,
  configKeyIndexerSyncProgress,
  configKeyIndexerSyncTail,
  configKeyPrimary,
} from '../domain/config-keys';
import type { BlockProjectionBatch, ProjectionUtxoOutput } from '../domain/projection-models';
import { DogecoinBlockProjector } from './dogecoin-block-projector';
import { EvmBlockProjector } from './evm-block-projector';
import { SourceLinkProjector } from './source-link-projector';

interface IndexingPipelineSettings {
  leaseHeartbeatIntervalMs: number;
  projectTimeoutMs: number;
  networkConcurrency: number;
  projectWindow: number;
  relinkBatchSize: number;
  relinkConcurrency: number;
  relinkFrontierBatch: number;
  relinkTimeoutMs: number;
  syncConcurrency: number;
  syncTimeoutMs: number;
  syncWindow: number;
}

interface PrimaryLease {
  heartbeatAt: string;
  instanceId: string;
}

type IndexedNetwork = Awaited<ReturnType<IndexedNetworkPort['listActiveNetworks']>>[number];

const defaultSettings: IndexingPipelineSettings = {
  leaseHeartbeatIntervalMs: 5_000,
  networkConcurrency: 2,
  syncWindow: 64,
  syncConcurrency: 8,
  syncTimeoutMs: 120_000,
  projectWindow: 32,
  projectTimeoutMs: 120_000,
  relinkBatchSize: 64,
  relinkConcurrency: 4,
  relinkFrontierBatch: 128,
  relinkTimeoutMs: 120_000,
};

export class IndexingPipelineService {
  private readonly instanceId = randomUUID();
  private readonly dogecoinProjector: DogecoinBlockProjector;
  private readonly evmProjector = new EvmBlockProjector();
  private readonly sourceLinkProjector: SourceLinkProjector;
  private lastIdleReason: string | null = null;
  private readonly leaseTimeoutMs = 15_000;

  public constructor(
    private readonly configs: CoordinatorConfigPort,
    private readonly networks: IndexedNetworkPort,
    private readonly seeds: ProjectionLinkSeedPort,
    private readonly rawBlocks: RawBlockStoragePort,
    private readonly rpc: BlockchainRpcPort,
    private readonly warehouse: ProjectionWarehousePort,
    private readonly settings: IndexingPipelineSettings = defaultSettings,
  ) {
    this.dogecoinProjector = new DogecoinBlockProjector(warehouse);
    this.sourceLinkProjector = new SourceLinkProjector(warehouse);
  }

  public async start(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      const didWork = await this.runOnce();
      if (!didWork) {
        await sleep(1000);
      }
    }
  }

  public async runOnce(): Promise<boolean> {
    const isPrimary = await this.leaseLeadership();
    if (!isPrimary) {
      this.logIdleOnce('not-primary');
      return false;
    }

    return this.withLeaseHeartbeat(async () => {
      const networks = await this.networks.listActiveNetworks();
      if (networks.length === 0) {
        this.logIdleOnce('no-active-networks');
        return false;
      }

      const latestHeights = await mapWithConcurrency(
        networks,
        this.settings.networkConcurrency,
        async (network) => {
          try {
            const latestBlockHeight = await this.rpc.getBlockHeight(network);
            await this.configs.setJsonValue(
              configKeyBlockHeight(network.networkId),
              latestBlockHeight,
            );

            return { latestBlockHeight, network };
          } catch (error) {
            console.error(
              `[onlydoge] sync height failed network=${network.id} error=${formatError(error)}`,
            );
            return null;
          }
        },
      );

      const didWorkByNetwork = await mapWithConcurrency(
        latestHeights.filter((item): item is NonNullable<typeof item> => Boolean(item)),
        this.settings.networkConcurrency,
        async ({ latestBlockHeight, network }) => {
          await this.refreshLeadership();

          try {
            return await this.processNetworkCycle(network, latestBlockHeight);
          } catch (error) {
            console.error(
              `[onlydoge] network cycle failed network=${network.id} error=${formatError(error)}`,
            );
            return false;
          }
        },
      );

      const didWork = didWorkByNetwork.some(Boolean);
      if (!didWork) {
        this.logIdleOnce('caught-up');
      } else {
        this.lastIdleReason = null;
      }

      return didWork;
    });
  }

  public async relinkNewAddress(networkId: number, addressId: number): Promise<void> {
    const address = await this.seeds.getTrackedAddress(networkId, addressId);
    if (!address) {
      return;
    }

    await this.seeds.markPendingRelinkSeed(networkId, addressId);
  }

  private async processNetworkCycle(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    let didWork = false;

    if (
      await this.runNetworkPhase(network, 'sync', this.settings.syncTimeoutMs, () =>
        this.syncNetworkWindow(network, latestBlockHeight),
      )
    ) {
      didWork = true;
    }
    await this.refreshLeadership();

    if (
      await this.runNetworkPhase(network, 'project', this.settings.projectTimeoutMs, () =>
        this.projectNetworkWindow(network, latestBlockHeight),
      )
    ) {
      didWork = true;
    }
    await this.refreshLeadership();

    if (
      await this.runNetworkPhase(network, 'relink', this.settings.relinkTimeoutMs, () =>
        this.processPendingRelinkSeeds(network.networkId),
      )
    ) {
      didWork = true;
    }

    return didWork;
  }

  private async leaseLeadership(): Promise<boolean> {
    const current = await this.configs.getJsonValue<PrimaryLease | string>(configKeyPrimary());
    const nextLease = this.createLease();

    if (!current) {
      const claimed = await this.configs.compareAndSwapJsonValue(
        configKeyPrimary(),
        null,
        nextLease,
      );
      if (claimed) {
        console.log(`[onlydoge] indexer primary instance=${this.instanceId}`);
      }
      return claimed;
    }

    const lease = this.toPrimaryLease(current);
    if (lease?.instanceId === this.instanceId) {
      await this.configs.setJsonValue(configKeyPrimary(), nextLease);
      return true;
    }

    if (this.isLeaseExpired(lease)) {
      const claimed = await this.configs.compareAndSwapJsonValue(
        configKeyPrimary(),
        current,
        nextLease,
      );
      if (claimed) {
        console.log(
          `[onlydoge] indexer primary instance=${this.instanceId} replaced-stale-primary`,
        );
      }
      return claimed;
    }

    return false;
  }

  private async refreshLeadership(): Promise<void> {
    await this.configs.setJsonValue(configKeyPrimary(), this.createLease());
  }

  private async withLeaseHeartbeat<T>(work: () => Promise<T>): Promise<T> {
    const interval = setInterval(() => {
      void this.refreshLeadership().catch((error) => {
        console.error(`[onlydoge] primary heartbeat failed error=${formatError(error)}`);
      });
    }, this.settings.leaseHeartbeatIntervalMs);

    interval.unref?.();

    try {
      return await work();
    } finally {
      clearInterval(interval);
    }
  }

  private async runNetworkPhase(
    network: IndexedNetwork,
    phase: 'sync' | 'project' | 'relink',
    timeoutMs: number,
    work: () => Promise<boolean>,
  ): Promise<boolean> {
    try {
      return await withTimeout(work(), timeoutMs, `${phase} timed out after ${timeoutMs}ms`);
    } catch (error) {
      console.error(`[onlydoge] ${phase} failed network=${network.id} error=${formatError(error)}`);
      return false;
    }
  }

  private async syncNetworkWindow(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    const currentSyncTail =
      (await this.configs.getJsonValue<number>(configKeyIndexerSyncTail(network.networkId))) ?? -1;
    if (currentSyncTail >= latestBlockHeight) {
      await this.configs.setJsonValue(
        configKeyIndexerSyncProgress(network.networkId),
        this.toProgress(currentSyncTail, latestBlockHeight),
      );
      return false;
    }

    const windowEnd = Math.min(latestBlockHeight, currentSyncTail + this.settings.syncWindow);
    const heights = range(currentSyncTail + 1, windowEnd);
    const storedHeights = new Set<number>();

    await mapWithConcurrency(heights, this.settings.syncConcurrency, async (blockHeight) => {
      try {
        const snapshot = await this.rpc.getBlockSnapshot(network, blockHeight);
        await this.rawBlocks.putPart(network.networkId, blockHeight, 'block', snapshot);
        storedHeights.add(blockHeight);
      } catch (error) {
        console.error(
          `[onlydoge] sync failed network=${network.id} block=${blockHeight} error=${formatError(error)}`,
        );
      }
    });

    let contiguousTail = currentSyncTail;
    for (const blockHeight of heights) {
      if (!storedHeights.has(blockHeight)) {
        break;
      }

      contiguousTail = blockHeight;
    }

    await this.configs.setJsonValue(
      configKeyIndexerSyncProgress(network.networkId),
      this.toProgress(contiguousTail, latestBlockHeight),
    );

    if (contiguousTail <= currentSyncTail) {
      return false;
    }

    await this.configs.setJsonValue(configKeyIndexerSyncTail(network.networkId), contiguousTail);
    console.log(
      `[onlydoge] synced network=${network.id} blocks=${currentSyncTail + 1}-${contiguousTail} latest=${latestBlockHeight}`,
    );

    return true;
  }

  private async projectNetworkWindow(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    const currentSyncTail =
      (await this.configs.getJsonValue<number>(configKeyIndexerSyncTail(network.networkId))) ?? -1;
    const currentProcessTail =
      (await this.configs.getJsonValue<number>(configKeyIndexerProcessTail(network.networkId))) ??
      -1;
    if (currentProcessTail >= currentSyncTail) {
      await this.configs.setJsonValue(
        configKeyIndexerProcessProgress(network.networkId),
        this.toProgress(currentProcessTail, latestBlockHeight),
      );
      return false;
    }

    const windowEnd = Math.min(currentSyncTail, currentProcessTail + this.settings.projectWindow);
    const heights = range(currentProcessTail + 1, windowEnd);
    const snapshots = await mapWithConcurrency(
      heights,
      Math.min(this.settings.syncConcurrency, heights.length),
      async (blockHeight) => {
        const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(
          network.networkId,
          blockHeight,
          'block',
        );
        if (!snapshot) {
          throw new Error(
            `missing stored snapshot for network=${network.networkId} block=${blockHeight}`,
          );
        }

        return [blockHeight, snapshot] as const;
      },
    );
    const snapshotsByHeight = new Map(snapshots);
    const orderedSnapshots = heights.map((blockHeight) => {
      const snapshot = snapshotsByHeight.get(blockHeight);
      if (!snapshot) {
        throw new Error(
          `missing snapshot in memory for network=${network.networkId} block=${blockHeight}`,
        );
      }

      return snapshot;
    });
    let recoveredProcessTail = currentProcessTail;
    let recoveryIndex = 0;
    for (const [index, snapshot] of orderedSnapshots.entries()) {
      const blockHeight = heights[index] ?? currentProcessTail;
      const blockHash = this.readSnapshotBlockHash(snapshot, network.architecture);
      const alreadyApplied = await this.warehouse.hasAppliedBlock(
        network.networkId,
        blockHeight,
        blockHash,
      );
      if (!alreadyApplied) {
        recoveryIndex = index;
        break;
      }

      recoveredProcessTail = blockHeight;
      recoveryIndex = index + 1;
    }

    if (recoveredProcessTail > currentProcessTail) {
      await this.configs.setJsonValue(
        configKeyIndexerProcessTail(network.networkId),
        recoveredProcessTail,
      );
      await this.configs.setJsonValue(
        configKeyIndexerProcessProgress(network.networkId),
        this.toProgress(recoveredProcessTail, latestBlockHeight),
      );
      console.log(
        `[onlydoge] recovered applied blocks network=${network.id} blocks=${currentProcessTail + 1}-${recoveredProcessTail} latest=${latestBlockHeight}`,
      );

      if (recoveryIndex >= orderedSnapshots.length) {
        return true;
      }
    }

    const snapshotsToProject = orderedSnapshots.slice(recoveryIndex);
    if (snapshotsToProject.length === 0) {
      return recoveredProcessTail > currentProcessTail;
    }

    const projections = await this.projectWindow(network, snapshotsToProject);
    await this.warehouse.applyProjectionWindow(projections);
    await this.markImpactedSeedsPendingRelink(network.networkId, projections);
    await this.configs.setJsonValue(configKeyIndexerProcessTail(network.networkId), windowEnd);
    await this.configs.setJsonValue(
      configKeyIndexerProcessProgress(network.networkId),
      this.toProgress(windowEnd, latestBlockHeight),
    );
    console.log(
      `[onlydoge] projected network=${network.id} blocks=${currentProcessTail + 1}-${windowEnd} latest=${latestBlockHeight}`,
    );

    return true;
  }

  private async projectWindow(
    network: IndexedNetwork,
    snapshots: Record<string, unknown>[],
  ): Promise<BlockProjectionBatch[]> {
    if (network.architecture === 'evm') {
      return snapshots.map((snapshot) => this.evmProjector.project(network.networkId, snapshot));
    }

    const knownOutputKeys = new Set<string>();
    const externalOutputKeys = new Set<string>();
    for (const snapshot of snapshots) {
      for (const outputKey of this.dogecoinProjector.collectExternalOutputKeys(
        snapshot,
        knownOutputKeys,
      )) {
        externalOutputKeys.add(outputKey);
      }
    }

    const persistedOutputs = await this.warehouse.getUtxoOutputs(network.networkId, [
      ...externalOutputKeys,
    ]);
    const localOutputs = new Map<string, ProjectionUtxoOutput>();
    const projections: BlockProjectionBatch[] = [];
    for (const snapshot of snapshots) {
      projections.push(
        await this.dogecoinProjector.project(network.networkId, snapshot, {
          localOutputs,
          persistedOutputs,
        }),
      );
    }

    return projections;
  }

  private async markImpactedSeedsPendingRelink(
    networkId: number,
    projections: BlockProjectionBatch[],
  ): Promise<void> {
    const fromAddresses = [
      ...new Set(
        projections.flatMap((projection) =>
          projection.directLinkDeltas.map(
            (delta: BlockProjectionBatch['directLinkDeltas'][number]) => delta.fromAddress,
          ),
        ),
      ),
    ];
    if (fromAddresses.length === 0) {
      return;
    }

    const [trackedAddresses, reachedSeedIds] = await Promise.all([
      this.seeds.listTrackedAddressesByValues(networkId, fromAddresses),
      this.warehouse.listSourceSeedIdsReachingAddresses(networkId, fromAddresses),
    ]);
    const seedIds = [
      ...new Set([...trackedAddresses.map((address) => address.addressId), ...reachedSeedIds]),
    ];
    if (seedIds.length === 0) {
      return;
    }

    await Promise.all(
      seedIds.map((addressId) => this.seeds.markPendingRelinkSeed(networkId, addressId)),
    );
  }

  private async processPendingRelinkSeeds(networkId: number): Promise<boolean> {
    const pendingSeeds = await this.seeds.listPendingRelinkSeeds(networkId);
    if (pendingSeeds.length === 0) {
      return false;
    }

    const batch = pendingSeeds.slice(0, this.settings.relinkBatchSize);
    let relinkedCount = 0;
    await mapWithConcurrency(batch, this.settings.relinkConcurrency, async (seed) => {
      try {
        await this.sourceLinkProjector.rebuild(networkId, seed, {
          frontierBatchSize: this.settings.relinkFrontierBatch,
        });
        await this.seeds.clearPendingRelinkSeed(networkId, seed.addressId);
        relinkedCount += 1;
      } catch (error) {
        console.error(
          `[onlydoge] relink failed network=${networkId} address=${seed.address} error=${formatError(error)}`,
        );
      }
    });

    if (relinkedCount === 0) {
      return false;
    }

    console.log(`[onlydoge] relinked network=${networkId} addresses=${relinkedCount}`);
    return true;
  }

  private toProgress(tail: number, latestBlockHeight: number): number {
    if (latestBlockHeight < 0 || tail < 0) {
      return 0;
    }

    return (tail + 1) / (latestBlockHeight + 1);
  }

  private logIdleOnce(reason: string): void {
    if (this.lastIdleReason === reason) {
      return;
    }

    this.lastIdleReason = reason;
    console.log(`[onlydoge] indexer idle reason=${reason}`);
  }

  private createLease(): PrimaryLease {
    return {
      instanceId: this.instanceId,
      heartbeatAt: nowIsoString(),
    };
  }

  private isLeaseExpired(lease: PrimaryLease | null): boolean {
    if (!lease) {
      return true;
    }

    const heartbeatAt = new Date(lease.heartbeatAt).getTime();
    if (Number.isNaN(heartbeatAt)) {
      return true;
    }

    return Date.now() - heartbeatAt > this.leaseTimeoutMs;
  }

  private toPrimaryLease(value: PrimaryLease | string): PrimaryLease | null {
    if (typeof value === 'string') {
      return null;
    }

    if (
      typeof value === 'object' &&
      value !== null &&
      typeof value.instanceId === 'string' &&
      typeof value.heartbeatAt === 'string'
    ) {
      return value;
    }

    return null;
  }

  private readSnapshotBlockHash(
    snapshot: Record<string, unknown>,
    architecture: 'dogecoin' | 'evm',
  ): string {
    const block = snapshot.block;
    if (!block || typeof block !== 'object' || Array.isArray(block)) {
      throw new Error('invalid snapshot block payload');
    }

    const hash = 'hash' in block ? block.hash : undefined;
    if (typeof hash !== 'string' || !hash.trim()) {
      throw new Error('missing snapshot block hash');
    }

    return architecture === 'evm' ? hash.trim().toLowerCase() : hash.trim();
  }
}

async function mapWithConcurrency<T, R>(
  values: T[],
  concurrency: number,
  worker: (value: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(values.length);
  let nextIndex = 0;
  const workerCount = Math.max(1, Math.min(concurrency, values.length || 1));

  await Promise.all(
    Array.from({ length: workerCount }, async () => {
      while (true) {
        const currentIndex = nextIndex;
        nextIndex += 1;
        if (currentIndex >= values.length) {
          return;
        }

        const value = values[currentIndex];
        if (value === undefined) {
          return;
        }

        results[currentIndex] = await worker(value, currentIndex);
      }
    }),
  );

  return results;
}

function formatError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | null = null;

  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), timeoutMs);
        timer.unref?.();
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

function range(start: number, end: number): number[] {
  if (end < start) {
    return [];
  }

  return Array.from({ length: end - start + 1 }, (_, index) => start + index);
}
