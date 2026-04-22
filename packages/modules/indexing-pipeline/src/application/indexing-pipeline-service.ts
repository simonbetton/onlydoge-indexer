import { randomUUID } from 'node:crypto';

import { nowIsoString, sleep } from '@onlydoge/shared-kernel';

import type {
  BlockchainRpcPort,
  CoordinatorConfigPort,
  IndexedNetworkPort,
  ProjectionFactWarehousePort,
  ProjectionLinkSeedPort,
  ProjectionStateStorePort,
  RawBlockStoragePort,
} from '../contracts/ports';
import { addAmountBase, formatAmountBase, parseAmountBase } from '../domain/amounts';
import {
  configKeyBlockHeight,
  configKeyIndexerFactProgress,
  configKeyIndexerFactTail,
  configKeyIndexerProcessProgress,
  configKeyIndexerProcessTail,
  configKeyIndexerSyncProgress,
  configKeyIndexerSyncTail,
  configKeyPrimary,
  configKeyProjectionBootstrapCursorBalance,
  configKeyProjectionBootstrapCursorUtxo,
  configKeyProjectionBootstrapPhase,
  configKeyProjectionBootstrapStartedAt,
  configKeyProjectionBootstrapTail,
  configKeyProjectionBootstrapTargetTail,
} from '../domain/config-keys';
import type {
  BlockProjectionBatch,
  DirectLinkRecord,
  ProjectionBalanceCursor,
  ProjectionBalanceSnapshot,
  ProjectionDirectLinkBatch,
  ProjectionFactWindow,
  ProjectionUtxoOutput,
} from '../domain/projection-models';
import { DogecoinBlockProjector } from './dogecoin-block-projector';
import { EvmBlockProjector } from './evm-block-projector';
import { SourceLinkProjector } from './source-link-projector';

interface IndexingPipelineSettings {
  dogecoinTransferMaxEdges: number;
  dogecoinTransferMaxInputAddresses: number;
  factTimeoutMs: number;
  factWindow: number;
  leaseHeartbeatIntervalMs: number;
  networkConcurrency: number;
  projectTargetMs: number;
  projectTimeoutMs: number;
  projectWindow: number;
  projectWindowMax: number;
  projectWindowMin: number;
  relinkBacklogThreshold: number;
  relinkBatchSize: number;
  relinkConcurrency: number;
  relinkFrontierBatch: number;
  relinkTipDistance: number;
  relinkTimeoutMs: number;
  syncBacklogHighWatermark: number;
  syncBacklogLowWatermark: number;
  syncConcurrency: number;
  syncTargetMs: number;
  syncTimeoutMs: number;
  syncWindow: number;
  syncWindowMax: number;
  syncWindowMin: number;
}

interface PrimaryLease {
  heartbeatAt: string;
  instanceId: string;
}

interface PhaseTuningState {
  successStreak: number;
  window: number;
}

interface PhaseWorkResult {
  didWork: boolean;
  metrics?: Record<string, number | string>;
  endBlock?: number;
  pendingCount?: number;
  startBlock?: number;
  workItems: number;
}

interface PhaseExecutionResult extends PhaseWorkResult {
  durationMs: number;
  error?: unknown;
}

interface BacklogState {
  backlog: number;
  latestBlockHeight: number;
  processTail: number;
  syncTail: number;
}

interface BootstrapStatus {
  bootstrapTail: number | null;
  phase: BootstrapPhase | null;
  required: boolean;
  startedAtMs: number | null;
  targetTail: number | null;
}

type IndexedNetwork = Awaited<ReturnType<IndexedNetworkPort['listActiveNetworks']>>[number];
type BootstrapPhase = 'balances' | 'done' | 'utxos';
type PhaseName = 'bootstrap' | 'facts' | 'project-state' | 'relink' | 'sync';
type ProjectionMode = 'facts' | 'state';

const bootstrapUtxoChunkSize = 1_000;
const bootstrapBalanceChunkSize = 5_000;

const defaultSettings: IndexingPipelineSettings = {
  dogecoinTransferMaxInputAddresses: 64,
  dogecoinTransferMaxEdges: 1024,
  factWindow: 64,
  factTimeoutMs: 300_000,
  leaseHeartbeatIntervalMs: 5_000,
  networkConcurrency: 2,
  syncWindow: 32,
  syncWindowMin: 32,
  syncWindowMax: 256,
  syncConcurrency: 4,
  syncTargetMs: 15_000,
  syncTimeoutMs: 120_000,
  projectWindow: 8,
  projectWindowMin: 2,
  projectWindowMax: 16,
  projectTargetMs: 30_000,
  projectTimeoutMs: 120_000,
  relinkBatchSize: 16,
  relinkConcurrency: 2,
  relinkFrontierBatch: 32,
  relinkBacklogThreshold: 256,
  relinkTipDistance: 512,
  relinkTimeoutMs: 120_000,
  syncBacklogHighWatermark: 2_048,
  syncBacklogLowWatermark: 512,
};

export class IndexingPipelineService {
  public readonly warehouse: Pick<ProjectionStateStorePort, 'applyProjectionWindow'>;
  private readonly instanceId = randomUUID();
  private readonly dogecoinFactProjector: DogecoinBlockProjector;
  private readonly dogecoinStateProjector: DogecoinBlockProjector;
  private readonly evmProjector = new EvmBlockProjector();
  private readonly sourceLinkProjector: SourceLinkProjector;
  private readonly latestHeights = new Map<number, number>();
  private readonly phaseSkipReasons = new Map<string, string>();
  private readonly projectTuning = new Map<number, PhaseTuningState>();
  private readonly syncTuning = new Map<number, PhaseTuningState>();
  private readonly syncPausedNetworks = new Set<number>();
  private readonly bootstrapInFlight = new Map<number, Promise<void>>();
  private readonly syncInFlight = new Map<number, Promise<void>>();
  private readonly factInFlight = new Map<number, Promise<void>>();
  private readonly projectInFlight = new Map<number, Promise<void>>();
  private readonly relinkInFlight = new Map<number, Promise<void>>();
  private lastIdleReason: string | null = null;
  private readonly leaseTimeoutMs = 15_000;
  private readonly latestHeightRefreshMs = 5_000;
  private readonly workerIdleMs = 250;

  public constructor(
    private readonly configs: CoordinatorConfigPort,
    private readonly networks: IndexedNetworkPort,
    private readonly seeds: ProjectionLinkSeedPort,
    private readonly rawBlocks: RawBlockStoragePort,
    private readonly rpc: BlockchainRpcPort,
    private readonly projectStateStore: ProjectionStateStorePort,
    private readonly stateStore: ProjectionStateStorePort,
    private readonly factWarehouse: ProjectionFactWarehousePort &
      Pick<
        ProjectionStateStorePort,
        'getBalanceSnapshots' | 'getDirectLinkSnapshots' | 'getUtxoOutputs'
      >,
    private readonly settings: IndexingPipelineSettings = defaultSettings,
  ) {
    this.warehouse = projectStateStore;
    this.dogecoinStateProjector = new DogecoinBlockProjector(projectStateStore);
    this.dogecoinFactProjector = new DogecoinBlockProjector(stateStore);
    this.sourceLinkProjector = new SourceLinkProjector(stateStore);
  }

  public async start(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      const isPrimary = await this.leaseLeadership();
      if (!isPrimary) {
        this.logIdleOnce('not-primary');
        await sleep(1_000);
        continue;
      }

      try {
        await this.withLeaseHeartbeat(() => this.runWorkerLoops(signal));
      } catch (error) {
        console.error(`[onlydoge] indexer worker set failed error=${formatError(error)}`);
        await sleep(1_000);
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
      const activeNetworks = await this.networks.listActiveNetworks();
      if (activeNetworks.length === 0) {
        this.logIdleOnce('no-active-networks');
        return false;
      }

      const latestHeights = await this.refreshNetworkHeights(activeNetworks);
      const didWorkByNetwork = await mapWithConcurrency(
        latestHeights.filter((item): item is NonNullable<typeof item> => Boolean(item)),
        this.settings.networkConcurrency,
        async ({ latestBlockHeight, network }) => {
          let didWork = false;
          const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);

          if (backlog.backlog > this.settings.syncBacklogLowWatermark) {
            didWork = (await this.bootstrapNetworkPhase(network, latestBlockHeight)) || didWork;
            didWork = (await this.projectNetworkPhase(network, latestBlockHeight)) || didWork;
            didWork = (await this.factNetworkPhase(network, latestBlockHeight)) || didWork;
            if (this.canRunRelink(backlog)) {
              didWork = (await this.relinkNetworkPhase(network, latestBlockHeight)) || didWork;
            }
            return didWork;
          }

          didWork = (await this.bootstrapNetworkPhase(network, latestBlockHeight)) || didWork;
          didWork = (await this.syncNetworkPhase(network, latestBlockHeight)) || didWork;
          didWork = (await this.projectNetworkPhase(network, latestBlockHeight)) || didWork;
          didWork = (await this.factNetworkPhase(network, latestBlockHeight)) || didWork;
          if (
            this.canRunRelink(await this.readBacklogState(network.networkId, latestBlockHeight))
          ) {
            didWork = (await this.relinkNetworkPhase(network, latestBlockHeight)) || didWork;
          }

          return didWork;
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

  private async runWorkerLoops(signal?: AbortSignal): Promise<void> {
    await Promise.all([
      this.runLatestHeightLoop(signal),
      this.runBootstrapLoop(signal),
      this.runSyncLoop(signal),
      this.runProjectLoop(signal),
      this.runFactLoop(signal),
      this.runRelinkLoop(signal),
    ]);
  }

  private async runLatestHeightLoop(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      try {
        const activeNetworks = await this.networks.listActiveNetworks();
        if (activeNetworks.length === 0) {
          this.logIdleOnce('no-active-networks');
        } else {
          await this.refreshNetworkHeights(activeNetworks);
        }
      } catch (error) {
        console.error(`[onlydoge] height refresh failed error=${formatError(error)}`);
      }

      await sleep(this.latestHeightRefreshMs);
    }
  }

  private async runSyncLoop(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      const didWork = await this.runSyncWorkerTick();
      if (!didWork) {
        await sleep(this.workerIdleMs);
      }
    }
  }

  private async runBootstrapLoop(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      const didWork = await this.runBootstrapWorkerTick();
      if (!didWork) {
        await sleep(this.workerIdleMs);
      }
    }
  }

  private async runProjectLoop(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      const didWork = await this.runProjectWorkerTick();
      if (!didWork) {
        await sleep(this.workerIdleMs);
      }
    }
  }

  private async runFactLoop(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      const didWork = await this.runFactWorkerTick();
      if (!didWork) {
        await sleep(this.workerIdleMs);
      }
    }
  }

  private async runRelinkLoop(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      const didWork = await this.runRelinkWorkerTick();
      if (!didWork) {
        await sleep(this.workerIdleMs);
      }
    }
  }

  private async runSyncWorkerTick(): Promise<boolean> {
    const activeNetworks = await this.networks.listActiveNetworks();
    if (activeNetworks.length === 0) {
      return false;
    }

    const didWorkByNetwork = await mapWithConcurrency(
      activeNetworks,
      this.settings.networkConcurrency,
      async (network) => {
        if (this.syncInFlight.has(network.networkId)) {
          return false;
        }

        const latestBlockHeight = await this.getOrRefreshLatestHeight(network);
        if (latestBlockHeight === null) {
          return false;
        }

        return this.syncNetworkPhase(network, latestBlockHeight);
      },
    );

    return didWorkByNetwork.some(Boolean);
  }

  private async runBootstrapWorkerTick(): Promise<boolean> {
    const activeNetworks = await this.networks.listActiveNetworks();
    if (activeNetworks.length === 0) {
      return false;
    }

    const didWorkByNetwork = await mapWithConcurrency(
      activeNetworks,
      this.settings.networkConcurrency,
      async (network) => {
        if (this.bootstrapInFlight.has(network.networkId)) {
          return false;
        }

        const latestBlockHeight = await this.getOrRefreshLatestHeight(network);
        if (latestBlockHeight === null) {
          return false;
        }

        return this.bootstrapNetworkPhase(network, latestBlockHeight);
      },
    );

    return didWorkByNetwork.some(Boolean);
  }

  private async runProjectWorkerTick(): Promise<boolean> {
    const activeNetworks = await this.networks.listActiveNetworks();
    if (activeNetworks.length === 0) {
      return false;
    }

    const didWorkByNetwork = await mapWithConcurrency(
      activeNetworks,
      this.settings.networkConcurrency,
      async (network) => {
        if (
          this.projectInFlight.has(network.networkId) ||
          this.bootstrapInFlight.has(network.networkId)
        ) {
          return false;
        }

        const latestBlockHeight = await this.getOrRefreshLatestHeight(network);
        if (latestBlockHeight === null) {
          return false;
        }

        return this.projectNetworkPhase(network, latestBlockHeight);
      },
    );

    return didWorkByNetwork.some(Boolean);
  }

  private async runFactWorkerTick(): Promise<boolean> {
    const activeNetworks = await this.networks.listActiveNetworks();
    if (activeNetworks.length === 0) {
      return false;
    }

    const didWorkByNetwork = await mapWithConcurrency(
      activeNetworks,
      this.settings.networkConcurrency,
      async (network) => {
        if (this.factInFlight.has(network.networkId)) {
          return false;
        }

        const latestBlockHeight = await this.getOrRefreshLatestHeight(network);
        if (latestBlockHeight === null) {
          return false;
        }

        return this.factNetworkPhase(network, latestBlockHeight);
      },
    );

    return didWorkByNetwork.some(Boolean);
  }

  private async runRelinkWorkerTick(): Promise<boolean> {
    const activeNetworks = await this.networks.listActiveNetworks();
    if (activeNetworks.length === 0) {
      return false;
    }

    const didWorkByNetwork = await mapWithConcurrency(
      activeNetworks,
      this.settings.networkConcurrency,
      async (network) => {
        if (this.relinkInFlight.has(network.networkId)) {
          return false;
        }

        const latestBlockHeight = await this.getOrRefreshLatestHeight(network);
        if (latestBlockHeight === null) {
          return false;
        }

        const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
        if (!this.canRunRelink(backlog)) {
          return false;
        }

        return await this.relinkNetworkPhase(network, latestBlockHeight);
      },
    );

    return didWorkByNetwork.some(Boolean);
  }

  private async syncNetworkPhase(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
    if (!this.shouldRunSync(network, backlog.backlog)) {
      return false;
    }

    const tuning = this.getSyncTuning(network.networkId);
    const result = await this.runNetworkPhase(
      network,
      'sync',
      this.settings.syncTimeoutMs,
      backlog,
      tuning.window,
      this.syncInFlight,
      () => this.syncNetworkWindow(network, latestBlockHeight, tuning.window),
    );
    this.recordWindowOutcome(network, 'sync', result);
    return result.didWork;
  }

  private async bootstrapNetworkPhase(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
    const status = await this.getBootstrapStatus(network.networkId, backlog.processTail);
    if (!status.required) {
      this.clearPhaseSkipReason(network.networkId, 'project-state');
      return false;
    }

    const result = await this.runNetworkPhase(
      network,
      'bootstrap',
      this.settings.projectTimeoutMs,
      backlog,
      null,
      this.bootstrapInFlight,
      () => this.bootstrapNetworkWindow(network.networkId, backlog.processTail),
    );
    return result.didWork;
  }

  private async projectNetworkPhase(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
    const bootstrapStatus = await this.getBootstrapStatus(network.networkId, backlog.processTail);
    if (bootstrapStatus.required || this.bootstrapInFlight.has(network.networkId)) {
      this.logPhaseSkipOnce(network, 'project-state', 'bootstrap-pending', {
        bootstrap_tail: bootstrapStatus.bootstrapTail ?? -1,
        bootstrap_phase: bootstrapStatus.phase ?? 'pending',
        target_tail: bootstrapStatus.targetTail ?? backlog.processTail,
      });
      return false;
    }

    this.clearPhaseSkipReason(network.networkId, 'project-state');
    const tuning = this.getProjectTuning(network.networkId);
    const result = await this.runNetworkPhase(
      network,
      'project-state',
      this.settings.projectTimeoutMs,
      backlog,
      tuning.window,
      this.projectInFlight,
      () => this.projectNetworkWindow(network, latestBlockHeight, tuning.window),
    );
    this.recordWindowOutcome(network, 'project-state', result);
    return result.didWork;
  }

  private async factNetworkPhase(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
    const result = await this.runNetworkPhase(
      network,
      'facts',
      this.settings.factTimeoutMs,
      backlog,
      this.settings.factWindow,
      this.factInFlight,
      () => this.factNetworkWindow(network, latestBlockHeight, this.settings.factWindow),
    );
    return result.didWork;
  }

  private async relinkNetworkPhase(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
    const result = await this.runNetworkPhase(
      network,
      'relink',
      this.settings.relinkTimeoutMs,
      backlog,
      null,
      this.relinkInFlight,
      () => this.processPendingRelinkSeeds(network.networkId),
    );
    return result.didWork;
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

  private async refreshNetworkHeights(networks: IndexedNetwork[]) {
    const latestHeights = await mapWithConcurrency(
      networks,
      this.settings.networkConcurrency,
      async (network) => {
        try {
          const latestBlockHeight = await this.rpc.getBlockHeight(network);
          this.latestHeights.set(network.networkId, latestBlockHeight);
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

    return latestHeights;
  }

  private async getOrRefreshLatestHeight(network: IndexedNetwork): Promise<number | null> {
    const cached = this.latestHeights.get(network.networkId);
    if (cached !== undefined) {
      return cached;
    }

    try {
      const latestBlockHeight = await this.rpc.getBlockHeight(network);
      this.latestHeights.set(network.networkId, latestBlockHeight);
      await this.configs.setJsonValue(configKeyBlockHeight(network.networkId), latestBlockHeight);
      return latestBlockHeight;
    } catch (error) {
      console.error(
        `[onlydoge] sync height failed network=${network.id} error=${formatError(error)}`,
      );
      return null;
    }
  }

  private async runNetworkPhase(
    network: IndexedNetwork,
    phase: PhaseName,
    timeoutMs: number,
    backlog: BacklogState,
    window: number | null,
    inFlight: Map<number, Promise<void>>,
    work: () => Promise<PhaseWorkResult>,
  ): Promise<PhaseExecutionResult> {
    if (inFlight.has(network.networkId)) {
      return {
        didWork: false,
        workItems: 0,
        durationMs: 0,
      };
    }

    const startedAt = Date.now();
    let timedOut = false;
    const workPromise = work();
    const trackedPromise = workPromise
      .then((result) => {
        if (timedOut) {
          console.log(
            `[onlydoge] phase=${phase} network=${network.id} completed-after-timeout durationMs=${Date.now() - startedAt} didWork=${result.didWork} workItems=${result.workItems}`,
          );
        }
      })
      .catch((error) => {
        if (timedOut) {
          console.error(
            `[onlydoge] phase=${phase} network=${network.id} failed-after-timeout error=${formatError(error)}`,
          );
        }
      })
      .finally(() => {
        if (inFlight.get(network.networkId) === trackedPromise) {
          inFlight.delete(network.networkId);
        }
      });
    inFlight.set(network.networkId, trackedPromise);

    try {
      const result = await withTimeout(
        workPromise,
        timeoutMs,
        `${phase} timed out after ${timeoutMs}ms`,
      );
      const execution = {
        ...result,
        durationMs: Date.now() - startedAt,
      };

      if (result.didWork) {
        this.logPhaseSuccess(network, phase, backlog, window, execution);
      }
      return execution;
    } catch (error) {
      timedOut = isTimeoutError(error);
      const execution = {
        didWork: false,
        workItems: 0,
        durationMs: Date.now() - startedAt,
        error,
      };
      this.logPhaseFailure(network, phase, backlog, window, execution);
      return execution;
    }
  }

  private async syncNetworkWindow(
    network: IndexedNetwork,
    latestBlockHeight: number,
    window: number,
  ): Promise<PhaseWorkResult> {
    const currentSyncTail =
      (await this.configs.getJsonValue<number>(configKeyIndexerSyncTail(network.networkId))) ?? -1;
    if (currentSyncTail >= latestBlockHeight) {
      await this.configs.setJsonValue(
        configKeyIndexerSyncProgress(network.networkId),
        this.toProgress(currentSyncTail, latestBlockHeight),
      );
      return { didWork: false, workItems: 0 };
    }

    const windowEnd = Math.min(latestBlockHeight, currentSyncTail + window);
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
      return { didWork: false, workItems: 0 };
    }

    await this.configs.setJsonValue(configKeyIndexerSyncTail(network.networkId), contiguousTail);

    return {
      didWork: true,
      workItems: contiguousTail - currentSyncTail,
      startBlock: currentSyncTail + 1,
      endBlock: contiguousTail,
    };
  }

  private async projectNetworkWindow(
    network: IndexedNetwork,
    latestBlockHeight: number,
    window: number,
  ): Promise<PhaseWorkResult> {
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
      return { didWork: false, workItems: 0 };
    }

    const windowEnd = Math.min(currentSyncTail, currentProcessTail + window);
    const heights = range(currentProcessTail + 1, windowEnd);
    const loadSnapshotsStartedAt = Date.now();
    const orderedSnapshots = await this.loadSnapshots(network.networkId, heights);
    const loadSnapshotsMs = Date.now() - loadSnapshotsStartedAt;
    const appliedBlocks = await this.projectStateStore.listAppliedBlockSet(
      network.networkId,
      orderedSnapshots.map((snapshot, index) => ({
        blockHeight: heights[index] ?? -1,
        blockHash: this.readSnapshotBlockHash(snapshot, network.architecture),
      })),
    );

    let recoveredProcessTail = currentProcessTail;
    let recoveryIndex = 0;
    for (const [index, snapshot] of orderedSnapshots.entries()) {
      const blockHeight = heights[index] ?? currentProcessTail;
      const blockHash = this.readSnapshotBlockHash(snapshot, network.architecture);
      if (!appliedBlocks.has(blockIdentity(network.networkId, blockHeight, blockHash))) {
        recoveryIndex = index;
        break;
      }

      recoveredProcessTail = blockHeight;
      recoveryIndex = index + 1;
    }

    if (recoveredProcessTail > currentProcessTail) {
      await this.projectStateStore.finalizeProjectionBootstrap(
        network.networkId,
        recoveredProcessTail,
      );
      await this.configs.setJsonValue(
        configKeyIndexerProcessTail(network.networkId),
        recoveredProcessTail,
      );
      await this.configs.setJsonValue(
        configKeyIndexerProcessProgress(network.networkId),
        this.toProgress(recoveredProcessTail, latestBlockHeight),
      );

      if (recoveryIndex >= orderedSnapshots.length) {
        return {
          didWork: true,
          workItems: recoveredProcessTail - currentProcessTail,
          startBlock: currentProcessTail + 1,
          endBlock: recoveredProcessTail,
          metrics: {
            load_snapshots_ms: loadSnapshotsMs,
          },
        };
      }
    }

    const snapshotsToProject = orderedSnapshots.slice(recoveryIndex);
    if (snapshotsToProject.length === 0) {
      return recoveredProcessTail > currentProcessTail
        ? {
            didWork: true,
            workItems: recoveredProcessTail - currentProcessTail,
            startBlock: currentProcessTail + 1,
            endBlock: recoveredProcessTail,
            metrics: {
              load_snapshots_ms: loadSnapshotsMs,
            },
          }
        : { didWork: false, workItems: 0 };
    }

    const projectBlocksStartedAt = Date.now();
    const projections = await this.projectWindow(
      network,
      snapshotsToProject,
      'state',
      recoveredProcessTail,
    );
    const projectBlocksMs = Date.now() - projectBlocksStartedAt;

    const applyStateStartedAt = Date.now();
    await this.projectStateStore.applyProjectionWindow(projections);
    const applyStateMs = Date.now() - applyStateStartedAt;
    await this.projectStateStore.finalizeProjectionBootstrap(network.networkId, windowEnd);
    await this.configs.setJsonValue(configKeyIndexerProcessTail(network.networkId), windowEnd);
    await this.configs.setJsonValue(
      configKeyIndexerProcessProgress(network.networkId),
      this.toProgress(windowEnd, latestBlockHeight),
    );

    return {
      didWork: true,
      workItems: windowEnd - currentProcessTail,
      startBlock: currentProcessTail + 1,
      endBlock: windowEnd,
      metrics: {
        apply_state_ms: applyStateMs,
        load_snapshots_ms: loadSnapshotsMs,
        project_blocks_ms: projectBlocksMs,
      },
    };
  }

  private async factNetworkWindow(
    network: IndexedNetwork,
    latestBlockHeight: number,
    window: number,
  ): Promise<PhaseWorkResult> {
    const currentProcessTail =
      (await this.configs.getJsonValue<number>(configKeyIndexerProcessTail(network.networkId))) ??
      -1;
    const currentFactTail = await this.getFactTail(network.networkId, currentProcessTail);
    if (currentFactTail >= currentProcessTail) {
      await this.configs.setJsonValue(
        configKeyIndexerFactProgress(network.networkId),
        this.toProgress(currentFactTail, latestBlockHeight),
      );
      return { didWork: false, workItems: 0 };
    }

    const windowEnd = Math.min(currentProcessTail, currentFactTail + window);
    const heights = range(currentFactTail + 1, windowEnd);
    const loadSnapshotsStartedAt = Date.now();
    const orderedSnapshots = await this.loadSnapshots(network.networkId, heights);
    const loadSnapshotsMs = Date.now() - loadSnapshotsStartedAt;
    const appliedBlocks = await this.factWarehouse.listAppliedBlockSet(
      network.networkId,
      orderedSnapshots.map((snapshot, index) => ({
        blockHeight: heights[index] ?? -1,
        blockHash: this.readSnapshotBlockHash(snapshot, network.architecture),
      })),
    );

    let recoveredFactTail = currentFactTail;
    let recoveryIndex = 0;
    for (const [index, snapshot] of orderedSnapshots.entries()) {
      const blockHeight = heights[index] ?? currentFactTail;
      const blockHash = this.readSnapshotBlockHash(snapshot, network.architecture);
      if (!appliedBlocks.has(blockIdentity(network.networkId, blockHeight, blockHash))) {
        recoveryIndex = index;
        break;
      }

      recoveredFactTail = blockHeight;
      recoveryIndex = index + 1;
    }

    if (recoveredFactTail > currentFactTail) {
      await this.configs.setJsonValue(
        configKeyIndexerFactTail(network.networkId),
        recoveredFactTail,
      );
      await this.configs.setJsonValue(
        configKeyIndexerFactProgress(network.networkId),
        this.toProgress(recoveredFactTail, latestBlockHeight),
      );

      if (recoveryIndex >= orderedSnapshots.length) {
        return {
          didWork: true,
          workItems: recoveredFactTail - currentFactTail,
          startBlock: currentFactTail + 1,
          endBlock: recoveredFactTail,
          metrics: {
            load_snapshots_ms: loadSnapshotsMs,
          },
        };
      }
    }

    const recoveredSnapshots = orderedSnapshots.slice(0, recoveryIndex);
    const snapshotsToPersist = orderedSnapshots.slice(recoveryIndex);
    let recoveredProjectBlocksMs = 0;
    let recoveredApplyLinksMs = 0;

    if (recoveredSnapshots.length > 0) {
      const recoveredProjectionsStartedAt = Date.now();
      const recoveredProjections = await this.projectWindow(network, recoveredSnapshots, 'facts');
      recoveredProjectBlocksMs = Date.now() - recoveredProjectionsStartedAt;
      const recoveredApplyLinksStartedAt = Date.now();
      await this.applyFactDirectLinks(network.networkId, recoveredProjections);
      recoveredApplyLinksMs = Date.now() - recoveredApplyLinksStartedAt;
    }

    if (snapshotsToPersist.length === 0) {
      const recoveryMetrics: Record<string, number> = {
        apply_links_ms: recoveredApplyLinksMs,
        load_snapshots_ms: loadSnapshotsMs,
        project_blocks_ms: recoveredProjectBlocksMs,
      };
      return recoveredFactTail > currentFactTail
        ? {
            didWork: true,
            workItems: recoveredFactTail - currentFactTail,
            startBlock: currentFactTail + 1,
            endBlock: recoveredFactTail,
            metrics: recoveryMetrics,
          }
        : { didWork: false, workItems: 0 };
    }

    const projectBlocksStartedAt = Date.now();
    const projections = await this.projectWindow(network, snapshotsToPersist, 'facts');
    const projectBlocksMs = Date.now() - projectBlocksStartedAt;
    const writeFactsStartedAt = Date.now();
    const factWindow = await this.buildProjectionFactWindow(
      network.networkId,
      projections,
      this.factWarehouse,
    );
    await this.factWarehouse.applyProjectionFacts(factWindow);
    const writeFactsMs = Date.now() - writeFactsStartedAt;
    const applyLinksStartedAt = Date.now();
    await this.applyFactDirectLinks(network.networkId, projections);
    const applyLinksMs = Date.now() - applyLinksStartedAt;
    await this.configs.setJsonValue(configKeyIndexerFactTail(network.networkId), windowEnd);
    await this.configs.setJsonValue(
      configKeyIndexerFactProgress(network.networkId),
      this.toProgress(windowEnd, latestBlockHeight),
    );

    return {
      didWork: true,
      workItems: windowEnd - currentFactTail,
      startBlock: currentFactTail + 1,
      endBlock: windowEnd,
      metrics: {
        apply_links_ms: applyLinksMs,
        load_snapshots_ms: loadSnapshotsMs,
        project_blocks_ms: projectBlocksMs,
        write_facts_ms: writeFactsMs,
      },
    };
  }

  private async buildProjectionFactWindow(
    networkId: number,
    projections: BlockProjectionBatch[],
    snapshotStore: Pick<
      ProjectionStateStorePort,
      'getBalanceSnapshots' | 'getDirectLinkSnapshots' | 'getUtxoOutputs'
    >,
  ): Promise<ProjectionFactWindow> {
    const orderedProjections = [...projections].sort(
      (left, right) => left.blockHeight - right.blockHeight,
    );
    const outputKeys = [
      ...new Set(
        orderedProjections.flatMap((projection) => [
          ...projection.utxoCreates.map((output) => output.outputKey),
          ...projection.utxoSpends.map((spend) => spend.outputKey),
        ]),
      ),
    ];
    const balanceKeys = [
      ...new Set(
        projections.flatMap((projection) =>
          projection.addressMovements.map((movement) =>
            balanceKey(movement.address, movement.assetAddress),
          ),
        ),
      ),
    ].map(parseBalanceKey);
    const directLinkKeys = [
      ...new Set(
        orderedProjections.flatMap((projection) =>
          projection.directLinkDeltas.map((delta) =>
            directLinkKey(delta.fromAddress, delta.toAddress, delta.assetAddress),
          ),
        ),
      ),
    ].map(parseDirectLinkKey);

    const [currentOutputs, currentBalances, currentDirectLinks] = await Promise.all([
      snapshotStore.getUtxoOutputs(networkId, outputKeys),
      snapshotStore.getBalanceSnapshots(networkId, balanceKeys),
      snapshotStore.getDirectLinkSnapshots(networkId, directLinkKeys),
    ]);

    const nextOutputs = new Map<string, ProjectionUtxoOutput>();
    for (const projection of orderedProjections) {
      for (const output of projection.utxoCreates) {
        nextOutputs.set(output.outputKey, { ...output });
      }

      for (const spend of projection.utxoSpends) {
        const current = nextOutputs.get(spend.outputKey) ?? currentOutputs.get(spend.outputKey);
        if (!current) {
          throw new Error(`missing utxo output: ${spend.outputKey}`);
        }

        nextOutputs.set(spend.outputKey, {
          ...current,
          spentByTxid: spend.spentByTxid,
          spentInBlock: spend.spentInBlock,
          spentInputIndex: spend.spentInputIndex,
        });
      }
    }

    const nextBalances = new Map<string, ProjectionBalanceSnapshot>();
    for (const projection of orderedProjections) {
      for (const movement of projection.addressMovements) {
        const key = balanceKey(movement.address, movement.assetAddress);
        const current = nextBalances.get(key) ?? currentBalances.get(key);
        const currentAmount = parseAmountBase(current?.balance ?? '0');
        const movementAmount = parseAmountBase(movement.amountBase);
        const nextAmount =
          movement.direction === 'credit'
            ? currentAmount + movementAmount
            : currentAmount - movementAmount;
        if (nextAmount < 0n) {
          throw new Error(
            `negative balance for ${movement.networkId}:${movement.address}:${movement.assetAddress}`,
          );
        }

        nextBalances.set(key, {
          networkId: movement.networkId,
          address: movement.address,
          assetAddress: movement.assetAddress,
          balance: formatAmountBase(nextAmount),
          asOfBlockHeight: projection.blockHeight,
        });
      }
    }

    const nextDirectLinks = new Map<string, DirectLinkRecord>();
    for (const projection of orderedProjections) {
      for (const delta of projection.directLinkDeltas) {
        const key = directLinkKey(delta.fromAddress, delta.toAddress, delta.assetAddress);
        const current = nextDirectLinks.get(key) ?? currentDirectLinks.get(key);
        if (current) {
          nextDirectLinks.set(key, {
            ...current,
            transferCount: current.transferCount + delta.transferCount,
            totalAmountBase: addAmountBase(current.totalAmountBase, delta.totalAmountBase),
            firstSeenBlockHeight: Math.min(
              current.firstSeenBlockHeight,
              delta.firstSeenBlockHeight,
            ),
            lastSeenBlockHeight: Math.max(current.lastSeenBlockHeight, delta.lastSeenBlockHeight),
          });
          continue;
        }

        nextDirectLinks.set(key, { ...delta });
      }
    }

    return {
      networkId,
      addressMovements: orderedProjections.flatMap((projection) => projection.addressMovements),
      transfers: orderedProjections.flatMap((projection) => projection.transfers),
      appliedBlocks: orderedProjections.map((projection) => ({
        networkId: projection.networkId,
        blockHeight: projection.blockHeight,
        blockHash: projection.blockHash,
      })),
      utxoOutputs: outputKeys.flatMap((outputKey) => {
        const output = nextOutputs.get(outputKey) ?? currentOutputs.get(outputKey);
        return output ? [output] : [];
      }),
      balances: balanceKeys.flatMap((key) => {
        const snapshot =
          nextBalances.get(balanceKey(key.address, key.assetAddress)) ??
          currentBalances.get(balanceKey(key.address, key.assetAddress));
        return snapshot ? [snapshot] : [];
      }),
      directLinks: directLinkKeys.flatMap((key) => {
        const snapshot =
          nextDirectLinks.get(directLinkKey(key.fromAddress, key.toAddress, key.assetAddress)) ??
          currentDirectLinks.get(directLinkKey(key.fromAddress, key.toAddress, key.assetAddress));
        return snapshot ? [snapshot] : [];
      }),
    };
  }

  private async projectWindow(
    network: IndexedNetwork,
    snapshots: Record<string, unknown>[],
    mode: ProjectionMode,
    expectedBootstrapTail = -1,
  ): Promise<BlockProjectionBatch[]> {
    if (network.architecture === 'evm') {
      return snapshots.map((snapshot) =>
        this.evmProjector.project(network.networkId, snapshot, {
          includeDirectLinkDeltas: mode === 'facts',
          includeTransfers: mode === 'facts',
        }),
      );
    }

    const knownOutputKeys = new Set<string>();
    const externalOutputKeys = new Set<string>();
    for (const snapshot of snapshots) {
      for (const outputKey of this.dogecoinStateProjector.collectExternalOutputKeys(
        snapshot,
        knownOutputKeys,
      )) {
        externalOutputKeys.add(outputKey);
      }
    }

    const snapshotStore = mode === 'state' ? this.projectStateStore : this.stateStore;
    const projector = mode === 'state' ? this.dogecoinStateProjector : this.dogecoinFactProjector;
    const persistedOutputs = await snapshotStore.getUtxoOutputs(network.networkId, [
      ...externalOutputKeys,
    ]);
    if (mode === 'state' && persistedOutputs.size < externalOutputKeys.size) {
      const bootstrapTail = await this.projectStateStore.getProjectionBootstrapTail(
        network.networkId,
      );
      const missingPrevouts = externalOutputKeys.size - persistedOutputs.size;
      if (bootstrapTail === null || bootstrapTail < expectedBootstrapTail) {
        console.warn(
          `[onlydoge] phase=project-state network=${network.id} reason=bootstrap-required missing_prevouts=${missingPrevouts} required_prevouts=${externalOutputKeys.size} bootstrap_tail=${bootstrapTail ?? -1} required_tail=${expectedBootstrapTail}`,
        );
        throw new BootstrapRequiredError(
          `bootstrap required for project-state prevouts: ${missingPrevouts} missing`,
        );
      }

      throw new Error(`strict metadata prevouts missing: ${missingPrevouts}`);
    }

    const localOutputs = new Map<string, ProjectionUtxoOutput>();
    const projections: BlockProjectionBatch[] = [];
    for (const snapshot of snapshots) {
      projections.push(
        await projector.project(
          network.networkId,
          snapshot,
          {
            localOutputs,
            persistedOutputs,
          },
          {
            includeDirectLinkDeltas: mode === 'facts',
            includeTransfers: mode === 'facts',
            maxTransferEdges: this.settings.dogecoinTransferMaxEdges,
            maxTransferInputAddresses: this.settings.dogecoinTransferMaxInputAddresses,
          },
        ),
      );
    }

    return projections;
  }

  private async applyFactDirectLinks(
    networkId: number,
    projections: BlockProjectionBatch[],
  ): Promise<void> {
    const batches = projections
      .filter((projection) => projection.directLinkDeltas.length > 0)
      .map<ProjectionDirectLinkBatch>((projection) => ({
        networkId: projection.networkId,
        blockHeight: projection.blockHeight,
        blockHash: projection.blockHash,
        directLinkDeltas: projection.directLinkDeltas,
      }));
    if (batches.length === 0) {
      return;
    }

    await this.stateStore.applyDirectLinkDeltasWindow(batches);
    await this.markImpactedSeedsPendingRelink(networkId, projections);
  }

  private async markImpactedSeedsPendingRelink(
    networkId: number,
    projections: BlockProjectionBatch[],
  ): Promise<void> {
    const fromAddresses = [
      ...new Set(
        projections.flatMap((projection) =>
          projection.directLinkDeltas.map((delta) => delta.fromAddress),
        ),
      ),
    ];
    if (fromAddresses.length === 0) {
      return;
    }

    const [trackedAddresses, reachedSeedIds] = await Promise.all([
      this.seeds.listTrackedAddressesByValues(networkId, fromAddresses),
      this.stateStore.listSourceSeedIdsReachingAddresses(networkId, fromAddresses),
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

  private async processPendingRelinkSeeds(networkId: number): Promise<PhaseWorkResult> {
    const pendingSeeds = await this.seeds.listPendingRelinkSeeds(networkId);
    if (pendingSeeds.length === 0) {
      return { didWork: false, workItems: 0, pendingCount: 0 };
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

    return {
      didWork: relinkedCount > 0,
      workItems: relinkedCount,
      pendingCount: pendingSeeds.length,
    };
  }

  private async loadSnapshots(
    networkId: number,
    heights: number[],
  ): Promise<Record<string, unknown>[]> {
    const snapshots = await mapWithConcurrency(
      heights,
      Math.min(this.settings.syncConcurrency, heights.length),
      async (blockHeight) => {
        const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(
          networkId,
          blockHeight,
          'block',
        );
        if (!snapshot) {
          throw new Error(`missing stored snapshot for network=${networkId} block=${blockHeight}`);
        }

        return [blockHeight, snapshot] as const;
      },
    );
    const snapshotsByHeight = new Map(snapshots);

    return heights.map((blockHeight) => {
      const snapshot = snapshotsByHeight.get(blockHeight);
      if (!snapshot) {
        throw new Error(`missing snapshot in memory for network=${networkId} block=${blockHeight}`);
      }

      return snapshot;
    });
  }

  private async getFactTail(networkId: number, processTail: number): Promise<number> {
    const configured = await this.configs.getJsonValue<number>(configKeyIndexerFactTail(networkId));
    if (configured !== null) {
      return configured;
    }

    const warehouseTail = await this.factWarehouse.getAppliedBlockTail(networkId);
    const factTail = warehouseTail === null ? -1 : Math.min(warehouseTail, processTail);
    await this.configs.setJsonValue(configKeyIndexerFactTail(networkId), factTail);
    return factTail;
  }

  private async getBootstrapStatus(
    networkId: number,
    processTail: number,
  ): Promise<BootstrapStatus> {
    const [bootstrapTail, targetTail, phase, startedAtMs] = await Promise.all([
      this.projectStateStore.getProjectionBootstrapTail(networkId),
      this.configs.getJsonValue<number>(configKeyProjectionBootstrapTargetTail(networkId)),
      this.configs.getJsonValue<string>(configKeyProjectionBootstrapPhase(networkId)),
      this.configs.getJsonValue<number>(configKeyProjectionBootstrapStartedAt(networkId)),
    ]);

    return {
      bootstrapTail,
      targetTail,
      phase: isBootstrapPhase(phase) ? phase : null,
      startedAtMs,
      required: processTail >= 0 && (bootstrapTail === null || bootstrapTail < processTail),
    };
  }

  private async bootstrapNetworkWindow(
    networkId: number,
    processTail: number,
  ): Promise<PhaseWorkResult> {
    if (processTail < 0) {
      return { didWork: false, workItems: 0 };
    }

    let status = await this.getBootstrapStatus(networkId, processTail);
    if (!status.required) {
      return { didWork: false, workItems: 0 };
    }

    if (status.targetTail === null) {
      await this.initializeBootstrapState(networkId, processTail);
      status = await this.getBootstrapStatus(networkId, processTail);
    }

    const targetTail = status.targetTail ?? processTail;
    switch (status.phase ?? 'utxos') {
      case 'utxos':
        return this.bootstrapUtxoState(networkId, targetTail);
      case 'balances':
        return this.bootstrapBalanceState(networkId, targetTail, status.startedAtMs);
      case 'done':
        return this.completeBootstrapState(networkId, targetTail, status.startedAtMs, 0);
    }
  }

  private async initializeBootstrapState(networkId: number, targetTail: number): Promise<void> {
    await this.projectStateStore.clearProjectionBootstrapState(networkId);
    await this.deleteConfigKeys(
      configKeyProjectionBootstrapTail(networkId),
      configKeyProjectionBootstrapTargetTail(networkId),
      configKeyProjectionBootstrapPhase(networkId),
      configKeyProjectionBootstrapCursorUtxo(networkId),
      configKeyProjectionBootstrapCursorBalance(networkId),
      configKeyProjectionBootstrapStartedAt(networkId),
    );
    await this.configs.setJsonValue(configKeyProjectionBootstrapTargetTail(networkId), targetTail);
    await this.configs.setJsonValue(configKeyProjectionBootstrapPhase(networkId), 'utxos');
    await this.configs.setJsonValue(configKeyProjectionBootstrapStartedAt(networkId), Date.now());
  }

  private async bootstrapUtxoState(
    networkId: number,
    targetTail: number,
  ): Promise<PhaseWorkResult> {
    const cursor =
      (await this.configs.getJsonValue<string>(
        configKeyProjectionBootstrapCursorUtxo(networkId),
      )) ?? null;
    const page = await this.factWarehouse.listCurrentUtxoOutputsPage(
      networkId,
      cursor,
      bootstrapUtxoChunkSize,
    );
    await this.projectStateStore.upsertProjectionBootstrapUtxoOutputs(page.rows);

    if (page.nextCursor) {
      await this.configs.setJsonValue(
        configKeyProjectionBootstrapCursorUtxo(networkId),
        page.nextCursor,
      );
    } else {
      await this.deleteConfigKeys(configKeyProjectionBootstrapCursorUtxo(networkId));
      await this.configs.setJsonValue(configKeyProjectionBootstrapPhase(networkId), 'balances');
    }

    return {
      didWork: true,
      workItems: page.rows.length,
      metrics: {
        cursor: page.nextCursor ?? 'complete',
        rows_imported: page.rows.length,
        table_phase: 'utxos',
        target_tail: targetTail,
      },
    };
  }

  private async bootstrapBalanceState(
    networkId: number,
    targetTail: number,
    startedAtMs: number | null,
  ): Promise<PhaseWorkResult> {
    const cursor =
      (await this.configs.getJsonValue<ProjectionBalanceCursor>(
        configKeyProjectionBootstrapCursorBalance(networkId),
      )) ?? null;
    const page = await this.factWarehouse.listCurrentBalancesPage(
      networkId,
      cursor,
      bootstrapBalanceChunkSize,
    );
    await this.projectStateStore.upsertProjectionBootstrapBalances(page.rows);

    if (page.nextCursor) {
      await this.configs.setJsonValue(
        configKeyProjectionBootstrapCursorBalance(networkId),
        page.nextCursor,
      );
      return {
        didWork: true,
        workItems: page.rows.length,
        metrics: {
          cursor: `${page.nextCursor.address}:${page.nextCursor.assetAddress}`,
          rows_imported: page.rows.length,
          table_phase: 'balances',
          target_tail: targetTail,
        },
      };
    }

    await this.deleteConfigKeys(configKeyProjectionBootstrapCursorBalance(networkId));
    await this.configs.setJsonValue(configKeyProjectionBootstrapPhase(networkId), 'done');
    return this.completeBootstrapState(networkId, targetTail, startedAtMs, page.rows.length);
  }

  private async completeBootstrapState(
    networkId: number,
    targetTail: number,
    startedAtMs: number | null,
    importedRows: number,
  ): Promise<PhaseWorkResult> {
    await this.projectStateStore.finalizeProjectionBootstrap(networkId, targetTail);
    await this.deleteConfigKeys(
      configKeyProjectionBootstrapTargetTail(networkId),
      configKeyProjectionBootstrapPhase(networkId),
      configKeyProjectionBootstrapCursorUtxo(networkId),
      configKeyProjectionBootstrapCursorBalance(networkId),
      configKeyProjectionBootstrapStartedAt(networkId),
    );

    return {
      didWork: true,
      workItems: importedRows,
      metrics: {
        rows_imported: importedRows,
        table_phase: 'done',
        target_tail: targetTail,
        total_duration_ms: startedAtMs === null ? 0 : Date.now() - startedAtMs,
      },
    };
  }

  private async deleteConfigKeys(...keys: string[]): Promise<void> {
    await Promise.all(keys.map((key) => this.configs.deleteByPrefix(key)));
  }

  private async readBacklogState(
    networkId: number,
    latestBlockHeight: number,
  ): Promise<BacklogState> {
    const [syncTail, processTail] = await Promise.all([
      this.configs.getJsonValue<number>(configKeyIndexerSyncTail(networkId)),
      this.configs.getJsonValue<number>(configKeyIndexerProcessTail(networkId)),
    ]);

    return {
      latestBlockHeight,
      syncTail: syncTail ?? -1,
      processTail: processTail ?? -1,
      backlog: Math.max(0, (syncTail ?? -1) - (processTail ?? -1)),
    };
  }

  private shouldRunSync(network: IndexedNetwork, backlog: number): boolean {
    if (this.syncPausedNetworks.has(network.networkId)) {
      if (backlog > this.settings.syncBacklogLowWatermark) {
        return false;
      }

      this.syncPausedNetworks.delete(network.networkId);
      console.log(
        `[onlydoge] sync resumed network=${network.id} backlog=${backlog} reason=backpressure-cleared`,
      );
      return true;
    }

    if (backlog >= this.settings.syncBacklogHighWatermark) {
      this.syncPausedNetworks.add(network.networkId);
      console.log(
        `[onlydoge] sync paused network=${network.id} backlog=${backlog} reason=backpressure`,
      );
      return false;
    }

    return true;
  }

  private canRunRelink(backlog: BacklogState): boolean {
    const tipDistance = Math.max(0, backlog.latestBlockHeight - backlog.processTail);
    return (
      backlog.backlog <= this.settings.relinkBacklogThreshold &&
      tipDistance <= this.settings.relinkTipDistance
    );
  }

  private getSyncTuning(networkId: number): PhaseTuningState {
    const current = this.syncTuning.get(networkId);
    if (current) {
      return current;
    }

    const initial = {
      window: clamp(
        this.settings.syncWindow,
        this.settings.syncWindowMin,
        this.settings.syncWindowMax,
      ),
      successStreak: 0,
    };
    this.syncTuning.set(networkId, initial);
    return initial;
  }

  private getProjectTuning(networkId: number): PhaseTuningState {
    const current = this.projectTuning.get(networkId);
    if (current) {
      return current;
    }

    const initial = {
      window: clamp(
        this.settings.projectWindow,
        this.settings.projectWindowMin,
        this.settings.projectWindowMax,
      ),
      successStreak: 0,
    };
    this.projectTuning.set(networkId, initial);
    return initial;
  }

  private recordWindowOutcome(
    network: IndexedNetwork,
    phase: 'project-state' | 'sync',
    result: PhaseExecutionResult,
  ): void {
    const tuning =
      phase === 'sync'
        ? this.getSyncTuning(network.networkId)
        : this.getProjectTuning(network.networkId);
    const minWindow =
      phase === 'sync' ? this.settings.syncWindowMin : this.settings.projectWindowMin;
    const maxWindow =
      phase === 'sync' ? this.settings.syncWindowMax : this.settings.projectWindowMax;
    const targetMs = phase === 'sync' ? this.settings.syncTargetMs : this.settings.projectTargetMs;

    if (result.error && shouldBackoffWindow(result.error)) {
      const nextWindow = Math.max(minWindow, Math.floor(tuning.window / 2) || minWindow);
      if (nextWindow !== tuning.window) {
        console.log(
          `[onlydoge] adaptive backoff network=${network.id} phase=${phase} from=${tuning.window} to=${nextWindow} reason=${formatError(result.error)}`,
        );
        tuning.window = nextWindow;
      }
      tuning.successStreak = 0;
      return;
    }

    if (!result.didWork) {
      return;
    }

    if (result.durationMs <= targetMs) {
      tuning.successStreak += 1;
      if (tuning.successStreak >= 5 && tuning.window < maxWindow) {
        const nextWindow = Math.min(maxWindow, tuning.window * 2);
        if (nextWindow !== tuning.window) {
          console.log(
            `[onlydoge] adaptive growth network=${network.id} phase=${phase} from=${tuning.window} to=${nextWindow}`,
          );
          tuning.window = nextWindow;
        }
        tuning.successStreak = 0;
      }
      return;
    }

    tuning.successStreak = 0;
  }

  private logPhaseSkipOnce(
    network: IndexedNetwork,
    phase: PhaseName,
    reason: string,
    details: Record<string, number | string> = {},
  ): void {
    const key = `${phase}:${network.networkId}`;
    const message = `${reason}:${JSON.stringify(details)}`;
    if (this.phaseSkipReasons.get(key) === message) {
      return;
    }

    this.phaseSkipReasons.set(key, message);
    console.log(
      `[onlydoge] phase=${phase} network=${network.id} skipped reason=${reason}${formatMetrics(details)}`,
    );
  }

  private clearPhaseSkipReason(networkId: number, phase: PhaseName): void {
    this.phaseSkipReasons.delete(`${phase}:${networkId}`);
  }

  private logPhaseSuccess(
    network: IndexedNetwork,
    phase: PhaseName,
    backlog: BacklogState,
    window: number | null,
    result: PhaseExecutionResult,
  ): void {
    const rate =
      result.durationMs > 0 ? (result.workItems * 1000) / Math.max(result.durationMs, 1) : 0;
    const rangeText =
      result.startBlock !== undefined && result.endBlock !== undefined
        ? ` blocks=${result.startBlock}-${result.endBlock}`
        : '';
    const windowText = window === null ? '' : ` window=${window}`;
    const pendingText = result.pendingCount === undefined ? '' : ` pending=${result.pendingCount}`;
    const metricsText = formatMetrics(result.metrics);

    console.log(
      `[onlydoge] phase=${phase} network=${network.id}${rangeText} latest=${backlog.latestBlockHeight} backlog=${backlog.backlog}${windowText}${pendingText} items=${result.workItems} durationMs=${result.durationMs} rate=${rate.toFixed(2)}/s${metricsText}`,
    );
  }

  private logPhaseFailure(
    network: IndexedNetwork,
    phase: PhaseName,
    backlog: BacklogState,
    window: number | null,
    result: PhaseExecutionResult,
  ): void {
    const windowText = window === null ? '' : ` window=${window}`;
    const metricsText = formatMetrics(result.metrics);

    console.error(
      `[onlydoge] phase=${phase} network=${network.id} latest=${backlog.latestBlockHeight} backlog=${backlog.backlog}${windowText} durationMs=${result.durationMs}${metricsText} error=${formatError(result.error)}`,
    );
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

function balanceKey(address: string, assetAddress: string): string {
  return `${address}:${assetAddress}`;
}

function blockIdentity(networkId: number, blockHeight: number, blockHash: string): string {
  return `${networkId}:${blockHeight}:${blockHash}`;
}

function parseBalanceKey(key: string): { address: string; assetAddress: string } {
  const [address, ...assetAddressParts] = key.split(':');
  return {
    address: address ?? '',
    assetAddress: assetAddressParts.join(':'),
  };
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function directLinkKey(fromAddress: string, toAddress: string, assetAddress: string): string {
  return `${fromAddress}:${toAddress}:${assetAddress}`;
}

function parseDirectLinkKey(key: string): {
  assetAddress: string;
  fromAddress: string;
  toAddress: string;
} {
  const [fromAddress, toAddress, ...assetAddressParts] = key.split(':');
  return {
    fromAddress: fromAddress ?? '',
    toAddress: toAddress ?? '',
    assetAddress: assetAddressParts.join(':'),
  };
}

function formatError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function formatMetrics(metrics: Record<string, number | string> | undefined): string {
  if (!metrics || Object.keys(metrics).length === 0) {
    return '';
  }

  return ` ${Object.entries(metrics)
    .map(([key, value]) => `${key}=${value}`)
    .join(' ')}`;
}

function shouldBackoffWindow(error: unknown): boolean {
  const message = formatError(error);
  return [
    'timed out',
    'MEMORY_LIMIT_EXCEEDED',
    'warehouse query exceeded memory limit',
    'warehouse unavailable',
    'ECONNRESET',
    'ECONNREFUSED',
    'socket hang up',
  ].some((needle) => message.includes(needle));
}

function isTimeoutError(error: unknown): boolean {
  return formatError(error).includes('timed out');
}

function isBootstrapPhase(value: string | null): value is BootstrapPhase {
  return value === 'balances' || value === 'done' || value === 'utxos';
}

class BootstrapRequiredError extends Error {}

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
