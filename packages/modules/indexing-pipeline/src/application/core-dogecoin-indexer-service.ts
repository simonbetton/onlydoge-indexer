import { randomUUID } from 'node:crypto';

import type { PrimaryId } from '@onlydoge/shared-kernel';

import type {
  BlockchainRpcPort,
  CoordinatorConfigPort,
  CoreDogecoinStateStorePort,
  IndexedNetworkPort,
  RawBlockStoragePort,
} from '../contracts/ports';
import { fromDecimalUnits } from '../domain/amounts';
import {
  configKeyBlockHeight,
  configKeyIndexerProcessProgress,
  configKeyIndexerProcessTail,
  configKeyIndexerStage,
  configKeyIndexerSyncProgress,
  configKeyIndexerSyncTail,
  configKeyPrimary,
} from '../domain/config-keys';
import {
  extractDogecoinOutputAddress,
  isDogecoinTransaction,
  type ParsedDogecoinBlock,
} from '../domain/dogecoin-block';
import type {
  CoreDogecoinBlockApplication,
  CoreIndexerState,
  ProjectionUtxoOutput,
} from '../domain/projection-models';
import type { IndexingPipelineSettings } from './indexing-pipeline-service';

interface PrimaryLease {
  heartbeatAt: string;
  instanceId: string;
}

interface CoreDogecoinNetwork {
  architecture: 'dogecoin';
  blockTime: number;
  id: string;
  networkId: PrimaryId;
  rpcEndpoint: string;
  rps: number;
  zmqBlockEndpoint?: string | null;
}

type IndexedNetwork = Awaited<ReturnType<IndexedNetworkPort['listActiveNetworks']>>[number];

const workerIdleMs = 250;
const leaseTimeoutMs = 15_000;
const rawBlockPart = 'block';

export class CoreDogecoinIndexerService {
  private readonly instanceId = randomUUID();
  private latestLog: string | null = null;

  public constructor(
    private readonly configs: CoordinatorConfigPort,
    private readonly networks: IndexedNetworkPort,
    private readonly rawBlocks: RawBlockStoragePort,
    private readonly rpc: BlockchainRpcPort,
    private readonly stateStore: CoreDogecoinStateStorePort,
    private readonly settings: IndexingPipelineSettings,
  ) {}

  public async start(signal?: AbortSignal): Promise<void> {
    console.info('[onlydoge] core dogecoin indexer loop started');
    while (!signal?.aborted) {
      try {
        const isPrimary = await this.leaseLeadership();
        if (!isPrimary) {
          await sleep(1_000);
          continue;
        }

        const didWork = await this.runOnce();
        if (!didWork) {
          await sleep(workerIdleMs);
        }
      } catch (error) {
        console.error(`[onlydoge] core indexer loop failed error=${formatError(error)}`);
        await sleep(1_000);
      }
    }
  }

  public async runOnce(): Promise<boolean> {
    const isPrimary = await this.leaseLeadership();
    if (!isPrimary) {
      return false;
    }

    const dogecoinNetworks = (await this.networks.listActiveNetworks()).filter(isDogecoinNetwork);
    if (dogecoinNetworks.length === 0) {
      this.logOnce('[onlydoge] core indexer idle reason=no-dogecoin-networks');
      return false;
    }

    let didWork = false;
    for (const network of dogecoinNetworks) {
      didWork = (await this.runNetwork(network)) || didWork;
    }
    return didWork;
  }

  private async runNetwork(network: CoreDogecoinNetwork): Promise<boolean> {
    const latest = await this.rpc.getBlockHeight(network);
    await this.configs.setJsonValue(configKeyBlockHeight(network.networkId), latest);

    const state = await this.ensureState(network, latest);
    await this.publishProgress(network.networkId, latest, state);

    try {
      if (state.stage === 'sync_backfill') {
        return this.syncBackfill(network, latest, state);
      }

      if (state.stage === 'process_backfill') {
        return this.processBackfill(network, latest, state);
      }

      return this.online(network, latest, state);
    } catch (error) {
      await this.stateStore.setCoreIndexerError(network.networkId, formatError(error));
      throw error;
    }
  }

  private async ensureState(
    network: CoreDogecoinNetwork,
    latest: number,
  ): Promise<CoreIndexerState> {
    const current = await this.stateStore.getCoreIndexerState(network.networkId);
    if (current) {
      return current;
    }

    const legacySyncTail =
      (await this.configs.getJsonValue<number>(configKeyIndexerSyncTail(network.networkId))) ?? -1;
    const syncTail = Math.min(legacySyncTail, latest);
    const state = await this.stateStore.upsertCoreIndexerState({
      networkId: network.networkId,
      stage: 'sync_backfill',
      syncTail,
      processTail: -1,
      onlineTip: latest,
      lastError: null,
    });
    console.info(
      `[onlydoge] core indexer initialized network=${network.id} stage=sync_backfill sync_tail=${syncTail} process_tail=-1`,
    );
    return state;
  }

  private async syncBackfill(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    if (state.syncTail >= 0 && state.syncTail >= latest - this.settings.coreSyncCompleteDistance) {
      await this.stateStore.upsertCoreIndexerState({
        networkId: network.networkId,
        stage: 'process_backfill',
        onlineTip: latest,
      });
      await this.configs.setJsonValue(configKeyIndexerStage(network.networkId), 'process_backfill');
      console.info(
        `[onlydoge] core stage changed network=${network.id} stage=process_backfill sync_tail=${state.syncTail} latest=${latest}`,
      );
      return true;
    }

    const end = Math.min(latest, state.syncTail + this.settings.syncWindow);
    const heights = range(state.syncTail + 1, end);
    await mapWithConcurrency(heights, this.settings.syncConcurrency, async (height) => {
      const snapshot = await this.rpc.getBlockSnapshot(network, height);
      await this.rawBlocks.putPart(network.networkId, height, rawBlockPart, snapshot, {
        timeoutMs: this.settings.coreRawStorageTimeoutMs,
      });
      const block = parseDogecoinBlockSnapshot(snapshot);
      await this.stateStore.upsertCoreBlock({
        networkId: network.networkId,
        blockHeight: block.height,
        blockHash: block.hash,
        previousBlockHash: block.previousHash,
        blockTime: block.time,
        txCount: block.tx.length,
        rawStorageKey: rawBlockPart,
        fetchedAt: new Date().toISOString(),
        processedAt: null,
      });
    });

    const nextState = await this.stateStore.upsertCoreIndexerState({
      networkId: network.networkId,
      stage: 'sync_backfill',
      syncTail: end,
      onlineTip: latest,
      lastError: null,
    });
    await this.publishProgress(network.networkId, latest, nextState);
    console.info(
      `[onlydoge] core synced network=${network.id} blocks=${state.syncTail + 1}-${end} latest=${latest}`,
    );
    return true;
  }

  private async processBackfill(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    if (state.processTail >= state.syncTail) {
      if (state.processTail >= latest - this.settings.coreOnlineTipDistance) {
        await this.stateStore.upsertCoreIndexerState({
          networkId: network.networkId,
          stage: 'online',
          onlineTip: latest,
        });
        await this.configs.setJsonValue(configKeyIndexerStage(network.networkId), 'online');
        console.info(
          `[onlydoge] core stage changed network=${network.id} stage=online process_tail=${state.processTail} latest=${latest}`,
        );
        return true;
      }
      return false;
    }

    const end = Math.min(state.syncTail, state.processTail + this.settings.coreProcessWindow);
    let tail = state.processTail;
    for (const height of range(state.processTail + 1, end)) {
      await this.processBlock(network, height);
      tail = height;
      const nextState = await this.stateStore.upsertCoreIndexerState({
        networkId: network.networkId,
        stage: 'process_backfill',
        processTail: tail,
        onlineTip: latest,
        lastError: null,
      });
      await this.publishProgress(network.networkId, latest, nextState);
    }

    console.info(
      `[onlydoge] core processed network=${network.id} blocks=${state.processTail + 1}-${tail} sync_tail=${state.syncTail} latest=${latest}`,
    );
    return true;
  }

  private async online(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    if (state.syncTail >= latest && state.processTail >= latest) {
      await this.publishProgress(network.networkId, latest, state);
      return false;
    }

    const syncEnd = Math.min(latest, state.syncTail + this.settings.syncWindow);
    if (state.syncTail < syncEnd) {
      await this.syncBackfill(network, latest, { ...state, stage: 'sync_backfill' });
    }

    const refreshed = (await this.stateStore.getCoreIndexerState(network.networkId)) ?? {
      ...state,
      syncTail: syncEnd,
    };
    if (refreshed.processTail < refreshed.syncTail) {
      await this.processBackfill(network, latest, { ...refreshed, stage: 'process_backfill' });
    }

    return true;
  }

  private async processBlock(network: CoreDogecoinNetwork, height: number): Promise<void> {
    const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(
      network.networkId,
      height,
      rawBlockPart,
      { timeoutMs: this.settings.coreRawStorageTimeoutMs },
    );
    if (!snapshot) {
      throw new Error(`missing raw dogecoin block snapshot network=${network.id} height=${height}`);
    }

    const application = await this.buildBlockApplication(network.networkId, snapshot);
    await this.stateStore.applyCoreDogecoinBlock(application);
  }

  private async buildBlockApplication(
    networkId: PrimaryId,
    snapshot: Record<string, unknown>,
  ): Promise<CoreDogecoinBlockApplication> {
    const block = parseDogecoinBlockSnapshot(snapshot);
    const externalKeys = collectExternalOutputKeys(block);
    const persistedOutputs = await this.stateStore.getCoreUtxoOutputs(networkId, externalKeys);
    const localOutputs = new Map<string, ProjectionUtxoOutput>();
    const utxoCreates: ProjectionUtxoOutput[] = [];
    const utxoSpends: CoreDogecoinBlockApplication['utxoSpends'] = [];

    for (const [txIndex, tx] of block.tx.entries()) {
      const txid = requireString(tx.txid, 'tx.txid');
      for (const [inputIndex, input] of (tx.vin ?? []).entries()) {
        if (input.coinbase) {
          continue;
        }
        const outputKey = `${requireString(input.txid, 'vin.txid')}:${requireNumber(input.vout, 'vin.vout')}`;
        const prevout = localOutputs.get(outputKey) ?? persistedOutputs.get(outputKey);
        if (!prevout) {
          throw new Error(`missing core utxo output: ${outputKey}`);
        }
        markSpent(prevout, txid, block.height, inputIndex);
        utxoSpends.push({
          outputKey,
          spentByTxid: txid,
          spentInBlock: block.height,
          spentInputIndex: inputIndex,
          address: prevout.address,
          valueBase: prevout.valueBase,
        });
      }

      for (const [outputIndex, output] of (tx.vout ?? []).entries()) {
        const address = extractDogecoinOutputAddress(output);
        const created: ProjectionUtxoOutput = {
          networkId,
          blockHeight: block.height,
          blockHash: block.hash,
          blockTime: block.time,
          txid,
          txIndex,
          vout: requireNumber(output.n ?? outputIndex, 'vout.n'),
          outputKey: `${txid}:${outputIndex}`,
          address,
          scriptType: output.scriptPubKey?.type?.trim() ?? '',
          valueBase: fromDecimalUnits(requireAmount(output.value), 8),
          isCoinbase: Boolean((tx.vin ?? []).some((input) => input.coinbase)),
          isSpendable: Boolean(address),
          spentByTxid: null,
          spentInBlock: null,
          spentInputIndex: null,
        };
        localOutputs.set(created.outputKey, created);
        utxoCreates.push(created);
      }
    }

    return {
      networkId,
      blockHeight: block.height,
      blockHash: block.hash,
      previousBlockHash: block.previousHash,
      blockTime: block.time,
      txCount: block.tx.length,
      rawStorageKey: rawBlockPart,
      utxoCreates,
      utxoSpends,
    };
  }

  private async publishProgress(
    networkId: PrimaryId,
    latest: number,
    state: CoreIndexerState,
  ): Promise<void> {
    await Promise.all([
      this.configs.setJsonValue(configKeyPrimary(), createLease(this.instanceId)),
      this.configs.setJsonValue(configKeyIndexerStage(networkId), state.stage),
      this.configs.setJsonValue(configKeyIndexerSyncTail(networkId), state.syncTail),
      this.configs.setJsonValue(configKeyIndexerProcessTail(networkId), state.processTail),
      this.configs.setJsonValue(
        configKeyIndexerSyncProgress(networkId),
        toProgress(state.syncTail, latest),
      ),
      this.configs.setJsonValue(
        configKeyIndexerProcessProgress(networkId),
        toProgress(state.processTail, latest),
      ),
    ]);
  }

  private async leaseLeadership(): Promise<boolean> {
    const current = await this.configs.getJsonValue<PrimaryLease | string>(configKeyPrimary());
    const currentLease = toPrimaryLease(current);
    if (!currentLease) {
      const claimed = await this.configs.compareAndSwapJsonValue(
        configKeyPrimary(),
        current,
        createLease(this.instanceId),
      );
      if (claimed) {
        console.info(`[onlydoge] core indexer primary instance=${this.instanceId}`);
      }
      return claimed;
    }

    if (currentLease.instanceId === this.instanceId) {
      await this.configs.setJsonValue(configKeyPrimary(), createLease(this.instanceId));
      return true;
    }

    if (Date.now() - Date.parse(currentLease.heartbeatAt) <= leaseTimeoutMs) {
      return false;
    }

    const claimed = await this.configs.compareAndSwapJsonValue(
      configKeyPrimary(),
      current,
      createLease(this.instanceId),
    );
    if (claimed) {
      console.info(
        `[onlydoge] core indexer primary instance=${this.instanceId} replaced-stale-primary`,
      );
    }
    return claimed;
  }

  private logOnce(message: string): void {
    if (this.latestLog === message) {
      return;
    }

    this.latestLog = message;
    console.info(message);
  }
}

function isDogecoinNetwork(network: IndexedNetwork): network is CoreDogecoinNetwork {
  return network.architecture === 'dogecoin';
}

function createLease(instanceId: string): PrimaryLease {
  return {
    instanceId,
    heartbeatAt: new Date().toISOString(),
  };
}

function toPrimaryLease(value: PrimaryLease | string | null): PrimaryLease | null {
  if (!value || typeof value === 'string') {
    return null;
  }

  return typeof value.instanceId === 'string' && typeof value.heartbeatAt === 'string'
    ? value
    : null;
}

function toProgress(tail: number, latest: number): number {
  if (latest < 0) {
    return 0;
  }
  return Math.max(0, Math.min(1, (tail + 1) / (latest + 1)));
}

function range(start: number, end: number): number[] {
  if (end < start) {
    return [];
  }

  return Array.from({ length: end - start + 1 }, (_, index) => start + index);
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
        const index = nextIndex;
        nextIndex += 1;
        if (index >= values.length) {
          return;
        }
        const value = values[index];
        if (value === undefined) {
          return;
        }
        results[index] = await worker(value, index);
      }
    }),
  );

  return results;
}

function collectExternalOutputKeys(block: ParsedDogecoinBlock): string[] {
  const known = new Set<string>();
  const external = new Set<string>();
  for (const tx of block.tx) {
    const txid = requireString(tx.txid, 'tx.txid');
    for (const input of tx.vin ?? []) {
      if (input.coinbase) {
        continue;
      }
      const outputKey = `${requireString(input.txid, 'vin.txid')}:${requireNumber(input.vout, 'vin.vout')}`;
      if (!known.has(outputKey)) {
        external.add(outputKey);
      }
    }
    for (const [index] of (tx.vout ?? []).entries()) {
      known.add(`${txid}:${index}`);
    }
  }
  return [...external];
}

function parseDogecoinBlockSnapshot(snapshot: Record<string, unknown>): ParsedDogecoinBlock & {
  previousHash: string | null;
} {
  const block = snapshot.block;
  if (!block || typeof block !== 'object' || Array.isArray(block)) {
    throw new Error('invalid dogecoin block snapshot');
  }
  const candidate = block as Record<string, unknown>;
  const transactions = Array.isArray(candidate.tx)
    ? candidate.tx.filter(isDogecoinTransaction)
    : [];

  return {
    hash: requireString(candidate.hash, 'block.hash'),
    height: requireNumber(candidate.height, 'block.height'),
    time: requireNumber(candidate.time, 'block.time'),
    previousHash:
      typeof candidate.previousblockhash === 'string' && candidate.previousblockhash.trim()
        ? candidate.previousblockhash.trim()
        : null,
    tx: transactions,
  };
}

function requireString(value: unknown, label: string): string {
  if (typeof value !== 'string' || !value.trim()) {
    throw new Error(`missing ${label}`);
  }
  return value.trim();
}

function requireNumber(value: unknown, label: string): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new Error(`missing ${label}`);
  }
  return value;
}

function requireAmount(value: unknown): string {
  if (typeof value === 'number') {
    return value.toFixed(8);
  }
  if (typeof value === 'string' && value.trim()) {
    return value.trim();
  }
  throw new Error(`invalid dogecoin amount: ${String(value)}`);
}

function markSpent(
  output: ProjectionUtxoOutput,
  spentByTxid: string,
  spentInBlock: number,
  spentInputIndex: number,
): void {
  output.spentByTxid = spentByTxid;
  output.spentInBlock = spentInBlock;
  output.spentInputIndex = spentInputIndex;
}

function formatError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
