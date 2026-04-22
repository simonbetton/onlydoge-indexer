import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';

import { createClient } from '@clickhouse/client';
import type { ExplorerWarehousePort } from '@onlydoge/explorer-query';
import {
  type AddressMovement,
  addAmountBase,
  type BlockProjectionBatch,
  type DirectLinkRecord,
  formatAmountBase,
  type ProjectionBalanceCursor,
  type ProjectionBalanceSnapshot,
  type ProjectionCurrentBalancePage,
  type ProjectionCurrentUtxoPage,
  type ProjectionDirectLinkBatch,
  type ProjectionFactWarehousePort,
  type ProjectionFactWindow,
  type ProjectionStateBootstrapSnapshot,
  type ProjectionStateStorePort,
  type ProjectionUtxoOutput,
  type ProjectionWarehousePort,
  parseAmountBase,
  type SourceLinkRecord,
} from '@onlydoge/indexing-pipeline';
import type { InvestigationWarehousePort } from '@onlydoge/investigation-query';
import { InfrastructureError, type PrimaryId } from '@onlydoge/shared-kernel';

import type { WarehouseSettings } from './settings';

interface BalanceRow {
  address: string;
  assetAddress: string;
  asOfBlockHeight: number;
  balance: string;
  networkId: PrimaryId;
}

interface WarehouseState {
  appliedBlocks: Array<{
    blockHash: string;
    blockHeight: number;
    networkId: PrimaryId;
  }>;
  directLinkAppliedBlocks: Array<{
    blockHash: string;
    blockHeight: number;
    networkId: PrimaryId;
  }>;
  addressMovements: AddressMovement[];
  balances: BalanceRow[];
  directLinks: DirectLinkRecord[];
  sourceLinks: SourceLinkRecord[];
  tokens: Array<{
    address: string;
    decimals: number;
    id: string;
    name: string;
    networkId: PrimaryId;
    symbol: string;
  }>;
  transfers: BlockProjectionBatch['transfers'];
  utxoOutputs: ProjectionUtxoOutput[];
}

interface VersionedBalanceRow extends BalanceRow {
  version: number;
}

interface VersionedDirectLinkRow extends DirectLinkRecord {
  version: number;
}

const maxClickHouseQueryValuesPerChunk = 256;
const maxClickHouseQueryValueBytesPerChunk = 12_000;
const maxClickHouseHotOutputKeyValuesPerChunk = 128;
const maxClickHouseHotOutputKeyBytesPerChunk = 6_000;
const addressMovementsByAddressTable = 'address_movements_by_address_v2';
const utxoCurrentStateTable = 'utxo_outputs_current_v2';
const utxoCurrentStateByAddressTable = 'utxo_outputs_current_by_address_v2';
type ClickHouseClient = ReturnType<typeof createClient>;
type ClickHouseCommandParameters = Parameters<ClickHouseClient['command']>[0];
type ClickHouseInsertParameters = Parameters<ClickHouseClient['insert']>[0];
type ClickHouseJsonQueryParameters = Parameters<ClickHouseClient['query']>[0];

const emptyWarehouseState = (): WarehouseState => ({
  appliedBlocks: [],
  directLinkAppliedBlocks: [],
  utxoOutputs: [],
  addressMovements: [],
  transfers: [],
  balances: [],
  directLinks: [],
  sourceLinks: [],
  tokens: [],
});

function mergeWarehouseState(input: Partial<WarehouseState> | null | undefined): WarehouseState {
  return {
    ...emptyWarehouseState(),
    ...(input ?? {}),
    appliedBlocks: input?.appliedBlocks ?? [],
    directLinkAppliedBlocks: input?.directLinkAppliedBlocks ?? [],
    utxoOutputs: input?.utxoOutputs ?? [],
    addressMovements: input?.addressMovements ?? [],
    transfers: input?.transfers ?? [],
    balances: input?.balances ?? [],
    directLinks: input?.directLinks ?? [],
    sourceLinks: input?.sourceLinks ?? [],
    tokens: input?.tokens ?? [],
  };
}

export class InMemoryWarehouseAdapter
  implements
    InvestigationWarehousePort,
    ProjectionWarehousePort,
    ProjectionFactWarehousePort,
    ExplorerWarehousePort
{
  protected state: WarehouseState = emptyWarehouseState();
  private readonly bootstrapTails = new Map<PrimaryId, number>();

  public async getBalancesByAddresses(addresses: string[]) {
    return this.state.balances.filter((balance) => addresses.includes(balance.address));
  }

  public async getTokensByAddresses(addresses: string[]) {
    return this.state.tokens.filter((token) => addresses.includes(token.address));
  }

  public async getDistinctLinksByAddresses(addresses: string[]) {
    return this.state.sourceLinks
      .filter((link) => addresses.includes(link.toAddress))
      .map((link) => ({
        networkId: link.networkId,
        fromAddress: link.sourceAddress,
        toAddress: link.toAddress,
        transferCount: link.hopCount,
      }));
  }

  public async getBalanceSnapshots(
    networkId: PrimaryId,
    keys: Array<{
      address: string;
      assetAddress: string;
    }>,
  ): Promise<Map<string, ProjectionBalanceSnapshot>> {
    const keySet = new Set(keys.map((key) => balanceSnapshotKey(key.address, key.assetAddress)));
    const rows = this.state.balances.filter(
      (balance) =>
        balance.networkId === networkId &&
        keySet.has(balanceSnapshotKey(balance.address, balance.assetAddress)),
    );

    return new Map(
      rows.map((row) => [balanceSnapshotKey(row.address, row.assetAddress), { ...row }]),
    );
  }

  public async getDirectLinkSnapshots(
    networkId: PrimaryId,
    keys: Array<{
      assetAddress: string;
      fromAddress: string;
      toAddress: string;
    }>,
  ): Promise<Map<string, DirectLinkRecord>> {
    const keySet = new Set(
      keys.map((key) => directLinkSnapshotKey(key.fromAddress, key.toAddress, key.assetAddress)),
    );
    const rows = this.state.directLinks.filter(
      (link) =>
        link.networkId === networkId &&
        keySet.has(directLinkSnapshotKey(link.fromAddress, link.toAddress, link.assetAddress)),
    );

    return new Map(
      rows.map((row) => [
        directLinkSnapshotKey(row.fromAddress, row.toAddress, row.assetAddress),
        { ...row },
      ]),
    );
  }

  public async clearProjectionBootstrapState(networkId: PrimaryId): Promise<void> {
    this.state.utxoOutputs = this.state.utxoOutputs.filter((row) => row.networkId !== networkId);
    this.state.balances = this.state.balances.filter((row) => row.networkId !== networkId);
    this.state.appliedBlocks = this.state.appliedBlocks.filter(
      (row) => row.networkId !== networkId,
    );
    this.bootstrapTails.delete(networkId);
  }

  public async finalizeProjectionBootstrap(
    networkId: PrimaryId,
    processTail: number,
  ): Promise<void> {
    this.bootstrapTails.set(networkId, processTail);
  }

  public async getProjectionBootstrapTail(networkId: PrimaryId): Promise<number | null> {
    return this.bootstrapTails.get(networkId) ?? null;
  }

  public async listCurrentUtxoOutputsPage(
    networkId: PrimaryId,
    cursorOutputKey: string | null,
    limit: number,
  ): Promise<ProjectionCurrentUtxoPage> {
    const rows = this.state.utxoOutputs
      .filter(
        (row) =>
          row.networkId === networkId &&
          (cursorOutputKey === null || row.outputKey > cursorOutputKey),
      )
      .sort((left, right) => left.outputKey.localeCompare(right.outputKey))
      .slice(0, limit)
      .map((row) => ({ ...row }));

    return {
      rows,
      nextCursor: rows.length === limit ? (rows.at(-1)?.outputKey ?? null) : null,
    };
  }

  public async listCurrentBalancesPage(
    networkId: PrimaryId,
    cursor: ProjectionBalanceCursor | null,
    limit: number,
  ): Promise<ProjectionCurrentBalancePage> {
    const rows = this.state.balances
      .filter((row) => {
        if (row.networkId !== networkId) {
          return false;
        }

        if (cursor === null) {
          return true;
        }

        return (
          row.address > cursor.address ||
          (row.address === cursor.address && row.assetAddress > cursor.assetAddress)
        );
      })
      .sort(
        (left, right) =>
          left.address.localeCompare(right.address) ||
          left.assetAddress.localeCompare(right.assetAddress),
      )
      .slice(0, limit)
      .map((row) => ({ ...row }));

    return {
      rows,
      nextCursor:
        rows.length === limit
          ? {
              address: rows.at(-1)?.address ?? '',
              assetAddress: rows.at(-1)?.assetAddress ?? '',
            }
          : null,
    };
  }

  public async upsertProjectionBootstrapBalances(rows: ProjectionBalanceSnapshot[]): Promise<void> {
    for (const row of rows) {
      const index = this.state.balances.findIndex(
        (candidate) =>
          candidate.networkId === row.networkId &&
          candidate.address === row.address &&
          candidate.assetAddress === row.assetAddress,
      );
      if (index >= 0) {
        this.state.balances[index] = { ...row };
      } else {
        this.state.balances.push({ ...row });
      }
    }
  }

  public async upsertProjectionBootstrapUtxoOutputs(rows: ProjectionUtxoOutput[]): Promise<void> {
    for (const row of rows) {
      const index = this.state.utxoOutputs.findIndex(
        (candidate) =>
          candidate.networkId === row.networkId && candidate.outputKey === row.outputKey,
      );
      if (index >= 0) {
        this.state.utxoOutputs[index] = { ...row };
      } else {
        this.state.utxoOutputs.push({ ...row });
      }
    }
  }

  public async getCurrentAddressSummary(networkId: PrimaryId, address: string) {
    const balance =
      this.state.balances.find(
        (candidate) =>
          candidate.networkId === networkId &&
          candidate.address === address &&
          candidate.assetAddress === '',
      )?.balance ?? '0';
    const utxoCount = this.state.utxoOutputs.filter(
      (candidate) =>
        candidate.networkId === networkId &&
        candidate.address === address &&
        candidate.isSpendable &&
        candidate.spentByTxid === null,
    ).length;

    if (balance === '0' && utxoCount === 0) {
      return null;
    }

    return {
      balance,
      utxoCount,
    };
  }

  public async listAppliedBlocks(networkId: PrimaryId, offset = 0, limit?: number) {
    const rows = this.state.appliedBlocks
      .filter((block) => block.networkId === networkId)
      .sort((left, right) => right.blockHeight - left.blockHeight);

    return rows.slice(offset, limit === undefined ? undefined : offset + limit);
  }

  public async getAppliedBlockByHash(networkId: PrimaryId, blockHash: string) {
    return (
      this.state.appliedBlocks.find(
        (block) => block.networkId === networkId && block.blockHash === blockHash,
      ) ?? null
    );
  }

  public async getTransactionRef(networkId: PrimaryId, txid: string) {
    const output = this.state.utxoOutputs.find(
      (candidate) => candidate.networkId === networkId && candidate.txid === txid,
    );
    if (!output) {
      return null;
    }

    return {
      blockHeight: output.blockHeight,
      blockHash: output.blockHash,
      blockTime: output.blockTime,
      txIndex: output.txIndex,
    };
  }

  public async getAddressSummary(networkId: PrimaryId, address: string) {
    const balance =
      this.state.balances.find(
        (candidate) =>
          candidate.networkId === networkId &&
          candidate.address === address &&
          candidate.assetAddress === '',
      )?.balance ?? '0';
    const movements = this.state.addressMovements.filter(
      (candidate) =>
        candidate.networkId === networkId &&
        candidate.address === address &&
        candidate.assetAddress === '',
    );
    const utxoCount = this.state.utxoOutputs.filter(
      (candidate) =>
        candidate.networkId === networkId &&
        candidate.address === address &&
        candidate.isSpendable &&
        candidate.spentByTxid === null,
    ).length;

    if (movements.length === 0 && balance === '0' && utxoCount === 0) {
      return null;
    }

    let receivedBase = 0n;
    let sentBase = 0n;
    const txids = new Set<string>();
    for (const movement of movements) {
      txids.add(movement.txid);
      if (movement.direction === 'credit') {
        receivedBase += parseAmountBase(movement.amountBase);
      } else {
        sentBase += parseAmountBase(movement.amountBase);
      }
    }

    return {
      balance,
      receivedBase: formatAmountBase(receivedBase),
      sentBase: formatAmountBase(sentBase),
      txCount: txids.size,
      utxoCount,
    };
  }

  public async listAddressTransactions(
    networkId: PrimaryId,
    address: string,
    offset = 0,
    limit?: number,
  ) {
    const aggregates = new Map<
      string,
      {
        blockHash: string;
        blockHeight: number;
        blockTime: number;
        receivedBase: bigint;
        sentBase: bigint;
        txIndex: number;
        txid: string;
      }
    >();

    for (const movement of this.state.addressMovements) {
      if (
        movement.networkId !== networkId ||
        movement.address !== address ||
        movement.assetAddress !== ''
      ) {
        continue;
      }

      const current = aggregates.get(movement.txid) ?? {
        blockHeight: movement.blockHeight,
        blockHash: movement.blockHash,
        blockTime: movement.blockTime,
        txid: movement.txid,
        txIndex: movement.txIndex,
        receivedBase: 0n,
        sentBase: 0n,
      };

      if (movement.direction === 'credit') {
        current.receivedBase += parseAmountBase(movement.amountBase);
      } else {
        current.sentBase += parseAmountBase(movement.amountBase);
      }
      aggregates.set(movement.txid, current);
    }

    return [...aggregates.values()]
      .sort(
        (left, right) =>
          right.blockHeight - left.blockHeight ||
          right.txIndex - left.txIndex ||
          right.txid.localeCompare(left.txid),
      )
      .slice(offset, limit === undefined ? undefined : offset + limit)
      .map((row) => ({
        blockHash: row.blockHash,
        blockHeight: row.blockHeight,
        blockTime: row.blockTime,
        txid: row.txid,
        txIndex: row.txIndex,
        receivedBase: formatAmountBase(row.receivedBase),
        sentBase: formatAmountBase(row.sentBase),
      }));
  }

  public async listAddressUtxos(networkId: PrimaryId, address: string, offset = 0, limit?: number) {
    return this.state.utxoOutputs
      .filter(
        (candidate) =>
          candidate.networkId === networkId &&
          candidate.address === address &&
          candidate.isSpendable &&
          candidate.spentByTxid === null,
      )
      .sort(
        (left, right) =>
          right.blockHeight - left.blockHeight ||
          right.txIndex - left.txIndex ||
          left.vout - right.vout,
      )
      .slice(offset, limit === undefined ? undefined : offset + limit);
  }

  public async getUtxoOutput(
    networkId: PrimaryId,
    outputKey: string,
  ): Promise<ProjectionUtxoOutput | null> {
    return (
      this.state.utxoOutputs.find(
        (output) => output.networkId === networkId && output.outputKey === outputKey,
      ) ?? null
    );
  }

  public async getUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    const outputs = this.state.utxoOutputs.filter(
      (output) => output.networkId === networkId && outputKeys.includes(output.outputKey),
    );

    return new Map(outputs.map((output) => [output.outputKey, output]));
  }

  public async hasAppliedBlock(
    networkId: PrimaryId,
    blockHeight: number,
    blockHash: string,
  ): Promise<boolean> {
    return this.state.appliedBlocks.some(
      (candidate) =>
        candidate.networkId === networkId &&
        candidate.blockHeight === blockHeight &&
        candidate.blockHash === blockHash,
    );
  }

  public async listAppliedBlockSet(
    networkId: PrimaryId,
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>> {
    return new Set(
      blocks
        .filter((block) =>
          this.state.appliedBlocks.some(
            (candidate) =>
              candidate.networkId === networkId &&
              candidate.blockHeight === block.blockHeight &&
              candidate.blockHash === block.blockHash,
          ),
        )
        .map((block) => blockIdentity(networkId, block.blockHeight, block.blockHash)),
    );
  }

  public async hasProjectionState(networkId: PrimaryId): Promise<boolean> {
    return this.state.appliedBlocks.some((candidate) => candidate.networkId === networkId);
  }

  public async getAppliedBlockTail(networkId: PrimaryId): Promise<number | null> {
    const tail = this.state.appliedBlocks
      .filter((candidate) => candidate.networkId === networkId)
      .reduce<number | null>(
        (current, candidate) =>
          current === null ? candidate.blockHeight : Math.max(current, candidate.blockHeight),
        null,
      );
    return tail;
  }

  public async importProjectionStateSnapshot(
    networkId: PrimaryId,
    snapshot: ProjectionStateBootstrapSnapshot,
  ): Promise<void> {
    this.state.appliedBlocks = [
      ...this.state.appliedBlocks.filter((row) => row.networkId !== networkId),
      ...snapshot.appliedBlocks,
    ];
    this.state.utxoOutputs = [
      ...this.state.utxoOutputs.filter((row) => row.networkId !== networkId),
      ...snapshot.utxoOutputs,
    ];
    this.state.balances = [
      ...this.state.balances.filter((row) => row.networkId !== networkId),
      ...snapshot.balances,
    ];
    this.state.directLinks = [
      ...this.state.directLinks.filter((row) => row.networkId !== networkId),
      ...snapshot.directLinks,
    ];
    this.state.sourceLinks = [
      ...this.state.sourceLinks.filter((row) => row.networkId !== networkId),
      ...snapshot.sourceLinks,
    ];
    await this.afterMutation();
  }

  public async listDirectLinksFromAddresses(networkId: PrimaryId, fromAddresses: string[]) {
    return this.state.directLinks.filter(
      (link) => link.networkId === networkId && fromAddresses.includes(link.fromAddress),
    );
  }

  public async listSourceSeedIdsReachingAddresses(
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<PrimaryId[]> {
    return [
      ...new Set(
        this.state.sourceLinks
          .filter((row) => row.networkId === networkId && addresses.includes(row.toAddress))
          .map((row) => row.sourceAddressId),
      ),
    ];
  }

  public async applyProjectionWindow(batches: BlockProjectionBatch[]): Promise<void> {
    for (const batch of batches) {
      await this.applyBlockProjection(batch);
    }
  }

  public async applyDirectLinkDeltasWindow(batches: ProjectionDirectLinkBatch[]): Promise<void> {
    for (const batch of batches) {
      const alreadyApplied = this.state.directLinkAppliedBlocks.some(
        (candidate) =>
          candidate.networkId === batch.networkId &&
          candidate.blockHeight === batch.blockHeight &&
          candidate.blockHash === batch.blockHash,
      );
      if (alreadyApplied) {
        continue;
      }

      for (const delta of batch.directLinkDeltas) {
        const current = this.state.directLinks.find(
          (candidate) =>
            candidate.networkId === delta.networkId &&
            candidate.fromAddress === delta.fromAddress &&
            candidate.toAddress === delta.toAddress &&
            candidate.assetAddress === delta.assetAddress,
        );
        if (current) {
          current.transferCount += delta.transferCount;
          current.totalAmountBase = addAmountBase(current.totalAmountBase, delta.totalAmountBase);
          current.firstSeenBlockHeight = Math.min(
            current.firstSeenBlockHeight,
            delta.firstSeenBlockHeight,
          );
          current.lastSeenBlockHeight = Math.max(
            current.lastSeenBlockHeight,
            delta.lastSeenBlockHeight,
          );
          continue;
        }

        this.state.directLinks.push({ ...delta });
      }

      this.state.directLinkAppliedBlocks.push({
        networkId: batch.networkId,
        blockHeight: batch.blockHeight,
        blockHash: batch.blockHash,
      });
    }

    await this.afterMutation();
  }

  public async applyProjectionFacts(window: ProjectionFactWindow): Promise<void> {
    for (const output of window.utxoOutputs) {
      const existingIndex = this.state.utxoOutputs.findIndex(
        (candidate) =>
          candidate.networkId === output.networkId && candidate.outputKey === output.outputKey,
      );
      if (existingIndex >= 0) {
        this.state.utxoOutputs[existingIndex] = { ...output };
      } else {
        this.state.utxoOutputs.push({ ...output });
      }
    }

    for (const movement of window.addressMovements) {
      const exists = this.state.addressMovements.some(
        (candidate) =>
          candidate.networkId === movement.networkId &&
          candidate.movementId === movement.movementId,
      );
      if (!exists) {
        this.state.addressMovements.push(movement);
      }
    }

    for (const transfer of window.transfers) {
      const exists = this.state.transfers.some(
        (candidate) =>
          candidate.networkId === transfer.networkId &&
          candidate.transferId === transfer.transferId,
      );
      if (!exists) {
        this.state.transfers.push(transfer);
      }
    }

    for (const balance of window.balances) {
      const existing = this.state.balances.find(
        (candidate) =>
          candidate.networkId === balance.networkId &&
          candidate.address === balance.address &&
          candidate.assetAddress === balance.assetAddress,
      );
      if (existing) {
        existing.balance = balance.balance;
        existing.asOfBlockHeight = balance.asOfBlockHeight;
      } else {
        this.state.balances.push({ ...balance });
      }
    }

    for (const link of window.directLinks) {
      const existing = this.state.directLinks.find(
        (candidate) =>
          candidate.networkId === link.networkId &&
          candidate.fromAddress === link.fromAddress &&
          candidate.toAddress === link.toAddress &&
          candidate.assetAddress === link.assetAddress,
      );
      if (existing) {
        Object.assign(existing, link);
      } else {
        this.state.directLinks.push({ ...link });
      }
    }

    for (const block of window.appliedBlocks) {
      const exists = this.state.appliedBlocks.some(
        (candidate) =>
          candidate.networkId === block.networkId &&
          candidate.blockHeight === block.blockHeight &&
          candidate.blockHash === block.blockHash,
      );
      if (!exists) {
        this.state.appliedBlocks.push({ ...block });
      }
    }

    await this.afterMutation();
  }

  public async exportProjectionStateSnapshot(
    networkId: PrimaryId,
  ): Promise<ProjectionStateBootstrapSnapshot> {
    return {
      appliedBlocks: this.state.appliedBlocks.filter((row) => row.networkId === networkId),
      utxoOutputs: this.state.utxoOutputs.filter((row) => row.networkId === networkId),
      balances: this.state.balances.filter((row) => row.networkId === networkId),
      directLinks: this.state.directLinks.filter((row) => row.networkId === networkId),
      sourceLinks: this.state.sourceLinks.filter((row) => row.networkId === networkId),
    };
  }

  public async applyBlockProjection(batch: BlockProjectionBatch): Promise<void> {
    const alreadyApplied = await this.hasAppliedBlock(
      batch.networkId,
      batch.blockHeight,
      batch.blockHash,
    );
    if (alreadyApplied) {
      return;
    }

    for (const output of batch.utxoCreates) {
      const existingIndex = this.state.utxoOutputs.findIndex(
        (candidate) =>
          candidate.networkId === output.networkId && candidate.outputKey === output.outputKey,
      );
      if (existingIndex >= 0) {
        this.state.utxoOutputs[existingIndex] = output;
      } else {
        this.state.utxoOutputs.push(output);
      }
    }

    for (const spend of batch.utxoSpends) {
      const output = this.state.utxoOutputs.find(
        (candidate) =>
          candidate.networkId === batch.networkId && candidate.outputKey === spend.outputKey,
      );
      if (!output) {
        throw new Error(`missing utxo output: ${spend.outputKey}`);
      }

      output.spentByTxid = spend.spentByTxid;
      output.spentInBlock = spend.spentInBlock;
      output.spentInputIndex = spend.spentInputIndex;
    }

    for (const movement of batch.addressMovements) {
      const exists = this.state.addressMovements.some(
        (candidate) =>
          candidate.networkId === movement.networkId &&
          candidate.movementId === movement.movementId,
      );
      if (!exists) {
        this.state.addressMovements.push(movement);
        this.applyBalanceDelta(movement, batch.blockHeight);
      }
    }

    for (const transfer of batch.transfers) {
      const exists = this.state.transfers.some(
        (candidate) =>
          candidate.networkId === transfer.networkId &&
          candidate.transferId === transfer.transferId,
      );
      if (!exists) {
        this.state.transfers.push(transfer);
      }
    }

    for (const delta of batch.directLinkDeltas) {
      const current = this.state.directLinks.find(
        (candidate) =>
          candidate.networkId === delta.networkId &&
          candidate.fromAddress === delta.fromAddress &&
          candidate.toAddress === delta.toAddress &&
          candidate.assetAddress === delta.assetAddress,
      );
      if (current) {
        current.transferCount += delta.transferCount;
        current.totalAmountBase = addAmountBase(current.totalAmountBase, delta.totalAmountBase);
        current.firstSeenBlockHeight = Math.min(
          current.firstSeenBlockHeight,
          delta.firstSeenBlockHeight,
        );
        current.lastSeenBlockHeight = Math.max(
          current.lastSeenBlockHeight,
          delta.lastSeenBlockHeight,
        );
        continue;
      }

      this.state.directLinks.push({ ...delta });
    }

    this.state.appliedBlocks.push({
      networkId: batch.networkId,
      blockHeight: batch.blockHeight,
      blockHash: batch.blockHash,
    });
    await this.afterMutation();
  }

  public async replaceSourceLinks(
    networkId: PrimaryId,
    sourceAddressId: PrimaryId,
    rows: SourceLinkRecord[],
  ): Promise<void> {
    this.state.sourceLinks = this.state.sourceLinks.filter(
      (row) => !(row.networkId === networkId && row.sourceAddressId === sourceAddressId),
    );
    this.state.sourceLinks.push(...rows);
    await this.afterMutation();
  }

  protected async afterMutation(): Promise<void> {}

  private applyBalanceDelta(movement: AddressMovement, blockHeight: number): void {
    const current = this.state.balances.find(
      (candidate) =>
        candidate.networkId === movement.networkId &&
        candidate.address === movement.address &&
        candidate.assetAddress === movement.assetAddress,
    );
    const currentAmount = parseAmountBase(current?.balance ?? '0');
    const nextAmount =
      movement.direction === 'credit'
        ? currentAmount + parseAmountBase(movement.amountBase)
        : currentAmount - parseAmountBase(movement.amountBase);
    if (nextAmount < 0n) {
      throw new Error(
        `negative balance for ${movement.networkId}:${movement.address}:${movement.assetAddress}`,
      );
    }

    if (current) {
      current.balance = formatAmountBase(nextAmount);
      current.asOfBlockHeight = blockHeight;
      return;
    }

    this.state.balances.push({
      networkId: movement.networkId,
      address: movement.address,
      assetAddress: movement.assetAddress,
      balance: formatAmountBase(nextAmount),
      asOfBlockHeight: blockHeight,
    });
  }
}

export class DuckDbWarehouseAdapter extends InMemoryWarehouseAdapter {
  public constructor(private readonly path: string) {
    super();
  }

  public async boot(): Promise<void> {
    try {
      const contents = await readFile(this.path, 'utf8');
      const loadedState: Partial<WarehouseState> = JSON.parse(contents);
      this.state = mergeWarehouseState(loadedState);
    } catch {
      await this.afterMutation();
    }
  }

  protected override async afterMutation(): Promise<void> {
    await mkdir(dirname(this.path), { recursive: true });
    await writeFile(this.path, JSON.stringify(this.state, null, 2));
  }
}

export class ClickHouseWarehouseAdapter
  implements
    InvestigationWarehousePort,
    ProjectionWarehousePort,
    ProjectionFactWarehousePort,
    ExplorerWarehousePort
{
  private readonly client: ReturnType<typeof createClient>;

  public constructor(settings: WarehouseSettings) {
    this.client = createClient({
      url: settings.location,
      ...(settings.database ? { database: settings.database } : {}),
      ...(settings.user ? { username: settings.user } : {}),
      ...(settings.password ? { password: settings.password } : {}),
    });
  }

  public async boot(): Promise<void> {
    await this.migrate();
  }

  public async getCurrentAddressSummary(networkId: PrimaryId, address: string) {
    const summary = await this.getAddressSummary(networkId, address);
    if (!summary) {
      return null;
    }

    return {
      balance: summary.balance,
      utxoCount: summary.utxoCount,
    };
  }

  public async getBalancesByAddresses(addresses: string[]) {
    if (addresses.length === 0) {
      return [];
    }

    const rowChunks: BalanceRow[][] = await Promise.all(
      chunkQueryValues(addresses).map((chunk) =>
        this.queryRows<BalanceRow>({
          query: `
              SELECT
                network_id AS "networkId",
                asset_address AS "assetAddress",
                address,
                balance,
                as_of_block_height AS "asOfBlockHeight"
              FROM balances_v2
              WHERE address IN ({addresses:Array(String)})
              ORDER BY network_id ASC, asset_address ASC, address ASC, version DESC
              LIMIT 1 BY network_id, asset_address, address
            `,
          query_params: { addresses: chunk },
          format: 'JSONEachRow',
        }),
      ),
    );
    const rows = rowChunks.flat();

    return rows;
  }

  public async getTokensByAddresses() {
    return [];
  }

  public async getDistinctLinksByAddresses(addresses: string[]) {
    if (addresses.length === 0) {
      return [];
    }

    const rowChunks: Array<
      Array<{
        fromAddress: string;
        networkId: PrimaryId;
        toAddress: string;
        transferCount: number;
      }>
    > = await Promise.all(
      chunkQueryValues(addresses).map((chunk) =>
        this.queryRows<{
          fromAddress: string;
          networkId: PrimaryId;
          toAddress: string;
          transferCount: number;
        }>({
          query: `
              SELECT network_id AS "networkId", source_address AS "fromAddress", to_address AS "toAddress", hop_count AS "transferCount"
              FROM source_links
              WHERE to_address IN ({addresses:Array(String)})
            `,
          query_params: { addresses: chunk },
          format: 'JSONEachRow',
        }),
      ),
    );
    const rows = rowChunks.flat();

    return rows;
  }

  public async listAppliedBlocks(networkId: PrimaryId, offset = 0, limit?: number) {
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash"
          FROM applied_blocks_v2
          WHERE network_id = {networkId:UInt64}
          ORDER BY block_height DESC
          ${limit !== undefined ? 'LIMIT {limit:UInt64}' : ''}
          ${offset > 0 ? 'OFFSET {offset:UInt64}' : ''}
        `,
      query_params: {
        networkId,
        ...(limit !== undefined ? { limit } : {}),
        ...(offset > 0 ? { offset } : {}),
      },
      format: 'JSONEachRow',
    });

    return rows;
  }

  public async getAppliedBlockByHash(networkId: PrimaryId, blockHash: string) {
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash"
          FROM applied_blocks_v2
          WHERE network_id = {networkId:UInt64} AND block_hash = {blockHash:String}
          LIMIT 1
        `,
      query_params: { networkId, blockHash },
      format: 'JSONEachRow',
    });

    return rows[0] ?? null;
  }

  public async getTransactionRef(networkId: PrimaryId, txid: string) {
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      txIndex: number;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash",
            block_time AS "blockTime",
            tx_index AS "txIndex"
          FROM utxo_outputs_v2
          WHERE network_id = {networkId:UInt64} AND txid = {txid:String}
          ORDER BY version DESC
          LIMIT 1
        `,
      query_params: { networkId, txid },
      format: 'JSONEachRow',
    });

    return rows[0] ?? null;
  }

  public async getAddressSummary(networkId: PrimaryId, address: string) {
    const [movementRows, balanceRows, utxoRows] = await Promise.all([
      this.queryRows<{
        receivedBase: string;
        sentBase: string;
        txCount: number;
      }>({
        query: `
            SELECT
              CAST(sumIf(amount_base_i256, direction = 'credit') AS String) AS "receivedBase",
              CAST(sumIf(amount_base_i256, direction = 'debit') AS String) AS "sentBase",
              uniqExact(txid) AS "txCount"
            FROM ${addressMovementsByAddressTable}
            WHERE network_id = {networkId:UInt64} AND address = {address:String} AND asset_address = ''
          `,
        query_params: { networkId, address },
        format: 'JSONEachRow',
      }),
      this.queryRows<{ balance: string }>({
        query: `
            SELECT balance
            FROM balances_v2
            WHERE network_id = {networkId:UInt64} AND address = {address:String} AND asset_address = ''
            ORDER BY version DESC
            LIMIT 1
          `,
        query_params: { networkId, address },
        format: 'JSONEachRow',
      }),
      this.queryRows<{ utxoCount: number }>({
        query: `
            SELECT count() AS "utxoCount"
            FROM (
              SELECT
                output_key,
                is_spendable,
                spent_by_txid
              FROM ${utxoCurrentStateByAddressTable}
              WHERE network_id = {networkId:UInt64} AND address = {address:String}
              ORDER BY output_key ASC, version DESC
              LIMIT 1 BY output_key
            )
            WHERE is_spendable = 1 AND spent_by_txid IS NULL
          `,
        query_params: { networkId, address },
        format: 'JSONEachRow',
      }),
    ]);

    const movement = movementRows[0];
    const balance = balanceRows[0]?.balance ?? '0';
    const utxoCount = utxoRows[0]?.utxoCount ?? 0;
    if (!movement && balance === '0' && utxoCount === 0) {
      return null;
    }

    return {
      balance,
      receivedBase: movement?.receivedBase ?? '0',
      sentBase: movement?.sentBase ?? '0',
      txCount: movement?.txCount ?? 0,
      utxoCount,
    };
  }

  public async listAddressTransactions(
    networkId: PrimaryId,
    address: string,
    offset = 0,
    limit?: number,
  ) {
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      receivedBase: string;
      sentBase: string;
      txIndex: number;
      txid: string;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash",
            block_time AS "blockTime",
            txid,
            tx_index AS "txIndex",
            CAST(sumIf(amount_base_i256, direction = 'credit') AS String) AS "receivedBase",
            CAST(sumIf(amount_base_i256, direction = 'debit') AS String) AS "sentBase"
          FROM ${addressMovementsByAddressTable}
          WHERE network_id = {networkId:UInt64} AND address = {address:String} AND asset_address = ''
          GROUP BY block_height, block_hash, block_time, txid, tx_index
          ORDER BY block_height DESC, tx_index DESC, txid DESC
          ${limit !== undefined ? 'LIMIT {limit:UInt64}' : ''}
          ${offset > 0 ? 'OFFSET {offset:UInt64}' : ''}
        `,
      query_params: {
        networkId,
        address,
        ...(limit !== undefined ? { limit } : {}),
        ...(offset > 0 ? { offset } : {}),
      },
      format: 'JSONEachRow',
    });

    return rows;
  }

  public async listAddressUtxos(networkId: PrimaryId, address: string, offset = 0, limit?: number) {
    const pageRows = await this.queryRows<{
      blockHeight: number;
      outputKey: string;
      txIndex: number;
      vout: number;
    }>({
      query: `
          SELECT
            output_key AS "outputKey",
            block_height AS "blockHeight",
            tx_index AS "txIndex",
            vout,
            is_spendable,
            spent_by_txid
          FROM (
            SELECT
              output_key,
              block_height,
              tx_index,
              vout,
              is_spendable,
              spent_by_txid
            FROM ${utxoCurrentStateByAddressTable}
            WHERE network_id = {networkId:UInt64} AND address = {address:String}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          )
          WHERE is_spendable = 1 AND spent_by_txid IS NULL
          ORDER BY block_height DESC, tx_index DESC, vout ASC
          ${limit !== undefined ? 'LIMIT {limit:UInt64}' : ''}
          ${offset > 0 ? 'OFFSET {offset:UInt64}' : ''}
        `,
      query_params: {
        networkId,
        address,
        ...(limit !== undefined ? { limit } : {}),
        ...(offset > 0 ? { offset } : {}),
      },
      format: 'JSONEachRow',
    });

    if (pageRows.length === 0) {
      return [];
    }

    const outputsByKey = await this.getUtxoOutputs(
      networkId,
      pageRows.map((row) => row.outputKey),
    );

    return pageRows.flatMap((row) => {
      const output = outputsByKey.get(row.outputKey);
      if (!output || !output.isSpendable || output.spentByTxid !== null) {
        return [];
      }

      return [output];
    });
  }

  public async getUtxoOutput(
    networkId: PrimaryId,
    outputKey: string,
  ): Promise<ProjectionUtxoOutput | null> {
    return (await this.getUtxoOutputs(networkId, [outputKey])).get(outputKey) ?? null;
  }

  public async getUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    if (outputKeys.length === 0) {
      return new Map();
    }

    const currentRows = await this.queryUtxoOutputsFromTable(
      utxoCurrentStateTable,
      networkId,
      outputKeys,
    );
    const currentOutputs = new Map(currentRows.map((row) => [row.outputKey, row]));
    const missingOutputKeys = outputKeys.filter((outputKey) => !currentOutputs.has(outputKey));
    if (missingOutputKeys.length === 0) {
      return currentOutputs;
    }

    const fallbackRows = await this.queryUtxoOutputsFromTable(
      'utxo_outputs_v2',
      networkId,
      missingOutputKeys,
    );
    if (fallbackRows.length > 0) {
      await this.insertRows(
        utxoCurrentStateTable,
        fallbackRows.map((row) => toUtxoInsertRow(row, row.spentInBlock ?? row.blockHeight)),
      );
      for (const row of fallbackRows) {
        currentOutputs.set(row.outputKey, row);
      }
    }

    return currentOutputs;
  }

  public async hasAppliedBlock(
    networkId: PrimaryId,
    blockHeight: number,
    blockHash: string,
  ): Promise<boolean> {
    const rows = await this.queryRows<Record<string, unknown>>({
      query: `
          SELECT 1
          FROM applied_blocks_v2
          WHERE
            network_id = {networkId:UInt64}
            AND block_height = {blockHeight:UInt64}
            AND block_hash = {blockHash:String}
          LIMIT 1
        `,
      query_params: { networkId, blockHeight, blockHash },
      format: 'JSONEachRow',
    });

    return rows.length > 0;
  }

  public async listAppliedBlockSet(
    networkId: PrimaryId,
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>> {
    if (blocks.length === 0) {
      return new Set();
    }

    const heights = [...new Set(blocks.map((block) => block.blockHeight))];
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
      networkId: PrimaryId;
    }>({
      query: `
          SELECT
            network_id AS "networkId",
            block_height AS "blockHeight",
            block_hash AS "blockHash"
          FROM applied_blocks_v2
          WHERE network_id = {networkId:UInt64} AND block_height IN ({heights:Array(UInt64)})
        `,
      query_params: { networkId, heights },
      format: 'JSONEachRow',
    });

    const requested = new Set(
      blocks.map((block) => blockIdentity(networkId, block.blockHeight, block.blockHash)),
    );
    return new Set(
      rows
        .map((row) => blockIdentity(row.networkId, row.blockHeight, row.blockHash))
        .filter((identity) => requested.has(identity)),
    );
  }

  public async getAppliedBlockTail(networkId: PrimaryId): Promise<number | null> {
    const rows = await this.queryRows<{ blockHeight: number | null }>({
      query: `
          SELECT max(block_height) AS "blockHeight"
          FROM applied_blocks_v2
          WHERE network_id = {networkId:UInt64}
        `,
      query_params: { networkId },
      format: 'JSONEachRow',
    });

    const blockHeight = rows[0]?.blockHeight;
    return blockHeight === null || blockHeight === undefined ? null : Number(blockHeight);
  }

  public async listDirectLinksFromAddresses(networkId: PrimaryId, fromAddresses: string[]) {
    if (fromAddresses.length === 0) {
      return [];
    }

    const rowChunks: DirectLinkRecord[][] = await Promise.all(
      chunkQueryValues(fromAddresses).map((chunk) =>
        this.queryRows<DirectLinkRecord>({
          query: `
              SELECT
                {networkId:UInt64} AS "networkId",
                from_address AS "fromAddress",
                to_address AS "toAddress",
                asset_address AS "assetAddress",
                transfer_count AS "transferCount",
                total_amount_base AS "totalAmountBase",
                first_seen_block_height AS "firstSeenBlockHeight",
                last_seen_block_height AS "lastSeenBlockHeight"
              FROM direct_links_v2
              WHERE network_id = {networkId:UInt64} AND from_address IN ({fromAddresses:Array(String)})
              ORDER BY from_address ASC, to_address ASC, asset_address ASC, version DESC
              LIMIT 1 BY network_id, from_address, to_address, asset_address
            `,
          query_params: { networkId, fromAddresses: chunk },
          format: 'JSONEachRow',
        }),
      ),
    );
    const rows = rowChunks.flat();

    return rows;
  }

  public async listSourceSeedIdsReachingAddresses(
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<PrimaryId[]> {
    if (addresses.length === 0) {
      return [];
    }

    const rowChunks: Array<Array<{ sourceAddressId: PrimaryId }>> = await Promise.all(
      chunkQueryValues(addresses).map((chunk) =>
        this.queryRows<{ sourceAddressId: PrimaryId }>({
          query: `
              SELECT DISTINCT source_address_id AS "sourceAddressId"
              FROM source_links
              WHERE network_id = {networkId:UInt64} AND to_address IN ({addresses:Array(String)})
            `,
          query_params: { networkId, addresses: chunk },
          format: 'JSONEachRow',
        }),
      ),
    );
    const rows = rowChunks.flat();

    return [...new Set(rows.map((row) => row.sourceAddressId))];
  }

  public async applyProjectionWindow(batches: BlockProjectionBatch[]): Promise<void> {
    if (batches.length === 0) {
      return;
    }

    const orderedBatches = [...batches].sort((left, right) => left.blockHeight - right.blockHeight);
    const networkId = orderedBatches[0]?.networkId;
    if (networkId === undefined) {
      return;
    }

    const appliedBlocks = await this.listAppliedBlockSet(networkId, orderedBatches);
    const pendingBatches = orderedBatches.filter(
      (batch) =>
        !appliedBlocks.has(blockIdentity(batch.networkId, batch.blockHeight, batch.blockHash)),
    );
    if (pendingBatches.length === 0) {
      return;
    }

    const windowEnd = pendingBatches.at(-1)?.blockHeight ?? 0;
    const spendKeys = [
      ...new Set(
        pendingBatches.flatMap((batch) => batch.utxoSpends.map((spend) => spend.outputKey)),
      ),
    ];
    const currentOutputs = await this.getUtxoOutputs(networkId, spendKeys);
    const nextOutputs = new Map<string, ProjectionUtxoOutput>();

    for (const batch of pendingBatches) {
      for (const output of batch.utxoCreates) {
        nextOutputs.set(output.outputKey, { ...output });
      }

      for (const spend of batch.utxoSpends) {
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

    const balanceKeys = [
      ...new Set(
        pendingBatches.flatMap((batch) =>
          batch.addressMovements.map((movement) =>
            balanceKey(movement.networkId, movement.address, movement.assetAddress),
          ),
        ),
      ),
    ];
    const currentBalances = await this.getBalanceRowsByKeys(networkId, balanceKeys);
    const nextBalances = new Map<string, VersionedBalanceRow>();

    for (const batch of pendingBatches) {
      for (const movement of batch.addressMovements) {
        const key = balanceKey(movement.networkId, movement.address, movement.assetAddress);
        const current = nextBalances.get(key) ?? currentBalances.get(key);
        const currentAmount = parseAmountBase(current?.balance ?? '0');
        const nextAmount =
          movement.direction === 'credit'
            ? currentAmount + parseAmountBase(movement.amountBase)
            : currentAmount - parseAmountBase(movement.amountBase);
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
          asOfBlockHeight: batch.blockHeight,
          version: windowEnd,
        });
      }
    }

    const directLinkKeys = [
      ...new Set(
        pendingBatches.flatMap((batch) =>
          batch.directLinkDeltas.map((delta) =>
            directLinkKey(delta.networkId, delta.fromAddress, delta.toAddress, delta.assetAddress),
          ),
        ),
      ),
    ];
    const currentDirectLinks = await this.getDirectLinkRowsByKeys(networkId, directLinkKeys);
    const nextDirectLinks = new Map<string, VersionedDirectLinkRow>();

    for (const batch of pendingBatches) {
      for (const delta of batch.directLinkDeltas) {
        const key = directLinkKey(
          delta.networkId,
          delta.fromAddress,
          delta.toAddress,
          delta.assetAddress,
        );
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
            version: windowEnd,
          });
          continue;
        }

        nextDirectLinks.set(key, {
          ...delta,
          version: windowEnd,
        });
      }
    }

    await this.insertRows(
      'address_movements_v2',
      pendingBatches.flatMap((batch) =>
        batch.addressMovements.map((row) => ({
          movement_id: row.movementId,
          network_id: row.networkId,
          block_height: row.blockHeight,
          block_hash: row.blockHash,
          block_time: row.blockTime,
          txid: row.txid,
          tx_index: row.txIndex,
          entry_index: row.entryIndex,
          address: row.address,
          asset_address: row.assetAddress,
          direction: row.direction,
          amount_base: row.amountBase,
          output_key: row.outputKey,
          derivation_method: row.derivationMethod,
        })),
      ),
    );
    await this.insertRows(
      'transfers_v2',
      pendingBatches.flatMap((batch) =>
        batch.transfers.map((row) => ({
          transfer_id: row.transferId,
          network_id: row.networkId,
          block_height: row.blockHeight,
          block_hash: row.blockHash,
          block_time: row.blockTime,
          txid: row.txid,
          tx_index: row.txIndex,
          transfer_index: row.transferIndex,
          asset_address: row.assetAddress,
          from_address: row.fromAddress,
          to_address: row.toAddress,
          amount_base: row.amountBase,
          derivation_method: row.derivationMethod,
          confidence: row.confidence,
          is_change: row.isChange ? 1 : 0,
          input_address_count: row.inputAddressCount,
          output_address_count: row.outputAddressCount,
        })),
      ),
    );
    await this.insertRows(
      'utxo_outputs_v2',
      [...nextOutputs.values()].map((row) => toUtxoInsertRow(row, windowEnd)),
    );
    await this.insertRows(
      utxoCurrentStateTable,
      [...nextOutputs.values()].map((row) => toUtxoInsertRow(row, windowEnd)),
    );
    await this.insertRows(
      'balances_v2',
      [...nextBalances.values()].map((row) => ({
        network_id: row.networkId,
        address: row.address,
        asset_address: row.assetAddress,
        balance: row.balance,
        as_of_block_height: row.asOfBlockHeight,
        version: row.version,
      })),
    );
    await this.insertRows(
      'direct_links_v2',
      [...nextDirectLinks.values()].map((row) => ({
        network_id: row.networkId,
        from_address: row.fromAddress,
        to_address: row.toAddress,
        asset_address: row.assetAddress,
        transfer_count: row.transferCount,
        total_amount_base: row.totalAmountBase,
        first_seen_block_height: row.firstSeenBlockHeight,
        last_seen_block_height: row.lastSeenBlockHeight,
        version: row.version,
      })),
    );
    await this.insertRows(
      'applied_blocks_v2',
      pendingBatches.map((batch) => ({
        network_id: batch.networkId,
        block_height: batch.blockHeight,
        block_hash: batch.blockHash,
      })),
    );
  }

  public async applyProjectionFacts(window: ProjectionFactWindow): Promise<void> {
    await this.insertRows(
      'address_movements_v2',
      window.addressMovements.map((row) => ({
        movement_id: row.movementId,
        network_id: row.networkId,
        block_height: row.blockHeight,
        block_hash: row.blockHash,
        block_time: row.blockTime,
        txid: row.txid,
        tx_index: row.txIndex,
        entry_index: row.entryIndex,
        address: row.address,
        asset_address: row.assetAddress,
        direction: row.direction,
        amount_base: row.amountBase,
        output_key: row.outputKey,
        derivation_method: row.derivationMethod,
      })),
    );
    await this.insertRows(
      'transfers_v2',
      window.transfers.map((row) => ({
        transfer_id: row.transferId,
        network_id: row.networkId,
        block_height: row.blockHeight,
        block_hash: row.blockHash,
        block_time: row.blockTime,
        txid: row.txid,
        tx_index: row.txIndex,
        transfer_index: row.transferIndex,
        asset_address: row.assetAddress,
        from_address: row.fromAddress,
        to_address: row.toAddress,
        amount_base: row.amountBase,
        derivation_method: row.derivationMethod,
        confidence: row.confidence,
        is_change: row.isChange ? 1 : 0,
        input_address_count: row.inputAddressCount,
        output_address_count: row.outputAddressCount,
      })),
    );
    await this.insertRows(
      'utxo_outputs_v2',
      window.utxoOutputs.map((row) => toUtxoInsertRow(row, row.spentInBlock ?? row.blockHeight)),
    );
    await this.insertRows(
      utxoCurrentStateTable,
      window.utxoOutputs.map((row) => toUtxoInsertRow(row, row.spentInBlock ?? row.blockHeight)),
    );
    await this.insertRows(
      'balances_v2',
      window.balances.map((row) => ({
        network_id: row.networkId,
        address: row.address,
        asset_address: row.assetAddress,
        balance: row.balance,
        as_of_block_height: row.asOfBlockHeight,
        version: row.asOfBlockHeight,
      })),
    );
    await this.insertRows(
      'direct_links_v2',
      window.directLinks.map((row) => ({
        network_id: row.networkId,
        from_address: row.fromAddress,
        to_address: row.toAddress,
        asset_address: row.assetAddress,
        transfer_count: row.transferCount,
        total_amount_base: row.totalAmountBase,
        first_seen_block_height: row.firstSeenBlockHeight,
        last_seen_block_height: row.lastSeenBlockHeight,
        version: row.lastSeenBlockHeight,
      })),
    );
    await this.insertRows(
      'applied_blocks_v2',
      window.appliedBlocks.map((row) => ({
        network_id: row.networkId,
        block_height: row.blockHeight,
        block_hash: row.blockHash,
      })),
    );
  }

  public async applyBlockProjection(batch: BlockProjectionBatch): Promise<void> {
    await this.applyProjectionWindow([batch]);
  }

  public async replaceSourceLinks(
    networkId: PrimaryId,
    sourceAddressId: PrimaryId,
    rows: SourceLinkRecord[],
  ): Promise<void> {
    await this.executeCommand({
      query: `
        ALTER TABLE source_links
        DELETE WHERE network_id = {networkId:UInt64} AND source_address_id = {sourceAddressId:UInt64}
      `,
      query_params: { networkId, sourceAddressId },
    });
    await this.insertRows(
      'source_links',
      rows.map((row) => ({
        network_id: row.networkId,
        source_address_id: row.sourceAddressId,
        source_address: row.sourceAddress,
        to_address: row.toAddress,
        hop_count: row.hopCount,
        path_transfer_count: row.pathTransferCount,
        path_addresses: row.pathAddresses,
        first_seen_block_height: row.firstSeenBlockHeight,
        last_seen_block_height: row.lastSeenBlockHeight,
      })),
    );
  }

  public async exportProjectionStateSnapshot(
    networkId: PrimaryId,
  ): Promise<ProjectionStateBootstrapSnapshot> {
    const [utxoOutputs, balances, directLinks, sourceLinks] = await Promise.all([
      this.queryRows<ProjectionUtxoOutput>({
        query: `
            SELECT
              network_id AS "networkId",
              block_height AS "blockHeight",
              block_hash AS "blockHash",
              block_time AS "blockTime",
              txid,
              tx_index AS "txIndex",
              vout,
              output_key AS "outputKey",
              address,
              script_type AS "scriptType",
              value_base AS "valueBase",
              is_coinbase = 1 AS "isCoinbase",
              is_spendable = 1 AS "isSpendable",
              spent_by_txid AS "spentByTxid",
              spent_in_block AS "spentInBlock",
              spent_input_index AS "spentInputIndex"
            FROM ${utxoCurrentStateTable}
            WHERE network_id = {networkId:UInt64}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          `,
        query_params: { networkId },
        format: 'JSONEachRow',
      }),
      this.queryRows<ProjectionBalanceSnapshot>({
        query: `
            SELECT
              network_id AS "networkId",
              address,
              asset_address AS "assetAddress",
              balance,
              as_of_block_height AS "asOfBlockHeight"
            FROM balances_v2
            WHERE network_id = {networkId:UInt64}
            ORDER BY address ASC, asset_address ASC, version DESC
            LIMIT 1 BY network_id, address, asset_address
          `,
        query_params: { networkId },
        format: 'JSONEachRow',
      }),
      this.queryRows<DirectLinkRecord>({
        query: `
            SELECT
              network_id AS "networkId",
              from_address AS "fromAddress",
              to_address AS "toAddress",
              asset_address AS "assetAddress",
              transfer_count AS "transferCount",
              total_amount_base AS "totalAmountBase",
              first_seen_block_height AS "firstSeenBlockHeight",
              last_seen_block_height AS "lastSeenBlockHeight"
            FROM direct_links_v2
            WHERE network_id = {networkId:UInt64}
            ORDER BY from_address ASC, to_address ASC, asset_address ASC, version DESC
            LIMIT 1 BY network_id, from_address, to_address, asset_address
          `,
        query_params: { networkId },
        format: 'JSONEachRow',
      }),
      this.queryRows<SourceLinkRecord>({
        query: `
            SELECT
              network_id AS "networkId",
              source_address_id AS "sourceAddressId",
              source_address AS "sourceAddress",
              to_address AS "toAddress",
              hop_count AS "hopCount",
              path_transfer_count AS "pathTransferCount",
              path_addresses AS "pathAddresses",
              first_seen_block_height AS "firstSeenBlockHeight",
              last_seen_block_height AS "lastSeenBlockHeight"
            FROM source_links
            WHERE network_id = {networkId:UInt64}
          `,
        query_params: { networkId },
        format: 'JSONEachRow',
      }),
    ]);

    return {
      appliedBlocks: [],
      utxoOutputs,
      balances,
      directLinks,
      sourceLinks: sourceLinks.map((row) => ({
        ...row,
        pathAddresses: Array.isArray(row.pathAddresses) ? row.pathAddresses : [],
      })),
    };
  }

  public async listCurrentUtxoOutputsPage(
    networkId: PrimaryId,
    cursorOutputKey: string | null,
    limit: number,
  ): Promise<ProjectionCurrentUtxoPage> {
    const rows = await this.queryRows<ProjectionUtxoOutput>({
      query: `
          SELECT
            {networkId:UInt64} AS "networkId",
            block_height AS "blockHeight",
            block_hash AS "blockHash",
            block_time AS "blockTime",
            txid,
            tx_index AS "txIndex",
            vout,
            output_key AS "outputKey",
            address,
            script_type AS "scriptType",
            value_base AS "valueBase",
            is_coinbase = 1 AS "isCoinbase",
            is_spendable = 1 AS "isSpendable",
            spent_by_txid AS "spentByTxid",
            spent_in_block AS "spentInBlock",
            spent_input_index AS "spentInputIndex"
          FROM (
            SELECT
              block_height,
              block_hash,
              block_time,
              txid,
              tx_index,
              vout,
              output_key,
              address,
              script_type,
              value_base,
              is_coinbase,
              is_spendable,
              spent_by_txid,
              spent_in_block,
              spent_input_index,
              version
            FROM ${utxoCurrentStateTable}
            WHERE
              network_id = {networkId:UInt64}
              ${cursorOutputKey === null ? '' : 'AND output_key > {cursorOutputKey:String}'}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          )
          ORDER BY output_key ASC
          LIMIT {limit:UInt64}
        `,
      query_params: {
        networkId,
        limit,
        ...(cursorOutputKey === null ? {} : { cursorOutputKey }),
      },
      format: 'JSONEachRow',
    });

    return {
      rows,
      nextCursor: rows.length === limit ? (rows.at(-1)?.outputKey ?? null) : null,
    };
  }

  public async listCurrentBalancesPage(
    networkId: PrimaryId,
    cursor: ProjectionBalanceCursor | null,
    limit: number,
  ): Promise<ProjectionCurrentBalancePage> {
    const rows = await this.queryRows<ProjectionBalanceSnapshot>({
      query: `
          SELECT
            network_id AS "networkId",
            address,
            asset_address AS "assetAddress",
            balance,
            as_of_block_height AS "asOfBlockHeight"
          FROM (
            SELECT
              network_id,
              address,
              asset_address,
              balance,
              as_of_block_height,
              version
            FROM balances_v2
            WHERE
              network_id = {networkId:UInt64}
              ${
                cursor === null
                  ? ''
                  : `AND (
                    address > {cursorAddress:String}
                    OR (address = {cursorAddress:String} AND asset_address > {cursorAssetAddress:String})
                  )`
              }
            ORDER BY address ASC, asset_address ASC, version DESC
            LIMIT 1 BY network_id, address, asset_address
          )
          ORDER BY address ASC, asset_address ASC
          LIMIT {limit:UInt64}
        `,
      query_params: {
        networkId,
        limit,
        ...(cursor === null
          ? {}
          : {
              cursorAddress: cursor.address,
              cursorAssetAddress: cursor.assetAddress,
            }),
      },
      format: 'JSONEachRow',
    });

    return {
      rows,
      nextCursor:
        rows.length === limit
          ? {
              address: rows.at(-1)?.address ?? '',
              assetAddress: rows.at(-1)?.assetAddress ?? '',
            }
          : null,
    };
  }

  private async getBalanceRowsByKeys(
    networkId: PrimaryId,
    keys: string[],
  ): Promise<Map<string, VersionedBalanceRow>> {
    if (keys.length === 0) {
      return new Map();
    }

    const rowChunks: Array<
      Array<
        BalanceRow & {
          version: number;
        }
      >
    > = await Promise.all(
      chunkQueryValues(keys).map((chunk) =>
        this.queryRows<
          BalanceRow & {
            version: number;
          }
        >({
          query: `
              SELECT
                network_id AS "networkId",
                address,
                asset_address AS "assetAddress",
                balance,
                as_of_block_height AS "asOfBlockHeight",
                version
              FROM balances_v2
              WHERE network_id = {networkId:UInt64}
                AND (address, asset_address) IN ${formatBalanceTupleList(chunk)}
              ORDER BY address ASC, asset_address ASC, version DESC
              LIMIT 1 BY network_id, address, asset_address
            `,
          query_params: { networkId },
          format: 'JSONEachRow',
        }),
      ),
    );
    const rows = rowChunks.flat();

    return new Map(
      rows.map((row) => [
        balanceKey(row.networkId, row.address, row.assetAddress),
        {
          networkId: row.networkId,
          address: row.address,
          assetAddress: row.assetAddress,
          balance: row.balance,
          asOfBlockHeight: row.asOfBlockHeight,
          version: row.version,
        },
      ]),
    );
  }

  private async getDirectLinkRowsByKeys(
    networkId: PrimaryId,
    keys: string[],
  ): Promise<Map<string, VersionedDirectLinkRow>> {
    if (keys.length === 0) {
      return new Map();
    }

    const rowChunks: Array<
      Array<
        DirectLinkRecord & {
          version: number;
        }
      >
    > = await Promise.all(
      chunkQueryValues(keys).map((chunk) =>
        this.queryRows<
          DirectLinkRecord & {
            version: number;
          }
        >({
          query: `
              SELECT
                network_id AS "networkId",
                from_address AS "fromAddress",
                to_address AS "toAddress",
                asset_address AS "assetAddress",
                transfer_count AS "transferCount",
                total_amount_base AS "totalAmountBase",
                first_seen_block_height AS "firstSeenBlockHeight",
                last_seen_block_height AS "lastSeenBlockHeight",
                version
              FROM direct_links_v2
              WHERE network_id = {networkId:UInt64}
                AND (from_address, to_address, asset_address) IN ${formatDirectLinkTupleList(chunk)}
              ORDER BY from_address ASC, to_address ASC, asset_address ASC, version DESC
              LIMIT 1 BY network_id, from_address, to_address, asset_address
            `,
          query_params: { networkId },
          format: 'JSONEachRow',
        }),
      ),
    );
    const rows = rowChunks.flat();

    return new Map(
      rows.map((row) => [
        directLinkKey(row.networkId, row.fromAddress, row.toAddress, row.assetAddress),
        {
          networkId: row.networkId,
          fromAddress: row.fromAddress,
          toAddress: row.toAddress,
          assetAddress: row.assetAddress,
          transferCount: row.transferCount,
          totalAmountBase: row.totalAmountBase,
          firstSeenBlockHeight: row.firstSeenBlockHeight,
          lastSeenBlockHeight: row.lastSeenBlockHeight,
          version: row.version,
        },
      ]),
    );
  }

  private async insertRows(table: string, values: Record<string, unknown>[]): Promise<void> {
    if (values.length === 0) {
      return;
    }

    await this.executeInsert({
      table,
      values,
      format: 'JSONEachRow',
    });
  }

  private async queryUtxoOutputsFromTable(
    table: string,
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<ProjectionUtxoOutput[]> {
    const rowChunks: ProjectionUtxoOutput[][] = await Promise.all(
      chunkQueryValues(outputKeys, {
        maxBytes: maxClickHouseHotOutputKeyBytesPerChunk,
        maxValues: maxClickHouseHotOutputKeyValuesPerChunk,
      }).map((chunk) =>
        this.queryRows<ProjectionUtxoOutput>({
          query: `
              SELECT
                {networkId:UInt64} AS "networkId",
                block_height AS "blockHeight",
                block_hash AS "blockHash",
                block_time AS "blockTime",
                txid,
                tx_index AS "txIndex",
                vout,
                output_key AS "outputKey",
                address,
                script_type AS "scriptType",
                value_base AS "valueBase",
                is_coinbase = 1 AS "isCoinbase",
                is_spendable = 1 AS "isSpendable",
                spent_by_txid AS "spentByTxid",
                spent_in_block AS "spentInBlock",
                spent_input_index AS "spentInputIndex"
              FROM ${table}
              WHERE network_id = {networkId:UInt64} AND output_key IN ({outputKeys:Array(String)})
              ORDER BY output_key ASC, version DESC
              LIMIT 1 BY output_key
            `,
          query_params: { networkId, outputKeys: chunk },
          format: 'JSONEachRow',
        }),
      ),
    );

    return rowChunks.flat();
  }

  private async migrate(): Promise<void> {
    for (const statement of clickHouseWarehouseBootstrapStatements) {
      await this.executeCommand({ query: statement });
    }

    await this.backfillTableIfEmpty(
      utxoCurrentStateByAddressTable,
      `
        INSERT INTO ${utxoCurrentStateByAddressTable} (
          network_id,
          block_height,
          block_hash,
          block_time,
          txid,
          tx_index,
          vout,
          output_key,
          address,
          script_type,
          value_base,
          is_coinbase,
          is_spendable,
          spent_by_txid,
          spent_in_block,
          spent_input_index,
          version
        )
        SELECT
          network_id,
          block_height,
          block_hash,
          block_time,
          txid,
          tx_index,
          vout,
          output_key,
          address,
          script_type,
          value_base,
          is_coinbase,
          is_spendable,
          spent_by_txid,
          spent_in_block,
          spent_input_index,
          version
        FROM ${utxoCurrentStateTable}
      `,
    );
    await this.backfillTableIfEmpty(
      addressMovementsByAddressTable,
      `
        INSERT INTO ${addressMovementsByAddressTable} (
          movement_id,
          network_id,
          block_height,
          block_hash,
          block_time,
          txid,
          tx_index,
          entry_index,
          address,
          asset_address,
          direction,
          amount_base,
          output_key,
          derivation_method
        )
        SELECT
          movement_id,
          network_id,
          block_height,
          block_hash,
          block_time,
          txid,
          tx_index,
          entry_index,
          address,
          asset_address,
          direction,
          amount_base,
          output_key,
          derivation_method
        FROM address_movements_v2
      `,
    );
  }

  private async backfillTableIfEmpty(table: string, statement: string): Promise<void> {
    if (await this.tableHasRows(table)) {
      return;
    }

    await this.executeCommand({ query: statement });
  }

  private async tableHasRows(table: string): Promise<boolean> {
    const rows = await this.queryRows<{ present: number }>({
      query: `
        SELECT 1 AS present
        FROM ${table}
        LIMIT 1
      `,
      format: 'JSONEachRow',
    });

    return rows.length > 0;
  }

  private async queryRows<T>(parameters: ClickHouseJsonQueryParameters): Promise<T[]> {
    try {
      const result = await this.client.query(parameters);
      return (await result.json<T>()) as T[];
    } catch (error) {
      throw this.toInfrastructureError(error);
    }
  }

  private async executeCommand(parameters: ClickHouseCommandParameters): Promise<void> {
    try {
      await this.client.command(parameters);
    } catch (error) {
      throw this.toInfrastructureError(error);
    }
  }

  private async executeInsert(parameters: ClickHouseInsertParameters): Promise<void> {
    try {
      await this.client.insert(parameters);
    } catch (error) {
      throw this.toInfrastructureError(error);
    }
  }

  private toInfrastructureError(error: unknown): InfrastructureError {
    if (error instanceof InfrastructureError) {
      return error;
    }

    const message = describeWarehouseError(error);
    if (isWarehouseUnavailableMessage(message)) {
      return new InfrastructureError('warehouse unavailable', {
        cause: error,
      });
    }

    if (isWarehouseMemoryLimitMessage(message)) {
      return new InfrastructureError('warehouse query exceeded memory limit', {
        cause: error,
      });
    }

    return new InfrastructureError('warehouse query failed', {
      cause: error,
    });
  }
}

export async function createWarehouse(
  settings: WarehouseSettings,
): Promise<InvestigationWarehousePort & ProjectionWarehousePort & ExplorerWarehousePort> {
  if (settings.driver === 'clickhouse') {
    const adapter = new ClickHouseWarehouseAdapter(settings);
    await adapter.boot();
    return adapter;
  }

  const adapter = new DuckDbWarehouseAdapter(settings.location);
  await adapter.boot();
  return adapter;
}

export async function createFactWarehouse(
  settings: WarehouseSettings,
): Promise<
  ProjectionFactWarehousePort &
    Pick<
      ProjectionStateStorePort,
      | 'getCurrentAddressSummary'
      | 'getBalanceSnapshots'
      | 'getDirectLinkSnapshots'
      | 'getDistinctLinksByAddresses'
      | 'getBalancesByAddresses'
      | 'getUtxoOutputs'
      | 'hasAppliedBlock'
      | 'listAddressUtxos'
      | 'listAppliedBlockSet'
      | 'listDirectLinksFromAddresses'
      | 'listSourceSeedIdsReachingAddresses'
    > &
    InvestigationWarehousePort &
    ProjectionWarehousePort &
    ExplorerWarehousePort
> {
  return createWarehouse(settings) as Promise<
    ProjectionFactWarehousePort &
      Pick<
        ProjectionStateStorePort,
        | 'getCurrentAddressSummary'
        | 'getBalanceSnapshots'
        | 'getDirectLinkSnapshots'
        | 'getDistinctLinksByAddresses'
        | 'getBalancesByAddresses'
        | 'getUtxoOutputs'
        | 'hasAppliedBlock'
        | 'listAddressUtxos'
        | 'listAppliedBlockSet'
        | 'listDirectLinksFromAddresses'
        | 'listSourceSeedIdsReachingAddresses'
      > &
      InvestigationWarehousePort &
      ProjectionWarehousePort &
      ExplorerWarehousePort
  >;
}

export class CompositeWarehouseAdapter
  implements
    InvestigationWarehousePort,
    ExplorerWarehousePort,
    Pick<ProjectionWarehousePort, 'getUtxoOutputs'>
{
  public constructor(
    private readonly stateStore: Pick<
      ProjectionStateStorePort,
      | 'getBalancesByAddresses'
      | 'getCurrentAddressSummary'
      | 'getDistinctLinksByAddresses'
      | 'getUtxoOutputs'
      | 'listAddressUtxos'
    >,
    private readonly historyWarehouse: InvestigationWarehousePort & ExplorerWarehousePort,
  ) {}

  public getBalancesByAddresses(addresses: string[]) {
    return this.stateStore.getBalancesByAddresses(addresses);
  }

  public getDistinctLinksByAddresses(addresses: string[]) {
    return this.stateStore.getDistinctLinksByAddresses(addresses);
  }

  public getTokensByAddresses(addresses: string[]) {
    return this.historyWarehouse.getTokensByAddresses(addresses);
  }

  public getUtxoOutputs(networkId: PrimaryId, outputKeys: string[]) {
    return this.stateStore.getUtxoOutputs(networkId, outputKeys);
  }

  public getAddressSummary(networkId: PrimaryId, address: string) {
    return Promise.all([
      this.stateStore.getCurrentAddressSummary(networkId, address),
      this.historyWarehouse.getAddressSummary(networkId, address),
    ]).then(([current, historical]) => {
      if (!current && !historical) {
        return null;
      }

      return {
        balance: current?.balance ?? historical?.balance ?? '0',
        receivedBase: historical?.receivedBase ?? '0',
        sentBase: historical?.sentBase ?? '0',
        txCount: historical?.txCount ?? 0,
        utxoCount: current?.utxoCount ?? historical?.utxoCount ?? 0,
      };
    });
  }

  public getAppliedBlockByHash(networkId: PrimaryId, blockHash: string) {
    return this.historyWarehouse.getAppliedBlockByHash(networkId, blockHash);
  }

  public getTransactionRef(networkId: PrimaryId, txid: string) {
    return this.historyWarehouse.getTransactionRef(networkId, txid);
  }

  public listAddressTransactions(
    networkId: PrimaryId,
    address: string,
    offset?: number,
    limit?: number,
  ) {
    return this.historyWarehouse.listAddressTransactions(networkId, address, offset, limit);
  }

  public listAddressUtxos(networkId: PrimaryId, address: string, offset?: number, limit?: number) {
    return this.stateStore.listAddressUtxos(networkId, address, offset, limit);
  }

  public listAppliedBlocks(networkId: PrimaryId, offset?: number, limit?: number) {
    return this.historyWarehouse.listAppliedBlocks(networkId, offset, limit);
  }
}

export class MirroredProjectionStateStore implements ProjectionStateStorePort {
  public constructor(
    private readonly primary: ProjectionStateStorePort,
    private readonly fallback?: Pick<
      ProjectionStateStorePort,
      | 'getCurrentAddressSummary'
      | 'getBalanceSnapshots'
      | 'getBalancesByAddresses'
      | 'getDirectLinkSnapshots'
      | 'getDistinctLinksByAddresses'
      | 'getUtxoOutputs'
      | 'hasAppliedBlock'
      | 'listAddressUtxos'
      | 'listAppliedBlockSet'
      | 'listDirectLinksFromAddresses'
      | 'listSourceSeedIdsReachingAddresses'
    >,
    private readonly mirror?: Pick<ProjectionWarehousePort, 'replaceSourceLinks'>,
  ) {}

  public applyProjectionWindow(batches: BlockProjectionBatch[]) {
    return this.primary.applyProjectionWindow(batches);
  }

  public applyDirectLinkDeltasWindow(batches: ProjectionDirectLinkBatch[]) {
    return this.primary.applyDirectLinkDeltasWindow(batches);
  }

  public clearProjectionBootstrapState(networkId: PrimaryId) {
    return this.primary.clearProjectionBootstrapState(networkId);
  }

  public finalizeProjectionBootstrap(networkId: PrimaryId, processTail: number) {
    return this.primary.finalizeProjectionBootstrap(networkId, processTail);
  }

  public async getCurrentAddressSummary(networkId: PrimaryId, address: string) {
    const primary = await this.primary.getCurrentAddressSummary(networkId, address);
    if (primary) {
      return primary;
    }

    return this.fallback?.getCurrentAddressSummary(networkId, address) ?? null;
  }

  public getBalanceSnapshots(
    networkId: PrimaryId,
    keys: Array<{
      address: string;
      assetAddress: string;
    }>,
  ) {
    return this.withFallbackMap(
      this.primary.getBalanceSnapshots(networkId, keys),
      keys,
      (missingKeys) =>
        this.fallback
          ? this.fallback.getBalanceSnapshots(networkId, missingKeys)
          : Promise.resolve(new Map()),
      ({ address, assetAddress }) => balanceSnapshotKey(address, assetAddress),
    );
  }

  public getDirectLinkSnapshots(
    networkId: PrimaryId,
    keys: Array<{
      assetAddress: string;
      fromAddress: string;
      toAddress: string;
    }>,
  ) {
    return this.withFallbackMap(
      this.primary.getDirectLinkSnapshots(networkId, keys),
      keys,
      (missingKeys) =>
        this.fallback
          ? this.fallback.getDirectLinkSnapshots(networkId, missingKeys)
          : Promise.resolve(new Map()),
      ({ fromAddress, toAddress, assetAddress }) =>
        directLinkSnapshotKey(fromAddress, toAddress, assetAddress),
    );
  }

  public async getDistinctLinksByAddresses(addresses: string[]) {
    const primaryRows = await this.primary.getDistinctLinksByAddresses(addresses);
    const fallbackRows = this.fallback
      ? await this.fallback.getDistinctLinksByAddresses(addresses)
      : [];

    return dedupeRecords(
      [...fallbackRows, ...primaryRows],
      (row) => `${row.networkId}:${row.fromAddress}:${row.toAddress}:${row.transferCount}`,
    );
  }

  public async getBalancesByAddresses(addresses: string[]) {
    const primaryRows = await this.primary.getBalancesByAddresses(addresses);
    const fallbackRows = this.fallback ? await this.fallback.getBalancesByAddresses(addresses) : [];

    return dedupeRecords(
      [...fallbackRows, ...primaryRows],
      (row) => `${row.networkId}:${row.assetAddress}:${row.balance}`,
    );
  }

  public getProjectionBootstrapTail(networkId: PrimaryId) {
    return this.primary.getProjectionBootstrapTail(networkId);
  }

  public getUtxoOutputs(networkId: PrimaryId, outputKeys: string[]) {
    return this.withFallbackMap(
      this.primary.getUtxoOutputs(networkId, outputKeys),
      outputKeys,
      (missingKeys) =>
        this.fallback
          ? this.fallback.getUtxoOutputs(networkId, missingKeys)
          : Promise.resolve(new Map()),
      (outputKey) => outputKey,
    );
  }

  public async hasAppliedBlock(networkId: PrimaryId, blockHeight: number, blockHash: string) {
    if (await this.primary.hasAppliedBlock(networkId, blockHeight, blockHash)) {
      return true;
    }

    return this.fallback?.hasAppliedBlock(networkId, blockHeight, blockHash) ?? false;
  }

  public async listAppliedBlockSet(
    networkId: PrimaryId,
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>> {
    const primaryRows = await this.primary.listAppliedBlockSet(networkId, blocks);
    const fallbackRows = this.fallback
      ? await this.fallback.listAppliedBlockSet(networkId, blocks)
      : new Set<string>();

    return new Set([...fallbackRows, ...primaryRows]);
  }

  public hasProjectionState(networkId: PrimaryId) {
    return this.primary.hasProjectionState(networkId);
  }

  public importProjectionStateSnapshot(
    networkId: PrimaryId,
    snapshot: ProjectionStateBootstrapSnapshot,
    processTail: number,
  ) {
    return this.primary.importProjectionStateSnapshot(networkId, snapshot, processTail);
  }

  public async listDirectLinksFromAddresses(networkId: PrimaryId, fromAddresses: string[]) {
    const primaryRows = await this.primary.listDirectLinksFromAddresses(networkId, fromAddresses);
    const fallbackRows = this.fallback
      ? await this.fallback.listDirectLinksFromAddresses(networkId, fromAddresses)
      : [];

    return dedupeRecords([...fallbackRows, ...primaryRows], (row) =>
      directLinkKey(row.networkId, row.fromAddress, row.toAddress, row.assetAddress),
    );
  }

  public async listSourceSeedIdsReachingAddresses(networkId: PrimaryId, addresses: string[]) {
    const primaryIds = await this.primary.listSourceSeedIdsReachingAddresses(networkId, addresses);
    const fallbackIds = this.fallback
      ? await this.fallback.listSourceSeedIdsReachingAddresses(networkId, addresses)
      : [];

    return [...new Set([...fallbackIds, ...primaryIds])];
  }

  public async replaceSourceLinks(
    networkId: PrimaryId,
    sourceAddressId: PrimaryId,
    rows: SourceLinkRecord[],
  ) {
    await this.primary.replaceSourceLinks(networkId, sourceAddressId, rows);
    if (this.mirror) {
      await this.mirror.replaceSourceLinks(networkId, sourceAddressId, rows);
    }
  }

  public upsertProjectionBootstrapBalances(rows: ProjectionBalanceSnapshot[]) {
    return this.primary.upsertProjectionBootstrapBalances(rows);
  }

  public upsertProjectionBootstrapUtxoOutputs(rows: ProjectionUtxoOutput[]) {
    return this.primary.upsertProjectionBootstrapUtxoOutputs(rows);
  }

  public async listAddressUtxos(
    networkId: PrimaryId,
    address: string,
    offset?: number,
    limit?: number,
  ) {
    const primaryRows = await this.primary.listAddressUtxos(networkId, address, offset, limit);
    if (primaryRows.length > 0 || !this.fallback) {
      return primaryRows;
    }

    return this.fallback.listAddressUtxos(networkId, address, offset, limit);
  }

  private async withFallbackMap<TKey, TValue>(
    primaryPromise: Promise<Map<string, TValue>>,
    keys: TKey[],
    fallbackLoader: (missingKeys: TKey[]) => Promise<Map<string, TValue>>,
    toKey: (key: TKey) => string,
  ): Promise<Map<string, TValue>> {
    const primaryRows = await primaryPromise;
    if (!this.fallback || keys.length === 0) {
      return primaryRows;
    }

    const missingKeys = keys.filter((key) => !primaryRows.has(toKey(key)));
    if (missingKeys.length === 0) {
      return primaryRows;
    }

    const fallbackRows = await fallbackLoader(missingKeys);
    return new Map([...fallbackRows, ...primaryRows]);
  }
}

function balanceKey(networkId: PrimaryId, address: string, assetAddress: string): string {
  return `${networkId}:${address}:${assetAddress}`;
}

function balanceSnapshotKey(address: string, assetAddress: string): string {
  return `${address}:${assetAddress}`;
}

function blockIdentity(networkId: PrimaryId, blockHeight: number, blockHash: string): string {
  return `${networkId}:${blockHeight}:${blockHash}`;
}

function directLinkKey(
  networkId: PrimaryId,
  fromAddress: string,
  toAddress: string,
  assetAddress: string,
): string {
  return `${networkId}:${fromAddress}:${toAddress}:${assetAddress}`;
}

function directLinkSnapshotKey(
  fromAddress: string,
  toAddress: string,
  assetAddress: string,
): string {
  return `${fromAddress}:${toAddress}:${assetAddress}`;
}

function dedupeRecords<T>(rows: T[], keyFor: (row: T) => string): T[] {
  const deduped = new Map<string, T>();
  for (const row of rows) {
    deduped.set(keyFor(row), row);
  }

  return [...deduped.values()];
}

function chunkQueryValues<T>(
  values: T[],
  options?: {
    maxBytes?: number;
    maxValues?: number;
  },
): T[][] {
  if (values.length === 0) {
    return [];
  }

  const chunks: T[][] = [];
  let currentChunk: T[] = [];
  let currentBytes = 0;
  const maxBytes = options?.maxBytes ?? maxClickHouseQueryValueBytesPerChunk;
  const maxValues = options?.maxValues ?? maxClickHouseQueryValuesPerChunk;

  for (const value of values) {
    const valueBytes = String(value).length + 3;
    const wouldOverflow = currentChunk.length >= maxValues || currentBytes + valueBytes > maxBytes;

    if (wouldOverflow && currentChunk.length > 0) {
      chunks.push(currentChunk);
      currentChunk = [];
      currentBytes = 0;
    }

    currentChunk.push(value);
    currentBytes += valueBytes;
  }

  if (currentChunk.length > 0) {
    chunks.push(currentChunk);
  }

  return chunks;
}

function describeWarehouseError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === 'object' && error !== null && 'message' in error) {
    const message = Reflect.get(error, 'message');
    if (typeof message === 'string') {
      return message;
    }
  }

  return String(error);
}

function isWarehouseUnavailableMessage(message: string): boolean {
  return ['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'socket hang up', 'EAI_AGAIN'].some((needle) =>
    message.includes(needle),
  );
}

function isWarehouseMemoryLimitMessage(message: string): boolean {
  return (
    message.includes('MEMORY_LIMIT_EXCEEDED') || message.includes('User memory limit exceeded')
  );
}

const clickHouseWarehouseBootstrapStatements = [
  `
    CREATE TABLE IF NOT EXISTS ${utxoCurrentStateByAddressTable}
    (
      network_id UInt64,
      block_height UInt64,
      block_hash String,
      block_time UInt64,
      txid String,
      tx_index UInt64,
      vout UInt64,
      output_key String,
      address String,
      script_type String,
      value_base String,
      is_coinbase UInt8,
      is_spendable UInt8,
      spent_by_txid Nullable(String),
      spent_in_block Nullable(UInt64),
      spent_input_index Nullable(UInt64),
      version UInt64
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY (network_id, address, output_key)
  `,
  `
    CREATE MATERIALIZED VIEW IF NOT EXISTS ${utxoCurrentStateByAddressTable}_mv
    TO ${utxoCurrentStateByAddressTable}
    AS
    SELECT
      network_id,
      block_height,
      block_hash,
      block_time,
      txid,
      tx_index,
      vout,
      output_key,
      address,
      script_type,
      value_base,
      is_coinbase,
      is_spendable,
      spent_by_txid,
      spent_in_block,
      spent_input_index,
      version
    FROM ${utxoCurrentStateTable}
  `,
  `
    CREATE TABLE IF NOT EXISTS ${addressMovementsByAddressTable}
    (
      movement_id String,
      network_id UInt64,
      block_height UInt64,
      block_hash String,
      block_time UInt64,
      txid String,
      tx_index UInt64,
      entry_index UInt64,
      address String,
      asset_address String,
      direction String,
      amount_base String,
      amount_base_i256 Int256 MATERIALIZED toInt256(amount_base),
      output_key Nullable(String),
      derivation_method String
    )
    ENGINE = MergeTree
    ORDER BY (network_id, address, block_height, tx_index, entry_index, movement_id)
  `,
  `
    CREATE MATERIALIZED VIEW IF NOT EXISTS ${addressMovementsByAddressTable}_mv
    TO ${addressMovementsByAddressTable}
    AS
    SELECT
      movement_id,
      network_id,
      block_height,
      block_hash,
      block_time,
      txid,
      tx_index,
      entry_index,
      address,
      asset_address,
      direction,
      amount_base,
      output_key,
      derivation_method
    FROM address_movements_v2
  `,
];

function formatBalanceTupleList(keys: string[]): string {
  return formatTupleList(
    keys.map((key) => {
      const [, address = '', assetAddress = ''] = key.split(':');
      return [address, assetAddress];
    }),
  );
}

function formatDirectLinkTupleList(keys: string[]): string {
  return formatTupleList(
    keys.map((key) => {
      const [, fromAddress = '', toAddress = '', assetAddress = ''] = key.split(':');
      return [fromAddress, toAddress, assetAddress];
    }),
  );
}

function formatTupleList(rows: string[][]): string {
  return `(${rows.map((row) => `(${row.map(formatClickHouseStringLiteral).join(', ')})`).join(', ')})`;
}

function toUtxoInsertRow(row: ProjectionUtxoOutput, version: number): Record<string, unknown> {
  return {
    network_id: row.networkId,
    block_height: row.blockHeight,
    block_hash: row.blockHash,
    block_time: row.blockTime,
    txid: row.txid,
    tx_index: row.txIndex,
    vout: row.vout,
    output_key: row.outputKey,
    address: row.address,
    script_type: row.scriptType,
    value_base: row.valueBase,
    is_coinbase: row.isCoinbase ? 1 : 0,
    is_spendable: row.isSpendable ? 1 : 0,
    spent_by_txid: row.spentByTxid,
    spent_in_block: row.spentInBlock,
    spent_input_index: row.spentInputIndex,
    version,
  };
}

function formatClickHouseStringLiteral(value: string): string {
  return `'${value.replaceAll('\\', '\\\\').replaceAll("'", "\\'")}'`;
}
