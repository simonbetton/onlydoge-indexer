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
const utxoCurrentStateTable = 'utxo_outputs_current_v2';
type ClickHouseClient = ReturnType<typeof createClient>;
type ClickHouseCommandParameters = Parameters<ClickHouseClient['command']>[0];
type ClickHouseInsertParameters = Parameters<ClickHouseClient['insert']>[0];
type ClickHouseJsonQueryParameters = Parameters<ClickHouseClient['query']>[0];

const emptyWarehouseState = (): WarehouseState => ({
  appliedBlocks: [],
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
  implements InvestigationWarehousePort, ProjectionWarehousePort, ExplorerWarehousePort
{
  protected state: WarehouseState = emptyWarehouseState();

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
  implements InvestigationWarehousePort, ProjectionWarehousePort, ExplorerWarehousePort
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
              CAST(sumIf(toInt256(amount_base), direction = 'credit') AS String) AS "receivedBase",
              CAST(sumIf(toInt256(amount_base), direction = 'debit') AS String) AS "sentBase",
              uniqExact(txid) AS "txCount"
            FROM address_movements_v2
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
              FROM utxo_outputs_current_v2
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
            CAST(sumIf(toInt256(amount_base), direction = 'credit') AS String) AS "receivedBase",
            CAST(sumIf(toInt256(amount_base), direction = 'debit') AS String) AS "sentBase"
          FROM address_movements_v2
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
            FROM utxo_outputs_current_v2
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
      true,
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
      false,
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

  private async listAppliedBlockSet(
    networkId: PrimaryId,
    batches: BlockProjectionBatch[],
  ): Promise<Set<string>> {
    const heights = [...new Set(batches.map((batch) => batch.blockHeight))];
    if (heights.length === 0) {
      return new Set();
    }

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

    return new Set(rows.map((row) => blockIdentity(row.networkId, row.blockHeight, row.blockHash)));
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
    useFinal: boolean,
  ): Promise<ProjectionUtxoOutput[]> {
    const rowChunks: ProjectionUtxoOutput[][] = await Promise.all(
      chunkQueryValues(outputKeys).map((chunk) =>
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
              FROM ${table}${useFinal ? ' FINAL' : ''}
              WHERE network_id = {networkId:UInt64} AND output_key IN ({outputKeys:Array(String)})
              ${useFinal ? '' : 'ORDER BY output_key ASC, version DESC LIMIT 1 BY output_key'}
            `,
          query_params: { networkId, outputKeys: chunk },
          format: 'JSONEachRow',
        }),
      ),
    );

    return rowChunks.flat();
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
    return new ClickHouseWarehouseAdapter(settings);
  }

  const adapter = new DuckDbWarehouseAdapter(settings.location);
  await adapter.boot();
  return adapter;
}

function balanceKey(networkId: PrimaryId, address: string, assetAddress: string): string {
  return `${networkId}:${address}:${assetAddress}`;
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

function chunkQueryValues<T>(values: T[]): T[][] {
  if (values.length === 0) {
    return [];
  }

  const chunks: T[][] = [];
  let currentChunk: T[] = [];
  let currentBytes = 0;

  for (const value of values) {
    const valueBytes = String(value).length + 3;
    const wouldOverflow =
      currentChunk.length >= maxClickHouseQueryValuesPerChunk ||
      currentBytes + valueBytes > maxClickHouseQueryValueBytesPerChunk;

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
