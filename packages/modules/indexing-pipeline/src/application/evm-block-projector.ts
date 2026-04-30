import { InfrastructureError } from '@onlydoge/shared-kernel';

import { fromHexUnits } from '../domain/amounts';
import type { BlockProjectionBatch, TransferFact } from '../domain/projection-models';
import { buildDirectLinkDeltas } from '../domain/projection-models';
import { readSnapshotItems, readSnapshotString, requireSnapshotRecord } from './block-snapshot';

const erc20TransferTopic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a9df523b3ef';

interface EvmTransaction {
  from?: string;
  hash?: string;
  to?: string | null;
  value?: string;
}

interface EvmLog {
  address?: string;
  data?: string;
  topics?: string[];
}

interface EvmReceipt {
  logs?: EvmLog[];
  transactionHash?: string;
}

interface ParsedEvmBlock {
  hash: string;
  height: number;
  time: number;
  transactions: EvmTransaction[];
}

type EvmProjectedTransfer = {
  amountBase: string;
  assetAddress: string;
  derivationMethod: 'evm_erc20_transfer_v1' | 'evm_native_transfer_v1';
  fromAddress: string;
  toAddress: string;
};

type EvmProjectionAccumulator = {
  addressMovements: BlockProjectionBatch['addressMovements'];
  nextTransferIndex: number;
  transfers: TransferFact[];
};

type EvmTransactionContext = {
  accumulator: EvmProjectionAccumulator;
  block: ParsedEvmBlock;
  networkId: number;
  options: Required<EvmProjectOptions>;
  txIndex: number;
  txid: string;
};

type EvmMovementSpec = {
  creditEntryIndex: number;
  creditMovementId: string;
  debitEntryIndex: number;
  debitMovementId: string;
};

export interface EvmProjectOptions {
  includeDirectLinkDeltas?: boolean;
  includeTransfers?: boolean;
}

const defaultProjectOptions: Required<EvmProjectOptions> = {
  includeDirectLinkDeltas: true,
  includeTransfers: true,
};

function resolveEvmProjectOptions(
  options: EvmProjectOptions | undefined,
): Required<EvmProjectOptions> {
  return {
    ...defaultProjectOptions,
    ...(options ?? {}),
  };
}

function receiptLogs(
  receiptByHash: Map<string, EvmReceipt & { transactionHash: string }>,
  txid: string,
): EvmLog[] {
  return receiptByHash.get(txid)?.logs ?? [];
}

function evmDirectLinkDeltas(
  networkId: number,
  blockHeight: number,
  transfers: TransferFact[],
  options: Required<EvmProjectOptions>,
): BlockProjectionBatch['directLinkDeltas'] {
  if (!options.includeDirectLinkDeltas) {
    return [];
  }

  return buildDirectLinkDeltas(networkId, blockHeight, transfers);
}

function normalizeEvmAddress(value: string | null | undefined): string {
  return value?.trim().toLowerCase() ?? '';
}

function evmHexValue(value: string | undefined): string {
  return value?.trim() ?? '0x0';
}

function hasEvmTransferValue(toAddress: string, amountBase: string): boolean {
  return Boolean(toAddress) && amountBase !== '0';
}

function erc20TransferTopics(log: EvmLog): { from: string; to: string } | null {
  const topics = evmLogTopics(log);
  if (!isErc20TransferTopic(topics[0]?.toLowerCase())) {
    return null;
  }

  return erc20TransferTopicPair(topics[1], topics[2]);
}

function erc20TransferTopicPair(
  topic1: string | undefined,
  topic2: string | undefined,
): { from: string; to: string } | null {
  if (!topic1) {
    return null;
  }
  if (!topic2) {
    return null;
  }

  return { from: topic1, to: topic2 };
}

function evmLogTopics(log: EvmLog): string[] {
  return log.topics ?? [];
}

function isErc20TransferTopic(topic: string | undefined): boolean {
  return topic === erc20TransferTopic;
}

export class EvmBlockProjector {
  public project(
    networkId: number,
    snapshot: Record<string, unknown>,
    options?: EvmProjectOptions,
  ): BlockProjectionBatch {
    const resolvedOptions = resolveEvmProjectOptions(options);
    const block = this.parseBlock(snapshot);
    const receipts = this.parseReceipts(snapshot);
    const receiptByHash = new Map(receipts.map((receipt) => [receipt.transactionHash, receipt]));
    const accumulator: EvmProjectionAccumulator = {
      addressMovements: [],
      transfers: [],
      nextTransferIndex: 0,
    };

    for (const [txIndex, transaction] of block.transactions.entries()) {
      const txid = this.requireString(transaction.hash, 'tx.hash').toLowerCase();
      this.projectTransaction(
        {
          accumulator,
          block,
          networkId,
          options: resolvedOptions,
          txIndex,
          txid,
        },
        transaction,
        receiptLogs(receiptByHash, txid),
      );
    }

    return {
      networkId,
      blockHeight: block.height,
      blockHash: block.hash,
      blockTime: block.time,
      utxoCreates: [],
      utxoSpends: [],
      addressMovements: accumulator.addressMovements,
      transfers: accumulator.transfers,
      directLinkDeltas: evmDirectLinkDeltas(
        networkId,
        block.height,
        accumulator.transfers,
        resolvedOptions,
      ),
    };
  }

  private projectTransaction(
    context: EvmTransactionContext,
    transaction: EvmTransaction,
    logs: EvmLog[],
  ): void {
    const nativeTransfer = this.parseNativeTransfer(transaction);
    if (nativeTransfer) {
      this.projectTransfer(context, nativeTransfer, nativeMovementSpec(context.txid));
    }

    this.projectErc20Transfers(context, logs);
  }

  private parseNativeTransfer(transaction: EvmTransaction): EvmProjectedTransfer | null {
    const fromAddress = this.requireString(transaction.from, 'tx.from').toLowerCase();
    const toAddress = normalizeEvmAddress(transaction.to);
    const amountBase = fromHexUnits(evmHexValue(transaction.value));

    if (!hasEvmTransferValue(toAddress, amountBase)) {
      return null;
    }

    return {
      amountBase,
      assetAddress: '',
      derivationMethod: 'evm_native_transfer_v1',
      fromAddress,
      toAddress,
    };
  }

  private projectErc20Transfers(context: EvmTransactionContext, logs: EvmLog[]): void {
    for (const [logIndex, log] of logs.entries()) {
      const parsedTransfer = this.parseErc20Transfer(log);
      if (!parsedTransfer) {
        continue;
      }

      this.projectTransfer(context, parsedTransfer, erc20MovementSpec(context.txid, logIndex));
    }
  }

  private projectTransfer(
    context: EvmTransactionContext,
    transfer: EvmProjectedTransfer,
    movementSpec: EvmMovementSpec,
  ): void {
    context.accumulator.addressMovements.push(
      this.transferMovement(
        context,
        transfer,
        movementSpec.debitMovementId,
        'debit',
        movementSpec.debitEntryIndex,
      ),
      this.transferMovement(
        context,
        transfer,
        movementSpec.creditMovementId,
        'credit',
        movementSpec.creditEntryIndex,
      ),
    );
    this.appendTransferFact(context, transfer);
  }

  private transferMovement(
    context: EvmTransactionContext,
    transfer: EvmProjectedTransfer,
    movementId: string,
    direction: 'credit' | 'debit',
    entryIndex: number,
  ): BlockProjectionBatch['addressMovements'][number] {
    return {
      movementId,
      networkId: context.networkId,
      blockHeight: context.block.height,
      blockHash: context.block.hash,
      blockTime: context.block.time,
      txid: context.txid,
      txIndex: context.txIndex,
      entryIndex,
      address: direction === 'debit' ? transfer.fromAddress : transfer.toAddress,
      assetAddress: transfer.assetAddress,
      direction,
      amountBase: transfer.amountBase,
      outputKey: null,
      derivationMethod: transfer.derivationMethod,
    };
  }

  private appendTransferFact(context: EvmTransactionContext, transfer: EvmProjectedTransfer): void {
    if (!context.options.includeTransfers) {
      return;
    }

    const transferIndex = context.accumulator.nextTransferIndex;
    context.accumulator.transfers.push({
      transferId: `${context.txid}:${transferIndex}`,
      networkId: context.networkId,
      blockHeight: context.block.height,
      blockHash: context.block.hash,
      blockTime: context.block.time,
      txid: context.txid,
      txIndex: context.txIndex,
      transferIndex,
      assetAddress: transfer.assetAddress,
      fromAddress: transfer.fromAddress,
      toAddress: transfer.toAddress,
      amountBase: transfer.amountBase,
      derivationMethod: transfer.derivationMethod,
      confidence: 1,
      isChange: false,
      inputAddressCount: 1,
      outputAddressCount: 1,
    });
    context.accumulator.nextTransferIndex += 1;
  }

  private parseBlock(snapshot: Record<string, unknown>): ParsedEvmBlock {
    const candidate = requireSnapshotRecord(snapshot.block, 'block', 'evm');
    return {
      hash: this.requireString(readSnapshotString(candidate, 'hash'), 'block.hash').toLowerCase(),
      height: Number(
        BigInt(this.requireString(readSnapshotString(candidate, 'number'), 'block.number')),
      ),
      time: Number(
        BigInt(this.requireString(readSnapshotString(candidate, 'timestamp'), 'block.timestamp')),
      ),
      transactions: readSnapshotItems(candidate.transactions, isEvmTransaction),
    };
  }

  private parseErc20Transfer(log: EvmLog): EvmProjectedTransfer | null {
    const topics = erc20TransferTopics(log);
    if (!topics) {
      return null;
    }

    const fromAddress = this.topicAddress(topics.from);
    const toAddress = this.topicAddress(topics.to);
    const assetAddress = this.requireString(log.address, 'log.address').toLowerCase();
    const amountBase = fromHexUnits(evmHexValue(log.data));
    if (amountBase === '0') {
      return null;
    }

    return {
      fromAddress,
      toAddress,
      assetAddress,
      amountBase,
      derivationMethod: 'evm_erc20_transfer_v1',
    };
  }

  private parseReceipts(
    snapshot: Record<string, unknown>,
  ): Array<EvmReceipt & { transactionHash: string }> {
    const receipts = snapshot.receipts;
    if (!Array.isArray(receipts)) {
      return [];
    }

    return receipts
      .filter((receipt): receipt is EvmReceipt => Boolean(receipt && typeof receipt === 'object'))
      .map((receipt) => ({
        ...receipt,
        transactionHash: this.requireString(
          receipt.transactionHash,
          'receipt.transactionHash',
        ).toLowerCase(),
      }));
  }

  private requireString(value: string | undefined | null, field: string): string {
    const trimmed = value?.trim();
    if (!trimmed) {
      throw new InfrastructureError(`invalid evm field: ${field}`);
    }

    return trimmed;
  }

  private topicAddress(value: string): string {
    const normalized = value.trim().toLowerCase().replace(/^0x/u, '');
    if (normalized.length < 40) {
      throw new InfrastructureError('invalid evm transfer topic');
    }

    return `0x${normalized.slice(-40)}`;
  }
}

function isEvmTransaction(value: unknown): value is EvmTransaction {
  return typeof value === 'object' && value !== null;
}

function nativeMovementSpec(txid: string): EvmMovementSpec {
  return {
    debitMovementId: `${txid}:native:debit`,
    creditMovementId: `${txid}:native:credit`,
    debitEntryIndex: 0,
    creditEntryIndex: 1,
  };
}

function erc20MovementSpec(txid: string, logIndex: number): EvmMovementSpec {
  return {
    debitMovementId: `${txid}:erc20:debit:${logIndex}`,
    creditMovementId: `${txid}:erc20:credit:${logIndex}`,
    debitEntryIndex: logIndex * 2,
    creditEntryIndex: logIndex * 2 + 1,
  };
}
