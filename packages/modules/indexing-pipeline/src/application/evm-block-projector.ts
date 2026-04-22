import { InfrastructureError } from '@onlydoge/shared-kernel';

import { fromHexUnits } from '../domain/amounts';
import type {
  BlockProjectionBatch,
  DirectLinkDelta,
  TransferFact,
} from '../domain/projection-models';

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

export interface EvmProjectOptions {
  includeDirectLinkDeltas?: boolean;
  includeTransfers?: boolean;
}

const defaultProjectOptions: Required<EvmProjectOptions> = {
  includeDirectLinkDeltas: true,
  includeTransfers: true,
};

export class EvmBlockProjector {
  public project(
    networkId: number,
    snapshot: Record<string, unknown>,
    options?: EvmProjectOptions,
  ): BlockProjectionBatch {
    const resolvedOptions = {
      ...defaultProjectOptions,
      ...(options ?? {}),
    };
    const block = this.parseBlock(snapshot);
    const receipts = this.parseReceipts(snapshot);
    const receiptByHash = new Map(receipts.map((receipt) => [receipt.transactionHash, receipt]));
    const addressMovements: BlockProjectionBatch['addressMovements'] = [];
    const transfers: TransferFact[] = [];
    let transferIndex = 0;

    for (const [txIndex, transaction] of block.transactions.entries()) {
      const txid = this.requireString(transaction.hash, 'tx.hash').toLowerCase();
      const fromAddress = this.requireString(transaction.from, 'tx.from').toLowerCase();
      const toAddress = transaction.to?.trim().toLowerCase() ?? '';
      const valueBase = fromHexUnits(transaction.value?.trim() ?? '0x0');

      if (toAddress && valueBase !== '0') {
        addressMovements.push({
          movementId: `${txid}:native:debit`,
          networkId,
          blockHeight: block.height,
          blockHash: block.hash,
          blockTime: block.time,
          txid,
          txIndex,
          entryIndex: 0,
          address: fromAddress,
          assetAddress: '',
          direction: 'debit',
          amountBase: valueBase,
          outputKey: null,
          derivationMethod: 'evm_native_transfer_v1',
        });
        addressMovements.push({
          movementId: `${txid}:native:credit`,
          networkId,
          blockHeight: block.height,
          blockHash: block.hash,
          blockTime: block.time,
          txid,
          txIndex,
          entryIndex: 1,
          address: toAddress,
          assetAddress: '',
          direction: 'credit',
          amountBase: valueBase,
          outputKey: null,
          derivationMethod: 'evm_native_transfer_v1',
        });
        if (resolvedOptions.includeTransfers) {
          transfers.push({
            transferId: `${txid}:${transferIndex}`,
            networkId,
            blockHeight: block.height,
            blockHash: block.hash,
            blockTime: block.time,
            txid,
            txIndex,
            transferIndex,
            assetAddress: '',
            fromAddress,
            toAddress,
            amountBase: valueBase,
            derivationMethod: 'evm_native_transfer_v1',
            confidence: 1,
            isChange: false,
            inputAddressCount: 1,
            outputAddressCount: 1,
          });
          transferIndex += 1;
        }
      }

      const receipt = receiptByHash.get(txid);
      for (const [logIndex, log] of (receipt?.logs ?? []).entries()) {
        const parsedTransfer = this.parseErc20Transfer(log);
        if (!parsedTransfer) {
          continue;
        }

        addressMovements.push({
          movementId: `${txid}:erc20:debit:${logIndex}`,
          networkId,
          blockHeight: block.height,
          blockHash: block.hash,
          blockTime: block.time,
          txid,
          txIndex,
          entryIndex: logIndex * 2,
          address: parsedTransfer.fromAddress,
          assetAddress: parsedTransfer.assetAddress,
          direction: 'debit',
          amountBase: parsedTransfer.amountBase,
          outputKey: null,
          derivationMethod: 'evm_erc20_transfer_v1',
        });
        addressMovements.push({
          movementId: `${txid}:erc20:credit:${logIndex}`,
          networkId,
          blockHeight: block.height,
          blockHash: block.hash,
          blockTime: block.time,
          txid,
          txIndex,
          entryIndex: logIndex * 2 + 1,
          address: parsedTransfer.toAddress,
          assetAddress: parsedTransfer.assetAddress,
          direction: 'credit',
          amountBase: parsedTransfer.amountBase,
          outputKey: null,
          derivationMethod: 'evm_erc20_transfer_v1',
        });
        if (resolvedOptions.includeTransfers) {
          transfers.push({
            transferId: `${txid}:${transferIndex}`,
            networkId,
            blockHeight: block.height,
            blockHash: block.hash,
            blockTime: block.time,
            txid,
            txIndex,
            transferIndex,
            assetAddress: parsedTransfer.assetAddress,
            fromAddress: parsedTransfer.fromAddress,
            toAddress: parsedTransfer.toAddress,
            amountBase: parsedTransfer.amountBase,
            derivationMethod: 'evm_erc20_transfer_v1',
            confidence: 1,
            isChange: false,
            inputAddressCount: 1,
            outputAddressCount: 1,
          });
          transferIndex += 1;
        }
      }
    }

    return {
      networkId,
      blockHeight: block.height,
      blockHash: block.hash,
      blockTime: block.time,
      utxoCreates: [],
      utxoSpends: [],
      addressMovements,
      transfers,
      directLinkDeltas: resolvedOptions.includeDirectLinkDeltas
        ? this.buildDirectLinkDeltas(networkId, block.height, transfers)
        : [],
    };
  }

  private buildDirectLinkDeltas(
    networkId: number,
    blockHeight: number,
    transfers: TransferFact[],
  ): DirectLinkDelta[] {
    const result = new Map<string, DirectLinkDelta>();
    for (const transfer of transfers) {
      const key = [transfer.fromAddress, transfer.toAddress, transfer.assetAddress].join(':');
      const current = result.get(key);
      if (current) {
        current.transferCount += 1;
        current.totalAmountBase = (
          BigInt(current.totalAmountBase) + BigInt(transfer.amountBase)
        ).toString();
        current.lastSeenBlockHeight = blockHeight;
        continue;
      }

      result.set(key, {
        networkId,
        fromAddress: transfer.fromAddress,
        toAddress: transfer.toAddress,
        assetAddress: transfer.assetAddress,
        transferCount: 1,
        totalAmountBase: transfer.amountBase,
        firstSeenBlockHeight: blockHeight,
        lastSeenBlockHeight: blockHeight,
      });
    }

    return [...result.values()];
  }

  private parseBlock(snapshot: Record<string, unknown>): ParsedEvmBlock {
    const candidate = this.requireRecord(snapshot.block, 'block');
    return {
      hash: this.requireString(this.readString(candidate, 'hash'), 'block.hash').toLowerCase(),
      height: Number(
        BigInt(this.requireString(this.readString(candidate, 'number'), 'block.number')),
      ),
      time: Number(
        BigInt(this.requireString(this.readString(candidate, 'timestamp'), 'block.timestamp')),
      ),
      transactions: this.readTransactions(candidate.transactions),
    };
  }

  private parseErc20Transfer(log: EvmLog): {
    amountBase: string;
    assetAddress: string;
    fromAddress: string;
    toAddress: string;
  } | null {
    const topic0 = log.topics?.[0]?.toLowerCase();
    const topic1 = log.topics?.[1];
    const topic2 = log.topics?.[2];
    if (topic0 !== erc20TransferTopic || !topic1 || !topic2) {
      return null;
    }

    const fromAddress = this.topicAddress(topic1);
    const toAddress = this.topicAddress(topic2);
    const assetAddress = this.requireString(log.address, 'log.address').toLowerCase();
    const amountBase = fromHexUnits(log.data?.trim() ?? '0x0');
    if (amountBase === '0') {
      return null;
    }

    return {
      fromAddress,
      toAddress,
      assetAddress,
      amountBase,
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

  private readString(record: Record<string, unknown>, key: string): string | undefined {
    const value = record[key];
    return typeof value === 'string' ? value : undefined;
  }

  private readTransactions(value: unknown): EvmTransaction[] {
    return Array.isArray(value) ? value.filter(isEvmTransaction) : [];
  }

  private requireRecord(value: unknown, field: string): Record<string, unknown> {
    if (!value || typeof value !== 'object') {
      throw new InfrastructureError(`invalid evm field: ${field}`);
    }

    return Object.fromEntries(Object.entries(value));
  }
}

function isEvmTransaction(value: unknown): value is EvmTransaction {
  return typeof value === 'object' && value !== null;
}
