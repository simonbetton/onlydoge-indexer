import { InfrastructureError } from '@onlydoge/shared-kernel';

import type { ProjectionWarehousePort } from '../contracts/ports';
import {
  addAmountBase,
  formatAmountBase,
  fromDecimalUnits,
  parseAmountBase,
} from '../domain/amounts';
import type {
  BlockProjectionBatch,
  DirectLinkDelta,
  ProjectionUtxoOutput,
  TransferFact,
} from '../domain/projection-models';

interface DogecoinVin {
  coinbase?: string;
  txid?: string;
  vout?: number;
}

interface DogecoinVout {
  n?: number;
  value?: number | string;
  scriptPubKey?: {
    address?: string;
    addresses?: string[];
    type?: string;
  };
}

interface DogecoinTransaction {
  txid?: string;
  vin?: DogecoinVin[];
  vout?: DogecoinVout[];
}

interface ParsedDogecoinBlock {
  hash: string;
  height: number;
  time: number;
  tx: DogecoinTransaction[];
}

interface ProjectionState {
  localOutputs?: Map<string, ProjectionUtxoOutput>;
  persistedOutputs?: Map<string, ProjectionUtxoOutput>;
}

export class DogecoinBlockProjector {
  public constructor(private readonly warehouse: ProjectionWarehousePort) {}

  public async project(
    networkId: number,
    snapshot: Record<string, unknown>,
    state?: ProjectionState,
  ): Promise<BlockProjectionBatch> {
    const block = this.parseBlock(snapshot);
    const utxoCreates: ProjectionUtxoOutput[] = [];
    const utxoSpends: BlockProjectionBatch['utxoSpends'] = [];
    const addressMovements: BlockProjectionBatch['addressMovements'] = [];
    const transfers: TransferFact[] = [];
    const localOutputs = state?.localOutputs ?? new Map<string, ProjectionUtxoOutput>();
    const persistedOutputs = state?.persistedOutputs;

    for (const [txIndex, transaction] of block.tx.entries()) {
      const txid = this.requireString(transaction.txid, 'tx.txid');
      const inputs = transaction.vin ?? [];
      const outputs = transaction.vout ?? [];
      const isCoinbase = inputs.some((input) => Boolean(input.coinbase));
      const resolvedInputs: Array<{
        address: string;
        amountBase: string;
        outputKey: string;
      }> = [];
      const projectedOutputs: Array<{
        address: string;
        amountBase: string;
        isSpendable: boolean;
      }> = [];

      for (const [entryIndex, input] of inputs.entries()) {
        if (input.coinbase) {
          continue;
        }

        const prevTxid = this.requireString(input.txid, 'vin.txid');
        const prevVout = this.requireNumber(input.vout, 'vin.vout');
        const outputKey = `${prevTxid}:${prevVout}`;
        const resolved = await this.resolveOutput(
          networkId,
          block.height,
          txid,
          entryIndex,
          outputKey,
          localOutputs,
          persistedOutputs,
        );
        const resolvedOutput = resolved.output;
        if (resolved.isLocal) {
          resolvedOutput.spentByTxid = txid;
          resolvedOutput.spentInBlock = block.height;
          resolvedOutput.spentInputIndex = entryIndex;
        } else {
          utxoSpends.push({
            outputKey,
            spentByTxid: txid,
            spentInBlock: block.height,
            spentInputIndex: entryIndex,
          });
        }
        resolvedInputs.push({
          address: resolvedOutput.address,
          amountBase: resolvedOutput.valueBase,
          outputKey,
        });
        addressMovements.push({
          movementId: `${txid}:vin:${entryIndex}`,
          networkId,
          blockHeight: block.height,
          blockHash: block.hash,
          blockTime: block.time,
          txid,
          txIndex,
          entryIndex,
          address: resolvedOutput.address,
          assetAddress: '',
          direction: 'debit',
          amountBase: resolvedOutput.valueBase,
          outputKey,
          derivationMethod: 'dogecoin_utxo_input_v1',
        });
      }

      for (const [entryIndex, output] of outputs.entries()) {
        const amountBase = fromDecimalUnits(this.requireAmount(output.value), 8);
        const outputKey = `${txid}:${entryIndex}`;
        const scriptType = output.scriptPubKey?.type?.trim() ?? '';
        const address = this.extractOutputAddress(output);
        const isSpendable = Boolean(address);
        const createdOutput: ProjectionUtxoOutput = {
          networkId,
          blockHeight: block.height,
          blockHash: block.hash,
          blockTime: block.time,
          txid,
          txIndex,
          vout: this.requireNumber(output.n ?? entryIndex, 'vout.n'),
          outputKey,
          address,
          scriptType,
          valueBase: amountBase,
          isCoinbase,
          isSpendable,
          spentByTxid: null,
          spentInBlock: null,
          spentInputIndex: null,
        };

        utxoCreates.push(createdOutput);
        localOutputs.set(outputKey, createdOutput);

        if (!isSpendable) {
          continue;
        }

        projectedOutputs.push({
          address,
          amountBase,
          isSpendable,
        });
        addressMovements.push({
          movementId: `${txid}:vout:${entryIndex}`,
          networkId,
          blockHeight: block.height,
          blockHash: block.hash,
          blockTime: block.time,
          txid,
          txIndex,
          entryIndex,
          address,
          assetAddress: '',
          direction: 'credit',
          amountBase,
          outputKey,
          derivationMethod: 'dogecoin_utxo_output_v1',
        });
      }

      transfers.push(
        ...this.projectTransfers({
          txid,
          txIndex,
          networkId,
          blockHeight: block.height,
          blockHash: block.hash,
          blockTime: block.time,
          inputs: resolvedInputs,
          outputs: projectedOutputs,
        }),
      );
    }

    return {
      networkId,
      blockHeight: block.height,
      blockHash: block.hash,
      blockTime: block.time,
      utxoCreates,
      utxoSpends,
      addressMovements,
      transfers,
      directLinkDeltas: this.buildDirectLinkDeltas(networkId, block.height, transfers),
    };
  }

  public collectExternalOutputKeys(
    snapshot: Record<string, unknown>,
    knownOutputKeys: Set<string>,
  ): string[] {
    const block = this.parseBlock(snapshot);
    const externalOutputKeys = new Set<string>();

    for (const transaction of block.tx) {
      const txid = this.requireString(transaction.txid, 'tx.txid');
      for (const input of transaction.vin ?? []) {
        if (input.coinbase) {
          continue;
        }

        const prevTxid = this.requireString(input.txid, 'vin.txid');
        const prevVout = this.requireNumber(input.vout, 'vin.vout');
        const outputKey = `${prevTxid}:${prevVout}`;
        if (!knownOutputKeys.has(outputKey)) {
          externalOutputKeys.add(outputKey);
        }
      }

      for (const [entryIndex] of (transaction.vout ?? []).entries()) {
        knownOutputKeys.add(`${txid}:${entryIndex}`);
      }
    }

    return [...externalOutputKeys];
  }

  private buildDirectLinkDeltas(
    networkId: number,
    blockHeight: number,
    transfers: TransferFact[],
  ): DirectLinkDelta[] {
    const result = new Map<string, DirectLinkDelta>();
    for (const transfer of transfers) {
      if (transfer.isChange) {
        continue;
      }

      const key = [transfer.fromAddress, transfer.toAddress, transfer.assetAddress].join(':');
      const current = result.get(key);
      if (current) {
        current.transferCount += 1;
        current.totalAmountBase = addAmountBase(current.totalAmountBase, transfer.amountBase);
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

  private parseBlock(snapshot: Record<string, unknown>): ParsedDogecoinBlock {
    const candidate = this.requireRecord(snapshot.block, 'block');
    return {
      hash: this.requireString(this.readString(candidate, 'hash'), 'block.hash'),
      height: this.requireNumber(this.readNumber(candidate, 'height'), 'block.height'),
      time: this.requireNumber(this.readNumber(candidate, 'time'), 'block.time'),
      tx: this.readTransactions(candidate.tx),
    };
  }

  private extractOutputAddress(output: DogecoinVout): string {
    const direct = output.scriptPubKey?.address?.trim();
    if (direct) {
      return direct;
    }

    const first = output.scriptPubKey?.addresses?.find((value) => value.trim());
    return first?.trim() ?? '';
  }

  private projectTransfers(input: {
    blockHash: string;
    blockHeight: number;
    blockTime: number;
    inputs: Array<{ address: string; amountBase: string; outputKey: string }>;
    networkId: number;
    outputs: Array<{ address: string; amountBase: string; isSpendable: boolean }>;
    txIndex: number;
    txid: string;
  }): TransferFact[] {
    if (input.inputs.length === 0 || input.outputs.length === 0) {
      return [];
    }

    const inputTotals = new Map<string, bigint>();
    for (const item of input.inputs) {
      const current = inputTotals.get(item.address) ?? 0n;
      inputTotals.set(item.address, current + parseAmountBase(item.amountBase));
    }

    const totalInput = [...inputTotals.values()].reduce((sum, value) => sum + value, 0n);
    const inputAddresses = [...inputTotals.keys()].sort((left, right) => left.localeCompare(right));
    const uniqueOutputs = [...new Set(input.outputs.map((output) => output.address))].length;
    const confidence = inputAddresses.length <= 1 ? 1 : 0.5;
    const transfers: TransferFact[] = [];
    let transferIndex = 0;

    for (const output of input.outputs) {
      const allocations = this.allocateOutputAmount(
        output.amountBase,
        inputAddresses,
        inputTotals,
        totalInput,
      );
      const isChange = inputTotals.has(output.address);
      for (const allocation of allocations) {
        if (allocation.amount <= 0n) {
          continue;
        }

        transfers.push({
          transferId: `${input.txid}:${transferIndex}`,
          networkId: input.networkId,
          blockHeight: input.blockHeight,
          blockHash: input.blockHash,
          blockTime: input.blockTime,
          txid: input.txid,
          txIndex: input.txIndex,
          transferIndex,
          assetAddress: '',
          fromAddress: allocation.address,
          toAddress: output.address,
          amountBase: formatAmountBase(allocation.amount),
          derivationMethod: 'dogecoin_pro_rata_v1',
          confidence,
          isChange,
          inputAddressCount: inputAddresses.length,
          outputAddressCount: uniqueOutputs,
        });
        transferIndex += 1;
      }
    }

    return transfers;
  }

  private allocateOutputAmount(
    amountBase: string,
    inputAddresses: string[],
    inputTotals: Map<string, bigint>,
    totalInput: bigint,
  ): Array<{ address: string; amount: bigint }> {
    const target = parseAmountBase(amountBase);
    if (totalInput <= 0n) {
      return [];
    }

    const allocations: Array<{ address: string; amount: bigint }> = [];
    let allocated = 0n;
    for (const [index, address] of inputAddresses.entries()) {
      if (index === inputAddresses.length - 1) {
        allocations.push({
          address,
          amount: target - allocated,
        });
        continue;
      }

      const contribution = inputTotals.get(address) ?? 0n;
      const amount = (target * contribution) / totalInput;
      allocations.push({ address, amount });
      allocated += amount;
    }

    return allocations;
  }

  private async resolveOutput(
    networkId: number,
    spendingBlockHeight: number,
    spendingTxid: string,
    spendingInputIndex: number,
    outputKey: string,
    localOutputs: Map<string, ProjectionUtxoOutput>,
    persistedOutputs?: Map<string, ProjectionUtxoOutput>,
  ): Promise<{ isLocal: boolean; output: ProjectionUtxoOutput }> {
    const localOutput = localOutputs.get(outputKey);
    if (localOutput) {
      if (
        localOutput.spentByTxid &&
        !this.isReplayOfSameSpend(
          localOutput,
          spendingBlockHeight,
          spendingTxid,
          spendingInputIndex,
        )
      ) {
        throw new InfrastructureError(`dogecoin output already spent in block: ${outputKey}`);
      }

      return { isLocal: true, output: localOutput };
    }

    const persistedOutput =
      persistedOutputs?.get(outputKey) ??
      (await this.warehouse.getUtxoOutputs(networkId, [outputKey])).get(outputKey);
    if (!persistedOutput) {
      throw new InfrastructureError(`dogecoin output not found: ${outputKey}`);
    }
    if (
      persistedOutput.spentByTxid &&
      persistedOutput.spentInBlock !== null &&
      persistedOutput.spentInBlock <= spendingBlockHeight &&
      !this.isReplayOfSameSpend(
        persistedOutput,
        spendingBlockHeight,
        spendingTxid,
        spendingInputIndex,
      )
    ) {
      throw new InfrastructureError(`dogecoin output already spent: ${outputKey}`);
    }

    return { isLocal: false, output: persistedOutput };
  }

  private isReplayOfSameSpend(
    output: ProjectionUtxoOutput,
    spendingBlockHeight: number,
    spendingTxid: string,
    spendingInputIndex: number,
  ): boolean {
    return (
      output.spentByTxid === spendingTxid &&
      output.spentInBlock === spendingBlockHeight &&
      output.spentInputIndex === spendingInputIndex
    );
  }

  private requireAmount(value: number | string | undefined): number | string {
    if (value === undefined) {
      throw new InfrastructureError('missing dogecoin output value');
    }

    return value;
  }

  private requireNumber(value: number | undefined, field: string): number {
    if (value === undefined || !Number.isInteger(value) || value < 0) {
      throw new InfrastructureError(`invalid dogecoin field: ${field}`);
    }

    return value;
  }

  private requireString(value: string | undefined, field: string): string {
    const trimmed = value?.trim();
    if (!trimmed) {
      throw new InfrastructureError(`invalid dogecoin field: ${field}`);
    }

    return trimmed;
  }

  private readNumber(record: Record<string, unknown>, key: string): number | undefined {
    const value = record[key];
    return typeof value === 'number' ? value : undefined;
  }

  private readString(record: Record<string, unknown>, key: string): string | undefined {
    const value = record[key];
    return typeof value === 'string' ? value : undefined;
  }

  private readTransactions(value: unknown): DogecoinTransaction[] {
    return Array.isArray(value) ? value.filter(isDogecoinTransaction) : [];
  }

  private requireRecord(value: unknown, field: string): Record<string, unknown> {
    if (!value || typeof value !== 'object') {
      throw new InfrastructureError(`invalid dogecoin field: ${field}`);
    }

    return Object.fromEntries(Object.entries(value));
  }
}

function isDogecoinTransaction(value: unknown): value is DogecoinTransaction {
  return typeof value === 'object' && value !== null;
}
