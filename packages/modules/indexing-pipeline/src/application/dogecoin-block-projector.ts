import { InfrastructureError } from '@onlydoge/shared-kernel';

import type { ProjectionStateStorePort } from '../contracts/ports';
import {
  allocateProRataAmount,
  formatAmountBase,
  fromDecimalUnits,
  parseAmountBase,
} from '../domain/amounts';
import {
  type DogecoinTransaction,
  type DogecoinVout,
  extractDogecoinOutputAddress,
  isDogecoinTransaction,
  type ParsedDogecoinBlock,
} from '../domain/dogecoin-block';
import type {
  BlockProjectionBatch,
  ProjectionUtxoOutput,
  TransferFact,
} from '../domain/projection-models';
import { buildDirectLinkDeltas } from '../domain/projection-models';
import { readSnapshotItems, readSnapshotString, requireSnapshotRecord } from './block-snapshot';

export interface ProjectionState {
  localOutputs?: Map<string, ProjectionUtxoOutput>;
  persistedOutputs?: Map<string, ProjectionUtxoOutput>;
}

export interface DogecoinProjectOptions {
  includeDirectLinkDeltas?: boolean;
  includeTransfers?: boolean;
  maxTransferEdges?: number;
  maxTransferInputAddresses?: number;
}

type ResolvedDogecoinInput = {
  address: string;
  amountBase: string;
  outputKey: string;
};

type ProjectedDogecoinOutput = {
  address: string;
  amountBase: string;
  isSpendable: boolean;
};

type DogecoinProjectionAccumulator = Pick<
  BlockProjectionBatch,
  'addressMovements' | 'utxoCreates' | 'utxoSpends'
> & {
  transfers: TransferFact[];
};

type DogecoinTransactionContext = {
  accumulator: DogecoinProjectionAccumulator;
  block: ParsedDogecoinBlock;
  localOutputs: Map<string, ProjectionUtxoOutput>;
  networkId: number;
  options: Required<DogecoinProjectOptions>;
  persistedOutputs: Map<string, ProjectionUtxoOutput> | undefined;
  transaction: DogecoinTransaction;
  txIndex: number;
  txid: string;
};

type DogecoinVin = NonNullable<DogecoinTransaction['vin']>[number];

type DogecoinTransferInput = {
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  inputs: ResolvedDogecoinInput[];
  maxTransferEdges: number;
  maxTransferInputAddresses: number;
  networkId: number;
  outputs: ProjectedDogecoinOutput[];
  txIndex: number;
  txid: string;
};

type DogecoinAddressMovementBase = Omit<
  BlockProjectionBatch['addressMovements'][number],
  'derivationMethod' | 'direction' | 'movementId' | 'outputKey'
>;

type DogecoinTransferBasis = {
  confidence: number;
  inputAddresses: string[];
  inputTotals: Map<string, bigint>;
  totalInput: bigint;
  uniqueOutputs: number;
};

const defaultProjectOptions: Required<DogecoinProjectOptions> = {
  includeDirectLinkDeltas: true,
  includeTransfers: true,
  maxTransferEdges: 1024,
  maxTransferInputAddresses: 64,
};

function resolveDogecoinProjectOptions(
  options: DogecoinProjectOptions | undefined,
): Required<DogecoinProjectOptions> {
  return {
    ...defaultProjectOptions,
    ...(options ?? {}),
  };
}

function resolveProjectionState(state: ProjectionState | undefined): Required<ProjectionState> {
  return {
    localOutputs: projectionLocalOutputs(state),
    persistedOutputs: projectionPersistedOutputs(state),
  };
}

function projectionLocalOutputs(
  state: ProjectionState | undefined,
): Map<string, ProjectionUtxoOutput> {
  return state?.localOutputs ?? new Map<string, ProjectionUtxoOutput>();
}

function projectionPersistedOutputs(
  state: ProjectionState | undefined,
): Map<string, ProjectionUtxoOutput> {
  return state?.persistedOutputs ?? new Map<string, ProjectionUtxoOutput>();
}

function dogecoinDirectLinkDeltas(
  networkId: number,
  blockHeight: number,
  transfers: TransferFact[],
  options: Required<DogecoinProjectOptions>,
): BlockProjectionBatch['directLinkDeltas'] {
  if (!options.includeDirectLinkDeltas) {
    return [];
  }

  return buildDirectLinkDeltas(networkId, blockHeight, transfers, { includeChange: false });
}

function hasTransferEndpoints(input: DogecoinTransferInput): boolean {
  return input.inputs.length > 0 && input.outputs.length > 0;
}

function dogecoinInputTotals(inputs: ResolvedDogecoinInput[]): Map<string, bigint> {
  const inputTotals = new Map<string, bigint>();
  for (const item of inputs) {
    const current = inputTotals.get(item.address) ?? 0n;
    inputTotals.set(item.address, current + parseAmountBase(item.amountBase));
  }

  return inputTotals;
}

function exceedsTransferGuardrail(
  input: DogecoinTransferInput,
  inputAddresses: string[],
  estimatedEdges: number,
): boolean {
  return (
    inputAddresses.length > input.maxTransferInputAddresses ||
    estimatedEdges > input.maxTransferEdges
  );
}

function dogecoinTransferGuardrailMessage(
  input: DogecoinTransferInput,
  inputAddresses: string[],
  estimatedEdges: number,
): string {
  return `[onlydoge] dogecoin transfer derivation skipped tx=${input.txid} inputAddresses=${inputAddresses.length} outputs=${input.outputs.length} estimatedEdges=${estimatedEdges} reason=guardrail`;
}

function dogecoinInputs(transaction: DogecoinTransaction): DogecoinVin[] {
  return transaction.vin ?? [];
}

function dogecoinOutputs(transaction: DogecoinTransaction): DogecoinVout[] {
  return transaction.vout ?? [];
}

export class DogecoinBlockProjector {
  public constructor(private readonly warehouse: ProjectionStateStorePort) {}

  public async project(
    networkId: number,
    snapshot: Record<string, unknown>,
    state?: ProjectionState,
    options?: DogecoinProjectOptions,
  ): Promise<BlockProjectionBatch> {
    const resolvedOptions = resolveDogecoinProjectOptions(options);
    const block = this.parseBlock(snapshot);
    const accumulator: DogecoinProjectionAccumulator = {
      utxoCreates: [],
      utxoSpends: [],
      addressMovements: [],
      transfers: [],
    };
    const projectionState = resolveProjectionState(state);

    for (const [txIndex, transaction] of block.tx.entries()) {
      const txid = this.requireString(transaction.txid, 'tx.txid');
      await this.projectTransaction({
        accumulator,
        block,
        localOutputs: projectionState.localOutputs,
        networkId,
        options: resolvedOptions,
        persistedOutputs: projectionState.persistedOutputs,
        transaction,
        txIndex,
        txid,
      });
    }

    return {
      networkId,
      blockHeight: block.height,
      blockHash: block.hash,
      blockTime: block.time,
      utxoCreates: accumulator.utxoCreates,
      utxoSpends: accumulator.utxoSpends,
      addressMovements: accumulator.addressMovements,
      transfers: accumulator.transfers,
      directLinkDeltas: dogecoinDirectLinkDeltas(
        networkId,
        block.height,
        accumulator.transfers,
        resolvedOptions,
      ),
    };
  }

  public collectExternalOutputKeys(
    snapshot: Record<string, unknown>,
    knownOutputKeys: Set<string>,
  ): string[] {
    const block = this.parseBlock(snapshot);
    const externalOutputKeys = new Set<string>();

    for (const transaction of block.tx) {
      this.collectTransactionExternalOutputKeys(transaction, knownOutputKeys, externalOutputKeys);
    }

    return [...externalOutputKeys];
  }

  private collectTransactionExternalOutputKeys(
    transaction: DogecoinTransaction,
    knownOutputKeys: Set<string>,
    externalOutputKeys: Set<string>,
  ): void {
    const txid = this.requireString(transaction.txid, 'tx.txid');
    for (const input of dogecoinInputs(transaction)) {
      this.collectInputExternalOutputKey(input, knownOutputKeys, externalOutputKeys);
    }

    for (const [entryIndex] of dogecoinOutputs(transaction).entries()) {
      knownOutputKeys.add(`${txid}:${entryIndex}`);
    }
  }

  private collectInputExternalOutputKey(
    input: DogecoinVin,
    knownOutputKeys: Set<string>,
    externalOutputKeys: Set<string>,
  ): void {
    if (input.coinbase) {
      return;
    }

    const outputKey = `${this.requireString(input.txid, 'vin.txid')}:${this.requireNumber(input.vout, 'vin.vout')}`;
    if (!knownOutputKeys.has(outputKey)) {
      externalOutputKeys.add(outputKey);
    }
  }

  private parseBlock(snapshot: Record<string, unknown>): ParsedDogecoinBlock {
    const candidate = requireSnapshotRecord(snapshot.block, 'block', 'dogecoin');
    return {
      hash: this.requireString(readSnapshotString(candidate, 'hash'), 'block.hash'),
      height: this.requireNumber(this.readNumber(candidate, 'height'), 'block.height'),
      time: this.requireNumber(this.readNumber(candidate, 'time'), 'block.time'),
      tx: readSnapshotItems(candidate.tx, isDogecoinTransaction),
    };
  }

  private async projectTransaction(context: DogecoinTransactionContext): Promise<void> {
    const inputs = context.transaction.vin ?? [];
    const outputs = context.transaction.vout ?? [];
    const isCoinbase = inputs.some((input) => Boolean(input.coinbase));
    const resolvedInputs = await this.projectTransactionInputs(context, inputs);
    const projectedOutputs = this.projectTransactionOutputs(context, outputs, isCoinbase);

    if (!context.options.includeTransfers) {
      return;
    }

    context.accumulator.transfers.push(
      ...this.projectTransfers({
        txid: context.txid,
        txIndex: context.txIndex,
        networkId: context.networkId,
        blockHeight: context.block.height,
        blockHash: context.block.hash,
        blockTime: context.block.time,
        inputs: resolvedInputs,
        outputs: projectedOutputs,
        maxTransferEdges: context.options.maxTransferEdges,
        maxTransferInputAddresses: context.options.maxTransferInputAddresses,
      }),
    );
  }

  private async projectTransactionInputs(
    context: DogecoinTransactionContext,
    inputs: DogecoinVin[],
  ): Promise<ResolvedDogecoinInput[]> {
    const resolvedInputs: ResolvedDogecoinInput[] = [];
    for (const [entryIndex, input] of inputs.entries()) {
      if (input.coinbase) {
        continue;
      }

      resolvedInputs.push(await this.projectTransactionInput(context, input, entryIndex));
    }

    return resolvedInputs;
  }

  private async projectTransactionInput(
    context: DogecoinTransactionContext,
    input: DogecoinVin,
    entryIndex: number,
  ): Promise<ResolvedDogecoinInput> {
    const prevTxid = this.requireString(input.txid, 'vin.txid');
    const prevVout = this.requireNumber(input.vout, 'vin.vout');
    const outputKey = `${prevTxid}:${prevVout}`;
    const resolved = await this.resolveOutput(
      context.networkId,
      context.block.height,
      context.txid,
      entryIndex,
      outputKey,
      context.localOutputs,
      context.persistedOutputs,
    );

    this.recordInputSpend(context, resolved, outputKey, entryIndex);
    context.accumulator.addressMovements.push(
      this.inputAddressMovement(context, resolved.output, outputKey, entryIndex),
    );

    return {
      address: resolved.output.address,
      amountBase: resolved.output.valueBase,
      outputKey,
    };
  }

  private recordInputSpend(
    context: DogecoinTransactionContext,
    resolved: { isLocal: boolean; output: ProjectionUtxoOutput },
    outputKey: string,
    entryIndex: number,
  ): void {
    if (resolved.isLocal) {
      this.markLocalSpend(resolved.output, context.block.height, context.txid, entryIndex);
      return;
    }

    context.accumulator.utxoSpends.push({
      outputKey,
      spentByTxid: context.txid,
      spentInBlock: context.block.height,
      spentInputIndex: entryIndex,
    });
  }

  private markLocalSpend(
    output: ProjectionUtxoOutput,
    blockHeight: number,
    txid: string,
    inputIndex: number,
  ): void {
    output.spentByTxid = txid;
    output.spentInBlock = blockHeight;
    output.spentInputIndex = inputIndex;
  }

  private inputAddressMovement(
    context: DogecoinTransactionContext,
    output: ProjectionUtxoOutput,
    outputKey: string,
    entryIndex: number,
  ): BlockProjectionBatch['addressMovements'][number] {
    return {
      ...this.addressMovementBase(context, output, entryIndex),
      movementId: `${context.txid}:vin:${entryIndex}`,
      direction: 'debit',
      outputKey,
      derivationMethod: 'dogecoin_utxo_input_v1',
    };
  }

  private projectTransactionOutputs(
    context: DogecoinTransactionContext,
    outputs: DogecoinVout[],
    isCoinbase: boolean,
  ): ProjectedDogecoinOutput[] {
    const projectedOutputs: ProjectedDogecoinOutput[] = [];
    for (const [entryIndex, output] of outputs.entries()) {
      const createdOutput = this.createUtxoOutput(context, output, entryIndex, isCoinbase);
      context.accumulator.utxoCreates.push(createdOutput);
      context.localOutputs.set(createdOutput.outputKey, createdOutput);
      this.projectSpendableOutput(context, createdOutput, entryIndex, projectedOutputs);
    }

    return projectedOutputs;
  }

  private createUtxoOutput(
    context: DogecoinTransactionContext,
    output: DogecoinVout,
    entryIndex: number,
    isCoinbase: boolean,
  ): ProjectionUtxoOutput {
    const amountBase = fromDecimalUnits(this.requireAmount(output.value), 8);
    const address = extractDogecoinOutputAddress(output);

    return {
      networkId: context.networkId,
      blockHeight: context.block.height,
      blockHash: context.block.hash,
      blockTime: context.block.time,
      txid: context.txid,
      txIndex: context.txIndex,
      vout: this.requireNumber(output.n ?? entryIndex, 'vout.n'),
      outputKey: `${context.txid}:${entryIndex}`,
      address,
      scriptType: output.scriptPubKey?.type?.trim() ?? '',
      valueBase: amountBase,
      isCoinbase,
      isSpendable: Boolean(address),
      spentByTxid: null,
      spentInBlock: null,
      spentInputIndex: null,
    };
  }

  private projectSpendableOutput(
    context: DogecoinTransactionContext,
    output: ProjectionUtxoOutput,
    entryIndex: number,
    projectedOutputs: ProjectedDogecoinOutput[],
  ): void {
    if (!output.isSpendable) {
      return;
    }

    projectedOutputs.push({
      address: output.address,
      amountBase: output.valueBase,
      isSpendable: output.isSpendable,
    });
    context.accumulator.addressMovements.push(
      this.outputAddressMovement(context, output, entryIndex),
    );
  }

  private outputAddressMovement(
    context: DogecoinTransactionContext,
    output: ProjectionUtxoOutput,
    entryIndex: number,
  ): BlockProjectionBatch['addressMovements'][number] {
    return {
      ...this.addressMovementBase(context, output, entryIndex),
      movementId: `${context.txid}:vout:${entryIndex}`,
      direction: 'credit',
      outputKey: output.outputKey,
      derivationMethod: 'dogecoin_utxo_output_v1',
    };
  }

  private addressMovementBase(
    context: DogecoinTransactionContext,
    output: ProjectionUtxoOutput,
    entryIndex: number,
  ): DogecoinAddressMovementBase {
    return {
      networkId: context.networkId,
      blockHeight: context.block.height,
      blockHash: context.block.hash,
      blockTime: context.block.time,
      txid: context.txid,
      txIndex: context.txIndex,
      entryIndex,
      address: output.address,
      assetAddress: '',
      amountBase: output.valueBase,
    };
  }

  private projectTransfers(input: DogecoinTransferInput): TransferFact[] {
    const basis = this.buildTransferBasis(input);
    if (!basis) {
      return [];
    }

    return this.buildProRataTransfers(input, basis);
  }

  private buildTransferBasis(input: DogecoinTransferInput): DogecoinTransferBasis | null {
    if (!hasTransferEndpoints(input)) {
      return null;
    }

    const inputTotals = dogecoinInputTotals(input.inputs);
    const totalInput = [...inputTotals.values()].reduce((sum, value) => sum + value, 0n);
    const inputAddresses = [...inputTotals.keys()].sort((left, right) => left.localeCompare(right));
    const estimatedEdges = inputAddresses.length * input.outputs.length;
    if (exceedsTransferGuardrail(input, inputAddresses, estimatedEdges)) {
      console.warn(dogecoinTransferGuardrailMessage(input, inputAddresses, estimatedEdges));
      return null;
    }

    const uniqueOutputs = [...new Set(input.outputs.map((output) => output.address))].length;
    const confidence = inputAddresses.length <= 1 ? 1 : 0.5;

    return {
      confidence,
      inputAddresses,
      inputTotals,
      totalInput,
      uniqueOutputs,
    };
  }

  private buildProRataTransfers(
    input: DogecoinTransferInput,
    basis: DogecoinTransferBasis,
  ): TransferFact[] {
    const transfers: TransferFact[] = [];
    let transferIndex = 0;

    for (const output of input.outputs) {
      const allocations = allocateProRataAmount(
        parseAmountBase(output.amountBase),
        basis.inputAddresses,
        basis.inputTotals,
        basis.totalInput,
      );
      for (const allocation of allocations) {
        if (allocation.amount <= 0n) {
          continue;
        }

        transfers.push(this.transferFact(input, basis, output.address, allocation, transferIndex));
        transferIndex += 1;
      }
    }

    return transfers;
  }

  private transferFact(
    input: DogecoinTransferInput,
    basis: DogecoinTransferBasis,
    toAddress: string,
    allocation: { address: string; amount: bigint },
    transferIndex: number,
  ): TransferFact {
    return {
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
      toAddress,
      amountBase: formatAmountBase(allocation.amount),
      derivationMethod: 'dogecoin_pro_rata_v1',
      confidence: basis.confidence,
      isChange: basis.inputTotals.has(toAddress),
      inputAddressCount: basis.inputAddresses.length,
      outputAddressCount: basis.uniqueOutputs,
    };
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
    const localOutput = this.resolveLocalOutput(
      outputKey,
      localOutputs,
      spendingBlockHeight,
      spendingTxid,
      spendingInputIndex,
    );
    if (localOutput) {
      return { isLocal: true, output: localOutput };
    }

    const persistedOutput = await this.loadPersistedOutput(networkId, outputKey, persistedOutputs);
    if (!persistedOutput) {
      throw new InfrastructureError(`dogecoin output not found: ${outputKey}`);
    }

    if (
      this.isConflictingPersistedSpend(
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

  private resolveLocalOutput(
    outputKey: string,
    localOutputs: Map<string, ProjectionUtxoOutput>,
    spendingBlockHeight: number,
    spendingTxid: string,
    spendingInputIndex: number,
  ): ProjectionUtxoOutput | null {
    const localOutput = localOutputs.get(outputKey);
    if (!localOutput) {
      return null;
    }

    if (
      this.isConflictingLocalSpend(
        localOutput,
        spendingBlockHeight,
        spendingTxid,
        spendingInputIndex,
      )
    ) {
      throw new InfrastructureError(`dogecoin output already spent in block: ${outputKey}`);
    }

    return localOutput;
  }

  private async loadPersistedOutput(
    networkId: number,
    outputKey: string,
    persistedOutputs?: Map<string, ProjectionUtxoOutput>,
  ): Promise<ProjectionUtxoOutput | undefined> {
    const preloadedOutput = persistedOutputs?.get(outputKey);
    if (preloadedOutput) {
      return preloadedOutput;
    }

    return (await this.warehouse.getUtxoOutputs(networkId, [outputKey])).get(outputKey);
  }

  private isConflictingLocalSpend(
    output: ProjectionUtxoOutput,
    spendingBlockHeight: number,
    spendingTxid: string,
    spendingInputIndex: number,
  ): boolean {
    return (
      Boolean(output.spentByTxid) &&
      !this.isReplayOfSameSpend(output, spendingBlockHeight, spendingTxid, spendingInputIndex)
    );
  }

  private isConflictingPersistedSpend(
    output: ProjectionUtxoOutput,
    spendingBlockHeight: number,
    spendingTxid: string,
    spendingInputIndex: number,
  ): boolean {
    if (!output.spentByTxid || output.spentInBlock === null) {
      return false;
    }

    if (output.spentInBlock > spendingBlockHeight) {
      return false;
    }

    return !this.isReplayOfSameSpend(output, spendingBlockHeight, spendingTxid, spendingInputIndex);
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
}
