import type { PrimaryId } from '@onlydoge/shared-kernel';

export interface ProjectionUtxoOutput {
  address: string;
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  isCoinbase: boolean;
  isSpendable: boolean;
  networkId: PrimaryId;
  outputKey: string;
  scriptType: string;
  spentByTxid: string | null;
  spentInBlock: number | null;
  spentInputIndex: number | null;
  txIndex: number;
  txid: string;
  valueBase: string;
  vout: number;
}

export interface ProjectionUtxoSpend {
  outputKey: string;
  spentByTxid: string;
  spentInBlock: number;
  spentInputIndex: number;
}

export interface AddressMovement {
  address: string;
  amountBase: string;
  assetAddress: string;
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  derivationMethod: string;
  direction: 'credit' | 'debit';
  entryIndex: number;
  movementId: string;
  networkId: PrimaryId;
  outputKey: string | null;
  txIndex: number;
  txid: string;
}

export interface TransferFact {
  amountBase: string;
  assetAddress: string;
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  confidence: number;
  derivationMethod: string;
  fromAddress: string;
  inputAddressCount: number;
  isChange: boolean;
  networkId: PrimaryId;
  outputAddressCount: number;
  toAddress: string;
  transferId: string;
  transferIndex: number;
  txIndex: number;
  txid: string;
}

export interface DirectLinkDelta {
  assetAddress: string;
  firstSeenBlockHeight: number;
  fromAddress: string;
  lastSeenBlockHeight: number;
  networkId: PrimaryId;
  toAddress: string;
  totalAmountBase: string;
  transferCount: number;
}

export interface DirectLinkRecord extends DirectLinkDelta {}

export interface ProjectionBalanceSnapshot {
  address: string;
  assetAddress: string;
  asOfBlockHeight: number;
  balance: string;
  networkId: PrimaryId;
}

export interface ProjectionBalanceCursor {
  address: string;
  assetAddress: string;
}

export interface ProjectionPageRequestContext {
  abortSignal?: AbortSignal;
  timeoutMs?: number;
}

export interface ProjectionCurrentUtxoPage {
  nextCursor: string | null;
  rows: ProjectionUtxoOutput[];
}

export interface ProjectionCurrentBalancePage {
  nextCursor: ProjectionBalanceCursor | null;
  rows: ProjectionBalanceSnapshot[];
}

export interface ProjectionAppliedBlock {
  blockHash: string;
  blockHeight: number;
  networkId: PrimaryId;
}

export interface ProjectionDirectLinkBatch {
  blockHash: string;
  blockHeight: number;
  directLinkDeltas: DirectLinkDelta[];
  networkId: PrimaryId;
}

export interface SourceLinkRecord {
  firstSeenBlockHeight: number;
  hopCount: number;
  lastSeenBlockHeight: number;
  networkId: PrimaryId;
  pathAddresses: string[];
  pathTransferCount: number;
  sourceAddress: string;
  sourceAddressId: PrimaryId;
  toAddress: string;
}

export interface BlockProjectionBatch {
  addressMovements: AddressMovement[];
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  directLinkDeltas: DirectLinkDelta[];
  networkId: PrimaryId;
  transfers: TransferFact[];
  utxoCreates: ProjectionUtxoOutput[];
  utxoSpends: ProjectionUtxoSpend[];
}

export interface ProjectionFactWindow {
  addressMovements: AddressMovement[];
  appliedBlocks: ProjectionAppliedBlock[];
  balances: ProjectionBalanceSnapshot[];
  directLinks: DirectLinkRecord[];
  networkId: PrimaryId;
  transfers: TransferFact[];
  utxoOutputs: ProjectionUtxoOutput[];
}

export interface ProjectionStateBootstrapSnapshot {
  appliedBlocks: ProjectionAppliedBlock[];
  balances: ProjectionBalanceSnapshot[];
  directLinks: DirectLinkRecord[];
  sourceLinks: SourceLinkRecord[];
  utxoOutputs: ProjectionUtxoOutput[];
}

export interface TrackedAddress {
  address: string;
  addressId: PrimaryId;
}
