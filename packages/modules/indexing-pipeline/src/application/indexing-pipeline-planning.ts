import { randomUUID } from 'node:crypto';

import type { ProjectionBalanceCursor } from '../domain/projection-models';

export type BootstrapPhase = 'balances' | 'done' | 'utxos';
export type PhaseName = 'bootstrap' | 'facts' | 'project-state' | 'relink' | 'sync';
export type ProjectionMode = 'facts' | 'state';

export interface PhaseWorkResult {
  didWork: boolean;
  metrics?: Record<string, number | string>;
  endBlock?: number;
  pendingCount?: number;
  startBlock?: number;
  workItems: number;
}

export interface PhaseExecutionResult extends PhaseWorkResult {
  durationMs: number;
  error?: unknown;
}

export interface AppliedSnapshotRecovery {
  recoveredTail: number;
  recoveryIndex: number;
}

export interface BootstrapStatus {
  bootstrapTail: number | null;
  cursorBalance: ProjectionBalanceCursor | null;
  cursorUtxo: string | null;
  phase: BootstrapPhase | null;
  required: boolean;
  startedAtMs: number | null;
  targetTail: number | null;
}

export interface PhaseAttempt {
  attemptId: string;
  controller: AbortController;
  promise: Promise<void>;
  startedAtMs: number;
  timedOut: boolean;
}

export function formatError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function formatMetrics(metrics: Record<string, number | string> | undefined): string {
  if (!metrics || Object.keys(metrics).length === 0) {
    return '';
  }

  return ` ${Object.entries(metrics)
    .map(([key, value]) => `${key}=${value}`)
    .join(' ')}`;
}

export function phaseRate(result: PhaseExecutionResult): number {
  return result.durationMs > 0 ? (result.workItems * 1000) / Math.max(result.durationMs, 1) : 0;
}

export function phaseRangeText(result: PhaseExecutionResult): string {
  if (result.startBlock === undefined || result.endBlock === undefined) {
    return '';
  }

  return ` blocks=${result.startBlock}-${result.endBlock}`;
}

export function phaseWindowText(window: number | null): string {
  return window === null ? '' : ` window=${window}`;
}

export function phasePendingText(result: PhaseExecutionResult): string {
  return result.pendingCount === undefined ? '' : ` pending=${result.pendingCount}`;
}

export function recoveredTailResult(
  currentTail: number,
  recoveredTail: number,
  metrics: Record<string, number>,
): PhaseWorkResult {
  if (recoveredTail <= currentTail) {
    return { didWork: false, workItems: 0 };
  }

  return {
    didWork: true,
    workItems: recoveredTail - currentTail,
    startBlock: currentTail + 1,
    endBlock: recoveredTail,
    metrics,
  };
}

export function recoveredSnapshotResult(
  currentTail: number,
  recovery: AppliedSnapshotRecovery,
  snapshotCount: number,
  loadSnapshotsMs: number,
  extraMetrics: Record<string, number> = {},
): PhaseWorkResult | null {
  if (recovery.recoveredTail <= currentTail || recovery.recoveryIndex < snapshotCount) {
    return null;
  }

  return recoveredTailResult(currentTail, recovery.recoveredTail, {
    ...extraMetrics,
    load_snapshots_ms: loadSnapshotsMs,
  });
}

export function noPhaseWork(): PhaseWorkResult {
  return { didWork: false, workItems: 0 };
}

export function blockRangeWork(
  previousTail: number,
  nextTail: number,
  metrics?: Record<string, number>,
): PhaseWorkResult {
  return {
    didWork: true,
    workItems: nextTail - previousTail,
    startBlock: previousTail + 1,
    endBlock: nextTail,
    ...(metrics ? { metrics } : {}),
  };
}

export function factPhaseMetrics(
  loadSnapshotsMs: number,
  recoveryMetrics: Record<string, number>,
  projectBlocksMs: number,
  persistMetrics: Record<string, number>,
): Record<string, number> {
  return {
    apply_links_ms:
      metricValue(recoveryMetrics, 'apply_links_ms') +
      metricValue(persistMetrics, 'apply_links_ms'),
    load_snapshots_ms: loadSnapshotsMs,
    project_blocks_ms: metricValue(recoveryMetrics, 'project_blocks_ms') + projectBlocksMs,
    write_facts_ms: metricValue(persistMetrics, 'write_facts_ms'),
  };
}

export function idlePhaseExecution(): PhaseExecutionResult {
  return {
    didWork: false,
    workItems: 0,
    durationMs: 0,
  };
}

export function failedPhaseExecution(startedAt: number, error: unknown): PhaseExecutionResult {
  return {
    didWork: false,
    workItems: 0,
    durationMs: Date.now() - startedAt,
    error,
  };
}

export function createPhaseAttempt(startedAt: number): PhaseAttempt {
  return {
    attemptId: randomUUID(),
    controller: new AbortController(),
    startedAtMs: startedAt,
    timedOut: false,
    promise: Promise.resolve(),
  };
}

export function shouldBackoffWindow(error: unknown): boolean {
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

export function adaptiveWindowLogLine(
  networkId: string,
  phase: 'project-state' | 'sync',
  fromWindow: number,
  toWindow: number,
  event: { kind: 'backoff'; reason: string } | { kind: 'growth' },
): string {
  const base = `[onlydoge] adaptive ${event.kind} network=${networkId} phase=${phase} from=${fromWindow} to=${toWindow}`;
  return event.kind === 'backoff' ? `${base} reason=${event.reason}` : base;
}

export function isTimeoutError(error: unknown): boolean {
  return formatError(error).includes('timed out');
}

export function isBootstrapPhase(value: string | null): value is BootstrapPhase {
  return value === 'balances' || value === 'done' || value === 'utxos';
}

export function tailOrInitial(value: number | null): number {
  return value ?? -1;
}

export function contiguousStoredTail(
  currentTail: number,
  heights: number[],
  storedHeights: Set<number>,
): number {
  let contiguousTail = currentTail;
  for (const blockHeight of heights) {
    if (!storedHeights.has(blockHeight)) {
      break;
    }

    contiguousTail = blockHeight;
  }

  return contiguousTail;
}

export function bootstrapRetryMarker(status: BootstrapStatus, processTail: number): string {
  return `${status.phase ?? 'utxos'}:${status.targetTail ?? processTail}`;
}

export function bootstrapCursorLabel(status: BootstrapStatus): string {
  if (status.phase !== 'balances') {
    return status.cursorUtxo ?? 'start';
  }

  const cursor = status.cursorBalance;
  return cursor ? `${cursor.address}:${cursor.assetAddress}` : 'start';
}

export function bootstrapStatusPhase(status: BootstrapStatus): BootstrapPhase {
  return status.phase ?? 'utxos';
}

export function requiresDogecoinPrevoutBootstrap(
  mode: ProjectionMode,
  requiredPrevouts: number,
  persistedPrevouts: number,
): boolean {
  return mode === 'state' && persistedPrevouts < requiredPrevouts;
}

export function hasRequiredBootstrapTail(
  bootstrapTail: number | null,
  expectedBootstrapTail: number,
): boolean {
  return bootstrapTail !== null && bootstrapTail >= expectedBootstrapTail;
}

export function bootstrapRequiredMessage(
  networkId: string,
  missingPrevouts: number,
  requiredPrevouts: number,
  bootstrapTail: number | null,
  expectedBootstrapTail: number,
): string {
  return `[onlydoge] phase=project-state network=${networkId} reason=bootstrap-required missing_prevouts=${missingPrevouts} required_prevouts=${requiredPrevouts} bootstrap_tail=${bootstrapTail ?? -1} required_tail=${expectedBootstrapTail}`;
}

function metricValue(metrics: Record<string, number>, key: string): number {
  return metrics[key] ?? 0;
}
