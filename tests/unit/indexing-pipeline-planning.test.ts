import { describe, expect, it, vi } from 'vitest';

import {
  adaptiveWindowLogLine,
  type BootstrapStatus,
  blockRangeWork,
  bootstrapCursorLabel,
  bootstrapRequiredMessage,
  bootstrapRetryMarker,
  bootstrapStatusPhase,
  contiguousStoredTail,
  createPhaseAttempt,
  factPhaseMetrics,
  failedPhaseExecution,
  formatError,
  formatMetrics,
  hasRequiredBootstrapTail,
  idlePhaseExecution,
  isBootstrapPhase,
  isTimeoutError,
  noPhaseWork,
  phasePendingText,
  phaseRangeText,
  phaseRate,
  phaseWindowText,
  recoveredSnapshotResult,
  requiresDogecoinPrevoutBootstrap,
  shouldBackoffWindow,
  tailOrInitial,
} from '../../packages/modules/indexing-pipeline/src/application/indexing-pipeline-planning';

describe('indexing pipeline planning helpers', () => {
  it('formats phase execution and work-result decisions', () => {
    expect(noPhaseWork()).toEqual({ didWork: false, workItems: 0 });
    expect(blockRangeWork(9, 12, { write_ms: 3 })).toEqual({
      didWork: true,
      workItems: 3,
      startBlock: 10,
      endBlock: 12,
      metrics: { write_ms: 3 },
    });
    expect(recoveredSnapshotResult(9, { recoveredTail: 12, recoveryIndex: 2 }, 2, 11)).toEqual({
      didWork: true,
      workItems: 3,
      startBlock: 10,
      endBlock: 12,
      metrics: { load_snapshots_ms: 11 },
    });
    expect(recoveredSnapshotResult(9, { recoveredTail: 12, recoveryIndex: 1 }, 2, 11)).toBeNull();
    expect(factPhaseMetrics(5, { apply_links_ms: 2 }, 7, { write_facts_ms: 3 })).toEqual({
      apply_links_ms: 2,
      load_snapshots_ms: 5,
      project_blocks_ms: 7,
      write_facts_ms: 3,
    });
  });

  it('formats metrics and phase log fragments', () => {
    const result = {
      didWork: true,
      workItems: 10,
      durationMs: 2500,
      startBlock: 1,
      endBlock: 10,
      pendingCount: 2,
    };

    expect(formatMetrics({ a: 1, b: 'two' })).toBe(' a=1 b=two');
    expect(phaseRate(result)).toBe(4);
    expect(phaseRangeText(result)).toBe(' blocks=1-10');
    expect(phaseWindowText(8)).toBe(' window=8');
    expect(phasePendingText(result)).toBe(' pending=2');
    expect(
      adaptiveWindowLogLine('doge', 'sync', 8, 4, { kind: 'backoff', reason: 'timeout' }),
    ).toBe('[onlydoge] adaptive backoff network=doge phase=sync from=8 to=4 reason=timeout');
  });

  it('classifies bootstrap and recovery state', () => {
    const status: BootstrapStatus = {
      bootstrapTail: null,
      cursorBalance: { address: 'DA', assetAddress: '' },
      cursorUtxo: 'tx:1',
      phase: 'balances',
      required: true,
      startedAtMs: 1000,
      targetTail: 20,
    };

    expect(isBootstrapPhase('utxos')).toBe(true);
    expect(isBootstrapPhase('facts')).toBe(false);
    expect(tailOrInitial(null)).toBe(-1);
    expect(contiguousStoredTail(2, [3, 4, 5], new Set([3, 4]))).toBe(4);
    expect(bootstrapRetryMarker(status, 9)).toBe('balances:20');
    expect(bootstrapCursorLabel(status)).toBe('DA:');
    expect(bootstrapStatusPhase({ ...status, phase: null })).toBe('utxos');
    expect(requiresDogecoinPrevoutBootstrap('state', 10, 9)).toBe(true);
    expect(requiresDogecoinPrevoutBootstrap('facts', 10, 0)).toBe(false);
    expect(hasRequiredBootstrapTail(20, 20)).toBe(true);
    expect(bootstrapRequiredMessage('doge', 1, 10, null, 20)).toContain('bootstrap_tail=-1');
  });

  it('classifies timeout/backoff errors and creates phase attempts', () => {
    vi.useFakeTimers();
    vi.setSystemTime(10_000);
    expect(formatError(new Error('timed out'))).toBe('timed out');
    expect(isTimeoutError(new Error('timed out'))).toBe(true);
    expect(shouldBackoffWindow(new Error('warehouse unavailable'))).toBe(true);
    expect(idlePhaseExecution()).toEqual({ didWork: false, workItems: 0, durationMs: 0 });
    expect(failedPhaseExecution(9_500, new Error('boom'))).toMatchObject({
      didWork: false,
      workItems: 0,
      durationMs: 500,
    });

    const attempt = createPhaseAttempt(10_000);
    expect(attempt.attemptId).toEqual(expect.any(String));
    expect(attempt.controller.signal.aborted).toBe(false);
    vi.useRealTimers();
  });
});
