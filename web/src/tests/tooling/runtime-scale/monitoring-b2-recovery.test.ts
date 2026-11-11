import { describe, expect, test } from 'bun:test'
import {
    assessMonitoringB2Replay,
    monitoringB2PermissionDenied,
    runMonitoringB2Recovery,
} from '../../../../../scripts/runtime-scale/monitoring-b2-recovery.ts'
import type { MonitoringB2ReplayEvidence } from '../../../../../scripts/runtime-scale/Types/monitoring-b2-recovery.types.ts'
import type { MonitoringB2RecoveryContext } from '../../../../../scripts/runtime-scale/Types/monitoring-b2.types.ts'

const complete: MonitoringB2ReplayEvidence = {
    rawDurableRecords: 1000,
    distinctRawPolicies: 1000,
    nativeHeads: 1000,
    distinctNativeHosts: 1000,
    replayedResults: 1000,
    observations: 1000,
    replayMs: 30000,
    pending: 0,
    rejectedApplied: 0,
    staleCommits: 0,
}

describe('B2 recovery evidence accounting', () => {
    test('accepts exact permission SQLSTATE through Bun wrappers without treating unknown failures as denials', () => {
        for (const error of [
            { code: '42501' },
            { code: 'ERR_POSTGRES_SERVER_ERROR', errno: '42501' },
            new Error('wrapped', { cause: { errno: '42501' } }),
            { cause: { cause: { code: '42501' } } },
        ])
            expect(monitoringB2PermissionDenied(error)).toBe(true)
        const cyclic: { cause?: unknown } = {}
        cyclic.cause = cyclic
        for (const error of [
            undefined,
            null,
            '42501',
            new Error('permission denied 42501'),
            { code: 'ERR_POSTGRES_SERVER_ERROR' },
            { code: '08006', errno: '08006' },
            { errno: 42501 },
            { cause: { cause: { cause: { cause: { code: '42501' } } } } },
            cyclic,
        ])
            expect(monitoringB2PermissionDenied(error)).toBe(false)
    })

    test('requires every durable replay and actual network count at the inclusive time budget', () => {
        expect(assessMonitoringB2Replay(complete)).toEqual([])
        for (const key of [
            'rawDurableRecords',
            'distinctRawPolicies',
            'nativeHeads',
            'distinctNativeHosts',
            'replayedResults',
            'observations',
        ] as const)
            expect(assessMonitoringB2Replay({ ...complete, [key]: 999 })).toContain(
                key + ':expected-1000',
            )
        expect(assessMonitoringB2Replay({ ...complete, replayMs: 30000.01 })).toContain(
            'replay-exceeded-30-seconds',
        )
    })

    test('reservations or rejected receipts cannot substitute for applied original observations', () => {
        expect(assessMonitoringB2Replay({ ...complete, nativeHeads: 0 })).toContain(
            'nativeHeads:expected-1000',
        )
        expect(
            assessMonitoringB2Replay({ ...complete, observations: 999, rejectedApplied: 1 }),
        ).toContain('rejected-receipt-counted-applied')
        expect(assessMonitoringB2Replay({ ...complete, pending: 1 })).toContain(
            'durable-backlog-not-drained',
        )
    })

    test('unknown commit timing and nonfinite or negative observations fail closed', () => {
        expect(assessMonitoringB2Replay({ ...complete, staleCommits: null })).toContain(
            'expired-lease-commit-not-excluded',
        )
        for (const value of [Number.NaN, Number.POSITIVE_INFINITY, -1])
            expect(assessMonitoringB2Replay({ ...complete, replayMs: value })).toContain(
                'replayMs:invalid-measurement',
            )
    })

    test('unprepared contexts cannot execute operations or produce application evidence', async () => {
        let operations = 0
        const unexpected = () => {
            operations++
            throw new Error('unexpected fixture operation')
        }
        const context: MonitoringB2RecoveryContext = {
            database: undefined as never,
            state: () => undefined,
            authority: () => undefined,
            outboxPath: unexpected,
            heads: unexpected,
            start: unexpected,
            stopNative: unexpected,
            restartNative: unexpected,
            controllerAvailable: unexpected,
            fastProbes: unexpected,
            quiesce: unexpected,
        }
        const result = await runMonitoringB2Recovery(context, 'replay')
        expect(result.passed).toBe(false)
        expect(result.evidence).toEqual({})
        expect(result.failures).toEqual(['owned-prepared-B2-appliance-required'])
        expect(operations).toBe(0)
    })
})
