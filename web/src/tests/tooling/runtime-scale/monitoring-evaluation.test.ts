import { describe, expect, test } from 'bun:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { isAbsolute, join, relative, resolve } from 'node:path'
import { createMonitoringEvaluationOutbox } from '../../../../../scripts/runtime-scale/monitoring-evaluation-outbox.ts'
import {
    assessMonitoringMigration,
    monitoringEvaluationContract,
    monitoringResultIsCurrent,
    monitoringRevocationCounterexample,
} from '../../../../../scripts/runtime-scale/monitoring-evaluation.ts'
import type {
    MonitoringEvaluationEvidence,
    MonitoringEvaluationResult,
    MonitoringEvaluationSample,
} from '../../../../../scripts/runtime-scale/Types/monitoring-evaluation.types.ts'

function result(overrides: Partial<MonitoringEvaluationResult> = {}): MonitoringEvaluationResult {
    return {
        storeId: 'store-1',
        controllerEpoch: 'epoch-1',
        policyId: 'policy-1',
        policyVersion: 1,
        grantVersion: 1,
        revision: 'revision-1',
        hostProof: 'proof-1',
        leaseToken: 'lease-1',
        leaseExpiresAtMs: 45000,
        enabled: true,
        paused: false,
        attemptId: 'attempt-1',
        checkedAtMs: 1000,
        outcome: 'healthy',
        ...overrides,
    }
}

function evidence(profile: MonitoringEvaluationEvidence['profile']): MonitoringEvaluationEvidence {
    const sample: MonitoringEvaluationSample = {
        sourceMatched: true,
        wholeAppliance: true,
        durationMs: 120000,
        policies: 1000,
        intervalSeconds: 60,
        exactPolicyCoverage: true,
        dueToStartP99Ms: 20000,
        dueToCommitMaxMs: 30000,
        ssrP99Ms: 100,
        proxyP99Ms: 100,
        proxyErrors: 0,
        backlogGrowth: 0,
        baselineCpu: 1,
        candidateCpu: 1,
        baselineRssMiB: 500,
        candidateRssMiB: 520,
        extraDatabaseConnections: 2,
    }
    return {
        profile,
        samples: [sample, sample, sample],
        confirmedBunStopMs: 25000,
        distinctDurableStartsAfterStop: 128,
        retainedNativeIdentity: true,
        replayedResults: 1000,
        replayMs: 30000,
        securityAndRestorePassed: true,
        freshAuthorityPassed: true,
        staleCommits: 0,
        duplicateObservations: 0,
        duplicateIncidents: 0,
        outboxPressureVisible: true,
        measuredRelativeVariation: 0.01,
    }
}

describe('monitoring ownership investigation gates', () => {
    test('an unexpired cached policy admits after durable revocation; fresh authority denies', () => {
        expect(monitoringRevocationCounterexample()).toMatchObject({
            cachedAuthorityAdmits: true,
            freshAuthorityAdmits: false,
            remainingStaleWindowMs: 44000,
        })
    })
    test('edit, disable, grant revocation, host proof, global revision, epoch and restore fence results', () => {
        const original = result()
        expect(monitoringResultIsCurrent(original, original, 2000)).toBe(true)
        for (const changed of [
            { enabled: false },
            { paused: true },
            { policyVersion: 2 },
            { grantVersion: 2 },
            { revision: 'revision-2' },
            { hostProof: 'proof-2' },
            { controllerEpoch: 'epoch-2' },
            { storeId: 'restored-store' },
            { leaseToken: 'replacement-lease' },
            { leaseExpiresAtMs: 44000 },
        ])
            expect(monitoringResultIsCurrent(original, { ...original, ...changed }, 2000)).toBe(
                false,
            )
        for (const now of [45000, 45001, NaN, Infinity, -1])
            expect(monitoringResultIsCurrent(original, original, now)).toBe(false)
        expect(monitoringResultIsCurrent(result({ checkedAtMs: 2001 }), original, 2000)).toBe(false)
        expect(monitoringResultIsCurrent(result({ checkedAtMs: -1 }), original, 2000)).toBe(false)
    })
    test('one thousand retained raw results do not override the existing 45-second final-commit lease', () => {
        expect(monitoringEvaluationContract.retentionMs).toBeGreaterThan(
            monitoringEvaluationContract.leaseMs,
        )
        expect(monitoringResultIsCurrent(result(), result(), 60000)).toBe(false)
    })
    test('partial fixtures, no fault, 64 old requests and missing profiles cannot approve migration', () => {
        expect(assessMonitoringMigration([]).recommendation).toBe('keep-hybrid')
        const complete = [evidence('mixed'), evidence('all-3s-timeout')]
        expect(assessMonitoringMigration(complete)).toEqual({
            recommendation: 'eligible-for-separate-design-review',
            efficiencyJustified: false,
            failures: [],
        })
        for (const change of [
            { samples: [] },
            { samples: complete[0]!.samples.slice(0, 2) },
            { confirmedBunStopMs: 24999 },
            { distinctDurableStartsAfterStop: 64 },
            { retainedNativeIdentity: false },
            { replayedResults: 999 },
            { replayMs: 30001 },
            { freshAuthorityPassed: false },
            { securityAndRestorePassed: false },
            { staleCommits: 1 },
            { duplicateObservations: 1 },
            { duplicateIncidents: 1 },
            { outboxPressureVisible: false },
            { replayMs: NaN },
            { confirmedBunStopMs: NaN },
            { measuredRelativeVariation: Infinity },
        ])
            expect(
                assessMonitoringMigration([{ ...complete[0]!, ...change }, complete[1]!])
                    .recommendation,
            ).toBe('keep-hybrid')
        for (const change of [
            { sourceMatched: false },
            { wholeAppliance: false },
            { durationMs: 119999 },
            { policies: 100 },
            { intervalSeconds: 120 },
            { exactPolicyCoverage: false },
            { backlogGrowth: 1 },
            { proxyErrors: 1 },
            { dueToStartP99Ms: 55001 },
            { dueToCommitMaxMs: 60001 },
            { ssrP99Ms: 251 },
            { proxyP99Ms: 251 },
            { candidateCpu: 1.11 },
            { candidateRssMiB: 533 },
            { extraDatabaseConnections: 3 },
            { candidateCpu: NaN },
        ]) {
            const item = {
                ...complete[0]!,
                samples: complete[0]!.samples.map((sample) => Object.assign({}, sample, change)),
            }
            expect(assessMonitoringMigration([item, complete[1]!]).recommendation).toBe(
                'keep-hybrid',
            )
        }
    })
    test('efficiency justification requires at least twenty percent and twice observed variation in every pair', () => {
        const complete = [evidence('mixed'), evidence('all-3s-timeout')]
        const efficient = complete.map((entry) =>
            Object.assign({}, entry, {
                samples: entry.samples.map((sample) =>
                    Object.assign({}, sample, { candidateCpu: 0.75 }),
                ),
            }),
        )
        expect(assessMonitoringMigration(efficient).efficiencyJustified).toBe(true)
        expect(
            assessMonitoringMigration(
                efficient.map((entry) =>
                    Object.assign({}, entry, { measuredRelativeVariation: 0.13 }),
                ),
            ).efficiencyJustified,
        ).toBe(false)
    })
    test('duplicate and unexpected evidence profiles fail closed even when the first two runs pass', () => {
        const mixed = evidence('mixed')
        const timeout = evidence('all-3s-timeout')
        const failedDuplicate = { ...evidence('mixed'), securityAndRestorePassed: false }
        const unexpected = evidence('mixed')
        Object.defineProperty(unexpected, 'profile', { value: 'unexpected-profile' })
        for (const entries of [
            [mixed, timeout, failedDuplicate],
            [mixed, timeout, evidence('mixed')],
            [mixed, mixed],
            [mixed, timeout, unexpected],
            [unexpected, timeout],
        ]) {
            const assessment = assessMonitoringMigration(entries)
            expect(assessment.recommendation).toBe('keep-hybrid')
            expect(assessment.efficiencyJustified).toBe(false)
            expect(assessment.failures).toContain(
                'Exactly one evidence entry per reference profile is required',
            )
        }
        expect(assessMonitoringMigration([timeout, mixed]).recommendation).toBe(
            'eligible-for-separate-design-review',
        )
    })
})

describe('bounded durable raw-result experiment', () => {
    test('generation gap requires full resync, old publication cannot resurrect and restore requires reactivation', () => {
        const store = createMonitoringEvaluationOutbox(':memory:', 'store-1')
        try {
            expect(store.append(result(), 1000)).toBe('paused')
            expect(store.publish('store-1', 1, 'resync', true)).toBe(true)
            expect(store.publish('store-1', 3, 'delta')).toBe(false)
            expect(store.status()).toMatchObject({ paused: 1, resync_required: 1 })
            expect(store.append(result(), 1000)).toBe('paused')
            expect(store.publish('store-1', 3, 'resync', true)).toBe(true)
            expect(store.publish('store-1', 2, 'resync', true)).toBe(false)
            expect(store.append(result(), 1000)).toBe('recorded')
            expect(() => store.reset('store-1')).toThrow('new identity')
            store.reset('store-2')
            expect(store.status()).toMatchObject({
                store_id: 'store-2',
                generation: 0,
                paused: 1,
                records: 0,
                gaps: 1,
            })
            expect(store.publish('store-1', 999, 'resync', true)).toBe(false)
            expect(store.publish('store-2', 1, 'resync')).toBe(true)
            expect(store.append(result({ storeId: 'store-2' }), 1000)).toBe('paused')
            expect(store.publish('store-2', 2, 'resync', true)).toBe(true)
            expect(store.append(result(), 1000)).toBe('paused')
            expect(
                store.append(result({ storeId: 'store-2', attemptId: 'new-attempt' }), 1000),
            ).toBe('recorded')
        } finally {
            store.close()
        }
    })
    test('commit-before-ACK replay retains IDs; mismatched ACK and duplicate payload cannot discard evidence', () => {
        const store = createMonitoringEvaluationOutbox(':memory:', 'store-1')
        try {
            store.publish('store-1', 1, 'resync', true)
            expect(store.append(result(), 1000)).toBe('recorded')
            expect(store.append(result(), 1000)).toBe('duplicate')
            expect(store.append(result({ outcome: 'unhealthy' }), 1000)).toBe('conflict')
            const [row] = store.read()
            expect(store.read()).toEqual([row!])
            expect(
                store.acknowledge([
                    { sequence: row!.sequence, attemptId: 'other', storeId: 'store-1' },
                ]),
            ).toBe(0)
            expect(
                store.acknowledge([
                    { sequence: row!.sequence, attemptId: row!.attempt_id, storeId: 'old-store' },
                ]),
            ).toBe(0)
            expect(
                store.acknowledge([
                    { sequence: row!.sequence, attemptId: row!.attempt_id, storeId: 'store-1' },
                ]),
            ).toBe(1)
            expect(
                store.acknowledge([
                    { sequence: row!.sequence, attemptId: row!.attempt_id, storeId: 'store-1' },
                ]),
            ).toBe(0)
        } finally {
            store.close()
        }
    })
    test('outbox limits stop admission visibly without evicting old evidence or silently extending retention', () => {
        const store = createMonitoringEvaluationOutbox(':memory:', 'store-1')
        try {
            store.publish('store-1', 1, 'resync', true)
            expect(store.append(result({ hostProof: 'x'.repeat(4096) }), 1000)).toBe('oversize')
            for (let index = 0; index < 4096; index++)
                expect(store.append(result({ attemptId: `attempt-${index}` }), 1000)).toBe(
                    'recorded',
                )
            expect(store.status().records).toBe(4096)
            expect(store.status().bytes).toBeLessThanOrEqual(16 * 1024 * 1024)
            expect(store.append(result({ attemptId: 'over-limit' }), 1000)).toBe('pressure')
            expect(store.append(result({ attemptId: 'after-retention' }), 121000)).toBe('expired')
            expect(store.status().records).toBe(4096)
            expect(store.read()).toHaveLength(64)
        } finally {
            store.close()
        }
    })
    test('close/reopen retains raw observations and rejects a replaced store identity', async () => {
        const directory = await mkdtemp(join(tmpdir(), 'rentnerproxy-monitoring-evaluation-test-'))
        const cleanupRelative = relative(resolve(tmpdir()), resolve(directory))
        const cleanupIsOwned =
            !isAbsolute(cleanupRelative) &&
            !cleanupRelative.startsWith('..') &&
            cleanupRelative.startsWith('rentnerproxy-monitoring-evaluation-test-')
        const path = join(directory, 'outbox.sqlite')
        let store = createMonitoringEvaluationOutbox(path, 'store-1')
        try {
            store.publish('store-1', 1, 'resync', true)
            store.append(result(), 1000)
            const before = store.read()
            store.close()
            expect(() => createMonitoringEvaluationOutbox(path, 'store-2')).toThrow(
                'identity mismatch',
            )
            store = createMonitoringEvaluationOutbox(path, 'store-1')
            expect(store.read()).toEqual(before)
            expect(store.status()).toMatchObject({ generation: 1, records: 1 })
        } finally {
            store.close()
            if (cleanupIsOwned) await rm(directory, { recursive: true, force: true })
        }
    })
})
