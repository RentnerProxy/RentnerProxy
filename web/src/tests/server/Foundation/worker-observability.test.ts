import { describe, expect, test } from 'bun:test'
import {
    backgroundWorkerIds,
    backgroundWorkerMetricsConfig,
} from '@/config/background-workers.config.ts'
import { createWorkerObservability } from '@/server/Foundation/worker-observability.ts'

test('event-loop measurements stay unavailable until sampled and cannot imply worker success', () => {
    const metrics = createWorkerObservability({ now: () => 1_000 })
    expect(metrics.snapshot().eventLoopLag.sampledAt).toBeNull()
    metrics.eventLoopDelay(150)
    metrics.eventLoopDelay(70_000)
    metrics.eventLoopDelay(NaN)
    const sample = metrics.snapshot()
    expect(sample.eventLoopLag).toEqual({
        sampledAt: 1_000,
        count: 2,
        maximumMs: 70_000,
        buckets: [0, 0, 1, 0, 0, 1],
        overflowed: false,
    })
    expect(sample.workers.every((worker) => worker.lastSuccessAt === null)).toBe(true)
    sample.eventLoopLag.buckets.fill(900)
    expect(metrics.snapshot().eventLoopLag.buckets).toEqual([0, 0, 1, 0, 0, 1])
    metrics.reset()
    expect(metrics.snapshot().eventLoopLag.sampledAt).toBeNull()
})

test('successful bookkeeping cannot hide a delivery outage or invent recovery', () => {
    const metrics = createWorkerObservability()
    metrics.begin('notifications', 'delivery')('failure')
    metrics.begin('notifications', 'persistence')('success')
    metrics.begin('notifications', 'admission')('deferred')
    let worker = metrics.snapshot().workers.find((item) => item.id === 'notifications')!
    expect(worker.state).toBe('degraded')
    expect(worker.degradedLanes).toEqual(['delivery'])
    worker.degradedLanes.length = 0
    expect(
        metrics.snapshot().workers.find((item) => item.id === 'notifications')?.degradedLanes,
    ).toEqual(['delivery'])
    metrics.begin('notifications', 'delivery')('success')
    worker = metrics.snapshot().workers.find((item) => item.id === 'notifications')!
    expect(worker.state).toBe('idle')
    expect(worker.degradedLanes).toEqual([])
})
import type {
    BackgroundWorkerId,
    BackgroundWorkerState,
    WorkerCountEvent,
    WorkerOutcome,
    WorkerSample,
} from '@/server/Foundation/Types/worker-observability.types.ts'

function setup() {
    let time = 100
    const metrics = createWorkerObservability({ now: () => time })
    return {
        metrics,
        setTime: (value: number) => {
            time = value
        },
        read: () => metrics.snapshot().workers[0]!,
    }
}

describe('bounded worker observability', () => {
    test('starts unknown and records actual completion independently from observation', () => {
        const { metrics, read, setTime } = setup()
        expect(read().lastSuccessAt).toBeNull()
        expect(read().stateChangedAt).toBeNull()
        const finish = metrics.begin('proxy_reconciliation')
        setTime(10_000)
        expect(read().running).toBe(1)
        expect(read().lastFinishedAt).toBeNull()
        expect(read().counters.success).toBe(0)
        finish('success')
        finish('failure')
        expect(read()).toMatchObject({
            state: 'idle',
            running: 0,
            lastSuccessAt: 10_000,
            counters: { started: 1, success: 1, failure: 0 },
        })
        expect(read().duration).toMatchObject({
            count: 1,
            sumMs: 9_900,
            buckets: [0, 0, 0, 1, 0, 0],
        })
    })

    test('counts overlapping work accurately and recovers after failure', () => {
        const { metrics, read } = setup()
        const first = metrics.begin('proxy_reconciliation')
        const second = metrics.begin('proxy_reconciliation')
        first('failure')
        expect(read()).toMatchObject({ running: 1, state: 'degraded' })
        second('success')
        expect(read()).toMatchObject({
            running: 0,
            state: 'idle',
            counters: { failure: 1, success: 1 },
        })
        const third = metrics.begin('proxy_reconciliation')
        third('deferred')
        const fourth = metrics.begin('proxy_reconciliation')
        fourth('cancelled')
        expect(read().counters).toMatchObject({ deferred: 1, cancelled: 1 })
    })

    test.each(['maintenance_paused', 'stopping', 'stopped', 'failed'] as const)(
        'late completion preserves %s',
        (state) => {
            const { metrics, read } = setup()
            const finish = metrics.begin('proxy_reconciliation')
            metrics.state('proxy_reconciliation', state)
            finish('success')
            expect(read()).toMatchObject({ state, running: 0, counters: { success: 1 } })
        },
    )

    test('samples expose only fixed bounded numeric evidence and snapshots are detached', () => {
        const { metrics, read } = setup()
        metrics.sample('proxy_reconciliation', {
            backlog: 12.9,
            concurrency: Infinity,
            oldestPendingAt: -1,
            nextDueAt: 500,
            secret: 'password',
        } as WorkerSample)
        expect(read()).toMatchObject({
            backlog: 12,
            concurrency: null,
            oldestPendingAt: null,
            nextDueAt: 500,
        })
        const snapshot = metrics.snapshot()
        snapshot.workers[0]!.counters.success = 9
        snapshot.workers[0]!.duration.buckets[0] = 9
        expect(read().counters.success).toBe(0)
        expect(read().duration.buckets[0]).toBe(0)
        expect(JSON.stringify(snapshot)).not.toContain('password')
    })

    test('saturates accumulated evidence explicitly and resets without old completions leaking', () => {
        const { metrics, read, setTime } = setup()
        const maximum = backgroundWorkerMetricsConfig.maximumInteger
        setTime(0)
        const first = metrics.begin('proxy_reconciliation')
        setTime(maximum)
        first('success')
        setTime(0)
        const second = metrics.begin('proxy_reconciliation')
        setTime(maximum)
        second('failure')
        expect(read().duration.sumMs).toBe(maximum)
        expect(read().overflowed).toBe(true)
        const pending = metrics.begin('proxy_reconciliation')
        metrics.reset()
        pending('success')
        expect(metrics.snapshot().epoch).toEqual({ startedAt: maximum, sequence: 1 })
        expect(read()).toMatchObject({
            state: 'unknown',
            running: 0,
            overflowed: false,
            lastSuccessAt: null,
            counters: { started: 0 },
        })
    })

    test('rejects hostile labels and contains a throwing logger', () => {
        const events: unknown[] = []
        const metrics = createWorkerObservability({
            log: (event) => {
                events.push(event)
                throw new Error('private')
            },
        })
        metrics.state('proxy_reconciliation', 'starting')
        metrics.state('proxy_reconciliation', 'starting')
        metrics.state('proxy_reconciliation', 'private' as BackgroundWorkerState)
        metrics.state('__proto__' as BackgroundWorkerId, 'running')
        metrics.begin('private' as BackgroundWorkerId)('success')
        metrics.count('proxy_reconciliation', 'private' as WorkerCountEvent)
        const finish = metrics.begin('proxy_reconciliation')
        finish('private' as WorkerOutcome)
        expect(metrics.snapshot().workers[0]!.running).toBe(1)
        finish('success')
        expect(events).toEqual([
            { workerId: 'proxy_reconciliation', previousState: 'unknown', state: 'starting' },
            { workerId: 'proxy_reconciliation', previousState: 'starting', state: 'running' },
            { workerId: 'proxy_reconciliation', previousState: 'running', state: 'idle' },
        ])
        expect(metrics.snapshot().workers.map(({ id }) => id)).toEqual([...backgroundWorkerIds])
        expect(JSON.stringify(metrics.snapshot())).not.toContain('private')
    })

    test('bounds all owners, timestamps, samples and event counters within a small JSON snapshot', () => {
        const maximum = backgroundWorkerMetricsConfig.maximumInteger
        let time = 0
        const metrics = createWorkerObservability({ now: () => time })
        for (const id of backgroundWorkerIds) {
            time = 0
            const finish = metrics.begin(id)
            time = maximum * 2
            finish('success')
            metrics.sample(id, {
                backlog: maximum * 2,
                concurrency: maximum * 2,
                oldestPendingAt: maximum * 2,
                nextDueAt: maximum * 2,
            })
            for (const event of [
                'retry',
                'exhausted',
                'admissionRejected',
                'leaseRecovered',
                'drainTimeout',
            ] as const)
                metrics.count(id, event)
        }
        const snapshot = metrics.snapshot()
        expect(snapshot.workers).toHaveLength(9)
        for (const worker of snapshot.workers) {
            expect(worker.backlog).toBe(maximum)
            expect(worker.lastFinishedAt).toBe(maximum)
            expect(worker.duration.buckets.at(-1)).toBe(1)
            expect(worker.counters).toMatchObject({
                retry: 1,
                exhausted: 1,
                admissionRejected: 1,
                leaseRecovered: 1,
                drainTimeout: 1,
            })
        }
        expect(Buffer.byteLength(JSON.stringify(snapshot))).toBeLessThan(64 * 1024)
        expect(JSON.stringify(snapshot)).not.toContain('Infinity')
    })
})
