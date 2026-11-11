// oxlint-disable no-await-in-loop -- Each drift check must finish before its telemetry is asserted.
import { expect, spyOn, test } from 'bun:test'
import { createRuntimeReconciler } from '@/server/Foundation/runtime-reconciler.server.ts'
import { workerObservability } from '@/server/Foundation/worker-observability.ts'
import {
    admitDatabaseTransaction,
    withBackgroundDatabaseContext,
} from '@/db/background-admission.ts'
import { DATABASE_CONNECTION_BUDGET } from '@/config/database.config.ts'

const read = () =>
    workerObservability.snapshot().workers.find((worker) => worker.id === 'proxy_reconciliation')!

test('an independent reconciler outlives its first discovery owner while retaining shared SQL admission', async () => {
    const owner = new AbortController()
    let loads = 0
    let drifts = 0
    const reconciler = createRuntimeReconciler(
        {
            load: () => admitDatabaseTransaction(async () => ++loads),
            revision: () => 'revision',
            apply: async () => true,
            drift: {
                check: () =>
                    admitDatabaseTransaction(async () => {
                        drifts++
                        return false
                    }),
            },
        },
        { name: 'independent', timeoutMs: 100, stopTimeoutMs: 100 },
    )
    const occupied = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    let admitted = 0
    let held: Promise<void>[] = []
    try {
        expect(await withBackgroundDatabaseContext(() => reconciler(), owner.signal)).toBe(
            'applied',
        )
        owner.abort()
        held = Array.from({ length: DATABASE_CONNECTION_BUDGET.backgroundTransactions }, () =>
            withBackgroundDatabaseContext(() =>
                admitDatabaseTransaction(async () => {
                    if (++admitted === DATABASE_CONNECTION_BUDGET.backgroundTransactions)
                        occupied.resolve()
                    await release.promise
                }),
            ),
        )
        await occupied.promise
        const before = loads
        const reconciling = reconciler()
        const drifting = withBackgroundDatabaseContext(() => reconciler.checkDrift(), owner.signal)
        await Bun.sleep(0)
        expect(loads).toBe(before)
        expect(drifts).toBe(0)
        release.resolve()
        expect(await reconciling).toBe('applied')
        await drifting
        expect(loads).toBe(before + 2)
        expect(drifts).toBe(1)
    } finally {
        release.resolve()
        await Promise.allSettled(held)
        await reconciler.stop()
    }
})

for (const recovery of ['new-revision', 'manual-retry'] as const) {
    test(
        'cached rejected revision stays degraded until ' + recovery + ' actually applies',
        async () => {
            workerObservability.reset()
            const warnings = spyOn(console, 'warn').mockImplementation(() => undefined)
            let revision = 'rejected'
            let accepted = false
            let applies = 0
            let failLoad = false
            const reconciler = createRuntimeReconciler(
                {
                    load: async () => {
                        if (failLoad) throw new Error('fixture load failure')
                        return { revision }
                    },
                    revision: (snapshot) => snapshot.revision,
                    apply: async () => {
                        applies++
                        return (
                            accepted || {
                                applied: false as const,
                                retryable: false,
                                code: 'invalid_configuration',
                                status: 400,
                            }
                        )
                    },
                    drift: { check: async () => true },
                },
                {
                    workerId: 'proxy_reconciliation',
                    name: 'fixture',
                    timeoutMs: 100,
                    stopTimeoutMs: 100,
                },
            )
            try {
                expect(await reconciler()).toBe('pending')
                const initial = read()
                expect(initial.state).toBe('degraded')
                expect(initial.degradedLanes).toEqual(['execution'])
                expect(initial.counters).toMatchObject({
                    started: 1,
                    failure: 1,
                    success: 0,
                    cancelled: 0,
                })
                expect(initial.duration.count).toBe(1)
                for (let index = 0; index < 3; index++) {
                    await reconciler.checkDrift()
                    await Bun.sleep(0)
                    const skipped = read()
                    expect(skipped.state).toBe('degraded')
                    expect(skipped.degradedLanes).toEqual(['execution'])
                    expect(skipped.running).toBe(0)
                    expect(skipped.counters).toMatchObject({
                        started: 2 * index + 3,
                        failure: 1,
                        deferred: 2 * (index + 1),
                        cancelled: 0,
                        success: 0,
                    })
                    expect(skipped.duration.count).toBe(2 * index + 3)
                    expect(skipped.lastSuccessAt).toBeNull()
                    expect(skipped.lastFailureAt).toBe(initial.lastFailureAt)
                }
                expect(applies).toBe(1)
                if (recovery === 'new-revision') {
                    failLoad = true
                    await reconciler.checkDrift()
                    await Bun.sleep(0)
                    expect(read().counters.failure).toBe(2)
                    expect(read().duration.count).toBe(9)
                    failLoad = false
                }
                accepted = true
                if (recovery === 'new-revision') {
                    revision = 'accepted'
                    await reconciler.checkDrift()
                    await Bun.sleep(0)
                } else expect(await reconciler()).toBe('applied')
                const recovered = read()
                expect(applies).toBe(2)
                expect(recovered.state).toBe('idle')
                expect(recovered.degradedLanes).toEqual([])
                expect(recovered.running).toBe(0)
                expect(recovered.counters.success).toBe(1)
                expect(recovered.counters.failure).toBe(recovery === 'new-revision' ? 2 : 1)
                expect(recovered.lastSuccessAt).not.toBeNull()
            } finally {
                await reconciler.stop()
                warnings.mockRestore()
                workerObservability.reset()
            }
        },
    )
}

test('drift outages degrade their own lane and successful drift does not recover rejected applies', async () => {
    workerObservability.reset()
    const warnings = spyOn(console, 'warn').mockImplementation(() => undefined)
    let unavailable = false
    let rejected = false
    const reconciler = createRuntimeReconciler(
        {
            load: async () => 'revision',
            revision: (snapshot) => snapshot,
            apply: async () =>
                rejected
                    ? { applied: false, retryable: false, code: 'invalid', status: 400 }
                    : true,
            drift: {
                check: async () => {
                    if (unavailable) throw new Error('private observation payload')
                    return false
                },
            },
        },
        { workerId: 'proxy_reconciliation', name: 'fixture', timeoutMs: 100, stopTimeoutMs: 100 },
    )
    try {
        expect(await reconciler()).toBe('applied')
        unavailable = true
        await reconciler.checkDrift()
        await reconciler.checkDrift()
        expect(read()).toMatchObject({
            state: 'degraded',
            degradedLanes: ['collection'],
            counters: { failure: 2, success: 1 },
        })
        rejected = true
        expect(await reconciler()).toBe('pending')
        unavailable = false
        await reconciler.checkDrift()
        expect(read()).toMatchObject({
            state: 'degraded',
            degradedLanes: ['execution'],
            counters: { failure: 3, success: 1, deferred: 1 },
        })
        rejected = false
        expect(await reconciler()).toBe('applied')
        expect(read()).toMatchObject({ state: 'idle', degradedLanes: [] })
    } finally {
        await reconciler.stop()
        warnings.mockRestore()
        workerObservability.reset()
    }
})
