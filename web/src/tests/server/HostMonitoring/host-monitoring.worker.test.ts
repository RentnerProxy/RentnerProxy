import { describe, expect, test } from 'bun:test'
import { createHostMonitoringWorker } from '@/server/HostMonitoring/host-monitoring.worker.ts'
import type { HostMonitoringWorkerDependencies } from '@/server/HostMonitoring/Types/host-monitoring-worker.types.ts'
import { HOST_MONITORING_LIMITS } from '@/config/host-monitoring.config.ts'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test('monitoring completion publishes only after accepted durable writes', async () => {
    const stdout = await runBunScript(
        `
        import { mock } from 'bun:test'
        let probe
        let workerDependencies
        let complete
        let publications = 0
        mock.module('./server/Auth/Core/database.server.ts', () => ({ getAuthDatabase: () => { throw new Error('Unexpected database access') } }))
        mock.module('./server/Controller/domain-diagnostics.server.ts', () => ({ inspectControllerDomain: async () => {} }))
        mock.module('./server/RuntimeDiagnostics/domain-diagnostics-data.ts', () => ({ readHostMonitoringContextInTransaction: async () => {} }))
        mock.module('./server/HostMonitoring/host-monitoring.probe.ts', () => ({ createHostMonitoringProbe: dependencies => { probe = dependencies; return async () => {} } }))
        mock.module('./server/HostMonitoring/host-monitoring.worker.ts', () => ({ createHostMonitoringWorker: dependencies => { workerDependencies = dependencies; return {} } }))
        mock.module('./server/HostMonitoring/host-monitoring.repository.ts', () => ({
            claimDueHostMonitoringPolicies: async () => [],
            completeHostMonitoringClaims: (...args) => complete(...args),
            readHostMonitoringClaimContexts: async claims => claims.map(() => null),
            releaseHostMonitoringClaims: async items => items.map(() => true),
            pruneHostMonitoringHistory: async () => {},
            readNextHostMonitoringAdmissionAt: async () => null,
        }))
        mock.module('./server/WebSockets/applicationChanges.ts', () => ({ publishApplicationChange: () => publications++, subscribeToApplicationChanges: () => () => {} }))
        await import('./server/HostMonitoring/host-monitoring.worker.server.ts')
        const pending = Promise.withResolvers()
        const signal = new AbortController().signal
        await workerDependencies.runClaim({}, signal)
        complete = items => {
            if (items.length !== 16 || items.some(item => item.signal !== signal)) throw new Error('Wrong completion batch')
            return pending.promise
        }
        const result = Promise.allSettled(Array.from({ length: 16 }, () =>
            probe.complete({}, {}, { availability: {} }, signal)))
        if (publications !== 0) throw new Error('Published before commit')
        pending.resolve(Array.from({ length: 16 }, (_, index) => index === 2
            ? { status: 'rejected', reason: new Error('one stale write failed') }
            : { status: 'fulfilled', value: index === 0 }))
        const outcomes = await result
        const accepted = outcomes[0].value
        const stale = outcomes[1].value
        const rejected = outcomes[2].status === 'rejected'
        complete = async () => { throw new Error('root commit failed') }
        let rootRejected = false
        try { await probe.complete({}, {}, { availability: {} }, signal) } catch { rootRejected = true }
        console.log(JSON.stringify({ accepted, stale, rejected, rootRejected, publications }))
        `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    expect(JSON.parse(stdout)).toEqual({
        accepted: true,
        stale: false,
        rejected: true,
        rootRejected: true,
        publications: 1,
    })
})

function dependencies(overrides: Partial<HostMonitoringWorkerDependencies<string>> = {}) {
    return {
        claim: async () => [],
        runClaim: async () => {},
        releaseCancelled: async () => {},
        prune: async () => {},
        unavailable() {},
        ...overrides,
    } satisfies HostMonitoringWorkerDependencies<string>
}

function scheduler() {
    const pending = new Set<() => void>()
    const delays: number[] = []
    return {
        pending,
        delays,
        schedule(callback: () => void, delay: number) {
            pending.add(callback)
            delays.push(delay)
            return () => pending.delete(callback)
        },
        fire() {
            const callback = pending.values().next().value
            if (!callback) throw new Error('No scheduled tick')
            pending.delete(callback)
            callback()
        },
    }
}

async function flush() {
    await Bun.sleep(0)
}

describe('host monitoring worker', () => {
    test('idle/disabled admission sleeps until durable next-due with one timer and wakes on changes', async () => {
        const time = scheduler()
        let current = 1_000
        let due: number | null = 2_000
        let reads = 0
        let wake: (() => void) | undefined
        let unsubscribed = false
        const worker = createHostMonitoringWorker(
            dependencies({
                claim: async () => {
                    reads++
                    return []
                },
                now: () => current,
                schedule: time.schedule,
                nextAdmissionAt: async () => due,
                subscribe: (callback) => {
                    wake = callback
                    return () => {
                        unsubscribed = true
                    }
                },
            }),
        )
        worker.start()
        await flush()
        expect(time.delays.at(-1)).toBe(1_000)
        expect(time.pending.size).toBe(1)
        due = null
        current = 2_000
        time.fire()
        await flush()
        expect(time.delays.at(-1)).toBe(HOST_MONITORING_LIMITS.idlePollMaximumMs)
        for (let index = 0; index < 10; index++) {
            current += HOST_MONITORING_LIMITS.idlePollMaximumMs
            time.fire()
            // oxlint-disable-next-line no-await-in-loop -- Advance one deterministic idle admission at a time.
            await flush()
        }
        expect(reads).toBe(12)
        expect(time.pending.size).toBe(1)
        wake!()
        expect(time.delays.at(-1)).toBe(HOST_MONITORING_LIMITS.pollIntervalMs)
        await worker.stop()
        expect(time.pending.size).toBe(0)
        expect(unsubscribed).toBe(true)
    })

    test('an invalidation racing an idle due read cannot be lost behind the long sleep', async () => {
        const time = scheduler()
        const due = Promise.withResolvers<number | null>()
        let wake: (() => void) | undefined
        const worker = createHostMonitoringWorker(
            dependencies({
                schedule: time.schedule,
                nextAdmissionAt: () => due.promise,
                subscribe: (callback) => {
                    wake = callback
                    return () => {}
                },
            }),
        )
        worker.start()
        await flush()
        wake!()
        due.resolve(null)
        await flush()
        expect(time.delays.at(-1)).toBe(HOST_MONITORING_LIMITS.pollIntervalMs)
        expect(time.pending.size).toBe(1)
        await worker.stop()
    })

    test('bounds in-flight storage plus running probes at 64 with batches of 16', async () => {
        const done = Promise.withResolvers<void>()
        const late = Promise.withResolvers<readonly string[]>()
        const batchEntered = Promise.withResolvers<void>()
        const limits: number[] = []
        let probes = 0
        const worker = createHostMonitoringWorker(
            dependencies({
                claim: async (limit) => {
                    limits.push(limit)
                    if (limits.length === 4) {
                        batchEntered.resolve()
                        return late.promise
                    }
                    return Array.from({ length: limit }, (_, index) => `${limits.length}-${index}`)
                },
                runClaim: async () => {
                    probes++
                    await done.promise
                },
            }),
        )
        const tick = worker.runOnce()
        expect(worker.runOnce()).toBe(tick)
        await batchEntered.promise
        expect(probes).toBe(48)
        expect(limits).toEqual([16, 16, 16, 16])
        expect(worker.runOnce()).toBe(tick)
        late.resolve(Array.from({ length: 16 }, (_, index) => `late-${index}`))
        await tick
        expect(probes).toBe(64)
        await worker.runOnce()
        expect(limits.length).toBe(4)
        let stopped = false
        const stopping = worker.stop().then(() => {
            stopped = true
        })
        await flush()
        expect(stopped).toBeFalse()
        done.resolve()
        await stopping
        await worker.runOnce()
        expect(probes).toBe(64)
    })

    test('refills completed slots every tick while a slow sibling remains and never drains forever', async () => {
        const timer = scheduler()
        const slow = Promise.withResolvers<void>()
        const firstFast = Promise.withResolvers<void>()
        const limits: number[] = []
        const signals = new Set<AbortSignal>()
        let claimed = 0
        let completed = 0
        const worker = createHostMonitoringWorker(
            dependencies({
                schedule: timer.schedule,
                claim: async (limit) => {
                    limits.push(limit)
                    return Array.from({ length: limit }, () => `${claimed++}`)
                },
                runClaim: async (claim, signal) => {
                    signals.add(signal)
                    if (claim === '0') await slow.promise
                    else if (Number(claim) < 64) await firstFast.promise
                    completed++
                },
            }),
        )
        worker.start()
        await worker.runOnce()
        expect(claimed).toBe(64)
        expect(timer.pending.size).toBe(1)
        timer.fire()
        await worker.runOnce()
        expect(claimed).toBe(64)
        firstFast.resolve()
        await flush()
        expect(completed).toBe(63)
        timer.fire()
        await worker.runOnce()
        expect(limits.slice(4)).toEqual([16, 16, 16, 15])
        expect(claimed).toBe(127)
        expect(completed).toBe(126)
        expect(signals.size).toBe(1)
        expect(timer.pending.size).toBe(1)
        expect(timer.delays.every((delay) => delay === 250)).toBeTrue()
        slow.resolve()
        await worker.stop()
    })

    test('releases every late claim and awaits cleanup even when one release fails', async () => {
        const admitted = Promise.withResolvers<readonly string[]>()
        const cleaned = Promise.withResolvers<void>()
        const released: string[] = []
        let probes = 0
        let warnings = 0
        const worker = createHostMonitoringWorker(
            dependencies({
                claim: () => admitted.promise,
                releaseCancelled: (claim) => {
                    released.push(claim)
                    if (claim === 'failed') throw new Error('private release failure')
                    return cleaned.promise
                },
                runClaim: async () => {
                    probes++
                },
                unavailable: () => {
                    warnings++
                },
            }),
        )
        const tick = worker.runOnce()
        const stopping = worker.stop()
        admitted.resolve(['failed', 'late', 'later'])
        await flush()
        expect(released).toEqual(['failed', 'late', 'later'])
        let stopped = false
        void stopping.then(() => {
            stopped = true
        })
        await flush()
        expect(stopped).toBeFalse()
        cleaned.resolve()
        await stopping
        await tick
        expect({ probes, warnings }).toEqual({ probes: 0, warnings: 0 })
    })

    test('a failed probe does not abort a healthy sibling and shutdown awaits adapter cleanup', async () => {
        const cleanup = Promise.withResolvers<void>()
        const aborted = Promise.withResolvers<void>()
        let healthySignal: AbortSignal | undefined
        let claims = 0
        let releases = 0
        let warnings = 0
        const worker = createHostMonitoringWorker(
            dependencies({
                claim: async () => (claims++ === 0 ? ['failed', 'healthy'] : []),
                runClaim: async (claim, signal) => {
                    if (claim === 'failed') throw new Error('secret target and credentials')
                    healthySignal = signal
                    await new Promise<void>((resolve) =>
                        signal.addEventListener(
                            'abort',
                            () => {
                                aborted.resolve()
                                resolve()
                            },
                            { once: true },
                        ),
                    )
                    await cleanup.promise
                    throw new DOMException('Cancelled', 'AbortError')
                },
                releaseCancelled: async () => {
                    releases++
                },
                unavailable: (...args) => {
                    expect(args).toEqual([])
                    warnings++
                },
            }),
        )
        await worker.runOnce()
        await flush()
        expect(healthySignal?.aborted).toBeFalse()
        expect(warnings).toBe(1)
        const stopping = worker.stop()
        await aborted.promise
        cleanup.resolve()
        await stopping
        expect(releases).toBe(0)
        expect(warnings).toBe(1)
    })

    test('prunes independently of slow admission and probes, throttles failures and awaits prune on stop', async () => {
        const timer = scheduler()
        const admitted = Promise.withResolvers<readonly string[]>()
        const pruned = Promise.withResolvers<void>()
        let clock = 100
        let sweeps = 0
        let warnings = 0
        const worker = createHostMonitoringWorker(
            dependencies({
                now: () => clock,
                schedule: timer.schedule,
                claim: () => admitted.promise,
                prune: async () => {
                    sweeps++
                    if (sweeps === 1) throw new Error('private database URI')
                    await pruned.promise
                },
                unavailable: () => {
                    warnings++
                },
            }),
        )
        worker.start()
        await flush()
        expect(sweeps).toBe(1)
        clock += HOST_MONITORING_LIMITS.retentionSweepMs - 1
        timer.fire()
        await flush()
        expect(sweeps).toBe(1)
        clock++
        timer.fire()
        await flush()
        expect(sweeps).toBe(2)
        admitted.resolve([])
        await worker.runOnce()
        let stopped = false
        const stopping = worker.stop().then(() => {
            stopped = true
        })
        await flush()
        expect(stopped).toBeFalse()
        pruned.resolve()
        await stopping
        expect(warnings).toBe(1)
    })

    test('keeps admission coalesced while independent timers run and recovers warnings after restart', async () => {
        const timer = scheduler()
        const admitted = Promise.withResolvers<readonly string[]>()
        let claims = 0
        let broken = false
        let warnings = 0
        const worker = createHostMonitoringWorker(
            dependencies({
                schedule: timer.schedule,
                claim: async () => {
                    claims++
                    if (claims === 1) return admitted.promise
                    if (broken) throw new Error('private database password')
                    return []
                },
                unavailable: () => {
                    warnings++
                },
            }),
        )
        worker.start()
        const first = worker.runOnce()
        timer.fire()
        timer.fire()
        expect(worker.runOnce()).toBe(first)
        expect(claims).toBe(1)
        admitted.resolve([])
        await first
        broken = true
        timer.fire()
        await worker.runOnce()
        timer.fire()
        await worker.runOnce()
        expect(warnings).toBe(1)
        broken = false
        timer.fire()
        await worker.runOnce()
        broken = true
        timer.fire()
        await worker.runOnce()
        expect(warnings).toBe(2)
        await worker.stop()
        expect(timer.pending.size).toBe(0)
        broken = false
        worker.start()
        await worker.runOnce()
        await worker.stop()
        expect(claims).toBe(6)
    })
})
