import { afterEach, beforeEach, describe, expect, test } from 'bun:test'

import {
    createWorkerLifecycleSupervisor,
    drainWorker,
    settleWorkerTasks,
} from '@/server/Foundation/worker-lifecycle.server.ts'
import { workerObservability } from '@/server/Foundation/worker-observability.ts'
import type { WorkerLifecycle } from '@/server/Foundation/Types/worker-lifecycle.types.ts'
import { WorkerDrainTimeoutError } from '@/server/Foundation/Types/worker-lifecycle.types.ts'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

let startWorkerLifecycle: ReturnType<typeof createWorkerLifecycleSupervisor>['startWorkerLifecycle']
beforeEach(() => {
    startWorkerLifecycle = createWorkerLifecycleSupervisor().startWorkerLifecycle
})

const existingListeners = new Set(process.listeners('rentnerproxy:shutdown'))
const existingMaintenanceListeners = new Set(process.listeners('rentnerproxy:maintenance'))

afterEach(() => {
    for (const listener of process.listeners('rentnerproxy:shutdown')) {
        if (!existingListeners.has(listener))
            process.removeListener('rentnerproxy:shutdown', listener)
    }
    for (const listener of process.listeners('rentnerproxy:maintenance')) {
        if (!existingMaintenanceListeners.has(listener))
            process.removeListener('rentnerproxy:maintenance', listener)
    }
})

describe('worker lifecycle', () => {
    for (const event of ['shutdown', 'maintenance'] as const) {
        for (const outcome of [
            'success',
            'start failure',
            'cleanup failure',
            'pending early stop',
            'drain timeout',
            'nested timeout',
        ] as const) {
            test(`${event} owns deferred start settlement and late cleanup after ${outcome}`, async () => {
                const output = await runBunScript(
                    `
                    import { mock } from 'bun:test'
                    mock.module('@tanstack/react-start/server-only', () => ({}))
                    mock.module('./server/Backups/restore-maintenance-status.server.ts', () => ({ readRestoreMaintenanceObservation: async () => ({ maintenance: null, restoreResults: [] }) }))
                    mock.module('./server/Foundation/event-loop-observation.ts', () => ({ startEventLoopObservation() {} }))
                    const { createWorkerLifecycleSupervisor } = await import('./server/Foundation/worker-lifecycle.server.ts')
                    const { WorkerDrainTimeoutError } = await import('./server/Foundation/Types/worker-lifecycle.types.ts')
                    const { workerObservability } = await import('./server/Foundation/worker-observability.ts')
                    const timers = new Map()
                    globalThis.setTimeout = (callback, milliseconds) => { const timer = {}; timers.set(timer, { callback, milliseconds }); return timer }
                    globalThis.clearTimeout = timer => timers.delete(timer)
                    const fire = milliseconds => { const entry = [...timers].find(([, value]) => value.milliseconds === milliseconds); if (!entry) throw new Error('Missing deadline'); timers.delete(entry[0]); entry[1].callback() }
                    const flush = async () => { for (let index = 0; index < 50; index++) await Promise.resolve() }
                    const started = Promise.withResolvers(), entered = Promise.withResolvers(), cleanup = Promise.withResolvers()
                    const lateStart = Promise.withResolvers()
                    const earlyCleanup = Promise.withResolvers()
                    void cleanup.promise.catch(() => {})
                    const failure = new Error('late start/cleanup failure')
                    let writes = false, stops = 0, checkpoint = 'pending'
                    const supervisor = createWorkerLifecycleSupervisor()
                    const initializing = supervisor.startWorkerLifecycle(async () => ({
                        start: async () => { entered.resolve(); await started.promise; writes = true; if ('${outcome}' === 'start failure') throw failure },
                        stop: async () => { stops++; if (stops === 1) { if ('${outcome}' === 'pending early stop') await earlyCleanup.promise; return }; await cleanup.promise; writes = false },
                    }), 'npm_import_cleanup', { startupTimeoutMs: 5, drainTimeoutMs: 50 })
                    void initializing.catch(() => {})
                    await entered.promise
                    let stopping
                    if ('${event}' === 'maintenance') stopping = supervisor.stopWorkersForRestoreMaintenance()
                    else { const pending = []; process.emit('rentnerproxy:shutdown', pending); stopping = Promise.all(pending) }
                    void stopping.then(() => { checkpoint = 'success' }, () => { checkpoint = 'failure' })
                    await flush()
                    if ('${outcome}' === 'nested timeout') started.reject(new WorkerDrainTimeoutError(lateStart.promise.then(() => { writes = true })))
                    else fire(5)
                    await flush()
                    const beforeStart = checkpoint
                    if ('${outcome}' === 'drain timeout') { fire(50); await flush() }
                    if ('${outcome}' === 'nested timeout') lateStart.resolve()
                    else started.resolve()
                    await flush()
                    earlyCleanup.resolve(); await flush()
                    const beforeCleanup = { checkpoint, writes, stops }
                    if ('${outcome}' === 'cleanup failure') cleanup.reject(failure)
                    else cleanup.resolve()
                    const [result] = await Promise.allSettled([stopping])
                    if ('${outcome}' === 'drain timeout') {
                        if (result.status !== 'rejected' || !(result.reason instanceof WorkerDrainTimeoutError)) throw new Error('Missing bounded checkpoint failure')
                        await result.reason.drained
                        if ('${event}' === 'maintenance') {
                            const [again] = await Promise.allSettled([supervisor.stopWorkersForRestoreMaintenance()])
                            if (again.status !== 'rejected' || again.reason !== result.reason) throw new Error('Restore checkpoint forgot its deadline failure')
                        }
                    }
                    await Promise.allSettled([initializing])
                    await flush()
                    console.log(JSON.stringify({ beforeStart, beforeCleanup, checkpoint, writes,
                        exactFailure: result.status === 'rejected' && result.reason === failure,
                        state: workerObservability.snapshot().workers.find(value => value.id === 'npm_import_cleanup').state }))
                `,
                    { cwd: new URL('../../..', import.meta.url) },
                )
                expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
                    beforeStart: 'pending',
                    beforeCleanup: {
                        checkpoint: outcome === 'drain timeout' ? 'failure' : 'pending',
                        writes: true,
                        stops: 2,
                    },
                    checkpoint:
                        outcome === 'success' ||
                        outcome === 'nested timeout' ||
                        outcome === 'pending early stop'
                            ? 'success'
                            : 'failure',
                    writes: outcome === 'cleanup failure',
                    exactFailure: outcome === 'start failure' || outcome === 'cleanup failure',
                    state:
                        outcome === 'success' ||
                        outcome === 'pending early stop' ||
                        outcome === 'drain timeout' ||
                        outcome === 'nested timeout'
                            ? event === 'maintenance'
                                ? 'maintenance_paused'
                                : 'stopped'
                            : 'failed',
                })
            })
        }
    }
    test.each(['resolve', 'cancel'] as const)(
        'a cooperative asynchronous start shares one cleanup effect across idempotent stops: %s',
        async (settlement) => {
            const entered = Promise.withResolvers<void>()
            const cancelled = Promise.withResolvers<void>()
            const cleanup = Promise.withResolvers<void>()
            let stops = 0
            let stopCalls = 0
            let sharedCleanup: Promise<void> | undefined
            const supervisor = createWorkerLifecycleSupervisor()
            const initializing = supervisor.startWorkerLifecycle(async () => ({
                start: async (signal) => {
                    entered.resolve()
                    await cancelled.promise
                    if (settlement === 'cancel') throw signal!.reason
                },
                stop: () => {
                    stopCalls++
                    sharedCleanup ??= (async () => {
                        stops++
                        cancelled.resolve()
                        await cleanup.promise
                    })()
                    return sharedCleanup
                },
            }))
            await entered.promise
            let drained = false
            const stopping = supervisor.stopWorkersForRestoreMaintenance().then(() => {
                drained = true
            })
            await Bun.sleep(1)
            expect(stops).toBe(1)
            expect(stopCalls).toBe(1)
            expect(drained).toBe(false)
            cleanup.resolve()
            await Promise.all([
                stopping,
                initializing,
                supervisor.stopWorkersForRestoreMaintenance(),
            ])
            expect(stops).toBe(1)
            expect(stopCalls).toBe(2)
            expect(drained).toBe(true)
        },
    )
    test('retries a transient pre-start status read without stopping an unstarted singleton', async () => {
        const output = await runBunScript(
            `
            import { mock } from 'bun:test'
            mock.module('@tanstack/react-start/server-only', () => ({}))
            let reads = 0, loads = 0, starts = 0, stops = 0
            mock.module('./server/Backups/restore-maintenance-status.server.ts', () => ({
                readRestoreMaintenanceObservation: async () => {
                    if (++reads === 1) throw new Error('transient fixture read failure')
                    return { maintenance: null, restoreResults: [] }
                },
            }))
            mock.module('./server/Foundation/event-loop-observation.ts', () => ({ startEventLoopObservation() {} }))
            const { createWorkerLifecycleSupervisor } = await import('./server/Foundation/worker-lifecycle.server.ts')
            const cleanup = Promise.withResolvers()
            const cleanupEntered = Promise.withResolvers()
            const singleton = { start() { starts++ }, stop: () => { stops++; cleanupEntered.resolve(); return cleanup.promise } }
            const supervisor = createWorkerLifecycleSupervisor()
            await supervisor.startWorkerLifecycle(async () => { loads++; return singleton }, 'npm_import_cleanup', { retryDelayMs: 1 })
            const recovered = { reads, loads, starts, stops }
            let drained = false
            const stopping = supervisor.stopWorkersForRestoreMaintenance().then(() => { drained = true })
            await cleanupEntered.promise
            await Promise.resolve()
            const beforeCleanup = drained
            cleanup.resolve()
            await stopping
            console.log(JSON.stringify({ recovered, beforeCleanup, drained, stops }))
        `,
            { cwd: new URL('../../..', import.meta.url) },
        )
        expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
            recovered: { reads: 2, loads: 1, starts: 1, stops: 0 },
            beforeCleanup: false,
            drained: true,
            stops: 1,
        })
    })
    for (const event of ['shutdown', 'maintenance'] as const) {
        for (const deadline of ['complete', 'timeout'] as const) {
            test(`${event} retains a late load and its cleanup after startup ${deadline}`, async () => {
                const output = await runBunScript(
                    `
                    import { mock } from 'bun:test'
                    mock.module('@tanstack/react-start/server-only', () => ({}))
                    mock.module('./server/Backups/restore-maintenance-status.server.ts', () => ({ readRestoreMaintenanceObservation: async () => ({ maintenance: null, restoreResults: [] }) }))
                    mock.module('./server/Foundation/event-loop-observation.ts', () => ({ startEventLoopObservation() {} }))
                    const { createWorkerLifecycleSupervisor } = await import('./server/Foundation/worker-lifecycle.server.ts')
                    const { WorkerDrainTimeoutError } = await import('./server/Foundation/Types/worker-lifecycle.types.ts')
                    const { workerObservability } = await import('./server/Foundation/worker-observability.ts')
                    workerObservability.reset()
                    const timers = new Set()
                    globalThis.setTimeout = (callback, milliseconds) => { const timer = { callback, milliseconds }; timers.add(timer); return timer }
                    globalThis.clearTimeout = timer => timers.delete(timer)
                    const fire = milliseconds => {
                        const timer = [...timers].find(value => value.milliseconds === milliseconds)
                        if (!timer) throw new Error('Expected owned deadline unavailable')
                        timers.delete(timer); timer.callback()
                    }
                    const flush = async () => { for (let index = 0; index < 40; index++) await Promise.resolve() }
                    const loaded = Promise.withResolvers()
                    const cleanup = Promise.withResolvers()
                    let starts = 0, stops = 0, checkpoint = 'pending', failure, actual = 'pending'
                    const worker = { start() { starts++ }, stop() { stops++; return cleanup.promise } }
                    const supervisor = createWorkerLifecycleSupervisor()
                    const initializing = supervisor.startWorkerLifecycle(() => loaded.promise, 'npm_import_cleanup', { startupTimeoutMs: 7, drainTimeoutMs: 11 })
                    void initializing.catch(() => {})
                    let draining
                    if ('${event}' === 'maintenance') draining = supervisor.stopWorkersForRestoreMaintenance()
                    else { const pending = []; process.emit('rentnerproxy:shutdown', pending); draining = Promise.all(pending) }
                    void draining.then(() => { checkpoint = 'success' }, error => { checkpoint = 'failure'; failure = error })
                    const read = () => workerObservability.snapshot().workers.find(value => value.id === 'npm_import_cleanup')
                    fire(7)
                    await flush()
                    const afterStartup = { checkpoint, state: read().state }
                    if ('${deadline}' === 'timeout') {
                        fire(11); await flush()
                        if (!(failure instanceof WorkerDrainTimeoutError)) throw new Error('Missing bounded stop failure')
                        void failure.drained.then(() => { actual = 'success' }, () => { actual = 'failure' })
                    }
                    loaded.resolve(worker)
                    await flush()
                    const afterLoad = { checkpoint, actual, state: read().state, starts, stops }
                    cleanup.resolve()
                    if (failure) await failure.drained
                    else await draining
                    const [initial] = await Promise.allSettled([initializing])
                    if ('${event}' === 'maintenance' && failure && (initial.status !== 'rejected' || initial.reason !== failure))
                        throw new Error('Initialization forgot the failed maintenance checkpoint')
                    await flush()
                    if ('${event}' === 'maintenance' && failure) {
                        const [repeated] = await Promise.allSettled([supervisor.stopWorkersForRestoreMaintenance()])
                        if (repeated.status !== 'rejected' || repeated.reason !== failure) throw new Error('Failed restore checkpoint was forgotten')
                    }
                    console.log(JSON.stringify({ afterStartup, afterLoad,
                        finished: { checkpoint, state: read().state, starts, stops, drainTimeout: read().counters.drainTimeout } }))
                `,
                    { cwd: new URL('../../..', import.meta.url) },
                )
                expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
                    afterStartup: { checkpoint: 'pending', state: 'stopping' },
                    afterLoad: {
                        checkpoint: deadline === 'timeout' ? 'failure' : 'pending',
                        actual: 'pending',
                        state: 'stopping',
                        starts: 0,
                        stops: 1,
                    },
                    finished: {
                        checkpoint: deadline === 'timeout' ? 'failure' : 'success',
                        state: event === 'maintenance' ? 'maintenance_paused' : 'stopped',
                        starts: 0,
                        stops: 1,
                        drainTimeout: deadline === 'timeout' ? 1 : 0,
                    },
                })
            })
        }
    }

    test.each(['load', 'cleanup'] as const)(
        'a failed late %s rejects the actual drain instead of granting restore quiescence',
        async (failureAt) => {
            const output = await runBunScript(
                `
            import { mock } from 'bun:test'
            mock.module('@tanstack/react-start/server-only', () => ({}))
            mock.module('./server/Backups/restore-maintenance-status.server.ts', () => ({ readRestoreMaintenanceObservation: async () => ({ maintenance: null, restoreResults: [] }) }))
            mock.module('./server/Foundation/event-loop-observation.ts', () => ({ startEventLoopObservation() {} }))
            const { createWorkerLifecycleSupervisor } = await import('./server/Foundation/worker-lifecycle.server.ts')
            const { WorkerDrainTimeoutError } = await import('./server/Foundation/Types/worker-lifecycle.types.ts')
            const { workerObservability } = await import('./server/Foundation/worker-observability.ts')
            workerObservability.reset()
            const timers = new Map()
            globalThis.setTimeout = (callback, milliseconds) => { const timer = {}; timers.set(timer, { callback, milliseconds }); return timer }
            globalThis.clearTimeout = timer => timers.delete(timer)
            const fire = milliseconds => { const [timer, value] = [...timers].find(([, value]) => value.milliseconds === milliseconds); timers.delete(timer); value.callback() }
            const flush = async () => { for (let index = 0; index < 40; index++) await Promise.resolve() }
            const loaded = Promise.withResolvers()
            const cleanup = Promise.withResolvers()
            const lateFailure = new Error('late fixture load failed')
            const supervisor = createWorkerLifecycleSupervisor()
            const initializing = supervisor.startWorkerLifecycle(() => loaded.promise, 'npm_import_cleanup', { startupTimeoutMs: 7, drainTimeoutMs: 11 })
            void initializing.catch(() => {})
            let checkpoint = 'pending', failure
            const draining = supervisor.stopWorkersForRestoreMaintenance()
            void draining.then(() => { checkpoint = 'success' }, error => { checkpoint = 'failure'; failure = error })
            fire(7); await flush()
            const beforeDeadline = checkpoint
            fire(11); await flush()
            if (!(failure instanceof WorkerDrainTimeoutError)) throw new Error('Missing bounded stop failure')
            if ('${failureAt}' === 'load') loaded.reject(lateFailure)
            else {
                loaded.resolve({ start() { throw new Error('Late worker must never start') }, stop: () => cleanup.promise })
                await flush()
                cleanup.reject(lateFailure)
            }
            const [actual] = await Promise.allSettled([failure.drained])
            await Promise.allSettled([initializing, draining])
            await flush()
            const worker = workerObservability.snapshot().workers.find(value => value.id === 'npm_import_cleanup')
            console.log(JSON.stringify({ beforeDeadline, checkpoint, failedLateLoad: actual.status === 'rejected' && actual.reason === lateFailure, state: worker.state }))
        `,
                { cwd: new URL('../../..', import.meta.url) },
            )
            expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
                beforeDeadline: 'pending',
                checkpoint: 'failure',
                failedLateLoad: true,
                state: 'failed',
            })
        },
    )

    test('nested stop deadlines retain a running sibling before propagating an actual failure', async () => {
        const writing = Promise.withResolvers<void>()
        const [inner] = await Promise.allSettled([drainWorker(writing.promise, 0)])
        expect(inner?.status).toBe('rejected')
        if (inner?.status !== 'rejected') throw new Error('Expected the inner deadline')
        expect(inner.reason).toBeInstanceOf(WorkerDrainTimeoutError)
        const failure = new Error('actual sibling failure')
        const settling = settleWorkerTasks([Promise.reject(failure), Promise.reject(inner.reason)])
        const [outer] = await Promise.allSettled([drainWorker(settling, 0)])
        expect(outer?.status).toBe('rejected')
        if (outer?.status !== 'rejected') throw new Error('Expected the outer deadline')
        expect(outer.reason).toBeInstanceOf(WorkerDrainTimeoutError)
        writing.resolve()
        await expect(settling).rejects.toBe(failure)
        await expect((outer.reason as WorkerDrainTimeoutError).drained).rejects.toBe(failure)
    })

    test('a failed sibling cannot report quiescence while another admitted writer is still running', async () => {
        const writing = Promise.withResolvers<void>()
        let settled = false
        const failure = new Error('worker failed')
        const settling = settleWorkerTasks([Promise.reject(failure), writing.promise])
        void settling.then(
            () => {
                settled = true
            },
            () => {
                settled = true
            },
        )
        await Bun.sleep(1)
        expect(settled).toBe(false)
        writing.resolve()
        await expect(settling).rejects.toBe(failure)
        expect(settled).toBe(true)
    })

    test('shutdown aborts an asynchronous start and invokes stop before waiting for initialization', async () => {
        const started = Promise.withResolvers<void>()
        const entered = Promise.withResolvers<void>()
        let signal: AbortSignal | undefined
        let stopped = false
        const initializing = startWorkerLifecycle(async () => ({
            start: (cancellation) => {
                signal = cancellation
                entered.resolve()
                return started.promise
            },
            stop: async () => {
                stopped = true
            },
        }))
        await entered.promise
        const pending: Array<Promise<void>> = []
        process.emit('rentnerproxy:shutdown', pending)
        await Bun.sleep(1)
        expect(signal?.aborted).toBe(true)
        expect(stopped).toBe(true)
        let drained = false
        const draining = Promise.all(pending).then(() => {
            drained = true
        })
        await Bun.sleep(1)
        expect(drained).toBe(false)
        started.resolve()
        await Promise.all([initializing, draining])
        expect(drained).toBe(true)
    })

    test('cleans a failed first start before retrying a fresh instance', async () => {
        let attempts = 0
        const stopped: number[] = []
        await startWorkerLifecycle(
            async () => {
                const attempt = ++attempts
                return {
                    start: async () => {
                        if (attempt === 1) throw new Error('first dependency failed')
                    },
                    stop: async () => {
                        stopped.push(attempt)
                    },
                }
            },
            undefined,
            { retryDelayMs: 1 },
        )
        expect(attempts).toBe(2)
        expect(stopped).toEqual([1])
    })

    test('does not report a permanently stopped singleton as restarted after a startup failure', async () => {
        workerObservability.reset()
        const startupFailure = new Error('initialization failed')
        let starts = 0
        let stops = 0
        const worker = {
            start: () => {
                starts++
                throw startupFailure
            },
            stop: async () => { stops++ },
        }
        const failure = await startWorkerLifecycle(async () => worker, 'npm_import_cleanup', {
                retryDelayMs: 1,
            }).then(() => undefined, (error: unknown) => error)
        expect(failure).toBeInstanceOf(Error)
        if (!(failure instanceof Error)) throw new Error('Expected stopped-instance failure')
        expect(failure.message).toBe('worker_instance_stopped')
        expect(failure.cause).toBe(startupFailure)
        expect(starts).toBe(1)
        expect(stops).toBe(1)
        const observed = workerObservability.snapshot().workers.find(value => value.id === 'npm_import_cleanup')!
        expect(observed.state).toBe('failed')
        expect(observed.counters.retry).toBe(1)
        expect(observed.counters.exhausted).toBe(1)
        expect(JSON.stringify(observed)).not.toContain(startupFailure.message)
    })

    test('retries a transient import once and deduplicates its stable identity', async () => {
        workerObservability.reset()
        let loads = 0
        let starts = 0
        const load = async () => {
            loads++
            if (loads === 1) throw new Error('dependency unavailable')
            return {
                start: () => {
                    starts++
                },
                stop: async () => undefined,
            }
        }
        const first = startWorkerLifecycle(load, 'npm_import_cleanup', { retryDelayMs: 1 })
        expect(startWorkerLifecycle(load, 'npm_import_cleanup')).toBe(first)
        await first
        expect(loads).toBe(2)
        expect(starts).toBe(1)
        expect(
            workerObservability
                .snapshot()
                .workers.find((entry) => entry.id === 'npm_import_cleanup')?.counters.retry,
        ).toBe(1)
    })

    test('does not retry terminal startup failures', async () => {
        let loads = 0
        await expect(
            startWorkerLifecycle(
                async () => {
                    loads++
                    throw new Error('configuration invalid')
                },
                'npm_import_cleanup',
                { classifyFailure: () => 'terminal' },
            ),
        ).rejects.toThrow('configuration invalid')
        expect(loads).toBe(1)
    })

    test('shutdown cancels retry sleep and rejects subsequent registration', async () => {
        let loads = 0
        const first = startWorkerLifecycle(
            async () => {
                loads++
                throw new Error('transient')
            },
            undefined,
            { retryDelayMs: 10_000 },
        )
        await Bun.sleep(1)
        const pending: Array<Promise<void>> = []
        process.emit('rentnerproxy:shutdown', pending)
        await Promise.all([first, ...pending])
        await startWorkerLifecycle(async () => {
            throw new Error('late admission')
        })
        expect(loads).toBe(1)
    })

    test('a startup timeout prevents late admission and cleans the loaded worker once', async () => {
        const loading = Promise.withResolvers<WorkerLifecycle>()
        let stopped = 0
        let started = 0
        await expect(
            startWorkerLifecycle(() => loading.promise, undefined, {
                startupTimeoutMs: 5,
            }),
        ).rejects.toThrow('worker_drain_timeout')
        loading.resolve({
            start: () => {
                started++
            },
            stop: async () => {
                stopped++
            },
        })
        await Bun.sleep(1)
        expect(started).toBe(0)
        expect(stopped).toBe(1)
    })

    test('a stalled drain rejects and cannot become a successful restore checkpoint later', async () => {
        const supervisor = createWorkerLifecycleSupervisor()
        const running = Promise.withResolvers<void>()
        await supervisor.startWorkerLifecycle(
            async () => ({
                start: () => undefined,
                stop: () => running.promise,
            }),
            undefined,
            { drainTimeoutMs: 5 },
        )
        const draining = supervisor.stopWorkersForRestoreMaintenance()
        await expect(draining).rejects.toThrow('worker_drain_timeout')
        running.resolve()
        await expect(supervisor.stopWorkersForRestoreMaintenance()).rejects.toThrow(
            'worker_drain_timeout',
        )
    })

    test('a timeout is not confirmation that the underlying write completed', async () => {
        const writing = Promise.withResolvers<void>()
        let completed = false
        const write = writing.promise.then(() => {
            completed = true
        })
        await expect(drainWorker(write, 5)).rejects.toThrow('worker_drain_timeout')
        expect(completed).toBe(false)
        writing.resolve()
        await write
        expect(completed).toBe(true)
    })
    for (const event of ['shutdown', 'maintenance'] as const) {
        for (const phase of ['load', 'apply', 'drift'] as const) {
            test(`${event} timeout preserves ${phase} ownership until the actual drain`, async () => {
                const output = await runBunScript(
                    `
                    import { mock } from 'bun:test'
                    mock.module('@tanstack/react-start/server-only', () => ({}))
                    mock.module('./server/Backups/restore-maintenance-status.server.ts', () => ({ readRestoreMaintenanceObservation: async () => ({ maintenance: null, restoreResults: [] }) }))
                    mock.module('./server/Foundation/event-loop-observation.ts', () => ({ startEventLoopObservation() {} }))
                    const { createRuntimeReconciler } = await import('./server/Foundation/runtime-reconciler.server.ts')
                    const { startWorkerLifecycle, stopWorkersForRestoreMaintenance } = await import('./server/Foundation/worker-lifecycle.server.ts')
                    const { WorkerDrainTimeoutError } = await import('./server/Foundation/Types/worker-lifecycle.types.ts')
                    const { workerObservability } = await import('./server/Foundation/worker-observability.ts')
                    workerObservability.reset()
                    const gate = Promise.withResolvers()
                    const entered = Promise.withResolvers()
                    const phase = '${phase}'
                    const reconciler = createRuntimeReconciler({
                        load: async () => {
                            if (phase === 'load') { entered.resolve(); await gate.promise }
                            return 'revision'
                        },
                        revision: value => value,
                        apply: async () => {
                            if (phase === 'apply') { entered.resolve(); await gate.promise }
                            return true
                        },
                        drift: { check: async () => { entered.resolve(); await gate.promise; return false } },
                    }, { workerId: 'proxy_reconciliation', name: 'fixture', timeoutMs: 100, stopTimeoutMs: 0 })
                    await startWorkerLifecycle(async () => reconciler, 'proxy_reconciliation', { drainTimeoutMs: 0 })
                    if (phase === 'drift') {
                        await reconciler()
                        const first = reconciler.checkDrift()
                        if (reconciler.checkDrift() !== first) throw new Error('Overlapping drift ownership')
                    }
                    await entered.promise
                    const pending = []
                    if ('${event}' === 'maintenance') pending.push(stopWorkersForRestoreMaintenance())
                    else process.emit('rentnerproxy:shutdown', pending)
                    const [result] = await Promise.allSettled(pending)
                    if (result.status !== 'rejected' || !(result.reason instanceof WorkerDrainTimeoutError))
                        throw new Error('Undrained execution was acknowledged')
                    if ('${event}' === 'maintenance') {
                        const [repeated] = await Promise.allSettled([stopWorkersForRestoreMaintenance()])
                        if (repeated.status !== 'rejected' || repeated.reason !== result.reason)
                            throw new Error('Repeated maintenance forgot its failed drain')
                    }
                    const read = () => workerObservability.snapshot().workers.find(item => item.id === 'proxy_reconciliation')
                    const timedOut = read()
                    gate.resolve()
                    await result.reason.drained
                    const drained = read()
                    console.log(JSON.stringify({
                        timedOut: { state: timedOut.state, running: timedOut.running, drainTimeout: timedOut.counters.drainTimeout },
                        drained: { state: drained.state, running: drained.running, drainTimeout: drained.counters.drainTimeout },
                    }))
                `,
                    { cwd: new URL('../../..', import.meta.url) },
                )
                expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
                    timedOut: { state: 'stopping', running: 1, drainTimeout: 1 },
                    drained: {
                        state: event === 'maintenance' ? 'maintenance_paused' : 'stopped',
                        running: 0,
                        drainTimeout: 1,
                    },
                })
            })
        }
    }

    test('shutdown waits for a worker that is still loading and stops it once', async () => {
        const loaded = Promise.withResolvers<WorkerLifecycle>()
        const stopped = Promise.withResolvers<void>()
        const calls: string[] = []
        const initializing = startWorkerLifecycle(() => loaded.promise)
        const pending: Array<Promise<void>> = []
        process.emit('rentnerproxy:shutdown', pending)
        process.emit('rentnerproxy:shutdown', pending)
        expect(pending).toHaveLength(1)
        expect(calls).toEqual([])
        loaded.resolve({
            start: () => {
                calls.push('start')
            },
            stop: () => {
                calls.push('stop')
                return stopped.promise
            },
        })
        await initializing
        await Promise.resolve()
        expect(calls).toEqual(['stop'])
        let drained = false
        const draining = Promise.all(pending).then(() => {
            drained = true
        })
        await Promise.resolve()
        expect(drained).toBe(false)
        stopped.resolve()
        await draining
        expect(drained).toBe(true)
    })

    test('startup and shutdown preserve failures instead of reporting a successful drain', async () => {
        const startupError = new Error('startup failed')
        await expect(
            startWorkerLifecycle(
                async () => {
                    throw startupError
                },
                undefined,
                { maxAttempts: 1 },
            ),
        ).rejects.toBe(startupError)
        const pendingStartup: Array<Promise<void>> = []
        process.emit('rentnerproxy:shutdown', pendingStartup)
        await expect(Promise.all(pendingStartup)).rejects.toBe(startupError)

        startWorkerLifecycle = createWorkerLifecycleSupervisor().startWorkerLifecycle
        const shutdownError = new Error('shutdown failed')
        await startWorkerLifecycle(async () => ({
            start: () => undefined,
            stop: async () => {
                throw shutdownError
            },
        }))
        const pendingShutdown: Array<Promise<void>> = []
        process.emit('rentnerproxy:shutdown', pendingShutdown)
        await expect(Promise.all(pendingShutdown)).rejects.toBe(shutdownError)
    })
})
