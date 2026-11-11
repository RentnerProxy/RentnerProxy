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
        let starts = 0
        const worker = {
            start: () => {
                starts++
                throw new Error('initialization failed')
            },
            stop: async () => undefined,
        }
        await expect(
            startWorkerLifecycle(async () => worker, undefined, {
                retryDelayMs: 1,
            }),
        ).rejects.toThrow('worker_instance_stopped')
        expect(starts).toBe(1)
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
