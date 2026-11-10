import { afterEach, beforeEach, describe, expect, test } from 'bun:test'

import {
    createWorkerLifecycleSupervisor,
    drainWorker,
    settleWorkerTasks,
} from '@/server/Foundation/worker-lifecycle.server.ts'
import { workerObservability } from '@/server/Foundation/worker-observability.ts'
import type { WorkerLifecycle } from '@/server/Foundation/Types/worker-lifecycle.types.ts'

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
