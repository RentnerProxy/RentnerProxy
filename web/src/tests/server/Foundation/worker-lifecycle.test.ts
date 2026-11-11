import { afterEach, describe, expect, test } from 'bun:test'

import { startWorkerLifecycle } from '@/server/Foundation/worker-lifecycle.server.ts'
import type { WorkerLifecycle } from '@/server/Foundation/Types/worker-lifecycle.types.ts'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

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
        for (const phase of ['load', 'apply', 'drift'] as const) {
            test(`${event} timeout preserves ${phase} ownership until the actual drain`, async () => {
                const output = await runBunScript(
                    `
                    import { mock } from 'bun:test'
                    mock.module('@tanstack/react-start/server-only', () => ({}))
                    mock.module('./server/Backups/restore-maintenance-status.server.ts', () => ({ readRestoreMaintenanceStatus: async () => null }))
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
                    await startWorkerLifecycle(async () => reconciler, 'proxy_reconciliation')
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
            startWorkerLifecycle(async () => {
                throw startupError
            }),
        ).rejects.toBe(startupError)
        const pendingStartup: Array<Promise<void>> = []
        process.emit('rentnerproxy:shutdown', pendingStartup)
        await expect(Promise.all(pendingStartup)).rejects.toBe(startupError)

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
