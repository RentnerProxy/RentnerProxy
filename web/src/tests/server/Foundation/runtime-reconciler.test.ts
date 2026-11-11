import { expect, test } from 'bun:test'
import { createRuntimeReconciler } from '@/server/Foundation/runtime-reconciler.server.ts'
import { workerObservability } from '@/server/Foundation/worker-observability.ts'

test('controller cancellation during drain records cancelled work without inventing failure', async () => {
    workerObservability.reset()
    const entered = Promise.withResolvers<void>()
    const reconciler = createRuntimeReconciler(
        {
            load: async () => ({ revision: 'one' }),
            revision: (snapshot) => snapshot.revision,
            apply: (_, signal) =>
                new Promise((_resolve, reject) => {
                    entered.resolve()
                    signal!.addEventListener('abort', () => reject(new Error('cancelled')), {
                        once: true,
                    })
                }),
        },
        { name: 'test', workerId: 'proxy_reconciliation', timeoutMs: 10, stopTimeoutMs: 100 },
    )
    reconciler.start()
    await entered.promise
    await reconciler.stop()
    const worker = workerObservability
        .snapshot()
        .workers.find((item) => item.id === 'proxy_reconciliation')!
    expect(worker.running).toBe(0)
    expect(worker.counters.cancelled).toBe(1)
    expect(worker.counters.failure).toBe(0)
})

test('drift observations are single-flight and drain waits for the actual observation', async () => {
    const observation = Promise.withResolvers<boolean>()
    let checks = 0
    let applies = 0
    let observedSignal: AbortSignal | undefined
    const reconciler = createRuntimeReconciler(
        {
            load: async () => ({ revision: 'one' }),
            revision: (snapshot) => snapshot.revision,
            apply: async () => {
                applies++
                return true
            },
            drift: {
                check: (signal) => {
                    checks++
                    observedSignal = signal
                    return observation.promise
                },
            },
        },
        { name: 'test', timeoutMs: 10, stopTimeoutMs: 5 },
    )
    const first = reconciler.checkDrift()
    for (let index = 0; index < 100; index++) expect(reconciler.checkDrift()).toBe(first)
    expect(checks).toBe(1)
    await expect(reconciler.stop()).rejects.toThrow('worker_drain_timeout')
    expect(observedSignal?.aborted).toBe(true)
    observation.resolve(true)
    await first
    await reconciler.stop()
    reconciler.start()
    await reconciler.checkDrift()
    expect(checks).toBe(1)
    expect(applies).toBe(0)
})

test('repeated start admits once and aborts the controller operation before draining', async () => {
    const applying = Promise.withResolvers<boolean>()
    let applies = 0
    let observedSignal: AbortSignal | undefined
    const reconciler = createRuntimeReconciler(
        {
            load: async () => ({ revision: 'one' }),
            revision: (snapshot) => snapshot.revision,
            apply: (_, signal) => {
                applies++
                observedSignal = signal
                return applying.promise
            },
        },
        { name: 'test', timeoutMs: 10, stopTimeoutMs: 100 },
    )
    reconciler.start()
    reconciler.start()
    await Bun.sleep(1)
    const stopping = reconciler.stop()
    expect(observedSignal?.aborted).toBe(true)
    let stopped = false
    void stopping.then(() => {
        stopped = true
    })
    await Bun.sleep(1)
    expect(stopped).toBe(false)
    applying.resolve(true)
    await stopping
    reconciler.start()
    expect(await reconciler()).toBe('pending')
    expect(applies).toBe(1)
})
