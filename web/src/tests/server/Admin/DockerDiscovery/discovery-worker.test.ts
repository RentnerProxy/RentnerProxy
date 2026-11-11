import { expect, mock, spyOn, test } from 'bun:test'
import { createDiscoveryWorker } from '@/server/Admin/DockerDiscovery/discovery-scheduler.ts'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test('controller observation has independent failure recovery and treats empty watches and stop as normal', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        mock.module('@tanstack/react-start/server-only', () => ({}))
        let dependencies, mode = 'offline'
        const unavailable = new Error('private fixture payload')
        const snapshot = {}
        const cursor = { runtimeId: 'a'.repeat(32), generation: 1 }
        mock.module('./server/Auth/Core/database.server.ts', () => ({
            getAuthDatabase: () => ({ transaction: operation => operation({ execute: async () => {} }) }),
        }))
        mock.module('./server/ProxyRuntime/proxy-runtime-settings.ts', () => ({ lockProxyRuntimeSettings: async () => {} }))
        mock.module('./server/ProxyRuntime/proxy-runtime.service.ts', () => ({ reconcileProxyConfigurationService: async () => 'applied' }))
        mock.module('./server/Controller/docker-discovery.server.ts', () => ({
            dockerDiscoveryController: {
                watch: async (_, signal) => {
                    if (signal.aborted) throw signal.reason
                    if (mode === 'offline') throw unavailable
                    return { cursor, snapshot: mode === 'empty' ? null : snapshot }
                },
                status: async () => { throw unavailable },
            },
        }))
        mock.module('./server/WebSockets/applicationChanges.ts', () => ({ publishApplicationChange() {} }))
        mock.module('./server/WebSockets/discoveryChanges.ts', () => ({ subscribeToDiscoveryChanges() {} }))
        mock.module('./server/WebSockets/committedChanges.ts', () => ({ markDiscoveryReconciliation() {} }))
        mock.module('./server/Admin/DockerDiscovery/discovery-storage.server.ts', () => ({
            readDiscoveryRegistry: async () => ({ enabled: false, revision: 1, runtimeStatus: 'applied' }),
            writeDiscoveryRegistry: async () => {},
        }))
        mock.module('./server/Admin/DockerDiscovery/discovery-reconcile.server.ts', () => ({ reconcileDiscoveryInTransaction: async () => false }))
        mock.module('./server/Audit/audit.service.ts', () => ({ appendAuditEventInTransactionService: async () => {} }))
        mock.module('./server/Admin/DockerDiscovery/discovery-observer.ts', () => ({
            DiscoverySuperseded: class extends Error {},
            createDiscoveryObserver: value => { dependencies = value; return {} },
        }))
        const { workerObservability } = await import('./server/Foundation/worker-observability.ts')
        await import('./server/Admin/DockerDiscovery/discovery-worker.ts')
        workerObservability.reset()
        const read = () => workerObservability.snapshot().workers.find(item => item.id === 'docker_discovery')
        const signal = new AbortController().signal
        try { await dependencies.read(signal) } catch (error) { if (error !== unavailable) throw error }
        await dependencies.reconcile(snapshot, signal, () => {}, true)
        const afterReconcile = read()
        mode = 'snapshot'
        await dependencies.read(signal)
        const afterRead = read()
        mode = 'offline'
        try { await dependencies.watch(cursor, signal) } catch (error) { if (error !== unavailable) throw error }
        workerObservability.begin('docker_discovery')('failure')
        mode = 'empty'
        await dependencies.watch(cursor, signal)
        const afterExpiry = read()
        const stopped = AbortSignal.abort()
        try { await dependencies.watch(cursor, stopped) } catch (error) { if (error !== stopped.reason) throw error }
        const afterStop = read()
        mode = 'snapshot'
        await dependencies.watch(cursor, signal)
        const afterWatch = read()
        console.log(JSON.stringify({
            afterReconcile: { state: afterReconcile.state, lanes: afterReconcile.degradedLanes, failures: afterReconcile.counters.failure },
            afterRead: { state: afterRead.state, lanes: afterRead.degradedLanes },
            afterExpiry: { state: afterExpiry.state, lanes: afterExpiry.degradedLanes, failures: afterExpiry.counters.failure, deferred: afterExpiry.counters.deferred },
            afterStop: { lanes: afterStop.degradedLanes, failures: afterStop.counters.failure, cancelled: afterStop.counters.cancelled },
            afterWatch: { lanes: afterWatch.degradedLanes, failures: afterWatch.counters.failure },
        }))
    `,
        { cwd: new URL('../../../..', import.meta.url) },
    )
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
        afterReconcile: { state: 'degraded', lanes: ['collection'], failures: 1 },
        afterRead: { state: 'idle', lanes: [] },
        afterExpiry: { state: 'degraded', lanes: ['execution'], failures: 3, deferred: 1 },
        afterStop: { lanes: ['execution'], failures: 3, cancelled: 1 },
        afterWatch: { lanes: ['execution'], failures: 3 },
    })
})

test('event bursts share one reconciliation and shutdown drains it without new writes', async () => {
    let finish: (() => void) | undefined
    const step = mock(
        () =>
            new Promise<void>((resolve) => {
                finish = resolve
            }),
    )
    const worker = createDiscoveryWorker(step)
    const requests = Array.from({ length: 100 }, () => worker.trigger())
    expect(step).toHaveBeenCalledTimes(1)
    let drained = false
    const stopping = worker.stop().then(() => {
        drained = true
    })
    await Promise.resolve()
    expect(drained).toBe(false)
    finish?.()
    await Promise.all([...requests, stopping])
    await worker.trigger()
    expect(step).toHaveBeenCalledTimes(1)
})
test('a failed reconciliation is retried and logs only the outage transition', async () => {
    const warning = spyOn(console, 'warn').mockImplementation(() => undefined)
    let failing = true
    const step = mock(async () => {
        if (failing) throw new Error('synthetic private provider payload')
    })
    const worker = createDiscoveryWorker(step)
    try {
        await worker.trigger()
        await worker.trigger()
        expect(warning).toHaveBeenCalledTimes(1)
        expect(JSON.stringify(warning.mock.calls)).not.toContain('private provider')
        failing = false
        await worker.trigger()
        failing = true
        await worker.trigger()
        expect(warning).toHaveBeenCalledTimes(2)
        expect(step).toHaveBeenCalledTimes(4)
    } finally {
        await worker.stop()
        warning.mockRestore()
    }
})
