import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test('stored busy results defer admission without claiming success or hiding actual dependency failure', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        mock.module('@tanstack/react-start/server-only', () => ({}))
        let dependencies, current = { status: 'unknown', code: 'busy' }
        mock.module('./server/HostMonitoring/host-monitoring.worker.ts', () => ({
            createHostMonitoringWorker: value => (dependencies = value, {}),
        }))
        mock.module('./server/HostMonitoring/host-monitoring.repository.ts', () => ({
            claimDueHostMonitoringPolicies: async () => [],
            completeHostMonitoringClaims: async items => items.map(() => ({ status: 'fulfilled', value: true })),
            readHostMonitoringClaimContexts: async () => [],
            readNextHostMonitoringAdmissionAt: async () => null,
            releaseHostMonitoringClaims: async items => items.map(() => true), pruneHostMonitoringHistory: async () => {},
        }))
        mock.module('./server/HostMonitoring/host-monitoring.batch.ts', () => ({
            createHostMonitoringBatcher: options => async item => (await options.run([item]))[0].value,
        }))
        mock.module('./server/HostMonitoring/host-monitoring.probe.ts', () => ({
            createHostMonitoringProbe: options => async (claim, signal) => options.complete(claim, current, null, signal),
        }))
        mock.module('./server/HostMonitoring/host-monitoring.observability.ts', () => ({ sampleHostMonitoringBacklog: async () => {} }))
        mock.module('./server/Controller/domain-diagnostics.server.ts', () => ({ inspectControllerDomain: async () => {} }))
        mock.module('./server/WebSockets/applicationChanges.ts', () => ({
            publishApplicationChange() {}, subscribeToApplicationChanges: () => () => {},
        }))
        const { workerObservability } = await import('./server/Foundation/worker-observability.ts')
        await import('./server/HostMonitoring/host-monitoring.worker.server.ts')
        workerObservability.reset()
        const read = () => workerObservability.snapshot().workers.find(item => item.id === 'host_monitoring')
        const signal = new AbortController().signal
        await dependencies.runClaim({}, signal)
        const busy = read()
        current = { status: 'unknown', code: 'invalid_response' }
        await dependencies.runClaim({}, signal)
        const failed = read()
        current = { status: 'healthy', code: 'expected_status' }
        await dependencies.runClaim({}, signal)
        const recovered = read()
        console.log(JSON.stringify({
            busy: { failure: busy.counters.failure, deferred: busy.counters.deferred,
                rejected: busy.counters.admissionRejected, lastSuccessAt: busy.lastSuccessAt, state: busy.state },
            failed: { failure: failed.counters.failure, state: failed.state },
            recovered: { success: recovered.counters.success, state: recovered.state },
        }))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
        busy: { failure: 0, deferred: 1, rejected: 1, lastSuccessAt: null, state: 'idle' },
        failed: { failure: 1, state: 'degraded' },
        recovered: { success: 1, state: 'idle' },
    })
})
