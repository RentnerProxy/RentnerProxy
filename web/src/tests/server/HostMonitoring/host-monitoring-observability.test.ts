import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test('stored busy results defer admission without claiming success or hiding actual dependency failure', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        mock.module('@tanstack/react-start/server-only', () => ({}))
        let dependencies, current = { status: 'unknown', code: 'busy' }, stored = true, executed = true
        mock.module('./server/HostMonitoring/host-monitoring.worker.ts', () => ({
            createHostMonitoringWorker: value => (dependencies = value, {}),
        }))
        mock.module('./server/HostMonitoring/host-monitoring.repository.ts', () => ({
            claimDueHostMonitoringPolicies: async () => [],
            completeHostMonitoringClaims: async items => items.map(() => ({ status: 'fulfilled', value: stored })),
            readHostMonitoringClaimContexts: async () => [],
            releaseHostMonitoringClaim: async () => {}, pruneHostMonitoringHistory: async () => {},
        }))
        mock.module('./server/HostMonitoring/host-monitoring.batch.ts', () => ({
            createHostMonitoringBatcher: options => async item => (await options.run([item]))[0].value,
        }))
        mock.module('./server/HostMonitoring/host-monitoring.probe.ts', () => ({
            createHostMonitoringProbe: options => async (claim, signal) => executed ? options.complete(claim, current, null, signal) : undefined,
        }))
        mock.module('./server/HostMonitoring/host-monitoring.observability.ts', () => ({ sampleHostMonitoringBacklog: async () => {} }))
        mock.module('./server/Controller/domain-diagnostics.server.ts', () => ({ inspectControllerDomain: async () => {} }))
        mock.module('./server/WebSockets/applicationChanges.ts', () => ({ publishApplicationChange() {} }))
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
        current = { status: 'unknown', code: 'busy' }
        await dependencies.runClaim({}, signal)
        const busyAfterFailure = read()
        stored = false
        current = { status: 'healthy', code: 'expected_status' }
        await dependencies.runClaim({}, signal)
        const stale = read()
        executed = false
        await dependencies.runClaim({}, signal)
        const unexecuted = read()
        const admissionFailure = workerObservability.begin('host_monitoring', 'admission')
        admissionFailure('failure')
        await dependencies.claim(1)
        const admissionRecovered = read()
        stored = executed = true
        current = { status: 'healthy', code: 'expected_status' }
        await dependencies.runClaim({}, signal)
        const recovered = read()
        console.log(JSON.stringify({
            busy: { failure: busy.counters.failure, deferred: busy.counters.deferred,
                rejected: busy.counters.admissionRejected, lastSuccessAt: busy.lastSuccessAt, state: busy.state },
            failed: { failure: failed.counters.failure, state: failed.state },
            retained: [busyAfterFailure, stale, unexecuted, admissionRecovered].map(value => ({
                state: value.state, degradedLanes: value.degradedLanes, success: value.counters.success,
            })),
            recovered: { success: recovered.counters.success, state: recovered.state },
        }))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
        busy: { failure: 0, deferred: 1, rejected: 1, lastSuccessAt: null, state: 'idle' },
        failed: { failure: 1, state: 'degraded' },
        retained: Array.from({ length: 4 }, () => ({
            state: 'degraded',
            degradedLanes: ['execution'],
            success: 0,
        })),
        recovered: { success: 1, state: 'idle' },
    })
})

test('aborted probe and late-claim cleanup retain the shared SQL budget until one fenced release settles', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        import { admitDatabaseTransaction, withBackgroundDatabaseContext } from './db/background-admission.ts'
        import { DATABASE_CONNECTION_BUDGET } from './config/database.config.ts'
        let dependencies, invoked = 0, owned
        mock.module('@tanstack/react-start/server-only', () => ({}))
        mock.module('./server/HostMonitoring/host-monitoring.worker.ts', () => ({ createHostMonitoringWorker: value => (dependencies = value, {}) }))
        mock.module('./server/HostMonitoring/host-monitoring.repository.ts', () => ({
            claimDueHostMonitoringPolicies: async () => [], completeHostMonitoringClaims: async () => [],
            readHostMonitoringClaimContexts: async () => [], pruneHostMonitoringHistory: async () => {},
            releaseHostMonitoringClaim: claim => admitDatabaseTransaction(async () => {
                if (claim !== owned) throw new Error('Claim fence identity changed')
                invoked++
                return true
            }),
        }))
        mock.module('./server/HostMonitoring/host-monitoring.observability.ts', () => ({ sampleHostMonitoringBacklog: async () => {} }))
        mock.module('./server/Controller/domain-diagnostics.server.ts', () => ({ inspectControllerDomain: async () => {} }))
        mock.module('./server/WebSockets/applicationChanges.ts', () => ({ publishApplicationChange: () => {} }))
        await import('./server/HostMonitoring/host-monitoring.worker.server.ts')
        const results = []
        for (const path of ['probe', 'late-claim']) {
            invoked = 0
            owned = { policy: { id: 'owned-policy' }, leaseToken: 'original-owned-token' }
            let entered = 0, settled = false
            const gate = Promise.withResolvers(), ready = Promise.withResolvers()
            const held = Array.from({ length: DATABASE_CONNECTION_BUDGET.backgroundTransactions }, () => withBackgroundDatabaseContext(() =>
                admitDatabaseTransaction(async () => { if (++entered === DATABASE_CONNECTION_BUDGET.backgroundTransactions) ready.resolve(); await gate.promise })))
            await ready.promise
            const cancellation = new AbortController(); cancellation.abort()
            const pending = (path === 'probe'
                ? dependencies.runClaim(owned, cancellation.signal)
                : withBackgroundDatabaseContext(() => dependencies.releaseCancelled(owned), cancellation.signal))
                .then(() => ({ outcome: 'released' }), error => ({ outcome: error.code ?? error.reason }))
                .then(value => { settled = true; return value })
            await Bun.sleep(10)
            const before = { entered, invoked, settled }
            gate.resolve()
            const result = await pending
            await Promise.all(held)
            results.push({ path, before, invoked, ...result })
        }
        console.log(JSON.stringify(results))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual([
        {
            path: 'probe',
            before: { entered: 8, invoked: 0, settled: false },
            invoked: 1,
            outcome: 'aborted',
        },
        {
            path: 'late-claim',
            before: { entered: 8, invoked: 0, settled: false },
            invoked: 1,
            outcome: 'released',
        },
    ])
})
