import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test('a cancelled 64-probe flight shares one durable release and wakes only after an actual release', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        mock.module('@tanstack/react-start/server-only', () => ({}))
        let dependencies, published = 0, changed = true, settled = 0
        const batches = [], gate = Promise.withResolvers()
        mock.module('./server/HostMonitoring/host-monitoring.worker.ts', () => ({
            createHostMonitoringWorker: value => (dependencies = value, {}),
        }))
        mock.module('./server/HostMonitoring/host-monitoring.repository.ts', () => ({
            claimDueHostMonitoringPolicies: async () => [],
            completeHostMonitoringClaims: async () => [],
            readHostMonitoringClaimContexts: async () => [],
            readNextHostMonitoringAdmissionAt: async () => null,
            releaseHostMonitoringClaims: async claims => {
                batches.push(claims.length)
                await gate.promise
                return claims.map(() => changed)
            },
            pruneHostMonitoringHistory: async () => {},
        }))
        mock.module('./server/HostMonitoring/host-monitoring.observability.ts', () => ({ sampleHostMonitoringBacklog: async () => {} }))
        mock.module('./server/Controller/domain-diagnostics.server.ts', () => ({ inspectControllerDomain: async () => {} }))
        mock.module('./server/WebSockets/applicationChanges.ts', () => ({
            publishApplicationChange() { published++ }, subscribeToApplicationChanges: () => () => {},
        }))
        await import('./server/HostMonitoring/host-monitoring.worker.server.ts')
        const pending = Array.from({ length: 64 }, (_, id) =>
            dependencies.releaseCancelled({ id }).then(() => { settled++ }))
        const beforeCommit = { batches: [...batches], published, settled }
        gate.resolve()
        await Promise.all(pending)
        const afterCommit = { batches: [...batches], published, settled }
        changed = false
        await Promise.all(Array.from({ length: 3 }, (_, id) => dependencies.releaseCancelled({ id })))
        console.log(JSON.stringify({ beforeCommit, afterCommit, noChange: { batches, published } }))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
        beforeCommit: { batches: [64], published: 0, settled: 0 },
        afterCommit: { batches: [64], published: 1, settled: 64 },
        noChange: { batches: [64, 3], published: 1 },
    })
})
