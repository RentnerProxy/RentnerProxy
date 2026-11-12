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
            dependencies.releaseCancelled({ policy: { id: String(id) } }).then(() => { settled++ }))
        const beforeCommit = { batches: [...batches], published, settled }
        gate.resolve()
        await Promise.all(pending)
        const afterCommit = { batches: [...batches], published, settled }
        changed = false
        await Promise.all(Array.from({ length: 3 }, (_, id) => dependencies.releaseCancelled({ policy: { id: String(id) } })))
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

test('duplicate policies retain each old/new token result and a failed earlier batch cannot discard later siblings', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        mock.module('@tanstack/react-start/server-only', () => ({}))
        let dependencies, probe, published = 0, fail = false
        let tokens = new Map([['a', 'new'], ['b', 'live'], ['c', 'live']])
        let batches = []
        const gate = Promise.withResolvers(), entered = Promise.withResolvers()
        mock.module('./server/HostMonitoring/host-monitoring.worker.ts', () => ({ createHostMonitoringWorker: value => (dependencies = value, {}) }))
        mock.module('./server/HostMonitoring/host-monitoring.probe.ts', () => ({ createHostMonitoringProbe: value => (probe = value, async () => {}) }))
        mock.module('./server/HostMonitoring/host-monitoring.repository.ts', () => ({
            claimDueHostMonitoringPolicies: async () => [], completeHostMonitoringClaims: async () => [],
            readHostMonitoringClaimContexts: async () => [], readNextHostMonitoringAdmissionAt: async () => null,
            pruneHostMonitoringHistory: async () => {},
            releaseHostMonitoringClaims: async claims => {
                if (new Set(claims.map(claim => claim.policy.id)).size !== claims.length) throw new Error('duplicate policy')
                batches.push(claims.map(claim => claim.policy.id + ':' + claim.leaseToken))
                entered.resolve()
                await gate.promise
                if (fail && claims.some(claim => claim.policy.id === 'failed')) throw new Error('first transaction failed')
                return claims.map(claim => {
                    if (tokens.get(claim.policy.id) !== claim.leaseToken) return false
                    tokens.delete(claim.policy.id)
                    return true
                })
            },
        }))
        mock.module('./server/HostMonitoring/host-monitoring.observability.ts', () => ({ sampleHostMonitoringBacklog: async () => {} }))
        mock.module('./server/Controller/domain-diagnostics.server.ts', () => ({ inspectControllerDomain: async () => {} }))
        mock.module('./server/WebSockets/applicationChanges.ts', () => ({ publishApplicationChange: () => published++, subscribeToApplicationChanges: () => () => {} }))
        await import('./server/HostMonitoring/host-monitoring.worker.server.ts')
        await dependencies.runClaim({}, new AbortController().signal)
        const claim = (id, token) => ({ policy: { id }, leaseToken: token })
        const pending = Promise.allSettled([claim('a', 'old'), claim('b', 'live'), claim('a', 'new'), claim('c', 'live'), claim('a', 'new')].map(probe.release))
        await entered.promise
        const beforeCommit = { batches: [...batches], published }
        gate.resolve()
        const results = await pending
        const first = { beforeCommit, batches, published, values: results.map(result => result.status === 'fulfilled' ? result.value : 'rejected') }
        fail = true; batches = []; tokens = new Map([['a', 'new'], ['b', 'late']])
        const siblings = await Promise.allSettled([claim('a', 'old'), claim('failed', 'live'), claim('a', 'new'), claim('b', 'late')].map(probe.release))
        console.log(JSON.stringify({ first, second: { batches, published, values: siblings.map(result => result.status === 'fulfilled' ? result.value : { message: result.reason.message, cause: result.reason.cause.message }) } }))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
        first: {
            beforeCommit: { batches: [['a:old', 'b:live']], published: 0 },
            batches: [['a:old', 'b:live'], ['a:new', 'c:live'], ['a:new']],
            published: 1,
            values: [false, true, true, true, false],
        },
        second: {
            batches: [
                ['a:old', 'failed:live'],
                ['a:new', 'b:late'],
            ],
            published: 2,
            values: [
                { message: 'host_monitoring_cleanup_failed', cause: 'first transaction failed' },
                { message: 'host_monitoring_cleanup_failed', cause: 'first transaction failed' },
                true,
                true,
            ],
        },
    })
})

test('cancelled fenced cleanup retains the monitoring SQL budget and waits for its real permit', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        import { withBackgroundDatabaseContext, admitDatabaseTransaction, backgroundDatabaseResourceSnapshot } from './db/background-admission.ts'
        let dependencies, invoked = 0, resource
        mock.module('@tanstack/react-start/server-only', () => ({}))
        mock.module('./server/HostMonitoring/host-monitoring.worker.ts', () => ({ createHostMonitoringWorker: value => (dependencies = value, {}) }))
        mock.module('./server/HostMonitoring/host-monitoring.repository.ts', () => ({
            claimDueHostMonitoringPolicies: async () => [], completeHostMonitoringClaims: async () => [],
            readHostMonitoringClaimContexts: async () => [], readNextHostMonitoringAdmissionAt: async () => null,
            pruneHostMonitoringHistory: async () => {},
            releaseHostMonitoringClaims: claims => admitDatabaseTransaction(async selected => {
                invoked++; resource = selected; return claims.map(() => true)
            }),
        }))
        mock.module('./server/HostMonitoring/host-monitoring.observability.ts', () => ({ sampleHostMonitoringBacklog: async () => {} }))
        mock.module('./server/Controller/domain-diagnostics.server.ts', () => ({ inspectControllerDomain: async () => {} }))
        mock.module('./server/WebSockets/applicationChanges.ts', () => ({ publishApplicationChange: () => {}, subscribeToApplicationChanges: () => () => {} }))
        await import('./server/HostMonitoring/host-monitoring.worker.server.ts')
        const gate = Promise.withResolvers(), ready = Promise.withResolvers()
        let occupied = 0
        const held = Array.from({ length: 2 }, () => withBackgroundDatabaseContext(() => admitDatabaseTransaction(async () => {
            if (++occupied === 2) ready.resolve()
            await gate.promise
        }), undefined, 'monitoring'))
        await ready.promise
        const controller = new AbortController(); controller.abort()
        const result = withBackgroundDatabaseContext(() => dependencies.releaseCancelled({ policy: { id: 'owned' } }), controller.signal, 'monitoring')
            .then(() => 'released', error => error.reason)
        await Bun.sleep(10)
        const before = { invoked, ...backgroundDatabaseResourceSnapshot().monitoring }
        gate.resolve()
        const outcome = await result
        await Promise.all(held)
        console.log(JSON.stringify({ before: { invoked: before.invoked, running: before.running, pending: before.pending },
            outcome, invoked, resource, after: backgroundDatabaseResourceSnapshot().monitoring.running }))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
        before: { invoked: 0, running: 2, pending: 1 },
        outcome: 'released',
        invoked: 1,
        resource: 'monitoring',
        after: 0,
    })
})
