import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test.each(['success', 'sql-error', 'admission-error'] as const)(
    '64 cancelled leases drain through bounded shared SQL admission: %s',
    async (mode) => {
        const output = await runBunScript(
            `
            import { mock } from 'bun:test'
            import { admitDatabaseTransaction, withBackgroundDatabaseContext, backgroundDatabaseResourceSnapshot } from './db/background-admission.ts'
            import { BACKGROUND_DATABASE_POLICY } from './config/background-resources.config.ts'
            import { ResourceAdmissionError } from './server/Foundation/resource-admission.ts'
            const { createHostMonitoringWorker: createWorker } = await import('./server/HostMonitoring/host-monitoring.worker.ts')
            const mode = ${JSON.stringify(mode)}, failure = mode === 'admission-error' ? new ResourceAdmissionError('busy') : new Error('fixture release SQL failure')
            const claims = Array.from({ length: 64 }, (_, index) => ({ policy: { id: String(index), proxyHostId: 'host-' + index,
                domain: 'host-' + index + '.example.test', timeoutMs: 3000, expectedStatus: 200 }, leaseToken: 'original-token-' + index }))
            const originals = new Map(claims.map(claim => [claim.policy.id, claim]))
            const leases = new Map(claims.map(claim => [claim.policy.id, claim.leaseToken]))
            const calls = [], sqlCalls = [], allProbes = Promise.withResolvers(), releaseSql = Promise.withResolvers(), sqlEntered = Promise.withResolvers()
            let adapter, probes = 0, submitted = 0, peakSubmitted = 0
            mock.module('@tanstack/react-start/server-only', () => ({}))
            mock.module('./server/HostMonitoring/host-monitoring.worker.ts', () => ({ createHostMonitoringWorker: value => (adapter = value, {}) }))
            mock.module('./server/HostMonitoring/host-monitoring.repository.ts', () => ({
                claimDueHostMonitoringPolicies: async limit => claims.splice(0, limit),
                completeHostMonitoringClaims: async () => { throw new Error('Cancelled probes must not complete') },
                readHostMonitoringClaimContexts: async items => items.map(() => ({ enabled: true, revision: 'revision', availability: {} })),
                pruneHostMonitoringHistory: async () => {}, readNextHostMonitoringAdmissionAt: async () => null,
                releaseHostMonitoringClaims: async items => {
                    for (const claim of items) {
                        calls.push(claim.policy.id)
                        if (originals.get(claim.policy.id) !== claim || leases.get(claim.policy.id) !== claim.leaseToken) throw new Error('Claim identity or token changed')
                    }
                    peakSubmitted = Math.max(peakSubmitted, ++submitted)
                    // Inject a pre-admission failure without recording SQL or modifying any lease.
                    if (mode === 'admission-error') {
                        await releaseBudget.promise
                        sqlEntered.resolve()
                        await releaseSql.promise
                        submitted--
                        throw failure
                    }
                    try {
                        return await withBackgroundDatabaseContext(() => admitDatabaseTransaction(async () => {
                            sqlCalls.push(...items.map(claim => claim.policy.id))
                            sqlEntered.resolve()
                            await releaseSql.promise
                            if (mode === 'sql-error') throw failure
                            for (const claim of items) leases.delete(claim.policy.id)
                            return items.map(() => true)
                        }), undefined, 'monitoring')
                    } finally { submitted-- }
                },
            }))
            mock.module('./server/HostMonitoring/host-monitoring.observability.ts', () => ({ sampleHostMonitoringBacklog: async () => {} }))
            mock.module('./server/Controller/domain-diagnostics.server.ts', () => ({ inspectControllerDomain: async (_input, signal) => {
                if (++probes === 64) allProbes.resolve()
                return new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(new Error('cancelled probe')), { once: true }))
            } }))
            mock.module('./server/WebSockets/applicationChanges.ts', () => ({ publishApplicationChange: () => {}, subscribeToApplicationChanges: () => () => {} }))
            await import('./server/HostMonitoring/host-monitoring.worker.server.ts')
            const worker = createWorker({ ...adapter, schedule: () => () => {} })
            await worker.runOnce()
            await allProbes.promise
            const occupied = Promise.withResolvers(), releaseBudget = Promise.withResolvers()
            let holding = 0, stopped = false
            const held = Array.from({ length: BACKGROUND_DATABASE_POLICY.classes.monitoring.running }, () => withBackgroundDatabaseContext(() => admitDatabaseTransaction(async () => {
                if (++holding === BACKGROUND_DATABASE_POLICY.classes.monitoring.running) occupied.resolve()
                await releaseBudget.promise
            }), undefined, 'monitoring'))
            await occupied.promise
            const stopping = worker.stop().then(() => ({ status: 'clean' }), error => ({ status: 'failed', sameError: error === failure }))
                .then(result => { stopped = true; return result })
            await Bun.sleep(10)
            const snapshot = backgroundDatabaseResourceSnapshot().monitoring
            const before = { holding, sqlCalls: sqlCalls.length, stopped, monitoringRunning: snapshot.running, monitoringPending: snapshot.pending }
            releaseBudget.resolve()
            await sqlEntered.promise
            await Bun.sleep(5)
            const during = { stopped }
            releaseSql.resolve()
            const result = await stopping
            await Promise.all(held)
            const repeated = await worker.stop().then(() => 'clean', error => error === failure ? 'same failure' : 'different failure')
            console.log(JSON.stringify({ before, during, result, repeated, calls: calls.length, uniqueCalls: new Set(calls).size,
                sqlCalls: sqlCalls.length, uniqueSqlCalls: new Set(sqlCalls).size, remainingLeases: leases.size, peakSubmitted }))
        `,
            { cwd: new URL('../../..', import.meta.url) },
        )
        const failure = mode !== 'success'
        expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
            before: {
                holding: 2,
                sqlCalls: 0,
                stopped: false,
                monitoringRunning: 2,
                monitoringPending: mode === 'admission-error' ? 0 : 1,
            },
            during: { stopped: false },
            result: failure ? { status: 'failed', sameError: true } : { status: 'clean' },
            repeated: failure ? 'same failure' : 'clean',
            calls: 64,
            uniqueCalls: 64,
            sqlCalls: mode === 'admission-error' ? 0 : 64,
            uniqueSqlCalls: mode === 'admission-error' ? 0 : 64,
            remainingLeases: failure ? 64 : 0,
            peakSubmitted: 1,
        })
    },
)
