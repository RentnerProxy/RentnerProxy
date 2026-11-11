import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test.each(['success', 'sql-error', 'admission-error'] as const)(
    '64 cancelled leases drain through bounded shared SQL admission: %s',
    async (mode) => {
        const output = await runBunScript(
            `
            import { mock } from 'bun:test'
            import { admitDatabaseTransaction, withBackgroundDatabaseContext } from './db/background-admission.ts'
            import { DATABASE_CONNECTION_BUDGET } from './config/database.config.ts'
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
                pruneHostMonitoringHistory: async () => {},
                releaseHostMonitoringClaim: claim => {
                    calls.push(claim.policy.id)
                    peakSubmitted = Math.max(peakSubmitted, ++submitted)
                    const pending = mode === 'admission-error' && claim.policy.id === '0' ? Promise.reject(failure) : admitDatabaseTransaction(async () => {
                        sqlCalls.push(claim.policy.id)
                        if (originals.get(claim.policy.id) !== claim || leases.get(claim.policy.id) !== claim.leaseToken) throw new Error('Claim identity or token changed')
                        sqlEntered.resolve()
                        if (mode === 'sql-error' && claim.policy.id === '0') throw failure
                        await releaseSql.promise
                        leases.delete(claim.policy.id)
                        return true
                    })
                    return pending.finally(() => { submitted-- })
                },
            }))
            mock.module('./server/HostMonitoring/host-monitoring.observability.ts', () => ({ sampleHostMonitoringBacklog: async () => {} }))
            mock.module('./server/Controller/domain-diagnostics.server.ts', () => ({ inspectControllerDomain: async (_input, signal) => {
                if (++probes === 64) allProbes.resolve()
                return new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(new Error('cancelled probe')), { once: true }))
            } }))
            mock.module('./server/WebSockets/applicationChanges.ts', () => ({ publishApplicationChange: () => {} }))
            await import('./server/HostMonitoring/host-monitoring.worker.server.ts')
            const worker = createWorker({ ...adapter, schedule: () => () => {} })
            await worker.runOnce()
            await allProbes.promise
            const occupied = Promise.withResolvers(), releaseBudget = Promise.withResolvers()
            let holding = 0, stopped = false
            const held = Array.from({ length: DATABASE_CONNECTION_BUDGET.backgroundTransactions }, () => withBackgroundDatabaseContext(() => admitDatabaseTransaction(async () => {
                if (++holding === DATABASE_CONNECTION_BUDGET.backgroundTransactions) occupied.resolve()
                await releaseBudget.promise
            })))
            await occupied.promise
            const stopping = worker.stop().then(() => ({ status: 'clean' }), error => ({ status: 'failed', sameError: error === failure }))
                .then(result => { stopped = true; return result })
            await Bun.sleep(10)
            const before = { holding, sqlCalls: sqlCalls.length, stopped }
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
            before: { holding: 8, sqlCalls: 0, stopped: false },
            during: { stopped: false },
            result: failure ? { status: 'failed', sameError: true } : { status: 'clean' },
            repeated: failure ? 'same failure' : 'clean',
            calls: 64,
            uniqueCalls: 64,
            sqlCalls: mode === 'admission-error' ? 63 : 64,
            uniqueSqlCalls: mode === 'admission-error' ? 63 : 64,
            remainingLeases: failure ? 1 : 0,
            peakSubmitted: 8,
        })
    },
)
