import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'
import { readIsolationPoolSaturation } from '../../../../../../scripts/runtime-scale/isolation-role.ts'
import { assertIsolationPoolSaturation } from '../../../../../../scripts/runtime-scale/isolation-faults.ts'

test('role imports remain inert and reject an unflagged identity before database imports', async () => {
    const output = await runBunScript(`
        import assert from 'node:assert/strict'
        delete process.env.RENTNERPROXY_ISOLATION_FIXTURE
        delete process.env.DATABASE_URL
        const { runIsolationRole } = await import('./scripts/runtime-scale/isolation-role.ts')
        await assert.rejects(runIsolationRole('background', 12345), /Explicit unprivileged isolation role required/)
        console.log('guarded')
    `, { cwd: new URL('../../../../../..', import.meta.url) })
    expect(output.trim()).toBe('guarded')
})

test.each(['probe', 'late claim'] as const)(
    'isolation %s cleanup drains all 64 original leases through the real monitoring gate',
    async (phase) => {
        const output = await runBunScript(`
            import assert from 'node:assert/strict'
            import { createIsolationClaimRelease } from './scripts/runtime-scale/isolation-role.ts'
            import { createHostMonitoringWorker } from './web/src/server/HostMonitoring/host-monitoring.worker.ts'
            import { admitDatabaseTransaction, withBackgroundDatabaseContext, backgroundDatabaseResourceSnapshot } from './web/src/db/background-admission.ts'
            const owner = new AbortController()
            const ready = Promise.withResolvers(), late = Promise.withResolvers(), entered = Promise.withResolvers(), sqlGate = Promise.withResolvers()
            const occupied = Promise.withResolvers(), freeBudget = Promise.withResolvers()
            const claims = Array.from({ length: 64 }, (_, id) => ({ policy: { id: 'policy-'+id, version: id }, leaseToken: 'token-'+id }))
            const leases = new Map(claims.map(claim => [claim.policy.id, claim.leaseToken]))
            let claimed = 0, probes = 0, calls = 0, submitted = 0, peak = 0, sql = 0, holding = 0, stopped = false
            const release = createIsolationClaimRelease(async claim => {
                assert.equal(claim, claims.find(original => original === claim))
                assert.equal(claim.leaseToken, 'token-'+claim.policy.version)
                calls++; peak = Math.max(peak, ++submitted)
                try {
                    return await admitDatabaseTransaction(async resource => {
                        assert.equal(resource, 'monitoring')
                        if (++sql === 2) entered.resolve()
                        await sqlGate.promise
                        if (leases.get(claim.policy.id) !== claim.leaseToken) return false
                        leases.delete(claim.policy.id)
                        return true
                    })
                } finally { submitted-- }
            })
            const worker = createHostMonitoringWorker({
                claim: async limit => {
                    const batch = claims.slice(claimed, claimed + limit)
                    claimed += batch.length
                    if ('${phase}' === 'late claim' && claimed === 64) { ready.resolve(); await late.promise }
                    return batch
                },
                runClaim: (claim, signal) => withBackgroundDatabaseContext(async () => {
                    probes++
                    if (probes === 64) ready.resolve()
                    try { await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true })) }
                    finally { await release(claim) }
                }, owner.signal, 'monitoring'),
                releaseCancelled: async claim => { await release(claim) },
                prune: async () => {}, unavailable: () => {}, schedule: () => () => {},
            })
            const running = withBackgroundDatabaseContext(() => worker.runOnce(), owner.signal, 'monitoring')
            await ready.promise
            const held = Array.from({ length: 2 }, () => withBackgroundDatabaseContext(() => admitDatabaseTransaction(async () => {
                if (++holding === 2) occupied.resolve()
                await freeBudget.promise
            }), undefined, 'monitoring'))
            await occupied.promise
            owner.abort()
            const stopping = worker.stop().then(() => { stopped = true })
            late.resolve()
            await Bun.sleep(10)
            assert.equal(sql, 0)
            assert.equal(stopped, false)
            assert.equal(backgroundDatabaseResourceSnapshot().monitoring.pending, 2)
            leases.set(claims[0].policy.id, 'superseding-token')
            freeBudget.resolve()
            await entered.promise
            assert.equal(stopped, false)
            sqlGate.resolve()
            await Promise.all([stopping, running, ...held])
            await worker.stop()
            assert.deepEqual([...leases], [[claims[0].policy.id, 'superseding-token']])
            console.log(JSON.stringify({ calls, peak, sql, probes, stopped }))
        `, { cwd: new URL('../../../../../..', import.meta.url) })
        expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
            calls: 64, peak: 2, sql: 64, probes: phase === 'probe' ? 64 : 48, stopped: true,
        })
    },
)

test.each(['sql', 'admission'] as const)(
    'isolation cleanup retains failed drain ownership after a permanent %s failure',
    async (mode) => {
        const output = await runBunScript(`
            import assert from 'node:assert/strict'
            import { createIsolationClaimRelease } from './scripts/runtime-scale/isolation-role.ts'
            import { createHostMonitoringWorker } from './web/src/server/HostMonitoring/host-monitoring.worker.ts'
            import { HostMonitoringCleanupError } from './web/src/server/HostMonitoring/Types/host-monitoring-worker.types.ts'
            import { admitDatabaseTransaction, withBackgroundDatabaseContext, backgroundDatabaseResourceSnapshot } from './web/src/db/background-admission.ts'
            const ready = Promise.withResolvers(), free = Promise.withResolvers(), tail = Promise.withResolvers(), allCalls = Promise.withResolvers()
            const owner = new AbortController(), sqlFailure = new Error('permanent fixture SQL failure')
            let count = 0, calls = 0, sql = 0, firstFailure, typed = 0, settled = false
            const held = '${mode}' === 'admission' ? Array.from({ length: 10 }, () => withBackgroundDatabaseContext(() => admitDatabaseTransaction(() => free.promise), undefined, 'monitoring')) : []
            const release = createIsolationClaimRelease(async claim => {
                if (++calls === 64) allCalls.resolve()
                try {
                    return await admitDatabaseTransaction(async () => {
                        sql++
                        if (claim === 63) await tail.promise
                        throw sqlFailure
                    })
                } catch (error) { firstFailure ??= error; throw error }
            })
            const worker = createHostMonitoringWorker({
                claim: async limit => Array.from({ length: Math.min(limit, 64-count) }, () => count++),
                runClaim: async (claim, signal) => {
                    if (claim === 63) ready.resolve()
                    await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }))
                    try { await release(claim) } catch (error) { if (error instanceof HostMonitoringCleanupError && error.cause) typed++; throw error }
                },
                releaseCancelled: async claim => { await release(claim) }, prune: async () => {}, unavailable: () => {}, schedule: () => () => {},
            })
            await worker.runOnce()
            await ready.promise
            const stopping = worker.stop().then(() => { settled = true; return 'false clean' }, error => { settled = true; assert.equal(error, firstFailure); return 'failed' })
            await allCalls.promise
            if ('${mode}' === 'sql') assert.equal(settled, false)
            tail.resolve()
            assert.equal(await stopping, 'failed')
            assert.equal(await worker.stop().then(() => false, error => error === firstFailure), true)
            assert.equal(typed, 64)
            assert.equal(calls, 64)
            assert.equal(sql, '${mode}' === 'sql' ? 64 : 0)
            if ('${mode}' === 'admission') assert.equal(firstFailure.reason, 'busy')
            free.resolve()
            await Promise.all(held)
            assert.equal(backgroundDatabaseResourceSnapshot().monitoring.running, 0)
            console.log(JSON.stringify({ calls, typed, sql, settled }))
        `, { cwd: new URL('../../../../../..', import.meta.url) })
        expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
            calls: 64, typed: 64, sql: mode === 'sql' ? 64 : 0, settled: true,
        })
    },
)

test.each([0, 3, 8])('pool saturation records only fulfilled SQL queries: %s failures', async (failures) => {
    const sibling = Promise.withResolvers<void>()
    let calls = 0, settled = false
    const operation = readIsolationPoolSaturation(async () => {
        const index = calls++
        if (index === 7) await sibling.promise
        if (index < failures) throw new Error('injected query failure')
    }, false).then(response => { settled = true; return response })
    await Bun.sleep(0)
    expect(calls).toBe(8)
    expect(settled).toBe(false)
    sibling.resolve()
    const response = await operation
    expect(response.status).toBe(failures ? 503 : 200)
    expect(await response.json()).toEqual({ requested: 8, succeeded: 8-failures, failed: failures })
})

test('a stopping role refuses pool work before issuing SQL', async () => {
    let calls = 0
    const response = await readIsolationPoolSaturation(async () => { calls++ }, true)
    expect(response.status).toBe(503)
    expect(calls).toBe(0)
})

test('synchronous pool-query failures still settle every requested sibling', async () => {
    let calls = 0
    const response = await readIsolationPoolSaturation(() => { calls++; throw new Error('SQL dispatch failure') }, false)
    expect(calls).toBe(8)
    expect(response.status).toBe(503)
    expect(await response.json()).toEqual({ requested: 8, succeeded: 0, failed: 8 })
})

test('the fault caller requires successful status and exact complete saturation evidence', async () => {
    await expect(assertIsolationPoolSaturation(Response.json({ requested: 8, succeeded: 8, failed: 0 }))).resolves.toBeUndefined()
    await Promise.all([
        Response.json({ requested: 8, succeeded: 8, failed: 0 }, { status: 503 }),
        Response.json({ requested: 8, succeeded: 7, failed: 1 }),
        Response.json({ requested: 0, succeeded: 0, failed: 0 }),
        Response.json({ settled: 8 }),
    ].map(response => expect(assertIsolationPoolSaturation(response)).rejects.toThrow()))
})
