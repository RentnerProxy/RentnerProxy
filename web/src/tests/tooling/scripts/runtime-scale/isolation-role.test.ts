import { expect, spyOn, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'
import { readIsolationPoolSaturation } from '../../../../../../scripts/runtime-scale/isolation-role.ts'
import {
    assertIsolationPoolSaturation,
    hasCompleteIsolationPoolActivity,
    measureSsrUnderPoolActivity,
    isolationPoolFaultBudget,
} from '../../../../../../scripts/runtime-scale/isolation-faults.ts'
import { requestIsolationRole } from '../../../../../../scripts/runtime-scale/isolation-run.ts'

test('the bounded pool fault admits the worst threshold-compliant sequential SSR series', () => {
    const budget = isolationPoolFaultBudget
    const worst = 99 * 500 + budget.requestMs
    expect(budget.samples).toBe(100)
    expect(worst).toBe(65_500)
    expect(budget.measurementMs).toBeGreaterThan(worst + budget.activityReadMs)
    expect(budget.poolClientMs).toBeGreaterThanOrEqual(budget.readinessMs + budget.measurementMs + 5_000)
    expect(budget.poolServerSeconds * 1_000).toBeGreaterThan(budget.poolClientMs)
})

test('an aborted activity read cancels its exact SQL query and awaits actual settlement and connection release', async () => {
    const output = await runBunScript(`
        import assert from 'node:assert/strict'
        import { activeIsolationPoolQueries } from './scripts/runtime-scale/isolation-faults.ts'
        const started = Promise.withResolvers(), completed = Promise.withResolvers(), released = Promise.withResolvers()
        const controller = new AbortController(), reason = new Error('activity deadline')
        let cancels = 0, releases = 0, settled = false
        const query = Object.assign(completed.promise, { cancel: () => { cancels++; return query } })
        const unsafe = () => { started.resolve(); return query }
        const database = { unsafe, reserve: async ({ signal }) => {
            assert.equal(signal.aborted, false)
            return { unsafe, release: async () => { releases++; await released.promise } }
        } }
        const operation = activeIsolationPoolQueries(database, controller.signal).then(value => ({ value }), error => ({ error }))
            .then(result => { settled = true; return result })
        await started.promise
        try {
            controller.abort(reason)
            await Bun.sleep(0)
            assert.equal(cancels, 1)
            assert.equal(settled, false)
            assert.equal(releases, 0)
            completed.reject(new Error('query cancelled at server'))
            await Bun.sleep(0)
            assert.equal(settled, false)
            assert.equal(releases, 1)
            released.resolve()
            assert.equal((await operation).error, reason)
            console.log(JSON.stringify({ cancels, releases, settled }))
        } finally { completed.resolve([{ active: 8, pids: 8 }]); released.resolve(); await operation }
    `, { cwd: new URL('../../../../../..', import.meta.url) })
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({ cancels: 1, releases: 1, settled: true })
})

test('pool client requests retain the outer abort signal and use only the declared path-specific deadline', async () => {
    const deadlines: number[] = []
    const captured: AbortSignal[] = []
    const timeout = spyOn(AbortSignal, 'timeout').mockImplementation((milliseconds) => {
        deadlines.push(milliseconds)
        return new AbortController().signal
    })
    const fetcher = spyOn(globalThis, 'fetch').mockImplementation(Object.assign(async (_input: RequestInfo | URL, options?: RequestInit) => {
        captured.push(options!.signal!)
        return new Response()
    }, { preconnect: globalThis.fetch.preconnect }))
    const owner = new AbortController()
    try {
        const child = { ready: { port: 12345, pid: 1, uid: 20004 } }
        await requestIsolationRole(child, '/pool', 'POST', owner.signal)
        await requestIsolationRole(child, '/ssr', 'GET', owner.signal)
        expect(deadlines).toEqual([80_000, 16_000])
        owner.abort()
        expect(captured.every(signal => signal.aborted)).toBe(true)
    } finally { timeout.mockRestore(); fetcher.mockRestore() }
})

test('the actual pool route extends only its own Bun request timeout before dispatching SQL', async () => {
    const output = await runBunScript(`
        import assert from 'node:assert/strict'
        import { mock } from 'bun:test'
        const isolation = await import('./scripts/runtime-scale/isolation.ts')
        mock.module('./scripts/runtime-scale/isolation.ts', () => ({ ...isolation, assertIsolationRole: () => {} }))
        mock.module('@tanstack/react-start/server-only', () => ({}))
        await import('node:fs/promises')
        mock.module('node:fs/promises', () => ({ readdir: async () => [] }))
        let calls = 0
        mock.module('./web/src/db/index.ts', () => ({ db: { execute: async query => {
            if (query === 'select 1') return []
            assert.equal(query, 'select pg_advisory_xact_lock(1380864088)')
            calls++; return []
        } }, closeDatabaseForRestoreMaintenance: async () => {} }))
        mock.module('./web/src/server/ProxyRuntime/proxy-runtime-data.ts', () => ({ readProxyRuntimeSnapshot: async () => ({}) }))
        mock.module('./web/src/server/ProxyRuntime/proxy-reconcile.ts', () => ({ createProxyReconciler: () => Object.assign(async () => {}, { start: () => {}, stop: async () => {} }) }))
        mock.module('./web/src/server/HostMonitoring/host-monitoring.worker.ts', () => ({ createHostMonitoringWorker: () => ({ start: () => {}, stop: async () => {} }) }))
        mock.module('./web/src/server/HostMonitoring/host-monitoring.repository.ts', () => ({
            claimDueHostMonitoringPolicies: async () => [], releaseHostMonitoringClaim: async () => false,
            readHostMonitoringClaimContexts: async () => [], completeHostMonitoringClaims: async () => [], pruneHostMonitoringHistory: async () => {},
        }))
        const timeouts = []
        let handler
        Bun.serve = options => {
            assert.equal(options.idleTimeout, undefined)
            handler = options.fetch
            return { port: 12345, stop: async () => {} }
        }
        process.getuid = () => 20004
        const { runIsolationRole } = await import('./scripts/runtime-scale/isolation-role.ts')
        await runIsolationRole('background', 12345)
        const server = { timeout: (request, seconds) => { assert.equal(new URL(request.url).pathname, '/pool'); timeouts.push(seconds) } }
        const request = path => new Request('http://127.0.0.1:12345'+path, { method: 'POST', headers: { 'x-isolation-control': 'fixture' } })
        assert.equal((await handler(request('/unknown'), server)).status, 404)
        assert.deepEqual(timeouts, [])
        assert.equal((await handler(request('/pool'), server)).status, 200)
        assert.equal((await handler(request('/stop'), server)).status, 200)
        assert.deepEqual(timeouts, [85])
        assert.equal(calls, 8)
        console.log(JSON.stringify({ timeouts, calls }))
    `, { cwd: new URL('../../../../../..', import.meta.url) })
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({ timeouts: [85], calls: 8 })
})

test('settled activity reads remove cancellation ownership and pre-aborted reads never reserve', async () => {
    const output = await runBunScript(`
        import assert from 'node:assert/strict'
        import { activeIsolationPoolQueries } from './scripts/runtime-scale/isolation-faults.ts'
        const owner = new AbortController()
        let reserves = 0, cancels = 0, releases = 0
        const database = { reserve: async () => {
            reserves++
            return { unsafe: () => Object.assign(Promise.resolve([{ active: 8, pids: 8 }]), { cancel: () => { cancels++ } }), release: async () => { releases++ } }
        } }
        assert.equal(await activeIsolationPoolQueries(database, owner.signal), true)
        owner.abort(new Error('finished owner'))
        await assert.rejects(activeIsolationPoolQueries(database, owner.signal), error => error === owner.signal.reason)
        assert.deepEqual({ reserves, cancels, releases }, { reserves: 1, cancels: 0, releases: 1 })
        console.log('settled')
    `, { cwd: new URL('../../../../../..', import.meta.url) })
    expect(output.trim()).toBe('settled')
})

test('activity reservation cancellation preserves the phase deadline before any SQL can start', async () => {
    const output = await runBunScript(`
        import assert from 'node:assert/strict'
        import { activeIsolationPoolQueries } from './scripts/runtime-scale/isolation-faults.ts'
        const owner = new AbortController(), reason = new Error('phase deadline')
        let cancels = 0
        const database = { reserve: ({ signal }) => new Promise((_resolve, reject) => {
            signal.addEventListener('abort', () => { cancels++; reject(new Error('reservation cancelled')) }, { once: true })
        }) }
        const read = activeIsolationPoolQueries(database, owner.signal)
        const rejection = assert.rejects(read, error => error === reason)
        owner.abort(reason)
        await rejection
        assert.equal(cancels, 1)
        console.log('cancelled')
    `, { cwd: new URL('../../../../../..', import.meta.url) })
    expect(output.trim()).toBe('cancelled')
})

test('the activity read deadline cancels at two seconds and still owns delayed query settlement', async () => {
    const output = await runBunScript(`
        import assert from 'node:assert/strict'
        import { jest } from 'bun:test'
        import { activeIsolationPoolQueries } from './scripts/runtime-scale/isolation-faults.ts'
        const originalTimeout = AbortSignal.timeout
        const started = Promise.withResolvers(), completed = Promise.withResolvers()
        let cancels = 0, releases = 0, settled = false
        jest.useFakeTimers()
        AbortSignal.timeout = milliseconds => {
            assert.equal(milliseconds, 2000)
            const controller = new AbortController()
            setTimeout(() => controller.abort(new DOMException('activity deadline', 'TimeoutError')), milliseconds)
            return controller.signal
        }
        const query = Object.assign(completed.promise, { cancel: () => { cancels++; return query } })
        const database = { reserve: async () => ({ unsafe: () => { started.resolve(); return query }, release: async () => { releases++ } }) }
        const operation = activeIsolationPoolQueries(database).then(value => ({ value }), error => ({ error })).then(result => { settled = true; return result })
        try {
            await started.promise
            jest.advanceTimersByTime(1999)
            assert.equal(cancels, 0)
            jest.advanceTimersByTime(1)
            assert.equal(cancels, 1)
            assert.equal(settled, false)
            assert.equal(releases, 0)
            completed.reject(new Error('server cancellation settled'))
            assert.equal((await operation).error.name, 'TimeoutError')
            assert.equal(releases, 1)
            console.log('bounded')
        } finally { AbortSignal.timeout = originalTimeout; completed.resolve([{ active: 8, pids: 8 }]); await operation; jest.useRealTimers() }
    `, { cwd: new URL('../../../../../..', import.meta.url) })
    expect(output.trim()).toBe('bounded')
})

test('role imports remain inert and reject an unflagged identity before database imports', async () => {
    const output = await runBunScript(
        `
        import assert from 'node:assert/strict'
        delete process.env.RENTNERPROXY_ISOLATION_FIXTURE
        delete process.env.DATABASE_URL
        const { runIsolationRole } = await import('./scripts/runtime-scale/isolation-role.ts')
        await assert.rejects(runIsolationRole('background', 12345), /Explicit unprivileged isolation role required/)
        console.log('guarded')
    `,
        { cwd: new URL('../../../../../..', import.meta.url) },
    )
    expect(output.trim()).toBe('guarded')
})

test.each(['probe', 'late claim'] as const)(
    'isolation %s cleanup drains all 64 original leases through the real monitoring gate',
    async (phase) => {
        const output = await runBunScript(
            `
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
        `,
            { cwd: new URL('../../../../../..', import.meta.url) },
        )
        expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
            calls: 64,
            peak: 2,
            sql: 64,
            probes: phase === 'probe' ? 64 : 48,
            stopped: true,
        })
    },
)

test.each(['sql', 'admission'] as const)(
    'isolation cleanup retains failed drain ownership after a permanent %s failure',
    async (mode) => {
        const output = await runBunScript(
            `
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
        `,
            { cwd: new URL('../../../../../..', import.meta.url) },
        )
        expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
            calls: 64,
            typed: 64,
            sql: mode === 'sql' ? 64 : 0,
            settled: true,
        })
    },
)

test.each([0, 3, 8])(
    'pool saturation records only fulfilled SQL queries: %s failures',
    async (failures) => {
        const sibling = Promise.withResolvers<void>()
        let calls = 0,
            settled = false
        const operation = readIsolationPoolSaturation(async () => {
            const index = calls++
            if (index === 7) await sibling.promise
            if (index < failures) throw new Error('injected query failure')
        }, false).then((response) => {
            settled = true
            return response
        })
        await Bun.sleep(0)
        expect(calls).toBe(8)
        expect(settled).toBe(false)
        sibling.resolve()
        const response = await operation
        expect(response.status).toBe(failures ? 503 : 200)
        expect(await response.json()).toEqual({
            requested: 8,
            succeeded: 8 - failures,
            failed: failures,
        })
    },
)

test('a stopping role refuses pool work before issuing SQL', async () => {
    let calls = 0
    const response = await readIsolationPoolSaturation(async () => {
        calls++
    }, true)
    expect(response.status).toBe(503)
    expect(calls).toBe(0)
})

test('synchronous pool-query failures still settle every requested sibling', async () => {
    let calls = 0
    const response = await readIsolationPoolSaturation(() => {
        calls++
        throw new Error('SQL dispatch failure')
    }, false)
    expect(calls).toBe(8)
    expect(response.status).toBe(503)
    expect(await response.json()).toEqual({ requested: 8, succeeded: 0, failed: 8 })
})

test('the fault caller requires successful status and exact complete saturation evidence', async () => {
    await expect(
        assertIsolationPoolSaturation(Response.json({ requested: 8, succeeded: 8, failed: 0 })),
    ).resolves.toBeUndefined()
    await Promise.all(
        [
            Response.json({ requested: 8, succeeded: 8, failed: 0 }, { status: 503 }),
            Response.json({ requested: 8, succeeded: 7, failed: 1 }),
            Response.json({ requested: 0, succeeded: 0, failed: 0 }),
            Response.json({ settled: 8 }),
        ].map((response) => expect(assertIsolationPoolSaturation(response)).rejects.toThrow()),
    )
})

test('pool saturation evidence rejects queued work without eight active PostgreSQL worker PIDs', () => {
    expect(hasCompleteIsolationPoolActivity({ active: 8, pids: 8 })).toBe(true)
    expect(hasCompleteIsolationPoolActivity({ active: 0, pids: 0 })).toBe(false)
    expect(hasCompleteIsolationPoolActivity({ active: 8, pids: 7 })).toBe(false)
})

test('SSR rejection is observed while the measured pool promise remains owned', async () => {
    const outcome = { status: 'rejected' as const, reason: new Error('SSR failed') }
    let reads = 0
    await expect(
        measureSsrUnderPoolActivity(Promise.resolve(outcome), async () => {
            reads++
            return false
        }),
    ).resolves.toEqual({ held: false, outcome })
    expect(reads).toBe(1)
})

test('completed SSR still requires a final live PostgreSQL activity sample', async () => {
    let reads = 0
    const result = await measureSsrUnderPoolActivity(
        Promise.resolve({ status: 'fulfilled' as const, value: 100 }),
        async () => {
            reads++
            return false
        },
    )
    expect(result.held).toBe(false)
    expect(result.outcome).toEqual({ status: 'fulfilled', value: 100 })
    expect(reads).toBe(1)
})

test('pool activity loss aborts the owned SSR sequence and fails its overlap evidence', async () => {
    const measurement = Promise.withResolvers<{
        status: 'fulfilled'
        value: number
    }>()
    let reads = 0
    let aborted = 0
    const result = await measureSsrUnderPoolActivity(
        measurement.promise,
        async () => ++reads === 1,
        () => {
            aborted++
            measurement.resolve({ status: 'fulfilled', value: 100 })
        },
    )
    expect(result.held).toBe(false)
    expect(result.outcome).toEqual({ status: 'fulfilled', value: 100 })
    expect(aborted).toBe(1)
    expect(reads).toBe(3)
})
