// oxlint-disable no-await-in-loop -- Fault injection waits for durable leases and exact owned-process outcomes.
import assert from 'node:assert/strict'
import type { SQL } from 'bun'
import { isolationContract } from './isolation.config.ts'
import { isolationDistribution } from './isolation.ts'
import type { IsolationChild, IsolationLoad } from './Types/isolation.types.ts'

interface FaultContext {
    database: SQL
    signal?: AbortSignal
    seed: (load: IsolationLoad) => Promise<void>
    spawnRole: (
        role: 'foreground' | 'combined' | 'background',
        denied?: boolean,
    ) => Promise<IsolationChild>
    request: (
        child: IsolationChild,
        path: string,
        method?: string,
        signal?: AbortSignal,
    ) => Promise<Response>
    stopChild: (child: IsolationChild) => Promise<number>
    bounded: <T>(operation: Promise<T>, milliseconds: number) => Promise<T>
    command: (args: string[], timeoutMs?: number) => Promise<string>
    snapshotPath: string
}

type Settled<T> = { status: 'fulfilled'; value: T } | { status: 'rejected'; reason: unknown }
const isolationPoolBarrierKey = 1_380_864_088
export const isolationPoolFaultBudget = {
    samples: 100,
    requestMs: 16_000,
    readinessMs: 5_000,
    // 99 requests at p99=500ms plus one request at its 16s deadline take 65.5s.
    measurementMs: 70_000,
    poolClientMs: 80_000,
    poolServerSeconds: 85,
    activityReadMs: 2_000,
} as const

/** A completed HTTP request is not proof that every requested saturation query succeeded. */
export async function assertIsolationPoolSaturation(response: Response): Promise<void> {
    if (response.status !== 200) {
        await response.body?.cancel()
        throw new Error('Isolation pool saturation failed')
    }
    assert.deepEqual(await response.json(), {
        requested: isolationContract.workerConnections,
        succeeded: isolationContract.workerConnections,
        failed: 0,
    })
}

/** Accept saturation only when PostgreSQL reports eight distinct active worker backends for the exact blocked fixture query. */
export function hasCompleteIsolationPoolActivity(activity: {
    active: number | string
    pids: number | string
}) {
    return (
        Number(activity.active) === isolationContract.workerConnections &&
        Number(activity.pids) === isolationContract.workerConnections
    )
}

/** Observe only the dedicated worker role and exact advisory-lock statement issued by the private pool fault. */
export async function activeIsolationPoolQueries(
    database: Pick<SQL, 'reserve'>,
    signal?: AbortSignal,
) {
    const deadline = AbortSignal.timeout(isolationPoolFaultBudget.activityReadMs)
    const aborted = signal ? AbortSignal.any([signal, deadline]) : deadline
    aborted.throwIfAborted()
    const connection = await database.reserve({ signal: aborted }).catch((error: unknown) => {
        aborted.throwIfAborted()
        throw error
    })
    const result = await (async () => {
        aborted.throwIfAborted()
        // One query on an exclusive connection cannot be cancelled as a still-running pipelined sibling.
        const query = connection.unsafe<{ active: number; pids: number }[]>(
            "select count(*)::int as active,count(distinct pid)::int as pids from pg_stat_activity where usename='isolation_worker' and datname=current_database() and state='active' and wait_event_type='Lock' and query='select pg_advisory_xact_lock(1380864088)'",
        )
        const cancel = () => {
            query.cancel()
        }
        aborted.addEventListener('abort', cancel, { once: true })
        if (aborted.aborted) cancel()
        try {
            const rows = await query
            aborted.throwIfAborted()
            return hasCompleteIsolationPoolActivity(rows[0] ?? { active: 0, pids: 0 })
        } catch (error) {
            aborted.throwIfAborted()
            throw error
        } finally {
            aborted.removeEventListener('abort', cancel)
        }
    })().then(
        (value) => ({ status: 'fulfilled' as const, value }),
        (reason: unknown) => ({ status: 'rejected' as const, reason }),
    )
    try {
        await connection.release()
    } catch (error) {
        if (result.status === 'rejected' || aborted.aborted)
            throw new AggregateError(
                [result.status === 'rejected' ? result.reason : aborted.reason, error],
                'Isolation activity cancellation cleanup failed',
                { cause: error },
            )
        throw error
    }
    if (result.status === 'rejected') throw result.reason
    aborted.throwIfAborted()
    return result.value
}

/** Sample actual PostgreSQL activity while SSR runs; each loop iteration races measurement completion against the next observation tick. */
export async function measureSsrUnderPoolActivity<T>(
    measurement: Promise<Settled<T>>,
    readActivity: () => Promise<boolean>,
    onActivityLost: () => void = () => {},
) {
    let held = true
    while (true) {
        const next = await Promise.race([
            measurement.then((outcome) => ({ done: true as const, outcome })),
            Bun.sleep(25).then(() => ({ done: false as const })),
        ])
        if (next.done) {
            const finalActivity = await readActivity()
            return { held: held && finalActivity, outcome: next.outcome }
        }
        const active = await readActivity()
        if (!active) onActivityLost()
        held = active && held
    }
}

/** Poll a durable checkpoint every 25 milliseconds until its deadline; propagate checkpoint failures and reject an unmet deadline. */
async function waitFor(test: () => Promise<boolean>, limit: number) {
    const deadline = performance.now() + limit
    while (performance.now() < deadline) {
        if (await test()) return
        await Bun.sleep(25)
    }
    throw new Error('Isolation fault checkpoint timed out')
}

/** Exercise disposable role failures and writer drains, recording only completed assertions and leaving production parity explicitly unproven. */
export async function runIsolationFaults(context: FaultContext) {
    const { database, seed, spawnRole, request, stopChild, bounded } = context
    const roles = new Set<IsolationChild>()
    /** Track every successfully started role so each fault case owns its emergency cleanup. */
    async function spawn(role: 'foreground' | 'combined' | 'background') {
        const child = await spawnRole(role)
        roles.add(child)
        return child
    }
    /** Remove a role from cleanup ownership only after its graceful shutdown succeeds. */
    async function stop(child: IsolationChild) {
        const duration = await stopChild(child)
        roles.delete(child)
        return duration
    }
    /** Read durable observation totals, unique policies, and expected healthy outcomes for exact-progress assertions. */
    async function progress() {
        const rows = await database.unsafe(
            "select count(*)::int as total,count(distinct policy_id)::int as unique,count(*) filter(where status='healthy' and code='expected_status')::int as healthy from rentnerproxy.host_check_observations",
        )
        return {
            total: Number(rows[0]?.total),
            unique: Number(rows[0]?.unique),
            healthy: Number(rows[0]?.healthy),
        }
    }
    /** Count currently leased policies rather than infer in-flight work from process-local counters. */
    async function active() {
        const rows = await database.unsafe(
            'select count(*)::int as total from rentnerproxy.host_check_policies where lease_token is not null',
        )
        return Number(rows[0]?.total)
    }
    /** Measure sequential successful foreground responses; fail the case on a bad status or missing rendered fixture content. */
    async function ssr(child: IsolationChild, count: number, signal?: AbortSignal) {
        const values: number[] = []
        for (let index = 0; index < count; index++) {
            signal?.throwIfAborted()
            const start = performance.now()
            const response = await request(child, '/ssr', 'GET', signal)
            signal?.throwIfAborted()
            assert.equal(response.status, 200)
            assert.ok((await response.text()).includes('Proxy hosts'))
            signal?.throwIfAborted()
            values.push(performance.now() - start)
        }
        return isolationDistribution(values)
    }
    const results: {
        case: string
        passed: boolean
        elapsedMs: number
        detail: Record<string, number | boolean | string | null>
    }[] = []
    /** Record a fault only after its assertions succeed; rethrow failures and always kill/reap roles still owned by that case. */
    async function record(
        name: string,
        operation: () => Promise<Record<string, number | boolean | string | null>>,
    ) {
        const start = performance.now()
        console.log(JSON.stringify({ isolationFaultStart: name }))
        try {
            const detail = await operation()
            const result = {
                case: name,
                passed: true,
                elapsedMs: performance.now() - start,
                detail,
            }
            results.push(result)
            console.log(JSON.stringify({ isolationFault: result }))
        } catch (error) {
            const message = error instanceof Error ? error.message : ''
            console.log(
                JSON.stringify({
                    isolationFaultFailure: name,
                    category:
                        message === 'Isolation role failed before readiness'
                            ? 'startup'
                            : message === 'Isolation fault checkpoint timed out'
                              ? 'checkpoint'
                              : 'unexpected',
                }),
            )
            throw error
        } finally {
            for (const child of roles) {
                if (child.process.exitCode === null) {
                    child.process.kill('SIGKILL')
                    await child.process.exited
                }
            }
            roles.clear()
        }
    }
    await record('duplicate-claimers-and-lost-wake', async () => {
        await seed(100)
        const first = await spawn('combined'),
            second = await spawn('background')
        await Promise.all([request(first, '/start', 'POST'), request(second, '/start', 'POST')])
        let maximum = 0
        await waitFor(async () => {
            maximum = Math.max(maximum, await active())
            return (await progress()).total >= 100
        }, 10_000)
        const final = await progress()
        assert.equal(final.total, 100)
        assert.equal(final.unique, 100)
        assert.equal(final.healthy, 100)
        assert.ok(maximum <= 64)
        await Promise.all([stop(first), stop(second)])
        return {
            observations: final.total,
            duplicates: final.total - final.unique,
            maximumLeases: maximum,
            durablePollingWithoutIpcHint: true,
        }
    })
    await record('independent-pool-saturation-and-partial-startup', async () => {
        await seed(100)
        const foreground = await spawn('foreground'),
            background = await spawn('background')
        const barrierReady = Promise.withResolvers<void>()
        const releaseBarrier = Promise.withResolvers<void>()
        const barrier = database
            .transaction(async (transaction) => {
                await transaction.unsafe(
                    'select pg_advisory_xact_lock(' + isolationPoolBarrierKey + ')',
                )
                barrierReady.resolve()
                await releaseBarrier.promise
            })
            .then(
                () => ({ status: 'fulfilled' as const }),
                (reason: unknown) => ({ status: 'rejected' as const, reason }),
            )
        const barrierReadyPromise = Promise.race([
            barrierReady.promise,
            barrier.then((outcome) => {
                throw outcome.status === 'rejected'
                    ? outcome.reason
                    : new Error('Isolation saturation barrier ended before release')
            }),
        ]).then(
            () => ({ status: 'fulfilled' as const, value: undefined }),
            (reason: unknown) => ({ status: 'rejected' as const, reason }),
        )
        let saturation: Promise<Settled<Response>> | undefined
        let poolReadiness: Promise<Settled<void>> | undefined
        let latencyPromise: Promise<Settled<ReturnType<typeof isolationDistribution>>> | undefined
        let measurementPromise:
            | Promise<
                  Settled<{
                      held: boolean
                      outcome: Settled<ReturnType<typeof isolationDistribution>>
                  }>
              >
            | undefined
        const ssrAbort = new AbortController()
        let latency: ReturnType<typeof isolationDistribution> | undefined
        let scenarioFailure: unknown
        let scenarioFailed = false
        let saturationHeldThroughSsr = false
        try {
            const ready = await bounded(barrierReadyPromise, isolationPoolFaultBudget.readinessMs)
            if (ready.status === 'rejected') throw ready.reason
            saturation = request(background, '/pool', 'POST').then(
                (response) => ({ status: 'fulfilled' as const, value: response }),
                (reason: unknown) => ({ status: 'rejected' as const, reason }),
            )
            const readinessSignal = AbortSignal.any([
                ssrAbort.signal,
                AbortSignal.timeout(isolationPoolFaultBudget.readinessMs),
                ...(context.signal ? [context.signal] : []),
            ])
            poolReadiness = waitFor(
                () => activeIsolationPoolQueries(database, readinessSignal),
                isolationPoolFaultBudget.readinessMs,
            ).then(
                () => ({ status: 'fulfilled' as const, value: undefined }),
                (reason: unknown) => ({ status: 'rejected' as const, reason }),
            )
            const poolReady = await bounded(poolReadiness, isolationPoolFaultBudget.readinessMs)
            if (poolReady.status === 'rejected') throw poolReady.reason
            const measurementSignal = AbortSignal.any([
                ssrAbort.signal,
                AbortSignal.timeout(isolationPoolFaultBudget.measurementMs),
                ...(context.signal ? [context.signal] : []),
            ])
            latencyPromise = ssr(
                foreground,
                isolationPoolFaultBudget.samples,
                measurementSignal,
            ).then(
                (value) => ({ status: 'fulfilled' as const, value }),
                (reason: unknown) => ({ status: 'rejected' as const, reason }),
            )
            measurementPromise = measureSsrUnderPoolActivity(
                latencyPromise,
                () => activeIsolationPoolQueries(database, measurementSignal),
                () => ssrAbort.abort(new Error('PostgreSQL saturation ended during SSR')),
            ).then(
                (value) => ({ status: 'fulfilled' as const, value }),
                (reason: unknown) => ({ status: 'rejected' as const, reason }),
            )
            const measurement = await bounded(
                measurementPromise,
                isolationPoolFaultBudget.measurementMs,
            )
            if (measurement.status === 'rejected') throw measurement.reason
            const measured = measurement.value
            if (measured.outcome.status === 'rejected') throw measured.outcome.reason
            latency = measured.outcome.value
            saturationHeldThroughSsr = measured.held
            assert.ok(
                saturationHeldThroughSsr,
                'All eight PostgreSQL worker backends remained active for the complete SSR series',
            )
        } catch (error) {
            scenarioFailure = error
            scenarioFailed = true
            ssrAbort.abort(error)
        } finally {
            releaseBarrier.resolve()
        }
        const [
            barrierResult,
            barrierReadyResult,
            poolReadinessResult,
            saturationResult,
            latencyResult,
            measurementResult,
        ] = await Promise.all([
            barrier,
            barrierReadyPromise,
            poolReadiness ?? Promise.resolve(undefined),
            saturation ?? Promise.resolve(undefined),
            latencyPromise ?? Promise.resolve(undefined),
            measurementPromise ?? Promise.resolve(undefined),
        ])
        if (scenarioFailed) throw scenarioFailure
        if (barrierResult.status === 'rejected') throw barrierResult.reason
        if (barrierReadyResult.status === 'rejected') throw barrierReadyResult.reason
        if (poolReadinessResult?.status === 'rejected') throw poolReadinessResult.reason
        if (!saturationResult) throw new Error('Isolation pool saturation was not started')
        if (saturationResult.status === 'rejected') throw saturationResult.reason
        await assertIsolationPoolSaturation(saturationResult.value)
        if (latencyResult?.status === 'rejected') throw latencyResult.reason
        if (measurementResult?.status === 'rejected') throw measurementResult.reason
        assert.ok(latency)
        assert.ok(latency.p99 !== null && latency.p99 <= isolationContract.thresholds.ssrP99Ms)
        let denied = false
        try {
            const unexpected = await spawnRole('background', true)
            unexpected.process.kill('SIGKILL')
            await unexpected.process.exited
        } catch {
            denied = true
        }
        assert.ok(denied)
        const connections = await database.unsafe<
            { rolsuper: boolean; rolcreaterole: boolean; rolcreatedb: boolean }[]
        >(
            "select rolname,rolconnlimit,rolsuper,rolcreaterole,rolcreatedb from pg_roles where rolname in ('isolation_worker','isolation_foreground') order by rolname",
        )
        assert.ok(
            connections.every((item) => !item.rolsuper && !item.rolcreaterole && !item.rolcreatedb),
        )
        await Promise.all([stop(foreground), stop(background)])
        return {
            foregroundP99Ms: latency.p99,
            deniedStartup: denied,
            workerConnectionLimit: 8,
            saturationHeldThroughSsr,
            foregroundConnectionLimit: 16,
            postgresGlobalLimit: 100,
        }
    })
    await record('term-and-cancelled-claim-release', async () => {
        await seed(100)
        const foreground = await spawn('foreground'),
            background = await spawn('background')
        await request(background, '/start', 'POST')
        await waitFor(async () => (await active()) > 0, 5_000)
        const start = performance.now()
        background.process.kill('SIGTERM')
        assert.equal(await bounded(background.process.exited, 15_000), 0)
        roles.delete(background)
        const recovery = performance.now() - start
        assert.equal(await active(), 0)
        assert.ok(recovery <= isolationContract.thresholds.gracefulRecoveryMs)
        const latency = await ssr(foreground, 100)
        await stop(foreground)
        return { recoveryMs: recovery, remainingLeases: 0, foregroundP99Ms: latency.p99 }
    })
    await record('kill-and-expired-lease-recovery', async () => {
        await seed(100)
        const foreground = await spawn('foreground'),
            old = await spawn('background')
        await request(old, '/start', 'POST')
        await waitFor(async () => (await active()) > 0, 5_000)
        const start = performance.now()
        old.process.kill('SIGKILL')
        await old.process.exited
        roles.delete(old)
        const abandoned = await active()
        assert.ok(abandoned > 0)
        const replacement = await spawn('background')
        await request(replacement, '/start', 'POST')
        await waitFor(async () => (await progress()).total >= 100, 50_000)
        const recovery = performance.now() - start,
            final = await progress()
        assert.equal(final.total, 100)
        assert.equal(final.unique, 100)
        assert.equal(final.healthy, 100)
        assert.ok(recovery <= isolationContract.thresholds.crashRecoveryMs)
        const latency = await ssr(foreground, 100)
        await Promise.all([stop(foreground), stop(replacement)])
        return {
            abandonedLeases: abandoned,
            recoveryMs: recovery,
            duplicates: 0,
            foregroundP99Ms: latency.p99,
        }
    })
    await record('hang-stop-timeout-and-two-writer-maintenance', async () => {
        await seed(100)
        const foreground = await spawn('combined'),
            background = await spawn('background')
        await Promise.all([
            request(foreground, '/start', 'POST'),
            request(background, '/start', 'POST'),
        ])
        await waitFor(async () => (await active()) > 0, 5_000)
        background.process.kill('SIGSTOP')
        let timeout = false
        try {
            await fetch('http://127.0.0.1:' + background.ready.port + '/stop', {
                method: 'POST',
                headers: { 'x-isolation-control': 'fixture' },
                signal: AbortSignal.timeout(250),
            })
        } catch {
            timeout = true
        }
        assert.ok(timeout)
        const latency = await ssr(foreground, 100)
        background.process.kill('SIGCONT')
        const drains = await Promise.all([
            request(foreground, '/stop', 'POST'),
            request(background, '/stop', 'POST'),
        ])
        assert.ok(drains.every((response) => response.ok))
        assert.equal(await active(), 0)
        const sessions = await database.unsafe(
            "select count(*)::int as total from pg_stat_activity where usename in ('isolation_foreground','isolation_worker')",
        )
        assert.equal(Number(sessions[0]?.total), 0)
        await database.unsafe(
            'update rentnerproxy.host_check_policies set version=gen_random_uuid()',
        )
        const late = await Promise.all([
            request(foreground, '/write-after-fence', 'POST'),
            request(background, '/write-after-fence', 'POST'),
        ])
        assert.ok(late.every((response) => response.status === 503))
        await Promise.all([stop(foreground), stop(background)])
        return {
            hungRoleAcknowledged: false,
            localTwoWriterDrain: true,
            databaseSessionsAfterDrain: 0,
            oldRoleWritesRejected: true,
            foregroundP99Ms: latency.p99,
            productionRootMaintenanceIntegrated: false,
        }
    })
    await record('active-two-writer-logical-restore', async () => {
        await seed(100)
        const connection = ['--host=127.0.0.1', '--port=55439', '--username=postgres']
        await context.command([
            'pg_dump',
            ...connection,
            '--format=custom',
            '--schema=rentnerproxy',
            '--file=' + context.snapshotPath,
            'rentnerproxy_upstream_test',
        ])
        const foreground = await spawn('combined'),
            background = await spawn('background')
        await Promise.all([
            request(foreground, '/start', 'POST'),
            request(background, '/start', 'POST'),
        ])
        const writes = Promise.allSettled([
            request(foreground, '/hold-write', 'POST'),
            request(background, '/hold-write', 'POST'),
        ])
        await waitFor(async () => {
            const rows = await database.unsafe(
                "select count(distinct usename)::int as total from pg_stat_activity where usename in ('isolation_foreground','isolation_worker') and state='active' and backend_xid is not null and query like '%pg_sleep(2)%'",
            )
            return Number(rows[0]?.total) === 2
        }, 5_000)
        const drains = await Promise.all([
            request(foreground, '/stop', 'POST'),
            request(background, '/stop', 'POST'),
        ])
        assert.ok(drains.every((response) => response.ok))
        const written = await writes
        assert.ok(
            written.every((result) => result.status === 'fulfilled' && result.value.status === 204),
        )
        const sessions = await database.unsafe(
            "select count(*)::int as total from pg_stat_activity where usename in ('isolation_foreground','isolation_worker')",
        )
        assert.equal(Number(sessions[0]?.total), 0)
        const changed = await database.unsafe(
            'select count(*)::int as total from rentnerproxy.proxy_hosts where forward_port=9000',
        )
        assert.equal(Number(changed[0]?.total), 2)
        await context.command([
            'pg_restore',
            ...connection,
            '--dbname=rentnerproxy_upstream_test',
            '--clean',
            '--if-exists',
            '--single-transaction',
            '--exit-on-error',
            context.snapshotPath,
        ])
        const restored = await database.unsafe(
            'select count(*)::int as total from rentnerproxy.proxy_hosts where forward_port=9',
        )
        assert.equal(Number(restored[0]?.total), 100)
        const late = await Promise.all([
            request(foreground, '/hold-write', 'POST'),
            request(background, '/write-after-fence', 'POST'),
        ])
        assert.ok(late.every((response) => response.status === 503))
        assert.equal((await progress()).total, 0)
        await Promise.all([stop(foreground), stop(background)])
        return {
            activeWriteTransactionsBeforeDrain: 2,
            databaseSessionsAfterDrain: 0,
            committedWritesBeforeRestore: 2,
            restoredHosts: 100,
            logicalSchemaRestore: true,
            oldRoleWritesRejected: true,
            productionRootMaintenanceIntegrated: false,
        }
    })
    return {
        results,
        productionMaintenanceProven: false,
        supervisorRestartIndependenceProven: false,
        authorizationParityProven: false,
        versionSkewSupported: false,
    }
}
