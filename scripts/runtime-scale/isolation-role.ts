import { readdir } from 'node:fs/promises'
import {
    isolationDistribution,
    assertIsolationRole,
    isolationProbeIndex,
    isolationDiagnosticResult,
} from './isolation.ts'
import { isolationContract } from './isolation.config.ts'
import type { IsolationRoleMetrics } from './Types/isolation.types.ts'
import type {
    HostMonitoringClaim,
    HostMonitoringCompletion,
} from '../../web/src/server/HostMonitoring/Types/host-monitoring.repository.types.ts'
import type { HostMonitoringProbeContext } from '../../web/src/server/HostMonitoring/Types/host-monitoring-probe.types.ts'

export async function runIsolationRole(
    role: 'foreground' | 'combined' | 'background',
    networkPort: number,
) {
    assertIsolationRole(
        process.env.RENTNERPROXY_ISOLATION_FIXTURE,
        process.platform,
        process.getuid?.(),
        process.env.DATABASE_URL,
        role,
    )
    const [
        { eq, sql },
        { HOST_MONITORING_LIMITS: limits },
        { db, closeDatabaseForRestoreMaintenance },
        { proxyHosts },
        { readProxyRuntimeSnapshot },
        { createProxyReconciler },
        { createHostMonitoringWorker },
        { createHostMonitoringProbe },
        { createHostMonitoringBatcher },
        repository,
    ] = await Promise.all([
        import('drizzle-orm'),
        import('../../web/src/config/host-monitoring.config.ts'),
        import('../../web/src/db/index.ts'),
        import('../../web/src/db/schema.ts'),
        import('../../web/src/server/ProxyRuntime/proxy-runtime-data.ts'),
        import('../../web/src/server/ProxyRuntime/proxy-reconcile.ts'),
        import('../../web/src/server/HostMonitoring/host-monitoring.worker.ts'),
        import('../../web/src/server/HostMonitoring/host-monitoring.probe.ts'),
        import('../../web/src/server/HostMonitoring/host-monitoring.batch.ts'),
        import('../../web/src/server/HostMonitoring/host-monitoring.repository.ts'),
    ])
    let committed = 0,
        commitLagMaxMs: number | null = null
    let failures = 0,
        stopping: Promise<void> | null = null,
        started = false
    const lag: number[] = []
    const cpu = process.cpuUsage()
    let due = performance.now() + 25
    const lagTimer = setInterval(() => {
        const now = performance.now()
        lag.push(Math.max(0, now - due))
        due = now + 25
    }, 25)
    const readContext = createHostMonitoringBatcher<
        HostMonitoringClaim,
        HostMonitoringProbeContext | null
    >({
        size: limits.claimBatchSize,
        delayMs: 0,
        run: async (claims) =>
            (await repository.readHostMonitoringClaimContexts(claims)).map((value) => ({
                status: 'fulfilled',
                value,
            })),
    })
    const complete = createHostMonitoringBatcher<HostMonitoringCompletion, boolean>({
        size: limits.persistenceBatchSize,
        delayMs: limits.persistenceDelayMs,
        run: async (items) => {
            const results = await repository.completeHostMonitoringClaims(items)
            const acknowledgedAt = Date.now()
            results.forEach((result, index) => {
                if (result.status === 'fulfilled' && result.value) {
                    committed++
                    commitLagMaxMs = Math.max(
                        commitLagMaxMs ?? 0,
                        acknowledgedAt - items[index]!.claim.policy.nextCheckAt.getTime(),
                    )
                }
            })
            return results
        },
    })
    const execute = createHostMonitoringProbe({
        readContext,
        inspect: async (input, signal) => {
            const start = performance.now()
            const index = isolationProbeIndex(input.domain)
            const response = await fetch('http://127.0.0.1:' + networkPort + '/probe/' + index, {
                method: 'HEAD',
                signal: AbortSignal.any([signal, AbortSignal.timeout(1_500)]),
                redirect: 'error',
            })
            if (response.status !== 200) throw new Error('Fixture upstream failed')
            return isolationDiagnosticResult(input, performance.now() - start)
        },
        complete: (claim, outcome, before, signal) => complete({ claim, outcome, before, signal }),
        release: repository.releaseHostMonitoringClaim,
    })
    const worker = createHostMonitoringWorker({
        claim: repository.claimDueHostMonitoringPolicies,
        runClaim: execute,
        releaseCancelled: async (claim) => {
            await repository.releaseHostMonitoringClaim(claim)
        },
        prune: repository.pruneHostMonitoringHistory,
        unavailable: () => {
            failures++
        },
    })
    const reconcile = createProxyReconciler(
        {
            enabled: true,
            loadSnapshot: () =>
                db.transaction(readProxyRuntimeSnapshot, {
                    isolationLevel: 'repeatable read',
                    accessMode: 'read only',
                }),
            applySnapshot: async (snapshot) => {
                const response = await fetch('http://127.0.0.1:' + networkPort + '/snapshot', {
                    method: 'POST',
                    body: JSON.stringify(snapshot),
                    signal: AbortSignal.timeout(2_000),
                    redirect: 'error',
                })
                if (!response.ok) return null
                return { status: 'applied', activeRevision: snapshot.revision, lastApplyAt: null }
            },
        },
        2_000,
    )
    let renderer: Promise<typeof import('./isolation-ssr.ts')> | null = null
    let snapshotTimer: ReturnType<typeof setInterval> | null = null
    async function metrics(): Promise<IsolationRoleMetrics> {
        const usage = process.cpuUsage(cpu)
        return {
            pid: process.pid,
            uid: process.getuid!(),
            rssBytes: process.memoryUsage().rss,
            cpuMicroseconds: usage.user + usage.system,
            fds: (await readdir('/proc/self/fd')).length,
            lag: isolationDistribution(lag),
            failures,
            committed,
            commitLagMaxMs,
        }
    }
    const stop = () => {
        stopping ??= (async () => {
            if (snapshotTimer) clearInterval(snapshotTimer)
            snapshotTimer = null
            clearInterval(lagTimer)
            const results = await Promise.allSettled([worker.stop(), reconcile.stop()])
            const failed = results.find((item) => item.status === 'rejected')
            if (failed?.status === 'rejected') throw failed.reason
            await closeDatabaseForRestoreMaintenance()
        })()
        return stopping
    }
    await db.execute('select 1')
    const server = Bun.serve({
        hostname: '127.0.0.1',
        port: 0,
        maxRequestBodySize: 1024,
        async fetch(request) {
            const path = new URL(request.url).pathname
            if (path === '/ssr' && role !== 'background' && request.method === 'GET') {
                const rows = await db
                    .select({
                        id: proxyHosts.id,
                        name: proxyHosts.forwardHost,
                        enabled: proxyHosts.enabled,
                    })
                    .from(proxyHosts)
                    .orderBy(proxyHosts.id)
                    .limit(isolationContract.workload.ssrRows)
                const { renderIsolationRows } = await (renderer ??= import('./isolation-ssr.ts'))
                return renderIsolationRows(rows)
            }
            if (request.headers.get('x-isolation-control') !== 'fixture')
                return new Response(null, { status: 404 })
            if (path === '/metrics' && request.method === 'GET')
                return Response.json(await metrics())
            if (path === '/start' && request.method === 'POST') {
                if (!started && role !== 'foreground') {
                    started = true
                    worker.start()
                    reconcile.start()
                    snapshotTimer = setInterval(() => {
                        void reconcile()
                    }, isolationContract.workload.snapshotWakeMs)
                }
                return Response.json({ started })
            }
            if (path === '/write-after-fence' && request.method === 'POST') {
                if (stopping) return new Response(null, { status: 503 })
                await db
                    .update(proxyHosts)
                    .set({ forwardPort: 9000 })
                    .where(eq(proxyHosts.id, '00000000-0000-4000-8000-000000000001'))
                return new Response(null, { status: 204 })
            }
            if (path === '/hold-write' && request.method === 'POST') {
                if (stopping) return new Response(null, { status: 503 })
                await db.transaction(async (transaction) => {
                    await transaction
                        .update(proxyHosts)
                        .set({ forwardPort: 9000 })
                        .where(
                            eq(
                                proxyHosts.id,
                                role === 'background'
                                    ? '00000000-0000-4000-8000-000000000002'
                                    : '00000000-0000-4000-8000-000000000001',
                            ),
                        )
                    await transaction.execute(sql`select pg_sleep(2)`)
                })
                return new Response(null, { status: 204 })
            }
            if (path === '/pool' && request.method === 'POST') {
                const results = await Promise.allSettled(
                    Array.from({ length: isolationContract.workerConnections }, () =>
                        db.execute('select pg_sleep(1)'),
                    ),
                )
                return Response.json({ settled: results.length })
            }
            if (path === '/stop' && request.method === 'POST') {
                try {
                    await stop()
                    return Response.json({ drained: true, ...(await metrics()) })
                } catch {
                    return Response.json({ drained: false }, { status: 503 })
                }
            }
            return new Response(null, { status: 404 })
        },
    })
    const terminate = () => {
        void stop()
            .then(() => server.stop(true))
            .then(
                () => process.exit(0),
                () => process.exit(1),
            )
    }
    process.once('SIGTERM', terminate)
    process.once('SIGINT', terminate)
    console.log(
        JSON.stringify({
            ready: true,
            port: server.port,
            pid: process.pid,
            uid: process.getuid!(),
        }),
    )
}
