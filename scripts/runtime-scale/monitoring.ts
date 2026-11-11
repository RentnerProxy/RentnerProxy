import type { MonitoringScaleOptions, MonitoringDistribution } from './Types/monitoring.types.ts'
import type {
    DomainDiagnosticsControllerInput,
    DomainDiagnosticsResult,
} from '../../web/src/lib/RuntimeDiagnostics/Types/domain-diagnostics.types.ts'

const inputMessages = {
    options:
        'Expected --hosts 100|500|1000 --periods 2|3 --profile mixed|slow|timeout --runtime simulated|native --timeout-ms 3000|10000 --scheduler candidate|legacy',
    bounds: 'Monitoring workload outside fixed bounds',
    database: 'Isolated monitoring database required',
    isolation:
        'Requires RENTNERPROXY_DATABASE_INTEGRATION=1 and loopback rentnerproxy_upstream_test',
    nonempty: 'Refusing nonempty monitoring/host database',
    native: 'Native monitoring requires the explicitly isolated controller fixture.',
} as const

/** Identifies fixed, safe CLI guard diagnostics without accepting caller-provided error text. */
export class MonitoringInputError extends Error {
    constructor(readonly code: keyof typeof inputMessages) {
        super(inputMessages[code])
        this.name = 'MonitoringInputError'
    }
}

/** Shows recognized guard messages while hiding driver errors, connection URLs and credentials. */
export function formatMonitoringFailure(error: unknown): string {
    return error instanceof MonitoringInputError && Object.hasOwn(inputMessages, error.code)
        ? inputMessages[error.code]
        : 'Monitoring capacity failed; inspect isolated database/runtime availability. No credentials logged.'
}

/** Accepts only the fixed workload, duration, timeout and scheduler profiles supported by this opt-in fixture. */
export function parseMonitoringOptions(args: string[]): MonitoringScaleOptions {
    const flags = new Map<string, string>()
    for (let index = 0; index < args.length; index += 2) {
        const flag = args[index]!
        const value = args[index + 1]
        if (
            ![
                '--hosts',
                '--periods',
                '--profile',
                '--runtime',
                '--timeout-ms',
                '--scheduler',
            ].includes(flag) ||
            flags.has(flag) ||
            !value
        )
            throw new MonitoringInputError('options')
        flags.set(flag, value)
    }
    const hosts = flags.get('--hosts') ?? '100'
    const periods = flags.get('--periods') ?? '2'
    const profile = flags.get('--profile') ?? 'mixed'
    const runtime = flags.get('--runtime') ?? 'simulated'
    const timeoutMs = flags.get('--timeout-ms') ?? '3000'
    const scheduler = flags.get('--scheduler') ?? 'candidate'
    if (
        !['100', '500', '1000'].includes(hosts) ||
        !['2', '3'].includes(periods) ||
        !['mixed', 'slow', 'timeout'].includes(profile) ||
        !['native', 'simulated'].includes(runtime) ||
        !['3000', '10000'].includes(timeoutMs) ||
        !['legacy', 'candidate'].includes(scheduler)
    )
        throw new MonitoringInputError('bounds')
    return {
        hosts: Number(hosts) as MonitoringScaleOptions['hosts'],
        periods: Number(periods) as MonitoringScaleOptions['periods'],
        profile: profile as MonitoringScaleOptions['profile'],
        ...(runtime === 'native' ? { runtime: 'native' as const } : {}),
        ...(timeoutMs === '10000' ? { timeoutMs: 10000 as const } : {}),
        ...(scheduler === 'legacy' ? { scheduler: 'legacy' as const } : {}),
    }
}

/** Rejects unflagged or non-test database destinations before importing modules that can mutate fixture state. */
export function assertIsolatedMonitoringDatabase(
    flag: string | undefined,
    value: string | undefined,
): void {
    let url: URL
    try {
        url = new URL(value ?? '')
    } catch {
        throw new MonitoringInputError('database')
    }
    if (
        flag !== '1' ||
        !['postgres:', 'postgresql:'].includes(url.protocol) ||
        url.hostname !== '127.0.0.1' ||
        url.pathname !== '/rentnerproxy_upstream_test'
    )
        throw new MonitoringInputError('isolation')
}

/** Reports nearest-rank tail latency in the input's units, with null tails for an empty sample. */
export function monitoringDistribution(values: readonly number[]): MonitoringDistribution {
    const sorted = values.toSorted((left, right) => left - right)
    const percentile = (fraction: number) => sorted[Math.ceil(sorted.length * fraction) - 1] ?? null
    return {
        count: sorted.length,
        p95: percentile(0.95),
        p99: percentile(0.99),
        max: sorted.at(-1) ?? null,
        total: values.reduce((sum, value) => sum + value, 0),
    }
}

/** Assigns deterministic healthy/timeout delays while preserving the requested timeout and interval workload. */
export function monitoringLatency(
    profile: MonitoringScaleOptions['profile'],
    index: number,
    timeoutMs = 3000,
) {
    if (profile !== 'mixed')
        return {
            durationMs: profile === 'slow' ? timeoutMs * 0.8 : timeoutMs,
            timeout: profile === 'timeout',
        }
    const durationMs = [20, 150, 1000, timeoutMs][index % 4]!
    return { durationMs, timeout: index % 4 === 3 }
}

/** Produces a protocol-valid diagnostic for the simulation profile without claiming native network execution. */
export function simulatedMonitoringResult(
    input: DomainDiagnosticsControllerInput,
    durationMs: number,
    timeout: boolean,
): DomainDiagnosticsResult {
    const common = { durationMs: 0, addresses: [], httpStatus: null }
    return {
        ...input,
        activeRevision: input.expectedRevision,
        checkedAt: new Date().toISOString(),
        observationPoint: 'appliance',
        publicReachability: 'not_checked',
        stages: {
            configuration: { ...common, status: 'passed', code: 'configuration_active' },
            dns: { ...common, status: 'unknown', code: 'skipped' },
            upstream: {
                ...common,
                durationMs,
                status: timeout ? 'failed' : 'passed',
                code: timeout ? 'upstream_timeout' : 'upstream_responded',
                httpStatus: timeout ? null : 200,
            },
            tls: { ...common, status: 'not_applicable', code: 'http_upstream' },
        },
    }
}

function sleep(ms: number) {
    return new Promise<void>((resolve) => setTimeout(resolve, ms))
}

/** Accepts only the exact healthy and timeout status/code pairs emitted by controlled fixture upstreams. */
export function isMonitoringFixtureOutcome(status: string, code: string): boolean {
    return (
        (status === 'healthy' && code === 'expected_status') ||
        (status === 'unhealthy' && code === 'upstream_timeout')
    )
}

/** Seeds an empty isolated database, measures unchanged 60-second policies, and drains and removes its fixtures on exit. */
export async function runMonitoringCapacity(options: MonitoringScaleOptions) {
    assertIsolatedMonitoringDatabase(
        process.env.RENTNERPROXY_DATABASE_INTEGRATION,
        process.env.DATABASE_URL,
    )
    const [
        { db, closeDatabaseForRestoreMaintenance },
        schema,
        orm,
        repository,
        { createHostMonitoringWorker },
        { createHostMonitoringProbe },
        { createHostMonitoringBatcher },
        { HOST_MONITORING_LIMITS },
        { BACKGROUND_DATABASE_POLICY },
        { settleWorkerTasks },
    ] = await Promise.all([
        import('../../web/src/db/index.ts'),
        import('../../web/src/db/schema.ts'),
        import('drizzle-orm'),
        import('../../web/src/server/HostMonitoring/host-monitoring.repository.ts'),
        import('../../web/src/server/HostMonitoring/host-monitoring.worker.ts'),
        import('../../web/src/server/HostMonitoring/host-monitoring.probe.ts'),
        import('../../web/src/server/HostMonitoring/host-monitoring.batch.ts'),
        import('../../web/src/config/host-monitoring.config.ts'),
        import('../../web/src/config/background-resources.config.ts'),
        import('../../web/src/server/Foundation/worker-lifecycle.server.ts'),
    ])
    const { hostCheckPolicies, hostCheckObservations, proxyHosts, redirectHosts, hostDomains } =
        schema
    const { sql, inArray } = orm
    const nativeMode = options.runtime === 'native'
    const contract = {
        kind: nativeMode
            ? 'production monitoring pipeline with native controller, Caddy and deterministic HTTP upstreams; real isolated PostgreSQL'
            : 'production TypeScript monitoring pipeline; deterministic simulated inspect; real isolated PostgreSQL',
        options,
        intervalSeconds: 60,
        timeoutMs: options.timeoutMs ?? 3000,
        scheduler:
            options.scheduler === 'legacy'
                ? 'serial completion-relative one-second admission model on candidate storage/runtime'
                : 'candidate bounded pipeline',
        concurrency: HOST_MONITORING_LIMITS.workerConcurrency,
        claimBatch: HOST_MONITORING_LIMITS.claimBatchSize,
        persistenceBatch: HOST_MONITORING_LIMITS.persistenceBatchSize,
        pollMs: HOST_MONITORING_LIMITS.pollIntervalMs,
        cooperativeStopSeconds: 240,
        referenceSlo: {
            exactCompletionsPerPolicyPerPeriod: 1,
            dueToStartP99Ms: 55000,
            dueToCompletionMaxMs: 60000,
            managementReadP99Ms: 250,
            nativeProxyTrafficP99Ms: 250,
            queueGrowth: 0,
        },
        resourceEnvelope: {
            harnessRssMiB: 512,
            harnessAverageCpuCores: 2,
            databaseActiveConnections: 16,
            databaseLockWaiters: 3,
            sampledDatabaseLockWaitMs: 250,
            nativeControllerCaddyCpuLimit: 2,
            nativeControllerCaddyMemoryLimitMiB: 512,
            databaseCpuLimit: 2,
            databaseMemoryLimitMiB: 1024,
        },
        limits: [
            'Plain management list reads, no SSR/auth/UI load',
            'Claim, completion and retention transactions intentionally share configuration locks; at most three sampled waiters and 250ms sampled lock delay are allowed',
            'Harness CPU/RSS only; PostgreSQL and native controller CPU/RSS require external container sampling',
            nativeMode
                ? 'Actual HTTP through native controller; configured upstream name resolves through Docker; no TLS/SSR load'
                : 'No actual HTTP/DNS/TLS or native controller execution; simulated diagnostic durations are explicit',
            'Reference SLOs are test acceptance targets, not previously established appliance guarantees',
        ],
    }
    console.log(JSON.stringify({ predeclaredContract: contract }))
    const hostIds: string[] = Array.from({ length: options.hosts }, () => Bun.randomUUIDv7())
    const domains = hostIds.map((id, index) => `capacity-${index}-${id}.example.test`)
    let native:
        | Awaited<ReturnType<typeof import('./monitoring-native.ts').createNativeMonitoringFixture>>
        | undefined
    const phases: Record<string, number[]> = {
        claim: [],
        context: [],
        probe: [],
        commit: [],
        retention: [],
        managementRead: [],
        databaseSample: [],
        proxyTraffic: [],
    }
    const dueToStart: number[] = [],
        dueToCompletion: number[] = []
    const completed = Array.from({ length: options.periods }, () => new Map<string, number>())
    const queue: {
        elapsedMs: number
        due: number
        active: number
        lockWaiters: number
        lockWaitMs: number
    }[] = []
    const failures: string[] = []
    let origin = 0,
        startedAt = 0,
        peakRss = 0,
        maxActive = 0,
        maxLockWaiters = 0,
        maxLockWaitMs = 0
    let seeded = false
    const samplerController = new AbortController()
    let sampler: Promise<void> | undefined
    let worker: ReturnType<typeof createHostMonitoringWorker> | undefined
    let cpu = process.cpuUsage()
    let legacyActive = false
    let legacyNextAt = 0
    let runAborted = false
    let workerDrain: Promise<void> | undefined
    /** Own the first stop checkpoint and its actual writer drain, preserving failure evidence without detached rejections. */
    function stopWorker(): Promise<void> {
        if (!worker) return Promise.resolve()
        workerDrain ??= (async () => {
            const checkpoint = Promise.resolve().then(() => worker!.stop())
            void checkpoint.catch(() => {
                failures.push('Monitoring worker stop failed')
            })
            try {
                await settleWorkerTasks([checkpoint])
            } catch {
                failures.push('Monitoring worker drain failed')
            }
        })()
        return workerDrain
    }
    /** Stops admission once and lets the run unwind through fixture cleanup on deadlines or signals. */
    function abortRun(reason: 'budget' | 'SIGINT' | 'SIGTERM') {
        if (runAborted) return
        runAborted = true
        failures.push(
            reason === 'budget'
                ? 'Monitoring capacity wall-clock budget exhausted'
                : `Monitoring capacity interrupted by ${reason}`,
        )
        samplerController.abort()
        void stopWorker()
    }
    const budgetTimer = setTimeout(() => abortRun('budget'), 240000)
    const interrupt = () => abortRun('SIGINT')
    const terminate = () => abortRun('SIGTERM')
    process.once('SIGINT', interrupt)
    process.once('SIGTERM', terminate)
    /** Records full phase latency, including failures, without changing the operation's result or error. */
    async function measure<T>(name: string, operation: () => Promise<T>): Promise<T> {
        const start = performance.now()
        try {
            return await operation()
        } finally {
            phases[name]!.push(performance.now() - start)
        }
    }

    try {
        const [existingPolicies, existingHosts, existingRedirects] = await Promise.all([
            db.select({ id: hostCheckPolicies.id }).from(hostCheckPolicies).limit(1),
            db.select({ id: proxyHosts.id }).from(proxyHosts).limit(1),
            db.select({ id: redirectHosts.id }).from(redirectHosts).limit(1),
        ])
        if (existingPolicies.length || existingHosts.length || existingRedirects.length)
            throw new MonitoringInputError('nonempty')
        if (nativeMode) {
            const { createNativeMonitoringFixture } = await import('./monitoring-native.ts')
            native = await createNativeMonitoringFixture(options, domains)
        }
        await db.transaction(async (transaction) => {
            await transaction.insert(proxyHosts).values(
                hostIds.map((id) => ({
                    id,
                    forwardScheme: 'http' as const,
                    forwardHost: native?.forwardHost ?? '127.0.0.1',
                    forwardPort: native?.forwardPort ?? 9,
                })),
            )
            await transaction.insert(hostDomains).values(
                hostIds.map((id, index) => ({
                    proxyHostId: id,
                    domain: domains[index]!,
                })),
            )
            origin = Date.now() + 1000
            await transaction.insert(hostCheckPolicies).values(
                hostIds.map((id, index) => ({
                    proxyHostId: id,
                    domain: domains[index]!,
                    enabled: true,
                    intervalSeconds: 60,
                    timeoutMs: options.timeoutMs ?? 3000,
                    nextCheckAt: new Date(origin),
                })),
            )
        })
        seeded = true
        if (runAborted) throw new Error('Monitoring capacity stopped during setup.')
        if (native) {
            const { readProxyRuntimeSnapshot } =
                await import('../../web/src/server/ProxyRuntime/proxy-runtime-data.ts')
            await native.apply(
                await db.transaction(readProxyRuntimeSnapshot, {
                    isolationLevel: 'repeatable read',
                    accessMode: 'read only',
                }),
            )
            origin = Date.now() + 1000
            await db
                .update(hostCheckPolicies)
                .set({ nextCheckAt: new Date(origin) })
                .where(inArray(hostCheckPolicies.proxyHostId, hostIds))
        }
        const latencies = new Map(
            hostIds.map((id, index) => [
                id,
                monitoringLatency(options.profile, index, options.timeoutMs),
            ]),
        )
        const readContext = createHostMonitoringBatcher<
            Parameters<typeof repository.readHostMonitoringClaimContexts>[0][number],
            Awaited<ReturnType<typeof repository.readHostMonitoringClaimContexts>>[number]
        >({
            size: HOST_MONITORING_LIMITS.claimBatchSize,
            delayMs: 0,
            run: (claims) =>
                measure('context', async () =>
                    (await repository.readHostMonitoringClaimContexts(claims)).map((value) => ({
                        status: 'fulfilled' as const,
                        value,
                    })),
                ),
        })
        const complete = createHostMonitoringBatcher<
            Parameters<typeof repository.completeHostMonitoringClaims>[0][number],
            boolean
        >({
            size: HOST_MONITORING_LIMITS.persistenceBatchSize,
            delayMs: HOST_MONITORING_LIMITS.persistenceDelayMs,
            run: (items) =>
                measure('commit', async () => {
                    const results = await repository.completeHostMonitoringClaims(items)
                    results.forEach((result, index) => {
                        const claim = items[index]!.claim
                        if (result.status !== 'fulfilled' || !result.value) {
                            failures.push('Completion rejected or fenced')
                            return
                        }
                        const due = claim.policy.nextCheckAt.getTime()
                        const period = Math.floor((due - origin) / 60000)
                        dueToCompletion.push(Date.now() - due)
                        const coverage = completed[period]
                        if (coverage)
                            coverage.set(claim.policy.id, (coverage.get(claim.policy.id) ?? 0) + 1)
                        else failures.push('Completion outside requested periods')
                    })
                    return results
                }),
        })
        // A cancelled flight owns up to 64 claims. Send only the monitoring class's
        // running allowance to SQL at once; each original token/version fence stays intact.
        const release = createHostMonitoringBatcher<
            Parameters<typeof repository.releaseHostMonitoringClaim>[0],
            boolean
        >({
            size: BACKGROUND_DATABASE_POLICY.classes.monitoring.running,
            delayMs: 0,
            run: (claims) =>
                Promise.allSettled(
                    claims.map(async (claim) => repository.releaseHostMonitoringClaim(claim)),
                ),
        })
        const runClaim = createHostMonitoringProbe({
            readContext,
            inspect: async (input, signal, probeOptions) =>
                measure('probe', async () => {
                    if (native) return native.inspect(input, signal, probeOptions)
                    const latency = latencies.get(input.hostId)!
                    await new Promise<void>((resolve, reject) => {
                        const timer = setTimeout(() => {
                            signal.removeEventListener('abort', abort)
                            resolve()
                        }, latency.durationMs)
                        function abort() {
                            clearTimeout(timer)
                            reject(new Error('Capacity run stopped'))
                        }
                        signal.addEventListener('abort', abort, { once: true })
                        if (signal.aborted) abort()
                    })
                    return simulatedMonitoringResult(input, latency.durationMs, latency.timeout)
                }),
            complete: (claim, outcome, before, signal) =>
                complete({ claim, outcome, before, signal }),
            release,
        })
        worker = createHostMonitoringWorker({
            claim: (limit) => {
                if (options.scheduler === 'legacy' && (legacyActive || Date.now() < legacyNextAt))
                    return Promise.resolve([])
                return measure('claim', () =>
                    repository.claimDueHostMonitoringPolicies(
                        options.scheduler === 'legacy' ? 1 : limit,
                    ),
                )
            },
            runClaim: async (claim, signal) => {
                dueToStart.push(Date.now() - claim.policy.nextCheckAt.getTime())
                legacyActive = true
                try {
                    await runClaim(claim, signal)
                } finally {
                    legacyActive = false
                    legacyNextAt = Date.now() + 1000
                }
            },
            releaseCancelled: async (claim) => {
                await release(claim)
            },
            prune: () => measure('retention', repository.pruneHostMonitoringHistory),
            unavailable: () => {
                failures.push('Production worker reported unavailable')
            },
        })
        startedAt = Date.now()
        cpu = process.cpuUsage()
        sampler = (async () => {
            let nextDatabaseSample = 0
            while (!samplerController.signal.aborted) {
                peakRss = Math.max(peakRss, process.memoryUsage().rss)
                try {
                    if (native) {
                        // oxlint-disable-next-line no-await-in-loop -- Keep a single real proxy request in flight alongside management reads.
                        await measure('proxyTraffic', native.traffic)
                    }
                    // Plain management list read; intentionally not an SSR claim.
                    // oxlint-disable-next-line no-await-in-loop -- Keep a single continuous management read in flight.
                    await measure('managementRead', () =>
                        db
                            .select({ id: proxyHosts.id, enabled: proxyHosts.enabled })
                            .from(proxyHosts)
                            .orderBy(proxyHosts.id)
                            .limit(100),
                    )
                    if (Date.now() >= nextDatabaseSample) {
                        // oxlint-disable-next-line no-await-in-loop -- Sample serially to avoid adding database contention.
                        const stats = await measure('databaseSample', () =>
                            db.execute(
                                sql`select (select count(*)::int from ${hostCheckPolicies} where enabled and next_check_at <= clock_timestamp()) as due, (select count(*)::int from pg_stat_activity where datname = current_database() and state = 'active') as active, (select count(*)::int from pg_stat_activity where datname = current_database() and wait_event_type = 'Lock') as lock_waiters, (select coalesce(max(extract(epoch from (clock_timestamp() - locks.waitstart)) * 1000), 0)::float8 from pg_locks locks join pg_stat_activity activity on activity.pid = locks.pid where activity.datname = current_database() and not locks.granted) as lock_wait_ms`,
                            ),
                        )
                        const row = stats[0]!
                        const active = Number(row.active),
                            lockWaiters = Number(row.lock_waiters)
                        maxActive = Math.max(maxActive, active)
                        maxLockWaiters = Math.max(maxLockWaiters, lockWaiters)
                        const lockWaitMs = Math.max(0, Number(row.lock_wait_ms))
                        maxLockWaitMs = Math.max(maxLockWaitMs, lockWaitMs)
                        queue.push({
                            elapsedMs: Date.now() - origin,
                            due: Number(row.due),
                            active,
                            lockWaiters,
                            lockWaitMs,
                        })
                        nextDatabaseSample = Date.now() + 1000
                    }
                } catch {
                    failures.push('Management/database sampler failed')
                }
                // oxlint-disable-next-line no-await-in-loop -- Bound sampling frequency.
                await sleep(200)
            }
        })()
        worker.start()
        const end = origin + (options.periods - 1) * 60000 + 59000
        while (Date.now() < end) {
            if (runAborted) break
            // Bounded wall clock includes a final 59-second drain, never stretches policy cadence.
            // oxlint-disable-next-line no-await-in-loop -- Wait for the fixed final cadence window.
            await sleep(Math.min(200, end - Date.now()))
        }
        await stopWorker()
        samplerController.abort()
        await sampler
        const elapsedMs = Date.now() - startedAt
        const cpuUsed = process.cpuUsage(cpu)
        const averageCpuCores = (cpuUsed.user + cpuUsed.system) / (elapsedMs * 1000)
        const coverage = completed.map((period, index) => ({
            period: index + 1,
            policies: period.size,
            completions: [...period.values()].reduce((sum, count) => sum + count, 0),
            duplicates: [...period.values()].filter((count) => count !== 1).length,
        }))
        const boundaryQueue = Array.from(
            { length: options.periods },
            (_, index) =>
                queue.findLast(
                    (sample) =>
                        sample.elapsedMs >= index * 60000 + 55000 &&
                        sample.elapsedMs < (index + 1) * 60000,
                )?.due ?? null,
        )
        const observations = await db
            .select({
                status: hostCheckObservations.status,
                code: hostCheckObservations.code,
                count: sql<number>`count(*)::int`,
            })
            .from(hostCheckObservations)
            .groupBy(hostCheckObservations.status, hostCheckObservations.code)
        const observationCount = observations.reduce((sum, row) => sum + Number(row.count), 0)
        const expectedTimeouts =
            options.profile === 'timeout'
                ? observationCount
                : options.profile === 'slow'
                  ? 0
                  : (options.hosts * options.periods) / 4
        const timeoutCount = observations
            .filter((row) => row.code === 'upstream_timeout' && row.status === 'unhealthy')
            .reduce((sum, row) => sum + Number(row.count), 0)
        if (
            timeoutCount !== expectedTimeouts ||
            observations.some((row) => !isMonitoringFixtureOutcome(row.status, row.code))
        )
            failures.push('Controlled upstream outcomes differ from requested workload')
        const startDistribution = monitoringDistribution(dueToStart),
            completionDistribution = monitoringDistribution(dueToCompletion)
        if (
            coverage.some(
                (period) =>
                    period.policies !== options.hosts ||
                    period.duplicates ||
                    period.completions !== options.hosts,
            )
        )
            failures.push('Exact per-period policy coverage unmet')
        if (
            (startDistribution.p99 ?? Infinity) > contract.referenceSlo.dueToStartP99Ms ||
            (completionDistribution.max ?? Infinity) > contract.referenceSlo.dueToCompletionMaxMs
        )
            failures.push('Scheduling latency SLO unmet')
        if (
            (monitoringDistribution(phases.managementRead!).p99 ?? Infinity) >
            contract.referenceSlo.managementReadP99Ms
        )
            failures.push('Plain management read SLO unmet')
        if (
            native &&
            (monitoringDistribution(phases.proxyTraffic!).p99 ?? Infinity) >
                contract.referenceSlo.nativeProxyTrafficP99Ms
        )
            failures.push('Native proxy traffic latency SLO unmet')
        if (boundaryQueue.some((value) => value !== 0))
            failures.push('Queue did not drain before every next period')
        if (
            peakRss / 1048576 > contract.resourceEnvelope.harnessRssMiB ||
            averageCpuCores > contract.resourceEnvelope.harnessAverageCpuCores ||
            maxActive > contract.resourceEnvelope.databaseActiveConnections ||
            maxLockWaiters > contract.resourceEnvelope.databaseLockWaiters ||
            maxLockWaitMs > contract.resourceEnvelope.sampledDatabaseLockWaitMs
        )
            failures.push('Resource envelope unmet')
        if (observationCount !== options.hosts * options.periods)
            failures.push('Persisted observation count differs from exact workload')
        return {
            contract,
            passed: failures.length === 0,
            failures: [...new Set(failures)],
            elapsedMs,
            coverage,
            boundaryQueue,
            queue,
            dueToStartMs: startDistribution,
            dueToCompletionMs: completionDistribution,
            phaseMs: Object.fromEntries(
                Object.entries(phases).map(([name, values]) => [
                    name,
                    monitoringDistribution(values),
                ]),
            ),
            resources: {
                cpuUserMs: cpuUsed.user / 1000,
                cpuSystemMs: cpuUsed.system / 1000,
                averageCpuCores,
                peakRssMiB: peakRss / 1048576,
                maxActiveDatabaseConnections: maxActive,
                maxDatabaseLockWaiters: maxLockWaiters,
                sampledMaxDatabaseLockWaitMs: maxLockWaitMs,
            },
            observations: observationCount,
            outcomes: observations,
            checksPerSecond: observationCount / (elapsedMs / 1000),
        }
    } finally {
        clearTimeout(budgetTimer)
        process.removeListener('SIGINT', interrupt)
        process.removeListener('SIGTERM', terminate)
        samplerController.abort()
        try {
            await stopWorker()
        } finally {
            try {
                await sampler
            } finally {
                try {
                    if (seeded) await db.delete(proxyHosts).where(inArray(proxyHosts.id, hostIds))
                } finally {
                    try {
                        await native?.stop()
                    } finally {
                        await closeDatabaseForRestoreMaintenance()
                    }
                }
            }
        }
    }
}

if (import.meta.main) {
    if (process.argv.includes('--help'))
        console.log(
            'Opt-in bounded monitoring capacity: RENTNERPROXY_DATABASE_INTEGRATION=1 DATABASE_URL=<isolated loopback PostgreSQL> bun --no-env-file scripts/runtime-scale/monitoring.ts --hosts 100|500|1000 --periods 2|3 --profile mixed|slow|timeout --runtime simulated|native --timeout-ms 3000|10000 --scheduler candidate|legacy. Native requires the explicitly isolated controller fixture. Legacy models serial completion-relative one-second admission on candidate storage/runtime, not an old-release benchmark. Requires empty host/policy tables. Emits JSON; nonzero exit on unmet reference SLO. No full SSR measurement.',
        )
    else {
        try {
            const result = await runMonitoringCapacity(
                parseMonitoringOptions(process.argv.slice(2)),
            )
            console.log(JSON.stringify(result))
            if (!result.passed) process.exitCode = 1
        } catch (error) {
            console.error(formatMonitoringFailure(error))
            process.exitCode = 1
        }
    }
}
