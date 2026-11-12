// oxlint-disable no-await-in-loop -- Each resource sample and cadence slot depends on the preceding observation.
import { z } from 'zod'
import { monitoringEvaluationContract as contract } from './monitoring-evaluation.ts'
import { monitoringDistribution } from './monitoring.ts'
import type {
    MonitoringApplianceReport,
    MonitoringApplianceResource,
    MonitoringApplianceTraffic,
    MonitoringAppliancePairInput,
    MonitoringApplianceWindowClock,
    MonitoringApplianceWindowArming,
} from './Types/monitoring-whole-appliance.types.ts'

const number = z.number().finite().nonnegative()
const integer = number.int()
const role = z.strictObject({ processes: integer, rssBytes: integer, fds: integer })
const identity = z.strictObject({ pid: integer.positive(), startTicks: z.string().regex(/^\d+$/u) })
export const monitoringApplianceReportSchema = z.looseObject({
    windowId: z.string().regex(/^[a-z0-9-]{1,40}$/u),
    originMs: integer,
    endMs: integer,
    design: z.enum(['A', 'B2']),
    profile: z.enum(['mixed', 'all-3s-timeout']),
    active: integer,
    wakeProof: z
        .strictObject({
            design: z.enum(['A', 'B2']),
            runId: z.string().regex(/^[a-f0-9]{12}$/u),
            windowId: z.string().regex(/^[a-z0-9-]{1,40}$/u),
            originMs: integer,
            publishedAtMs: integer,
            webIdentity: identity,
            originalChangeChunkSha256: z.string().regex(/^[a-f0-9]{64}$/u),
            wakeCount: integer.positive(),
        })
        .nullable(),
    policies: z.array(
        z.looseObject({
            policyId: z.string().min(1),
            hostId: z.string().min(1),
            dueAtMs: integer,
            expectedStatus: z.enum(['healthy', 'unhealthy']),
            expectedCode: z.enum(['expected_status', 'upstream_timeout']),
        }),
    ),
    heads: z.array(z.looseObject({ hostId: z.string().min(1), atMs: integer, durationMs: number })),
    starts: z.array(
        z.looseObject({
            policyId: z.string().min(1),
            dueAtMs: integer,
            atMs: integer,
            admittedAtMs: integer,
            durableAtMs: integer,
            attemptId: z.string().min(1),
            durable: z.boolean(),
        }),
    ),
    observations: z.array(
        z.looseObject({
            policyId: z.string().min(1),
            checkedAtMs: integer,
            committedAtMs: integer,
            commitTimeSource: z.literal('first-visible-observation-upper-bound'),
            status: z.string(),
            code: z.string(),
        }),
    ),
    backlog: integer,
    admissionStore: z
        .strictObject({
            records: integer,
            admitted: integer,
            oldestAtMs: integer.nullable(),
            maxRecords: integer,
            retentionMs: integer,
            pressure: z.boolean(),
        })
        .nullable(),
    outbox: z.looseObject({ records: integer, bytes: integer, oldestAtMs: integer.nullable() }),
    nativeIdentity: identity.nullable(),
    controllerIdentity: identity,
})

/** A valid response alone cannot prove a wake for this run, due origin or live web owner. */
export function assessMonitoringApplianceWake(
    report: MonitoringApplianceReport,
    expected?: { runId: string; webIdentity: { pid: number; startTicks: string } },
) {
    const wake = report.wakeProof
    return wake &&
        wake.design === report.design &&
        wake.windowId === report.windowId &&
        wake.originMs === report.originMs &&
        wake.publishedAtMs < report.originMs &&
        wake.publishedAtMs >= report.originMs - 5000 &&
        wake.wakeCount >= 1 &&
        (!expected ||
            (wake.runId === expected.runId &&
                wake.webIdentity.pid === expected.webIdentity.pid &&
                wake.webIdentity.startTicks === expected.webIdentity.startTicks))
        ? []
        : ['common-original-web-wake-unproven']
}

export const monitoringApplianceTrafficSchema = z.looseObject({
    status: integer,
    elapsedMs: number,
    validBody: z.boolean(),
})

/** Resource comparisons require all matched windows; low CPU or absent telemetry cannot manufacture a benefit. */
export function assessMonitoringAppliancePairs(windows: readonly MonitoringAppliancePairInput[]) {
    const failures = new Set<string>()
    if (windows.length !== 12) failures.add('exact-twelve-matched-windows-required')
    const pairs: {
        profile: string
        pair: number
        addedCpuFraction: number
        addedRssMiB: number
        extraConnections: number
        cpuBenefit: number
        rssBenefit: number
    }[] = []
    let variation = 0
    for (const profile of ['mixed', 'all-3s-timeout'] as const) {
        for (const design of ['A', 'B2'] as const) {
            const entries = windows.filter(
                (window) => window.profile === profile && window.design === design,
            )
            for (const metric of ['cpuPercent', 'rssMiB'] as const) {
                const values = entries
                    .map((entry) => entry[metric])
                    .filter(
                        (value): value is number =>
                            value !== null && Number.isFinite(value) && value >= 0,
                    )
                const mean = values.reduce((sum, value) => sum + value, 0) / values.length
                if (values.length !== contract.samples || !Number.isFinite(mean) || mean <= 0)
                    failures.add('invalid-or-missing-resource-evidence')
                else
                    variation = Math.max(
                        variation,
                        (Math.max(...values) - Math.min(...values)) / mean,
                    )
            }
        }
        for (let pair = 1; pair <= contract.samples; pair++) {
            const baseline = windows.filter(
                (window) =>
                    window.profile === profile && window.pair === pair && window.design === 'A',
            )
            const candidate = windows.filter(
                (window) =>
                    window.profile === profile && window.pair === pair && window.design === 'B2',
            )
            if (baseline.length !== 1 || candidate.length !== 1) {
                failures.add('missing-or-duplicate-matched-pair')
                continue
            }
            const a = baseline[0]!,
                b = candidate[0]!
            if (a.failures.length || b.failures.length) failures.add('window-gate-failure')
            if (
                [
                    a.cpuPercent,
                    b.cpuPercent,
                    a.rssMiB,
                    b.rssMiB,
                    a.peakDatabaseConnections,
                    b.peakDatabaseConnections,
                ].some((value) => value === null || !Number.isFinite(value) || value < 0) ||
                !a.cpuPercent ||
                !a.rssMiB
            ) {
                failures.add('invalid-or-missing-resource-evidence')
                continue
            }
            const addedCpuFraction = b.cpuPercent! / a.cpuPercent - 1
            const addedRssMiB = b.rssMiB! - a.rssMiB
            const extraConnections = b.peakDatabaseConnections! - a.peakDatabaseConnections!
            if (
                b.cpuPercent! > a.cpuPercent * (1 + contract.addedCpuFraction) ||
                addedRssMiB > contract.addedRssMiB ||
                extraConnections > contract.extraDatabaseConnections
            )
                failures.add('added-resource-budget-exceeded')
            pairs.push({
                profile,
                pair,
                addedCpuFraction,
                addedRssMiB,
                extraConnections,
                cpuBenefit: (a.cpuPercent - b.cpuPercent!) / a.cpuPercent,
                rssBenefit: -addedRssMiB / a.rssMiB,
            })
        }
    }
    const minimumCpuBenefit = pairs.length
        ? Math.min(...pairs.map((pair) => pair.cpuBenefit))
        : null
    const minimumRssBenefit = pairs.length
        ? Math.min(...pairs.map((pair) => pair.rssBenefit))
        : null
    return {
        failures: [...failures],
        pairs,
        measuredRelativeVariation: variation,
        minimumCpuBenefit,
        minimumRssBenefit,
        efficiencyJustified:
            failures.size === 0 &&
            [minimumCpuBenefit, minimumRssBenefit].some(
                (value) =>
                    value !== null &&
                    value >= contract.minimumEfficiencyBenefit &&
                    value > 2 * variation,
            ),
    }
}
export const monitoringApplianceResourceSchema = z.strictObject({
    atMs: integer,
    monotonicMs: number,
    scheduledAtMs: integer.optional(),
    cpuMicroseconds: integer,
    rssBytes: integer,
    workingSetBytes: integer,
    memoryBytes: integer,
    pids: integer,
    fds: integer,
    postgresConnections: integer,
    memoryLimitBytes: integer.positive(),
    pidLimit: integer.positive(),
    cpuQuotaCores: number.positive(),
    postgresConnectionLimit: integer.positive(),
    oomKills: integer,
    roles: z.record(z.string(), role),
    database: z.strictObject({
        commits: integer,
        rollbacks: integer,
        blocksRead: integer,
        blocksHit: integer,
        inserted: integer,
        updated: integer,
        deleted: integer,
    }),
})

/** Runs in the sealed disposable appliance; emits numeric observations without credentials or descriptor targets. */
export async function collectMonitoringApplianceResources(
    durationMs: number,
    intervalMs = 1000,
    emit: (sample: MonitoringApplianceResource) => void = (sample) =>
        console.log(JSON.stringify(sample)),
    clock?: MonitoringApplianceWindowClock,
    schedule?: (
        clock: MonitoringApplianceWindowClock,
        intervalMs: number,
        sample: (target: number) => Promise<void>,
    ) => Promise<MonitoringApplianceWindowArming>,
) {
    const { readFile, readdir } = await import('node:fs/promises')
    const { SQL } = await import('bun')
    const text = (path: string) => readFile(path, 'utf8')
    const database = new SQL((await text('/run/rentnerproxy/database-url/value')).trim(), {
        max: 1,
        connectionTimeout: 2,
        idleTimeout: 2,
    })
    const readSample = async (scheduledAtMs?: number) => {
        const [memory, memoryStat, cpu, quota, maximumMemory, maximumPids, pids, events, sql] =
            await Promise.all([
                text('/sys/fs/cgroup/memory.current'),
                text('/sys/fs/cgroup/memory.stat'),
                text('/sys/fs/cgroup/cpu.stat'),
                text('/sys/fs/cgroup/cpu.max'),
                text('/sys/fs/cgroup/memory.max'),
                text('/sys/fs/cgroup/pids.max'),
                text('/sys/fs/cgroup/pids.current'),
                text('/sys/fs/cgroup/memory.events'),
                database`SELECT (SELECT count(*)::int FROM pg_stat_activity WHERE backend_type='client backend') AS connections,
                    current_setting('max_connections')::int AS maximum, xact_commit::float8 AS commits,
                    xact_rollback::float8 AS rollbacks, blks_read::float8 AS read, blks_hit::float8 AS hit,
                    tup_inserted::float8 AS inserted, tup_updated::float8 AS updated, tup_deleted::float8 AS deleted
                    FROM pg_stat_database WHERE datname=current_database()`,
            ])
        const roles: MonitoringApplianceResource['roles'] = {}
        let rssBytes = 0,
            fds = 0
        for (const entry of await readdir('/proc')) {
            if (!/^\d+$/u.test(entry)) continue
            try {
                const directory = '/proc/' + entry
                const [status, command, descriptors] = await Promise.all([
                    text(directory + '/status'),
                    text(directory + '/cmdline'),
                    readdir(directory + '/fd'),
                ])
                const rss = Number(/^VmRSS:[\t ]+(\d+)/mu.exec(status)?.[1] ?? 0) * 1024
                const args = command.split('\0')
                const executable = args[0]?.split('/').at(-1) ?? 'other'
                const roleName = args.some((arg) => arg.endsWith('/docker/web/serve.ts'))
                    ? 'web'
                    : executable === 'monitoring-b2'
                      ? 'monitoring-b2'
                      : executable === 'rentnerproxy-controller' && args[1] !== '--healthcheck'
                        ? 'controller'
                        : executable === 'caddy' && args[1] !== 'version'
                          ? 'caddy'
                          : /^Name:[\t ]+postgres$/mu.test(status)
                            ? 'postgres'
                            : executable === 'bun'
                              ? 'fixture-bun'
                              : 'other'
                const item = (roles[roleName] ??= { processes: 0, rssBytes: 0, fds: 0 })
                item.processes++
                item.rssBytes += rss
                item.fds += descriptors.length
                rssBytes += rss
                fds += descriptors.length
            } catch (error) {
                if (
                    typeof error === 'object' &&
                    error !== null &&
                    'code' in error &&
                    ['ENOENT', 'ESRCH'].includes(String(error.code))
                )
                    continue
                throw error
            }
        }
        const row = sql[0]
        if (!row) throw new Error('B2 database counters missing')
        const [allowed, period] = quota.trim().split(' ')
        const sample: MonitoringApplianceResource = {
            atMs: Date.now(),
            monotonicMs: performance.now(),
            cpuMicroseconds: Number(/^usage_usec (\d+)/mu.exec(cpu)?.[1]),
            rssBytes,
            memoryBytes: Number(memory),
            workingSetBytes: Math.max(
                0,
                Number(memory) - Number(/^inactive_file (\d+)/mu.exec(memoryStat)?.[1] ?? 0),
            ),
            pids: Number(pids),
            fds,
            postgresConnections: Number(row.connections),
            memoryLimitBytes: Number(maximumMemory),
            pidLimit: Number(maximumPids),
            cpuQuotaCores: Number(allowed) / Number(period),
            postgresConnectionLimit: Number(row.maximum),
            oomKills: Number(/^oom_kill (\d+)/mu.exec(events)?.[1]),
            roles,
            database: {
                commits: Number(row.commits),
                rollbacks: Number(row.rollbacks),
                blocksRead: Number(row.read),
                blocksHit: Number(row.hit),
                inserted: Number(row.inserted),
                updated: Number(row.updated),
                deleted: Number(row.deleted),
            },
        }
        emit({ ...sample, ...(scheduledAtMs === undefined ? {} : { scheduledAtMs }) })
    }
    try {
        if (clock) {
            if (durationMs !== 120000 || !schedule)
                throw new Error('B2 absolute resource scheduler required')
            return await schedule(clock, intervalMs, readSample)
        }
        const deadline = performance.now() + durationMs
        do {
            const started = performance.now()
            await readSample()
            const remaining = deadline - performance.now()
            if (remaining <= 0) break
            await Bun.sleep(
                Math.min(remaining, Math.max(1, intervalMs - (performance.now() - started))),
            )
        } while (performance.now() <= deadline)
    } finally {
        await database.close()
    }
}

/** Serialize the bounded in-container cgroup/process collector, never a host resource sampler. */
export function monitoringApplianceResourceProbe(durationMs: number) {
    if (durationMs !== contract.sampleMs)
        throw new Error('B2 resource duration must match contract')
    return `await (${collectMonitoringApplianceResources.toString()})(${durationMs});`
}

/** Require complete reference-envelope samples before summarizing cgroup CPU, summed RSS and connections. */
export function summarizeMonitoringApplianceResources(
    samples: readonly MonitoringApplianceResource[],
    expected?: { clock: MonitoringApplianceWindowClock; arming: MonitoringApplianceWindowArming },
) {
    const failures: string[] = []
    if (samples.length < contract.sampleMs / 2000) failures.push('insufficient-resource-samples')
    for (const sample of samples) {
        monitoringApplianceResourceSchema.parse(sample)
        if (
            sample.memoryLimitBytes !== contract.reference.memoryMiB * 1024 ** 2 ||
            sample.pidLimit !== contract.reference.pids ||
            sample.cpuQuotaCores !== contract.reference.cpus ||
            sample.postgresConnectionLimit !== contract.reference.databaseConnections
        )
            failures.push('reference-envelope-mismatch')
        if (
            sample.memoryBytes > sample.memoryLimitBytes ||
            sample.pids > sample.pidLimit ||
            sample.postgresConnections > sample.postgresConnectionLimit ||
            sample.oomKills !== 0
        )
            failures.push('reference-resource-limit')
        if (
            sample.roles.web?.processes !== 1 ||
            sample.roles.controller?.processes !== 1 ||
            sample.roles.caddy?.processes !== 1
        )
            failures.push('required-runtime-owner-missing-or-duplicated')
    }
    const first = samples[0],
        last = samples.at(-1)
    const elapsedMs = first && last ? last.monotonicMs - first.monotonicMs : 0
    if (expected) {
        const { clock, arming } = expected
        if (
            clock.endMs - clock.originMs !== contract.sampleMs ||
            Math.abs(clock.endMonotonicMs - clock.originMonotonicMs - contract.sampleMs) > 0.001 ||
            arming.armedAtMs >= clock.originMs ||
            arming.armedMonotonicMs >= clock.originMonotonicMs ||
            samples.length !== contract.sampleMs / 1000 + 1 ||
            first?.scheduledAtMs !== clock.originMs ||
            last?.scheduledAtMs !== clock.endMs ||
            samples.some(
                (sample, index) =>
                    sample.scheduledAtMs !== clock.originMs + index * 1000 ||
                    sample.atMs < sample.scheduledAtMs ||
                    sample.atMs - sample.scheduledAtMs >= 1000 ||
                    sample.monotonicMs < clock.originMonotonicMs + index * 1000 ||
                    sample.monotonicMs - (clock.originMonotonicMs + index * 1000) >= 1000,
            )
        )
            failures.push('resource-window-origin-or-end-misaligned')
    }

    if (
        elapsedMs < contract.sampleMs - 2000 ||
        !first ||
        !last ||
        last.cpuMicroseconds < first.cpuMicroseconds
    )
        failures.push('incomplete-resource-window')
    return {
        failures: [...new Set(failures)],
        elapsedMs,
        cpuPercent:
            first && last && elapsedMs > 0
                ? (last.cpuMicroseconds - first.cpuMicroseconds) / (elapsedMs * 10)
                : null,
        rssMiB: samples.length
            ? Math.max(...samples.map((sample) => sample.rssBytes)) / 1024 ** 2
            : null,
        workingSetMiB: samples.length
            ? Math.max(...samples.map((sample) => sample.workingSetBytes)) / 1024 ** 2
            : null,
        peakPids: samples.length ? Math.max(...samples.map((sample) => sample.pids)) : null,
        peakFds: samples.length ? Math.max(...samples.map((sample) => sample.fds)) : null,
        peakDatabaseConnections: samples.length
            ? Math.max(...samples.map((sample) => sample.postgresConnections))
            : null,
        databaseDelta:
            first && last
                ? Object.fromEntries(
                      Object.keys(first.database).map((key) => {
                          const field = key as keyof MonitoringApplianceResource['database']
                          return [key, last.database[field] - first.database[field]]
                      }),
                  )
                : null,
    }
}

/** Match actual native requests and committed observations to each of the two due slots, without filling missing work. */
export function summarizeMonitoringApplianceProgress(
    report: MonitoringApplianceReport,
    traffic: readonly MonitoringApplianceTraffic[],
) {
    monitoringApplianceReportSchema.parse(report)
    const failures = new Set<string>(assessMonitoringApplianceWake(report))
    if (
        report.design === 'B2' &&
        (!report.admissionStore ||
            report.admissionStore.maxRecords !== contract.maxOutboxRecords ||
            report.admissionStore.retentionMs !== contract.retentionMs ||
            report.admissionStore.records > contract.maxOutboxRecords ||
            report.admissionStore.admitted > report.admissionStore.records ||
            report.admissionStore.pressure)
    )
        failures.add('original-admission-store-bound-unproven')
    if (
        report.policies.length !== contract.policies ||
        new Set(report.policies.map((policy) => policy.policyId)).size !== contract.policies ||
        new Set(report.policies.map((policy) => policy.hostId)).size !== contract.policies
    )
        failures.add('policy-identity-coverage')
    if (report.endMs - report.originMs < contract.sampleMs)
        failures.add('incomplete-progress-window')
    const startLags: number[] = [],
        commitLags: number[] = []
    let coveredSlots = 0,
        duplicates = 0,
        unexpectedOutcomes = 0
    if (
        report.heads.length !== contract.policies * 2 ||
        report.observations.length !== contract.policies * 2
    )
        failures.add('unaccounted-execution-or-observation')
    if (
        report.starts.length !== contract.policies * 2 ||
        new Set(report.starts.map((start) => start.attemptId)).size !== contract.policies * 2
    )
        failures.add('execution-authority-coverage')
    for (const policy of report.policies) {
        if (policy.dueAtMs !== report.originMs) failures.add('unexpected-due-origin')
        const heads = report.heads
            .filter((head) => head.hostId === policy.hostId)
            .toSorted((a, b) => a.atMs - b.atMs)
        const observations = report.observations
            .filter((observation) => observation.policyId === policy.policyId)
            .toSorted((a, b) => a.checkedAtMs - b.checkedAtMs)
        const starts = report.starts
            .filter((start) => start.policyId === policy.policyId)
            .toSorted((a, b) => a.dueAtMs - b.dueAtMs)
        if (heads.length > 2 || observations.length > 2) duplicates++
        for (let period = 0; period < 2; period++) {
            const due = policy.dueAtMs + period * contract.intervalSeconds * 1000
            const head = heads[period],
                observation = observations[period]
            const start = starts[period]
            if (
                !start ||
                start.dueAtMs !== due ||
                start.admittedAtMs < due ||
                start.atMs < due ||
                (head && (start.admittedAtMs > head.atMs || start.durableAtMs > head.atMs)) ||
                (report.design === 'B2' &&
                    (!start.durable || start.durableAtMs < start.admittedAtMs))
            )
                failures.add('execution-authority-coverage')
            if (head && observation && head.atMs >= due && observation.checkedAtMs >= due)
                coveredSlots++
            if (head) {
                startLags.push(head.atMs - due)
                if (head.atMs < due) failures.add('execution-before-due')
            }
            if (observation) {
                commitLags.push(observation.committedAtMs - due)
                if (
                    observation.status !== policy.expectedStatus ||
                    observation.code !== policy.expectedCode ||
                    observation.checkedAtMs < due ||
                    observation.committedAtMs < observation.checkedAtMs
                )
                    unexpectedOutcomes++
            }
        }
    }
    if (coveredSlots !== contract.policies * 2) failures.add('exact-two-cadence-coverage')
    if (duplicates) failures.add('duplicate-execution-or-observation')
    if (unexpectedOutcomes) failures.add('unexpected-or-unproven-outcome')
    if (report.backlog !== 0 || report.outbox.records !== 0)
        failures.add('growing-or-undrained-backlog')
    const dueToStart = monitoringDistribution(startLags),
        dueToCommit = monitoringDistribution(commitLags)
    if (dueToStart.p99 === null || dueToStart.p99 > contract.dueToStartP99Ms)
        failures.add('due-to-start-gate')
    if (dueToCommit.max === null || dueToCommit.max > contract.dueToCommitMaxMs)
        failures.add('due-to-commit-gate')
    const latency = (kind: MonitoringApplianceTraffic['kind']) =>
        monitoringDistribution(
            traffic.filter((item) => item.kind === kind).map((item) => item.elapsedMs),
        )
    const ssr = latency('ssr'),
        proxy = latency('proxy')
    const errors = traffic.filter(
        (item) =>
            item.status !== 200 ||
            !item.validBody ||
            !Number.isFinite(item.elapsedMs) ||
            item.elapsedMs < 0 ||
            item.atMs < report.originMs ||
            item.atMs > report.endMs,
    ).length
    if (
        errors ||
        ssr.count < 100 ||
        proxy.count < 100 ||
        ssr.p99 === null ||
        proxy.p99 === null ||
        ssr.p99 > contract.ssrP99Ms ||
        proxy.p99 > contract.proxyP99Ms
    )
        failures.add('ssr-or-proxy-gate')
    return {
        failures: [...failures],
        coveredSlots,
        duplicates,
        unexpectedOutcomes,
        dueToStart,
        dueToCommit,
        ssr,
        proxy,
        trafficErrors: errors,
    }
}
