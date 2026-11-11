import { createHash } from 'node:crypto'
import { createSeededRandom, parseReliabilityOptions } from '../runtime-reliability/control.ts'
import { resourceLimits } from '../runtime-reliability/control.config.ts'
import type {
    BackgroundOptions,
    BackgroundPolicy,
    BackgroundReceipt,
    BackgroundResourceSample,
    BackgroundSnapshot,
} from './Types/background.types.ts'

export const backgroundSlo = {
    intervalSeconds: 60,
    dueToNetworkStartP99Ms: 55000,
    dueToCommitMaxMs: 60000,
    managementReadP99Ms: 500,
    ssrP99Ms: 1000,
    proxyP99Ms: 1000,
    proxyFailures: 0,
    recoveryMs: 150000,
    extendedRecoveryMs: 600000,
    minimumContinuousSamples: 8,
    minimumContinuousSpanMs: 30000,
    minimumChecksPerSecond: { 100: 100 / 60, 500: 500 / 60, 1000: 1000 / 60 },
    notifications: {
        attempted: 320,
        acknowledged: 280,
        permanentFailures: 40,
        schedulerDrainMs: 700000,
        wallBudgetMs: 45000,
    },
    discovery: { events: 256, inventory: 8, reconnects: 3, recoveryMs: 45000 },
} as const

export function parseBackgroundOptions(args: string[]): BackgroundOptions {
    const flags = new Map<string, string>()
    for (let index = 0; index < args.length; index += 2) {
        const flag = args[index]!
        const value = args[index + 1]
        if (
            ![
                '--policies',
                '--seed',
                '--pattern',
                '--profile',
                '--duration-seconds',
                '--image',
                '--report',
                '--extended-recovery',
                '--contention',
            ].includes(flag) ||
            flags.has(flag) ||
            !value ||
            value.startsWith('--')
        )
            throw new Error('Invalid background option')
        flags.set(flag, value)
    }
    const policies = flags.get('--policies') ?? '100'
    const pattern = flags.get('--pattern') ?? 'simultaneous'
    const profile = flags.get('--profile') ?? 'mixed'
    const extendedRecovery = flags.get('--extended-recovery') ?? 'false'
    const contention = flags.get('--contention') ?? 'false'
    if (
        !['100', '500', '1000'].includes(policies) ||
        !['simultaneous', 'staggered'].includes(pattern) ||
        !['mixed', 'healthy', 'slow', 'timeout'].includes(profile) ||
        !['true', 'false'].includes(extendedRecovery) ||
        !['true', 'false'].includes(contention)
    )
        throw new Error('Background workload outside fixed bounds')
    const inherited = parseReliabilityOptions(
        [...flags]
            .filter(([flag]) =>
                ['--seed', '--duration-seconds', '--image', '--report'].includes(flag),
            )
            .flat(),
    )
    if (inherited.durationSeconds > 300)
        throw new Error(
            'Background scenarios are bounded to five minutes; use release reliability for soaks',
        )
    return {
        policies: Number(policies) as BackgroundOptions['policies'],
        seed: inherited.seed,
        pattern: pattern as BackgroundOptions['pattern'],
        profile: profile as BackgroundOptions['profile'],
        durationSeconds: inherited.durationSeconds,
        ...(inherited.image ? { image: inherited.image } : {}),
        ...(inherited.reportPath ? { reportPath: inherited.reportPath } : {}),
        ...(extendedRecovery === 'true' ? { extendedRecovery: true as const } : {}),
        ...(contention === 'true' ? { contention: true as const } : {}),
    }
}

export function backgroundPolicies(options: BackgroundOptions): BackgroundPolicy[] {
    const random = createSeededRandom(options.seed)
    return Array.from({ length: options.policies }, (_, index) => {
        const digest = createHash('sha256')
            .update(`background:${options.seed}:${index}`)
            .digest('hex')
        return {
            id: `${digest.slice(0, 8)}-${digest.slice(8, 12)}-4${digest.slice(13, 16)}-8${digest.slice(17, 20)}-${digest.slice(20, 32)}`,
            index,
            latency:
                options.profile === 'mixed'
                    ? (['healthy', 'slow', 'timeout', 'failure'] as const)[
                          Math.floor(random() * 4)
                      ]!
                    : options.profile,
            offsetMs: options.pattern === 'staggered' ? Math.floor(random() * 10000) : 0,
        }
    })
}

export function backgroundDistribution(values: readonly number[]) {
    if (values.some((value) => !Number.isFinite(value) || value < 0))
        throw new Error('Invalid background measurement')
    const sorted = values.toSorted((a, b) => a - b)
    const percentile = (fraction: number) => sorted[Math.ceil(sorted.length * fraction) - 1] ?? null
    return {
        count: sorted.length,
        p50: percentile(0.5),
        p95: percentile(0.95),
        p99: percentile(0.99),
        max: sorted.at(-1) ?? null,
    }
}

export function backgroundResourceEvidence(samples: BackgroundResourceSample[]) {
    const numeric = [
        'elapsedSeconds',
        'memoryBytes',
        'cpuPercent',
        'pids',
        'postgresConnections',
        'webFds',
        'controllerFds',
        'caddyFds',
        'restartCount',
    ] as const
    const valid = samples.filter(
        (item) =>
            item.sample.phase === item.kind &&
            numeric.every((key) => Number.isFinite(item.sample[key]) && item.sample[key] >= 0),
    )
    const groups = Map.groupBy(
        valid.filter((item) => item.kind === 'continuous' && !item.warmup),
        (item) => item.generation,
    )
    const comparable = [...groups.values()].some(
        (group) =>
            group.length >= backgroundSlo.minimumContinuousSamples &&
            group.every(
                (item, index) =>
                    index === 0 ||
                    item.sample.elapsedSeconds > group[index - 1]!.sample.elapsedSeconds,
            ) &&
            (group.at(-1)!.sample.elapsedSeconds - group[0]!.sample.elapsedSeconds) * 1000 >=
                backgroundSlo.minimumContinuousSpanMs,
    )
    const values = valid.map((item) => item.sample)
    const maxima = Object.fromEntries(
        [
            'memoryBytes',
            'cpuPercent',
            'pids',
            'postgresConnections',
            'webFds',
            'controllerFds',
            'caddyFds',
        ].map((key) => [
            key,
            values.length
                ? Math.max(...values.map((value) => value[key as keyof typeof value] as number))
                : null,
        ]),
    )
    const violated = values.some(
        (value) =>
            value.memoryBytes > resourceLimits.memoryBytes ||
            value.pids > resourceLimits.pids ||
            value.postgresConnections > resourceLimits.postgresConnections ||
            value.oomKilled,
    )
    return {
        baseline: { name: 'appliance-1g-2cpu-512pid-100conn', ...resourceLimits, cpuCores: 2 },
        continuous: samples.filter((item) => item.kind === 'continuous').length,
        recovered: samples.filter((item) => item.kind === 'recovered').length,
        processGenerations: groups.size,
        missingTelemetry: samples.length - valid.length,
        trendEvidence: comparable ? 'comparable' : 'insufficient',
        status: violated
            ? 'unmet'
            : valid.length !== samples.length || !values.length
              ? 'insufficient'
              : 'met',
        maxima,
        stabilityClaimed: false,
    }
}

export function backgroundMonitoringReport(
    options: BackgroundOptions,
    snapshot: BackgroundSnapshot,
    receipts: BackgroundReceipt[],
    elapsedMs: number,
) {
    const expected = backgroundPolicies(options)
    const policies = snapshot.policies.map((policy) => {
        const observations = snapshot.observations.filter((item) => item.policyId === policy.id)
        const network = receipts.filter((item) => item.index === policy.index)
        const workload = expected[policy.index]!
        const expectedCode =
            workload.latency === 'timeout'
                ? 'upstream_timeout'
                : workload.latency === 'failure'
                  ? 'unexpected_status'
                  : 'expected_status'
        const first = observations[0]
        return {
            id: policy.id,
            index: policy.index,
            expectedCode,
            attempted: 1,
            networkAccepted: network.length,
            committed: observations.length,
            correctOutcome:
                first?.code === expectedCode &&
                first.policyVersion === policy.version &&
                first.status === (expectedCode === 'expected_status' ? 'healthy' : 'unhealthy'),
            dueToStartMs: network[0] ? Math.max(0, network[0].startedAt - policy.dueAt) : null,
            dueToCommitMs: first ? Math.max(0, first.checkedAt - policy.dueAt) : null,
            strandedLease: policy.leased,
        }
    })
    const starts = backgroundDistribution(
        policies.flatMap((policy) => (policy.dueToStartMs === null ? [] : [policy.dueToStartMs])),
    )
    const completions = backgroundDistribution(
        policies.flatMap((policy) => (policy.dueToCommitMs === null ? [] : [policy.dueToCommitMs])),
    )
    const committed = policies.reduce((sum, policy) => sum + policy.committed, 0)
    const throughput = elapsedMs > 0 ? committed / (elapsedMs / 1000) : null
    const complete =
        policies.length === options.policies &&
        new Set(policies.map((policy) => policy.id)).size === options.policies &&
        new Set(policies.map((policy) => policy.index)).size === options.policies &&
        policies.every(
            (policy) => policy.committed === 1 && policy.correctOutcome && !policy.strandedLease,
        )
    return {
        policies,
        attempted: options.policies,
        schedulerAdmitted: null,
        schedulerAdmissionEvidence:
            'unavailable; upstream receipts establish network acceptance only',
        networkAccepted: receipts.length,
        committed,
        checksPerSecond: throughput,
        queueLag: null,
        schedulerStartLag: null,
        startLag: starts,
        completionLag: completions,
        status: !complete
            ? 'unmet'
            : starts.count < options.policies
              ? 'insufficient'
              : starts.p99! > backgroundSlo.dueToNetworkStartP99Ms ||
                  completions.max! > backgroundSlo.dueToCommitMaxMs ||
                  throughput === null ||
                  throughput < backgroundSlo.minimumChecksPerSecond[options.policies]
                ? 'unmet'
                : 'met',
    }
}
