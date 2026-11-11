import { isolationContract as contract } from './isolation.config.ts'
import type {
    IsolationDistribution,
    IsolationLoad,
    IsolationMeasurement,
    IsolationOptions,
} from './Types/isolation.types.ts'

import { domainDiagnosticsResultSchema } from '../../web/src/lib/RuntimeDiagnostics/domainDiagnostics.ts'
import type { DomainDiagnosticsControllerInput } from '../../web/src/lib/RuntimeDiagnostics/Types/domain-diagnostics.types.ts'
import { simulatedMonitoringResult } from './monitoring.ts'

/** Validate a synthetic healthy probe through the production diagnostics schema; duration is rounded to milliseconds. */
export function isolationDiagnosticResult(
    input: DomainDiagnosticsControllerInput,
    durationMs: number,
) {
    return domainDiagnosticsResultSchema.parse(
        simulatedMonitoringResult(input, Math.round(durationMs), false),
    )
}

/** Accept only fixture domains with a canonical index from 0 through 999; reject all other probe targets. */
export function isolationProbeIndex(domain: string): number {
    const match = /^isolation-(0|[1-9]\d{0,2})\.example\.test$/u.exec(domain)
    if (!match) throw new Error('Invalid isolation probe domain')
    return Number(match[1])
}

/** Reject privileged or non-fixture roles before runtime imports; database identity must match the fixed local fixture and role. */
export function assertIsolationRole(
    flag: string | undefined,
    platform: string,
    uid: number | undefined,
    url: string | undefined,
    role: 'foreground' | 'combined' | 'background',
): void {
    if (flag !== '1' || platform !== 'linux' || uid === undefined || uid === 0)
        throw new Error('Explicit unprivileged isolation role required')
    let parsed: URL
    try {
        parsed = new URL(url ?? '')
    } catch {
        throw new Error('Isolated role database required')
    }
    const allowed =
        role === 'background' ? ['isolation_worker', 'isolation_denied'] : ['isolation_foreground']
    if (
        !['postgres:', 'postgresql:'].includes(parsed.protocol) ||
        parsed.hostname !== '127.0.0.1' ||
        parsed.port !== '55439' ||
        parsed.pathname !== '/rentnerproxy_upstream_test' ||
        !allowed.includes(parsed.username)
    )
        throw new Error('Isolated role database required')
}

/** Parse unique flag/value pairs within the predeclared load and duration bounds; fault-only runs must enable faults. */
export function parseIsolationOptions(args: string[]): IsolationOptions {
    const flags = new Map<string, string>()
    for (let index = 0; index < args.length; index += 2) {
        const flag = args[index]!,
            value = args[index + 1]
        if (
            !['--loads', '--duration-seconds', '--faults', '--faults-only'].includes(flag) ||
            flags.has(flag) ||
            !value
        )
            throw new Error('Invalid isolation option')
        flags.set(flag, value)
    }
    const loads = (flags.get('--loads') ?? '100,500,1000').split(',')
    const duration = flags.get('--duration-seconds') ?? String(contract.durationSeconds)
    const faults = flags.get('--faults') ?? 'true'
    const faultsOnly = flags.get('--faults-only') ?? 'false'
    if (
        !loads.length ||
        new Set(loads).size !== loads.length ||
        loads.some((item) => !['100', '500', '1000'].includes(item)) ||
        !/^(?:[5-9]|1[0-5])$/u.test(duration) ||
        !['true', 'false'].includes(faults) ||
        !['true', 'false'].includes(faultsOnly) ||
        (faultsOnly === 'true' && faults === 'false')
    )
        throw new Error('Isolation workload outside fixed bounds')
    return {
        loads: loads.map(Number) as IsolationLoad[],
        repetitions: contract.repetitions,
        durationSeconds: Number(duration),
        faults: faults === 'true',
        faultsOnly: faultsOnly === 'true',
    }
}

/** Return nearest-rank percentiles without mutating samples; reject negative/nonfinite values and use null for empty samples. */
export function isolationDistribution(values: readonly number[]): IsolationDistribution {
    if (values.some((value) => !Number.isFinite(value) || value < 0))
        throw new Error('Invalid isolation measurement')
    const sorted = values.toSorted((left, right) => left - right)
    return {
        count: sorted.length,
        p95: sorted[Math.ceil(sorted.length * 0.95) - 1] ?? null,
        p99: sorted[Math.ceil(sorted.length * 0.99) - 1] ?? null,
        max: sorted.at(-1) ?? null,
    }
}

/** Select the upper middle sample from the nonempty repetition sets validated by assessIsolation. */
const median = (values: number[]) =>
    values.toSorted((a, b) => a - b)[Math.floor(values.length / 2)]!

/** Require complete, healthy matched repetitions before comparing noise and resource overhead; this assessment never authorizes migration. */
export function assessIsolation(
    measurements: readonly IsolationMeasurement[],
    maintenanceProven: boolean,
) {
    const results = contract.loads.map((load) => {
        const baseline = measurements.filter(
            (item) => item.load === load && item.mode === 'in-process',
        )
        const candidate = measurements.filter(
            (item) => item.load === load && item.mode === 'separate-bun',
        )
        if (
            baseline.length !== contract.repetitions ||
            candidate.length !== contract.repetitions ||
            [...baseline, ...candidate].some(
                (item) =>
                    item.ssr.p99 === null ||
                    item.ssr.count < contract.thresholds.minimumSamples ||
                    item.observations !== item.load ||
                    item.healthyObservations !== item.observations ||
                    item.network.invalid > 0 ||
                    item.network.accepted !== item.observations ||
                    item.network.snapshots === 0,
            )
        )
            return { load, decision: 'insufficient' as const }
        const baselineP99 = baseline.map((item) => item.ssr.p99 ?? Infinity),
            candidateP99 = candidate.map((item) => item.ssr.p99 ?? Infinity)
        const baselineMedian = median(baselineP99),
            candidateMedian = median(candidateP99)
        const noiseMs = Math.max(
            Math.max(...baselineP99) - Math.min(...baselineP99),
            Math.max(...candidateP99) - Math.min(...candidateP99),
        )
        const baselineMeetsSlo = baseline.every((item) => meetsIsolationSlo(item))
        const overhead = {
            rssBytes:
                median(candidate.map((item) => item.bunRssBytes)) -
                median(baseline.map((item) => item.bunRssBytes)),
            cpuPercentagePoints:
                median(candidate.map((item) => item.totalCpuPercentagePoints)) -
                median(baseline.map((item) => item.totalCpuPercentagePoints)),
            connections:
                Math.max(...candidate.map((item) => item.postgresConnections)) -
                Math.max(...baseline.map((item) => item.postgresConnections)),
        }
        const material =
            baselineMedian - candidateMedian > noiseMs &&
            candidateMedian <= baselineMedian * (1 - contract.thresholds.minimumRelativeImprovement)
        const fits =
            overhead.rssBytes <= contract.thresholds.extraRssBytes &&
            overhead.cpuPercentagePoints <= contract.thresholds.extraCpuPercentagePoints &&
            overhead.connections <= contract.thresholds.extraDatabaseConnections &&
            candidate.every(meetsIsolationSlo)
        return {
            load,
            baselineMedianP99Ms: baselineMedian,
            candidateMedianP99Ms: candidateMedian,
            noiseMs,
            baselineMeetsSlo,
            material,
            fits,
            overhead,
            decision:
                baselineMeetsSlo || !material || !fits || !maintenanceProven
                    ? ('keep-in-process' as const)
                    : ('candidate-requires-separate-authorization' as const),
        }
    })
    return {
        recommendation: results.some((item) => item.decision === 'insufficient')
            ? 'insufficient-evidence'
            : results.some((item) => item.decision === 'candidate-requires-separate-authorization')
              ? 'selective-experiment-only'
              : 'keep-in-process',
        maintenanceProven,
        results,
        productionMigrationAuthorized: false,
    }
}

/** Require complete healthy progress, bounded foreground latency and recovery, and the shared resource envelope for one repetition. */
export function meetsIsolationSlo(item: IsolationMeasurement): boolean {
    return (
        item.ssr.count >= contract.thresholds.minimumSamples &&
        item.ssr.p95 !== null &&
        item.ssr.p95 <= contract.thresholds.ssrP95Ms &&
        item.ssr.p99 !== null &&
        item.ssr.p99 <= contract.thresholds.ssrP99Ms &&
        item.foreground.lag.p99 !== null &&
        item.foreground.lag.p99 <= contract.thresholds.eventLoopP99Ms &&
        item.stopMs <= contract.thresholds.gracefulRecoveryMs &&
        item.ssrFailures === 0 &&
        item.rejectedTraffic === 0 &&
        item.telemetryFailures === 0 &&
        item.foreground.failures === 0 &&
        (item.background?.failures ?? 0) === 0 &&
        item.observations === item.load &&
        item.healthyObservations === item.load &&
        item.network.probes === item.load &&
        item.network.accepted === item.load &&
        item.network.invalid === 0 &&
        item.network.snapshots > 0 &&
        item.duplicates === 0 &&
        item.dueToCommitMaxMs !== null &&
        item.dueToCommitMaxMs <= contract.thresholds.dueToCommitMaxMs &&
        item.totalMemoryBytes <= contract.envelope.memoryBytes &&
        item.pids <= contract.envelope.pids &&
        item.postgresConnections <= contract.envelope.postgresConnections
    )
}
