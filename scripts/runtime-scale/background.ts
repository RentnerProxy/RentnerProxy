// oxlint-disable no-await-in-loop -- Ownership proof requires ordered census/readiness checks within one deadline.
import { memoryResourceEvidence } from '../runtime-reliability/resource-probe.ts'
import { createHash } from 'node:crypto'
import assert from 'node:assert/strict'
import { createSeededRandom, parseReliabilityOptions } from '../runtime-reliability/control.ts'
import { resourceLimits } from '../runtime-reliability/control.config.ts'
import type {
    BackgroundOptions,
    BackgroundPolicy,
    BackgroundReceipt,
    BackgroundResourceSample,
    BackgroundSnapshot,
} from './Types/background.types.ts'
import type {
    ComponentRecoveryDependencies,
    ComponentRecoverySnapshot,
} from '../runtime-reliability/Types/component-recovery.types.ts'

/** Confirm live, stable owners before injecting a separate component fault. */
export async function backgroundRecoveryBaseline(
    dependencies: ComponentRecoveryDependencies,
    timeoutMs: number = backgroundSlo.recoveryMs,
): Promise<ComponentRecoverySnapshot> {
    const now = dependencies.now ?? (() => performance.now())
    const sleep = dependencies.sleep ?? Bun.sleep
    const deadline = now() + timeoutMs
    let startedAt: string | undefined
    /** Reject duplicate or replaced appliance owners and return an identity only when every required process is live. */
    const generation = (snapshot: ComponentRecoverySnapshot) => {
        assert.equal(snapshot.running, true, 'Baseline must retain the running appliance')
        startedAt ??= snapshot.startedAt
        assert.equal(snapshot.startedAt, startedAt, 'Baseline must retain appliance identity')
        let live = true
        const owners = (['web', 'controller', 'caddy'] as const).map((role) => {
            const matches = snapshot.processes.filter((process) => process.role === role)
            assert.ok(matches.length <= 1, 'Baseline rejects duplicate ' + role + ' owners')
            const owner = matches[0]
            if (!owner || owner.state === 'Z' || owner.state === 'X') live = false
            return owner ? [role, owner.pid, owner.startTicks] : null
        })
        return live ? JSON.stringify(owners) : undefined
    }
    while (now() <= deadline) {
        const before = await dependencies.snapshot()
        const expected = before && generation(before)
        if (expected && (await dependencies.ready())) {
            const after = await dependencies.snapshot()
            if (after && generation(after) === expected && now() <= deadline) return after
        }
        const remaining = deadline - now()
        if (remaining <= 0) break
        await sleep(Math.min(250, remaining))
    }
    throw dependencies.timeoutError('stable background recovery baseline')
}

/** Export at most four static source frames and a fixed owner role; never expose arbitrary error messages, paths, or payloads. */
export function backgroundFailureDiagnostic(error: unknown) {
    if (!(error instanceof Error)) return { frames: [] as string[] }
    const frames = [
        ...(error.stack ?? '').matchAll(
            /[\\/](background-run|background|background-extended|background-recovery|component-recovery)\.ts:(\d{1,7}):(\d{1,7})\b/gu,
        ),
    ].map((match) => match[1] + '.ts:' + match[2] + ':' + match[3])
    const role =
        error instanceof assert.AssertionError
            ? (['web', 'controller', 'caddy'] as const).find(
                  (candidate) =>
                      error.message.startsWith(
                          'component recovery requires one ' + candidate + ' process',
                      ) ||
                      error.message.startsWith('recovered ' + candidate + ' owner must be live') ||
                      error.message.startsWith(
                          'Baseline rejects duplicate ' + candidate + ' owners',
                      ),
              )
            : undefined
    return { frames: [...new Set(frames)].slice(0, 4), ...(role ? { role } : {}) }
}

/** A replay comparison is meaningful only after nonempty, one-to-one durable audit effects. */
export function assertCertificateReplayEvidence(evidence: {
    cursor: string | null | undefined
    receiptIds: readonly string[]
    auditIds: readonly string[]
    auditEventIds: readonly string[]
}) {
    assert.ok(
        evidence.cursor && evidence.receiptIds.length > 0,
        'Actual controller events are required to establish replay evidence',
    )
    assert.ok(
        evidence.auditIds.length > 0,
        'Nonempty actual certificate audit effects are required to establish replay evidence',
    )
    assert.equal(
        evidence.auditIds.length,
        evidence.auditEventIds.length,
        'Every observed audit row must carry its event identity',
    )
    assert.equal(
        new Set(evidence.auditEventIds).size,
        evidence.auditEventIds.length,
        'Every certificate event has at most one audit effect',
    )
    assert.deepEqual(
        [...evidence.auditEventIds].toSorted(),
        [...evidence.receiptIds].toSorted(),
        'Every actual certificate receipt must have exactly one durable audit effect',
    )
}

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

/** Accept unique flag/value pairs for fixed workload profiles and cap the cadence window at five minutes; extended recovery stays explicit. */
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

/** Derive stable policy identities, latency profiles, and due offsets from the seed without depending on global randomness or wall time. */
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

/** Compute nearest-rank tails without mutating input; reject invalid samples and represent missing evidence with null percentiles. */
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

/** Require monotonic continuous samples within one process generation for trends; recovery samples cannot repair missing continuous evidence. */
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
    const processMemory = memoryResourceEvidence(samples.map((item) => item.sample))
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
        maxima: { ...maxima, ...processMemory.maxima },
        processMemoryComplete: processMemory.complete,
        memoryMeasurementSemantics: processMemory.semantics,
        stabilityClaimed: false,
    }
}

/** Compare durable once-only outcomes and network receipts with the seeded workload; missing start evidence cannot establish scheduler admission. */
export function backgroundMonitoringReport(
    options: BackgroundOptions,
    snapshot: BackgroundSnapshot,
    receipts: BackgroundReceipt[],
    elapsedMs: number,
    expectedCapture?: { runId: string; sourceSha: string },
) {
    const expected = backgroundPolicies(options)
    const capture = snapshot.schedulerCapture
    const captured =
        !!capture &&
        !!expectedCapture &&
        capture.runId === expectedCapture.runId &&
        capture.sourceSha === expectedCapture.sourceSha &&
        capture.publication.fd !== null && capture.publication.generation > 0 &&
        !capture.overflowed &&
        !capture.expired &&
        !capture.clockRegressed
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
        const admitted = captured
            ? capture.events.filter(
                  (event) =>
                      event.kind === 'admitted' &&
                      event.policyId === policy.id &&
                      event.policyVersion === policy.version &&
                      event.dueAt === policy.dueAt,
              )
            : []
        const admission = admitted.length === 1 ? admitted[0] : undefined
        const started = captured
            ? capture!.events.filter(
                  (event) =>
                      event.kind === 'started' &&
                      event.policyId === policy.id &&
                      event.policyVersion === policy.version &&
                      event.dueAt === policy.dueAt,
              )
            : []
        const adapterStart =
            started.length === 1 && started[0]!.attemptHash === admission?.attemptHash
                ? started[0]
                : undefined
        const validTiming =
            admission &&
            adapterStart &&
            admission.at >= policy.dueAt &&
            adapterStart.at >= admission.at &&
            adapterStart.monotonicMs >= admission.monotonicMs
        return {
            id: policy.id,
            index: policy.index,
            expectedCode,
            attempted: 1,
            schedulerAdmitted: captured ? admitted.length : null,
            schedulerStarted: captured ? started.length : null,
            dueToAdmissionMs:
                admission && admission.at >= policy.dueAt ? admission.at - policy.dueAt : null,
            admissionToAdapterStartMs: validTiming
                ? adapterStart.monotonicMs - admission.monotonicMs
                : null,
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
    const admissions = captured
        ? policies.reduce((sum, policy) => sum + (policy.schedulerAdmitted ?? 0), 0)
        : null
    const queueLag = captured
        ? backgroundDistribution(
              policies.flatMap((policy) =>
                  policy.dueToAdmissionMs === null ? [] : [policy.dueToAdmissionMs],
              ),
          )
        : null
    const schedulerStartLag = captured
        ? backgroundDistribution(
              policies.flatMap((policy) =>
                  policy.admissionToAdapterStartMs === null
                      ? []
                      : [policy.admissionToAdapterStartMs],
              ),
          )
        : null
    const schedulerComplete =
        captured &&
        policies.every(
            (policy) =>
                policy.schedulerAdmitted === 1 &&
                policy.schedulerStarted === 1 &&
                policy.dueToAdmissionMs !== null &&
                policy.admissionToAdapterStartMs !== null,
        )
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
        schedulerAdmitted: admissions,
        schedulerAdmissionEvidence: captured
            ? 'actual web owner: committed claim returned to scheduler adapter; not the SQL commit instant'
            : 'unavailable or invalid capture; upstream receipts establish network acceptance only',
        schedulerCapture: {
            status: schedulerComplete ? 'complete' : 'insufficient',
            expectedAttempts: options.policies,
            capturedAdmissions: admissions,
            matchedStartedAttempts: schedulerStartLag?.count ?? null,
            identity: captured
                ? {
                      runId: capture.runId,
                      sourceSha: capture.sourceSha,
                      pid: capture.pid,
                      processStartTicks: capture.processStartTicks,
                      epoch: capture.epoch,
                      publication: capture.publication,
                  }
                : null,
            rejected: !!capture && !captured,
            flags: capture
                ? {
                      overflowed: capture.overflowed,
                      expired: capture.expired,
                      clockRegressed: capture.clockRegressed,
                  }
                : null,
        },
        networkAccepted: receipts.length,
        committed,
        checksPerSecond: throughput,
        queueLag,
        schedulerStartLag,
        timingSemantics: {
            queueLag:
                'policy due wall time to actual committed-claim-return observation in web; includes SQL/admission wait, not exact database commit',
            schedulerStartLag:
                'same-attempt monotonic committed-claim-return to probe adapter entry; excludes context lookup/controller/native network start',
            startLag:
                'policy due wall time to independent controlled upstream HEAD receipt; never scheduler admission',
            completionLag:
                'policy due wall time to persisted observation checkedAt; not exact SQL commit visibility',
        },
        startLag: starts,
        completionLag: completions,
        status:
            !complete ||
            starts.p99! > backgroundSlo.dueToNetworkStartP99Ms ||
            completions.max! > backgroundSlo.dueToCommitMaxMs ||
            throughput === null ||
            throughput < backgroundSlo.minimumChecksPerSecond[options.policies]
                ? 'unmet'
                : starts.count < options.policies || !schedulerComplete
                  ? 'insufficient'
                  : 'met',
    }
}
