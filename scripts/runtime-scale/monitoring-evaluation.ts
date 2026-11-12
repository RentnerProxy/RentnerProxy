import type {
    MonitoringEvaluationEvidence,
    MonitoringEvaluationFence,
    MonitoringEvaluationResult,
} from './Types/monitoring-evaluation.types.ts'

/** #401's predeclared investigation gates, not advertised product guarantees. */
export const monitoringEvaluationContract = {
    reference: { cpus: 2, memoryMiB: 1024, pids: 512, databaseConnections: 100 },
    samples: 3,
    sampleMs: 120000,
    policies: 1000,
    intervalSeconds: 60,
    scheduledSlots: 64,
    interactiveSlots: 2,
    dueToStartP99Ms: 55000,
    dueToCommitMaxMs: 60000,
    ssrP99Ms: 250,
    proxyP99Ms: 250,
    confirmedBunStopMs: 25000,
    distinctDurableStartsAfterStop: 128,
    replayRecords: 1000,
    replayMs: 30000,
    addedCpuFraction: 0.1,
    addedRssMiB: 32,
    extraDatabaseConnections: 2,
    minimumEfficiencyBenefit: 0.2,
    maxOutboxRecords: 4096,
    maxOutboxBytes: 16 * 1024 * 1024,
    maxRecordBytes: 4096,
    retentionMs: 120000,
    leaseMs: 45000,
} as const

/** A final application fence is deliberately stricter than cached execution authority. */
export function monitoringResultIsCurrent(
    result: MonitoringEvaluationResult,
    current: MonitoringEvaluationFence,
    nowMs: number,
): boolean {
    const identity = [
        'storeId',
        'controllerEpoch',
        'policyId',
        'policyVersion',
        'grantVersion',
        'revision',
        'hostProof',
        'leaseToken',
        'leaseExpiresAtMs',
    ] as const
    return (
        identity.every((field) => result[field] === current[field]) &&
        current.enabled &&
        !current.paused &&
        Number.isSafeInteger(nowMs) &&
        result.checkedAtMs <= nowMs &&
        result.checkedAtMs >= current.leaseExpiresAtMs - monitoringEvaluationContract.leaseMs &&
        nowMs < current.leaseExpiresAtMs
    )
}

/** Exhibits the stale authorization window without silently selecting new product semantics. */
export function monitoringRevocationCounterexample() {
    const issuedAtMs = 0
    const revokedAtMs = 1000
    const nextAdmissionMs = 2000
    const expiresAtMs = monitoringEvaluationContract.leaseMs
    return {
        issuedAtMs,
        revokedAtMs,
        nextAdmissionMs,
        expiresAtMs,
        cachedAuthorityAdmits: nextAdmissionMs < expiresAtMs,
        freshAuthorityAdmits: nextAdmissionMs < revokedAtMs,
        remainingStaleWindowMs: expiresAtMs - revokedAtMs,
        conclusion:
            'Cached publication alone cannot prove fresh admission after a committed revocation',
    }
}

/** Rejects incomplete/noisy evidence instead of mistaking a bounded protocol slice for migration proof. */
export function assessMonitoringMigration(evidence: readonly MonitoringEvaluationEvidence[]) {
    const failures = new Set<string>()
    const contract = monitoringEvaluationContract
    const profiles = ['mixed', 'all-3s-timeout'] as const
    if (
        evidence.length !== profiles.length ||
        profiles.some(
            (profile) => evidence.filter((entry) => entry.profile === profile).length !== 1,
        )
    )
        failures.add('Exactly one evidence entry per reference profile is required')
    for (const profile of profiles) {
        const item = evidence.find((entry) => entry.profile === profile)
        if (!item) {
            failures.add('Both reference latency profiles are required')
            continue
        }
        if (item.samples.length !== contract.samples)
            failures.add('Three paired whole-appliance samples per profile are required')
        if (
            [
                item.confirmedBunStopMs,
                item.distinctDurableStartsAfterStop,
                item.replayedResults,
                item.replayMs,
                item.measuredRelativeVariation,
            ].some((value) => !Number.isFinite(value) || value < 0)
        )
            failures.add('Invalid fault, replay or variation measurements')
        for (const sample of item.samples) {
            const numbers = [
                sample.durationMs,
                sample.dueToStartP99Ms,
                sample.dueToCommitMaxMs,
                sample.ssrP99Ms,
                sample.proxyP99Ms,
                sample.baselineCpu,
                sample.candidateCpu,
                sample.baselineRssMiB,
                sample.candidateRssMiB,
                sample.extraDatabaseConnections,
            ]
            if (numbers.some((value) => !Number.isFinite(value) || value < 0))
                failures.add('Invalid resource or latency measurements')
            if (
                !sample.sourceMatched ||
                !sample.wholeAppliance ||
                sample.durationMs < contract.sampleMs ||
                sample.policies !== contract.policies ||
                sample.intervalSeconds !== contract.intervalSeconds
            )
                failures.add('Source-matched 1000-policy/60-second reference envelope is unproven')
            if (
                !sample.exactPolicyCoverage ||
                sample.backlogGrowth !== 0 ||
                sample.proxyErrors !== 0 ||
                sample.dueToStartP99Ms > contract.dueToStartP99Ms ||
                sample.dueToCommitMaxMs > contract.dueToCommitMaxMs ||
                sample.ssrP99Ms > contract.ssrP99Ms ||
                sample.proxyP99Ms > contract.proxyP99Ms
            )
                failures.add('Reference freshness, fairness or traffic gate failed')
            if (
                sample.candidateCpu > sample.baselineCpu * (1 + contract.addedCpuFraction) ||
                sample.candidateRssMiB - sample.baselineRssMiB > contract.addedRssMiB ||
                sample.extraDatabaseConnections > contract.extraDatabaseConnections
            )
                failures.add('Added resource budget exceeded')
        }
        if (
            item.confirmedBunStopMs < contract.confirmedBunStopMs ||
            item.distinctDurableStartsAfterStop < contract.distinctDurableStartsAfterStop ||
            !item.retainedNativeIdentity
        )
            failures.add('Bun-stop durable execution continuity is unproven')
        if (item.replayedResults !== contract.replayRecords || item.replayMs > contract.replayMs)
            failures.add('Bounded replay recovery is unproven')
        if (
            !item.securityAndRestorePassed ||
            !item.freshAuthorityPassed ||
            item.staleCommits !== 0 ||
            item.duplicateObservations !== 0 ||
            item.duplicateIncidents !== 0 ||
            !item.outboxPressureVisible
        )
            failures.add('Authority, replay or restore safety gate failed')
    }
    const samples = evidence.flatMap((entry) => entry.samples)
    const variation = Math.max(...evidence.map((entry) => entry.measuredRelativeVariation))
    const cpuBenefit = Math.min(
        ...samples.map((sample) =>
            sample.baselineCpu > 0
                ? (sample.baselineCpu - sample.candidateCpu) / sample.baselineCpu
                : 0,
        ),
    )
    const rssBenefit = Math.min(
        ...samples.map((sample) =>
            sample.baselineRssMiB > 0
                ? (sample.baselineRssMiB - sample.candidateRssMiB) / sample.baselineRssMiB
                : 0,
        ),
    )
    const efficiencyJustified =
        failures.size === 0 &&
        [cpuBenefit, rssBenefit].some(
            (benefit) => benefit >= contract.minimumEfficiencyBenefit && benefit > 2 * variation,
        )
    return {
        recommendation: failures.size ? 'keep-hybrid' : 'eligible-for-separate-design-review',
        efficiencyJustified,
        failures: [...failures],
    }
}
