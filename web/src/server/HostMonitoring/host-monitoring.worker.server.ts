import '@tanstack/react-start/server-only'
import { HOST_MONITORING_LIMITS } from '@/config/host-monitoring.config.ts'
import { withBackgroundDatabaseContext } from '@/db/background-admission.ts'
import { observeWorkerOperation } from '@/server/Foundation/worker-operation.ts'
import { workerObservability } from '@/server/Foundation/worker-observability.ts'
import { inspectControllerDomain } from '@/server/Controller/domain-diagnostics.server.ts'
import {
    publishApplicationChange,
    subscribeToApplicationChanges,
} from '@/server/WebSockets/applicationChanges.ts'
import { createHostMonitoringBatcher } from './host-monitoring.batch.ts'
import { createHostMonitoringWorker } from './host-monitoring.worker.ts'
import { createHostMonitoringProbe } from './host-monitoring.probe.ts'
import { sampleHostMonitoringBacklog } from './host-monitoring.observability.ts'
import {
    claimDueHostMonitoringPolicies,
    completeHostMonitoringClaims,
    readHostMonitoringClaimContexts,
    releaseHostMonitoringClaims,
    pruneHostMonitoringHistory,
    readNextHostMonitoringAdmissionAt,
} from './host-monitoring.repository.ts'
import type {
    HostMonitoringClaim,
    HostMonitoringCompletion,
} from './Types/host-monitoring.repository.types.ts'
import type { HostMonitoringProbeContext } from './Types/host-monitoring-probe.types.ts'
import { HostMonitoringCleanupError } from './Types/host-monitoring-worker.types.ts'
import { hostMonitoringCapture } from './host-monitoring.capture.server.ts'

const readContext = createHostMonitoringBatcher<
    HostMonitoringClaim,
    HostMonitoringProbeContext | null
>({
    size: HOST_MONITORING_LIMITS.claimBatchSize,
    delayMs: 0,
    run: async (claims) =>
        (await readHostMonitoringClaimContexts(claims)).map((value) => ({
            status: 'fulfilled',
            value,
        })),
})

const complete = createHostMonitoringBatcher<HostMonitoringCompletion, boolean>({
    size: HOST_MONITORING_LIMITS.persistenceBatchSize,
    delayMs: HOST_MONITORING_LIMITS.persistenceDelayMs,
    /** Publish a change only when a durable batch completion was accepted; rejected or failed writes cannot announce progress. */
    run: async (items) => {
        const results = await completeHostMonitoringClaims(items)
        if (results.some((result) => result.status === 'fulfilled' && result.value))
            publishApplicationChange()
        return results
    },
})

// Coalesce a cancelled 64-probe flight into one fenced transaction within the drain budget.
const releaseBatch = createHostMonitoringBatcher<HostMonitoringClaim, boolean>({
    size: HOST_MONITORING_LIMITS.workerConcurrency,
    delayMs: 0,
    /** Keep repeated policies in separate ordered transactions so every old/new token retains its own fence and result. */
    run: async (claims) => {
        const batches: HostMonitoringClaim[][] = []
        let batch: HostMonitoringClaim[] = []
        let seen = new Set<string>()
        for (const claim of claims) {
            if (seen.has(claim.policy.id)) {
                batches.push(batch)
                batch = []
                seen = new Set()
            }
            batch.push(claim)
            seen.add(claim.policy.id)
        }
        if (batch.length) batches.push(batch)
        const results: PromiseSettledResult<boolean>[] = []
        let released = false
        for (const current of batches) {
            try {
                // oxlint-disable-next-line no-await-in-loop -- A later token for the same policy must follow the earlier fenced release.
                const values = await withBackgroundDatabaseContext(
                    () => releaseHostMonitoringClaims(current),
                    null,
                )
                if (values.length !== current.length)
                    throw new Error('Invalid monitoring release result.')
                released ||= values.some(Boolean)
                results.push(
                    ...values.map((value): PromiseFulfilledResult<boolean> => ({
                        status: 'fulfilled',
                        value,
                    })),
                )
            } catch (reason) {
                results.push(
                    ...current.map((): PromiseRejectedResult => ({ status: 'rejected', reason })),
                )
            }
        }
        if (released) publishApplicationChange()
        return results
    },
})

/** Retain failed cleanup ownership separately from ordinary probe errors, without bypassing SQL admission or changing claim fences. */
const release = (claim: HostMonitoringClaim) =>
    releaseBatch(claim).catch((cause: unknown) => {
        throw new HostMonitoringCleanupError(cause)
    })

const worker = createHostMonitoringWorker({
    claim: (limit) =>
        observeWorkerOperation(
            'host_monitoring',
            async () => {
                const claims = await claimDueHostMonitoringPolicies(limit)
                if (hostMonitoringCapture) for (const claim of claims) hostMonitoringCapture.record('admitted', claim)
                return claims
            },
            undefined,
            () => 'deferred',
            'admission',
        ),
    /** Count progress only after accepted persistence, treating busy outcomes as deferred and unknown dependency results as failures. */
    runClaim: (claim, signal) => {
        hostMonitoringCapture?.record(signal.aborted ? 'cancelled' : 'started', claim)
        let progress: 'success' | 'failure' | 'deferred' = 'deferred'
        const execute = createHostMonitoringProbe({
            readContext,
            inspect: inspectControllerDomain,
            /** Persist under the current claim fence before updating progress; admission rejection remains distinct from successful probes. */
            complete: async (current, outcome, before, currentSignal) => {
                const stored = await complete({
                    claim: current,
                    outcome,
                    before,
                    signal: currentSignal,
                })
                if (stored)
                    progress =
                        outcome.code === 'busy'
                            ? 'deferred'
                            : outcome.status === 'unknown'
                              ? 'failure'
                              : 'success'
                if (outcome.code === 'busy')
                    workerObservability.count('host_monitoring', 'admissionRejected')
                return stored
            },
            release,
        })
        return observeWorkerOperation(
            'host_monitoring',
            () => execute(claim, signal),
            signal,
            () => ({ outcome: progress, recoverLane: progress !== 'deferred' }),
        )
    },
    releaseCancelled: async (claim) => {
        hostMonitoringCapture?.record('cancelled', claim)
        await release(claim)
    },
    /** Observe retention independently from probe execution, then refresh queue evidence after successful pruning. */
    prune: async () => {
        await observeWorkerOperation(
            'host_monitoring',
            pruneHostMonitoringHistory,
            undefined,
            undefined,
            'retention',
        )
        await sampleHostMonitoringBacklog()
    },
    nextAdmissionAt: readNextHostMonitoringAdmissionAt,
    subscribe: subscribeToApplicationChanges,
    unavailable: () =>
        console.warn('[host-monitoring] Worker temporarily unavailable.', {
            retryMs: HOST_MONITORING_LIMITS.pollIntervalMs,
        }),
})

export const hostMonitoringWorker = {
    ...worker,
    async stop() {
        try { await worker.stop() } finally { await hostMonitoringCapture?.flush() }
    },
}

workerObservability.sample('host_monitoring', {
    concurrency: HOST_MONITORING_LIMITS.workerConcurrency,
})
