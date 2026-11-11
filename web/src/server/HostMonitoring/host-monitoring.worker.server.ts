import '@tanstack/react-start/server-only'
import { HOST_MONITORING_LIMITS } from '@/config/host-monitoring.config.ts'
import { DATABASE_CONNECTION_BUDGET } from '@/config/database.config.ts'
import { withBackgroundDatabaseContext } from '@/db/background-admission.ts'
import { observeWorkerOperation } from '@/server/Foundation/worker-operation.ts'
import { workerObservability } from '@/server/Foundation/worker-observability.ts'
import { inspectControllerDomain } from '@/server/Controller/domain-diagnostics.server.ts'
import { publishApplicationChange } from '@/server/WebSockets/applicationChanges.ts'
import { createHostMonitoringBatcher } from './host-monitoring.batch.ts'
import { createHostMonitoringWorker } from './host-monitoring.worker.ts'
import { createHostMonitoringProbe } from './host-monitoring.probe.ts'
import { sampleHostMonitoringBacklog } from './host-monitoring.observability.ts'
import {
    claimDueHostMonitoringPolicies,
    completeHostMonitoringClaims,
    readHostMonitoringClaimContexts,
    releaseHostMonitoringClaim,
    pruneHostMonitoringHistory,
} from './host-monitoring.repository.ts'
import type {
    HostMonitoringClaim,
    HostMonitoringCompletion,
} from './Types/host-monitoring.repository.types.ts'
import type { HostMonitoringProbeContext } from './Types/host-monitoring-probe.types.ts'
import { HostMonitoringCleanupError } from './Types/host-monitoring-worker.types.ts'

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

// The existing batcher retains at most 64 owned releases and sends only eight at a time to the shared SQL gate.
const releaseBatch = createHostMonitoringBatcher<HostMonitoringClaim, boolean>({
    size: DATABASE_CONNECTION_BUDGET.backgroundTransactions,
    delayMs: 0,
    /** Settle every sibling independently; one rejection cannot discard the remaining fenced releases. */
    run: (claims) =>
        Promise.allSettled(
            claims.map(async (claim) =>
                withBackgroundDatabaseContext(() => releaseHostMonitoringClaim(claim), null),
            ),
        ),
})

/** Retain failed cleanup ownership separately from ordinary probe errors, without bypassing SQL admission or changing claim fences. */
const release = (claim: HostMonitoringClaim) =>
    releaseBatch(claim).catch((cause: unknown) => {
        throw new HostMonitoringCleanupError(cause)
    })

export const hostMonitoringWorker = createHostMonitoringWorker({
    claim: (limit) =>
        observeWorkerOperation(
            'host_monitoring',
            () => claimDueHostMonitoringPolicies(limit),
            undefined,
            () => 'deferred',
            'admission',
        ),
    /** Count progress only after accepted persistence, treating busy outcomes as deferred and unknown dependency results as failures. */
    runClaim: (claim, signal) => {
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
    unavailable: () =>
        console.warn('[host-monitoring] Worker temporarily unavailable.', {
            retryMs: HOST_MONITORING_LIMITS.pollIntervalMs,
        }),
})

workerObservability.sample('host_monitoring', {
    concurrency: HOST_MONITORING_LIMITS.workerConcurrency,
})
