import '@tanstack/react-start/server-only'
import { HOST_MONITORING_LIMITS } from '@/config/host-monitoring.config.ts'
import { inspectControllerDomain } from '@/server/Controller/domain-diagnostics.server.ts'
import { publishApplicationChange } from '@/server/WebSockets/applicationChanges.ts'
import { createHostMonitoringBatcher } from './host-monitoring.batch.ts'
import { createHostMonitoringWorker } from './host-monitoring.worker.ts'
import { createHostMonitoringProbe } from './host-monitoring.probe.ts'
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
    run: async (items) => {
        const results = await completeHostMonitoringClaims(items)
        if (results.some((result) => result.status === 'fulfilled' && result.value))
            publishApplicationChange()
        return results
    },
})

const runClaim = createHostMonitoringProbe({
    readContext,
    inspect: inspectControllerDomain,
    complete: (claim, outcome, before, signal) => complete({ claim, outcome, before, signal }),
    release: releaseHostMonitoringClaim,
})

export const hostMonitoringWorker = createHostMonitoringWorker({
    claim: claimDueHostMonitoringPolicies,
    runClaim,
    releaseCancelled: async (claim) => {
        await releaseHostMonitoringClaim(claim)
    },
    prune: pruneHostMonitoringHistory,
    unavailable: () =>
        console.warn('[host-monitoring] Worker temporarily unavailable.', {
            retryMs: HOST_MONITORING_LIMITS.pollIntervalMs,
        }),
})
