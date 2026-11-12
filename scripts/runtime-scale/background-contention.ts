import { z } from 'zod'
import {
    readMemoryObservation,
    memoryMeasurementSemantics,
} from '../runtime-reliability/resource-probe.ts'
import {
    BACKGROUND_DATABASE_POLICY,
    BACKGROUND_RESOURCE_ENVELOPE,
} from '../../web/src/config/background-resources.config.ts'
import { DATABASE_CONNECTION_BUDGET } from '../../web/src/config/database.config.ts'
import { backgroundDistribution } from './background.ts'
import { readBackgroundResourceReport } from '../../web/src/lib/RuntimeDiagnostics/backgroundResourceReport.ts'
import { readBunWorkerReport } from '../../web/src/lib/RuntimeDiagnostics/backgroundWorkerReport.ts'
import { supportProbeDiagnosticSchema } from './background-contention-web.ts'

const count = z.number().int().nonnegative()
export const contentionStateSchema = z.strictObject({
    notification: z.strictObject({
        pending: count,
        inflight: count,
        succeeded: count,
        failed: count,
        cancelled: count,
        deferred: count,
        deferredIdentities: z
            .array(
                z.strictObject({
                    incidentId: z.uuid(),
                    channelId: z.uuid(),
                    channelRevision: count,
                    type: z.enum(['incident', 'recovery']),
                }),
            )
            .max(1024)
            .optional(),
        deferredObservedAt: z.number().finite().nonnegative().optional(),
    }),
    discovery: z.strictObject({
        revision: count,
        error: z.enum(['authorization_required', 'reconcile_failed']).nullable(),
        detached: count,
    }),
    postgres: z.strictObject({
        connections: count,
        active: count,
        lockWaiters: count,
        idleTransactions: count,
        monitoringLeases: count,
    }),
})
export const contentionReceiverSchema = z.strictObject({
    notifications: z.strictObject({
        accepted: count,
        emailAccepted: count,
        webhookAccepted: count,
        attempts: count,
        activeWebhooks: count,
        smtpConnections: count,
    }),
    discovery: z.strictObject({
        streams: count,
        emitted: count,
        lists: count,
        inspections: count,
        inventoryCount: count,
    }),
})
export const contentionNativeSchema = z.object({
    workers: z.array(z.object({ id: z.string(), attempts: count, successes: count })),
})
export const contentionWebSchema = z.strictObject({
    source: z.enum(['authenticated actual web support RPC', 'unavailable']),
    rpcMs: z.number().nonnegative().optional(),
    diagnostic: supportProbeDiagnosticSchema.optional(),
    resources: z.unknown().transform(readBackgroundResourceReport),
    bun: z.unknown().transform(readBunWorkerReport),
})
export type ContentionSample = {
    processMemory?: ReturnType<typeof readMemoryObservation>
    elapsedMs: number
    criticalObservedMs: number
    captureSpanMs: number
    leasedPolicies: number
    managementMs: number
    proxyMs: number
    state: z.output<typeof contentionStateSchema>
    receiver: z.output<typeof contentionReceiverSchema>
    critical: { attempts: number; successes: number }
    revisionUnchanged: boolean
    web?: z.output<typeof contentionWebSchema>
}

export const contentionContract = {
    targets: BACKGROUND_RESOURCE_ENVELOPE.targets,
    sql: BACKGROUND_DATABASE_POLICY,
    connectionPool: DATABASE_CONNECTION_BUDGET,
    notifications: {
        incidents: 80,
        potentialDeliveries: 320,
        durableAdmission: 256,
        receiverDelayMs: 1500,
    },
    discovery: { events: 256, detachedIdentities: 8, proxyAdoption: false },
    receiverOverhead:
        'One additional root-owned Bun receiver process inside the unchanged appliance cgroup; its CPU/cgroup memory/PIDs/connections count against the baseline',
} as const

/** Require observed overlapping work, actual web-process metrics, bounded SQL pressure, and native progress; missing evidence never supports a capacity claim. */
export function buildContentionReport(
    samples: ContentionSample[],
    baseline: { attempts: number; successes: number; discoveryRevision: number },
    proxyErrors: number,
) {
    samples = samples.map(({ processMemory, ...sample }) => ({
        ...sample,
        processMemory: readMemoryObservation(processMemory),
    }))
    const processMemoryComplete =
        samples.length > 0 &&
        samples.every((item) => {
            const memory = item.processMemory
            return (
                memory !== null &&
                memory !== undefined &&
                memory.cgroupMemoryCurrentBytes !== null &&
                memory.webRssBytes !== null &&
                memory.controllerRssBytes !== null &&
                memory.caddyRssBytes !== null
            )
        })
    const management = backgroundDistribution(samples.map((item) => item.managementMs))
    const proxy = backgroundDistribution(samples.map((item) => item.proxyMs))
    const isMixedOverlap = (item: ContentionSample) =>
        item.leasedPolicies > 0 &&
        item.state.notification.inflight > 0 &&
        item.receiver.notifications.activeWebhooks + item.receiver.notifications.smtpConnections > 0
    const overlap = samples.filter(isMixedOverlap)
    const critical = samples.slice(1).find((item, index) => {
        const previous = samples[index]!
        return (
            isMixedOverlap(previous) &&
            isMixedOverlap(item) &&
            item.critical.attempts > previous.critical.attempts &&
            item.critical.successes > previous.critical.successes
        )
    })
    const discovery = samples.find(
        (item) =>
            item.state.discovery.revision > baseline.discoveryRevision &&
            item.state.discovery.error === null &&
            item.state.discovery.detached === 8 &&
            item.receiver.discovery.emitted > 0,
    )
    const maximum = (read: (sample: ContentionSample) => number) =>
        samples.length ? Math.max(...samples.map(read)) : null
    const email = maximum((item) => item.receiver.notifications.emailAccepted)
    const webhook = maximum((item) => item.receiver.notifications.webhookAccepted)
    const durable = maximum((item) => item.state.notification.succeeded)
    const webResources = samples.flatMap((item) =>
        item.web?.resources ? [item.web.resources] : [],
    )
    const eventLoops = samples.flatMap((item) =>
        item.web?.bun?.eventLoopLag ? [item.web.bun.eventLoopLag] : [],
    )
    const eventLoopMaxima = eventLoops.flatMap((item) =>
        item.maximumMs === null ? [] : [item.maximumMs],
    )
    const sqlMaximum = webResources.length
        ? Object.fromEntries(
              Object.keys(BACKGROUND_DATABASE_POLICY.classes).map((key) => {
                  const resource = key as keyof typeof BACKGROUND_DATABASE_POLICY.classes
                  return [
                      resource,
                      {
                          running: Math.max(
                              ...webResources.map((value) => value.database[resource].running),
                          ),
                          pending: Math.max(
                              ...webResources.map((value) => value.database[resource].pending),
                          ),
                          admitted: Math.max(
                              ...webResources.map((value) => value.database[resource].admitted),
                          ),
                      },
                  ]
              }),
          )
        : null
    const resourceBoundsMet = webResources.every((value) => {
        const entries = Object.entries(BACKGROUND_DATABASE_POLICY.classes)
        return (
            entries.every(([key, policy]) => {
                const usage = value.database[key as keyof typeof value.database]
                return usage.running <= policy.running && usage.pending <= policy.pending
            }) &&
            Object.values(value.database).reduce((sum, usage) => sum + usage.running, 0) <=
                BACKGROUND_DATABASE_POLICY.ceiling
        )
    })
    const webEvidenceComplete =
        processMemoryComplete &&
        samples.length > 0 &&
        samples.every((item) => item.web?.resources != null && item.web.bun != null) &&
        eventLoops.some(
            (value) => value.count > 0 && value.sampledAt !== null && value.maximumMs !== null,
        )
    const passed =
        resourceBoundsMet &&
        webEvidenceComplete &&
        samples.length > 0 &&
        overlap.length > 0 &&
        email! > 0 &&
        webhook! > 0 &&
        durable! > 0 &&
        Boolean(discovery) &&
        Boolean(
            critical &&
            critical.criticalObservedMs <= contentionContract.targets.criticalProgressMs,
        ) &&
        proxyErrors === 0 &&
        samples.every(
            (item) =>
                item.revisionUnchanged &&
                item.state.notification.inflight <= 8 &&
                item.state.notification.pending + item.state.notification.inflight <= 256 &&
                item.leasedPolicies <= 64 &&
                item.state.postgres.connections <= 100,
        ) &&
        management.p95! <= contentionContract.targets.managementP95Ms &&
        management.p99! <= contentionContract.targets.managementP99Ms
    return {
        status: !resourceBoundsMet
            ? 'unmet'
            : !samples.length || !webEvidenceComplete
              ? 'insufficient'
              : passed
                ? 'met'
                : 'unmet',
        resourceEvidence: {
            complete: webEvidenceComplete,
            boundsMet: resourceBoundsMet,
            processMemoryComplete,
            memoryMeasurementSemantics,
        },
        model: 'Actual appliance notification, monitoring and discovery workers; controlled private slow receivers and Docker events; no manual delivery driver',
        contract: contentionContract,
        samples,
        simultaneousSamples: overlap.length,
        management: {
            ...management,
            kind: 'Authenticated management SSR/auth read in the actual web process',
        },
        proxy: { ...proxy, errors: proxyErrors },
        progress: {
            monitoringLeasesObserved: maximum((item) => item.leasedPolicies),
            emailAccepted: email,
            webhookAccepted: webhook,
            durableNotificationSuccesses: durable,
            bunDiscoveryDurableRevision: discovery?.state.discovery.revision ?? null,
            nativeCriticalProgressMs: critical?.criticalObservedMs ?? null,
            nativeCriticalKind:
                'Verified caddy_recovery probe progress during load; no crash injected',
        },
        postgres: {
            maximumConnections: maximum((item) => item.state.postgres.connections),
            maximumActiveExcludingObserverQuery: maximum((item) => item.state.postgres.active),
            maximumLockWaiters: maximum((item) => item.state.postgres.lockWaiters),
            backgroundRunningByClass: sqlMaximum,
            classLimitEvidence: webResources.length
                ? 'Authenticated support RPC samples the actual web-process admission singletons; separate unit/PostgreSQL tests prove saturation limits beyond point samples'
                : 'Configured fixed partitions plus separate background database/admission unit and PostgreSQL tests; pg_stat_activity cannot attribute the shared web pool to admission classes',
            observerOverhead:
                'Fixture child performs private snapshot/auth reads and one pg_stat_activity observer query; total connection counts include it',
        },
        webProcessMetrics: {
            probeFailures: samples.flatMap((item) =>
                item.web?.diagnostic ? [item.web.diagnostic] : [],
            ),
            singletonAdmissionCounters: sqlMaximum,
            eventLoopLag: eventLoops.length
                ? {
                      snapshots: eventLoops.length,
                      maximumMs: eventLoopMaxima.length ? Math.max(...eventLoopMaxima) : null,
                      latest: eventLoops.at(-1),
                  }
                : null,
            evidence: webResources.length
                ? 'Actual web support RPC via SDK and dynamically discovered built function metadata; one-shot fixture singletons are never sampled'
                : 'Actual web support RPC unavailable; one-shot fixture singletons are deliberately not sampled',
        },
        capacity: 'insufficient',
    }
}
