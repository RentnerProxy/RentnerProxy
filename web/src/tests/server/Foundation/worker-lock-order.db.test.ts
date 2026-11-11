// oxlint-disable no-await-in-loop -- Probe real PostgreSQL blockers under a bounded deadline before releasing the competing writer.
import { afterEach, beforeAll, expect } from 'bun:test'
import { and, eq, inArray, sql } from 'drizzle-orm'
import {
    HOST_MONITORING_DEFAULTS,
    HOST_MONITORING_LIMITS,
} from '@/config/host-monitoring.config.ts'
import { NOTIFICATION_SETTINGS_KEY, NOTIFICATION_STATE_KEY } from '@/config/notifications.config.ts'
import { PERMISSIONS } from '@/config/permissions.config.ts'
import {
    auditEvents,
    hostCheckIncidents,
    hostCheckPolicies,
    hostDomains,
    proxyHosts,
    systemSettings,
} from '@/db/schema.ts'
import type { AuthTransaction } from '@/server/Auth/Core/Types/database.types.ts'
import { requirePermissionInTransaction } from '@/server/Auth/Access/rbac.service.ts'
import {
    appendAuditEventInTransactionService,
    AUDIT_RETENTION_LOCK_ID,
} from '@/server/Audit/audit.service.ts'
import {
    discoveryEndpointHash,
    discoverySourceKey,
} from '@/server/Admin/DockerDiscovery/discovery-plan.ts'
import { reconcileDiscoveryInTransaction } from '@/server/Admin/DockerDiscovery/discovery-reconcile.server.ts'
import {
    readDiscoveryRegistry,
    writeDiscoveryRegistry,
} from '@/server/Admin/DockerDiscovery/discovery-storage.server.ts'
import {
    claimDueHostMonitoringPolicy,
    completeHostMonitoringClaim,
    saveHostMonitoringPolicy,
} from '@/server/HostMonitoring/host-monitoring.repository.ts'
import { persistNotificationConfigurationInTransaction } from '@/server/Notifications/notification-configuration-persistence.server.ts'
import {
    acknowledgeNotificationDelivery,
    claimNextNotificationDelivery,
} from '@/server/Notifications/notification-repository.ts'
import {
    lockNotificationConfiguration,
    lockNotificationRuntime,
    readNotificationConfiguration,
    readNotificationRuntime,
    writeNotificationConfiguration,
    writeNotificationRuntime,
} from '@/server/Notifications/notification-storage.ts'
import type { StoredNotificationConfiguration } from '@/server/Notifications/Types/notification-internal.types.ts'
import {
    lockProxyRuntimeSettings,
    PROXY_RUNTIME_SETTINGS_KEY,
} from '@/server/ProxyRuntime/proxy-runtime-settings.ts'
import { readHostMonitoringContextInTransaction } from '@/server/RuntimeDiagnostics/domain-diagnostics-data.ts'
import {
    discoveryCandidate,
    discoveryRegistry,
    discoverySnapshot,
} from '@/tests/Helpers/discoveryFixture.ts'
import {
    backendCoverageEnabled,
    cleanupBackendCoverage,
    createBackendActor,
    database,
    initializeBackendRegistry,
    integrationTest,
} from '@/tests/Helpers/backendCoverageFixture.ts'

const discoveryKey = 'docker_discovery_management_v1'
const settingsKeys = [
    PROXY_RUNTIME_SETTINGS_KEY,
    discoveryKey,
    NOTIFICATION_SETTINGS_KEY,
    NOTIFICATION_STATE_KEY,
]
const savedSettings: (typeof systemSettings.$inferSelect)[] = []
const hostIds: string[] = []
const domains: string[] = []
const endpoint = 'http://docker-proxy:2375'

beforeAll(async () => {
    if (backendCoverageEnabled) await initializeBackendRegistry()
})

afterEach(async () => {
    if (!backendCoverageEnabled) return
    const created = domains.length
        ? await database
              .select({ id: hostDomains.proxyHostId })
              .from(hostDomains)
              .where(inArray(hostDomains.domain, domains.splice(0)))
        : []
    const ids = [...hostIds.splice(0), ...created.flatMap(({ id }) => (id ? [id] : []))]
    if (ids.length) await database.delete(proxyHosts).where(inArray(proxyHosts.id, ids))
    await database.delete(systemSettings).where(inArray(systemSettings.key, settingsKeys))
    if (savedSettings.length) await database.insert(systemSettings).values(savedSettings.splice(0))
    await cleanupBackendCoverage()
})

async function prepare(transaction: AuthTransaction): Promise<number> {
    await transaction.execute(sql`set local lock_timeout = '2000ms'`)
    await transaction.execute(sql`set local statement_timeout = '4000ms'`)
    await transaction.execute(sql`set local transaction_timeout = '6000ms'`)
    await transaction.execute(sql`set local deadlock_timeout = '50ms'`)
    const [row] = await transaction.execute<{ pid: number }>(sql`select pg_backend_pid() pid`)
    if (!row) throw new Error('Owned transaction PID unavailable')
    return row.pid
}

async function waitForBlock(waiter: number, blocker: number): Promise<void> {
    const deadline = performance.now() + 1500
    while (performance.now() < deadline) {
        const [row] = await database.execute<{ blocked: boolean }>(
            sql`select ${blocker} = any(pg_blocking_pids(${waiter})) blocked`,
        )
        if (row?.blocked) return
        await Bun.sleep(5)
    }
    throw new Error('Expected competing PostgreSQL lock was not observed')
}

function notificationConfiguration(): StoredNotificationConfiguration {
    return {
        version: 1,
        revision: 1,
        enabled: true,
        pausedAfterRestore: false,
        channels: [
            {
                id: crypto.randomUUID(),
                name: 'Lock-order fixture',
                kind: 'webhook',
                revision: 1,
                enabled: true,
                events: ['host_availability'],
                minimumSeverity: 'error',
                thresholdSeconds: 0,
                recovery: true,
                destination: { ciphertext: 'a'.repeat(32), iv: 'b'.repeat(16) },
                hasAuthorization: false,
            },
        ],
    }
}

async function monitoringFixture() {
    savedSettings.push(
        ...(await database
            .select()
            .from(systemSettings)
            .where(inArray(systemSettings.key, settingsKeys))),
    )
    const actor = await createBackendActor()
    const hostId = Bun.randomUUIDv7()
    const domain = `lock-update-${hostId}.example.test`
    hostIds.push(hostId)
    await database.insert(proxyHosts).values({
        id: hostId,
        forwardScheme: 'http',
        forwardHost: '172.22.0.5',
        forwardPort: 8080,
    })
    await database.insert(hostDomains).values({ proxyHostId: hostId, domain })
    const configuration = notificationConfiguration()
    await database.transaction(async (transaction) => {
        await lockNotificationConfiguration(transaction)
        await writeNotificationConfiguration(transaction, configuration)
        await writeNotificationRuntime(transaction, await lockNotificationRuntime(transaction))
    })
    const policy = await database.transaction(async (transaction) => {
        await lockProxyRuntimeSettings(transaction)
        return saveHostMonitoringPolicy(transaction, {
            proxyHostId: hostId,
            configuredBy: actor,
            existing: null,
            settings: {
                ...HOST_MONITORING_DEFAULTS,
                domain,
                enabled: true,
                failureThreshold: 1,
                recoveryThreshold: 1,
            },
        })
    })
    const context = await database.transaction((transaction) =>
        readHostMonitoringContextInTransaction(transaction, {
            hostType: 'proxy',
            hostId,
            domain,
        }),
    )
    const claimed = await claimDueHostMonitoringPolicy()
    expect(claimed?.policy.id).toBe(policy.id)
    if (!claimed) throw new Error('Owned monitoring claim unavailable')
    expect(
        await completeHostMonitoringClaim(
            claimed,
            { status: 'unhealthy', code: 'upstream_timeout', durationMs: 1, httpStatus: null },
            async () => true,
            context.availability,
        ),
    ).toBe(true)
    expect(
        (await database.transaction(readNotificationRuntime)).engine.incidents.some(
            (incident) => incident.resourceId === hostId,
        ),
    ).toBe(true)
    return { actor, hostId, domain, configuration, policy, context }
}

integrationTest(
    'a mixed discovery batch and notification configuration commit without an audit/notification lock cycle',
    async () => {
        const { actor, hostId, domain, configuration, policy } = await monitoringFixture()
        const createdDomain = `lock-create-${hostId}.example.test`
        domains.push(createdDomain)
        const sources = ['compose:lock-order/a', 'compose:lock-order/b'].toSorted((left, right) =>
            discoverySourceKey(endpoint, left).localeCompare(discoverySourceKey(endpoint, right)),
        )
        const createSource = sources[0]!
        const updateSource = sources[1]!
        const snapshot = discoverySnapshot([
            discoveryCandidate({
                sourceId: createSource,
                containerId: 'a'.repeat(64),
                domains: [createdDomain],
            }),
            discoveryCandidate({
                sourceId: updateSource,
                containerId: 'b'.repeat(64),
                domains: [domain],
                target: { scheme: 'http', address: '172.22.0.6', port: 8080, network: 'proxy' },
            }),
        ])
        await database.transaction(async (transaction) => {
            await lockProxyRuntimeSettings(transaction)
            await writeDiscoveryRegistry(
                transaction,
                discoveryRegistry({
                    enabled: true,
                    actorUserId: actor,
                    connectorRevision: 1,
                    endpointHash: discoveryEndpointHash(endpoint),
                    owners: [
                        {
                            key: discoverySourceKey(endpoint, updateSource),
                            sourceId: updateSource,
                            hostId,
                            containerId: 'b'.repeat(64),
                            domains: [domain],
                            state: 'managed',
                            error: null,
                        },
                    ],
                }),
            )
        })

        const releaseHost = Promise.withResolvers<void>()
        const hostLocked = Promise.withResolvers<number>()
        const discoveryStarted = Promise.withResolvers<number>()
        const notificationStarted = Promise.withResolvers<number>()
        const blocker = database.transaction(async (transaction) => {
            const pid = await prepare(transaction)
            await transaction
                .select({ id: proxyHosts.id })
                .from(proxyHosts)
                .where(eq(proxyHosts.id, hostId))
                .for('update')
            hostLocked.resolve(pid)
            await releaseHost.promise
        })
        let discovery: Promise<boolean> | undefined
        let notifications: Promise<void> | undefined
        try {
            const blockerPid = await hostLocked.promise
            discovery = database.transaction(async (transaction) => {
                discoveryStarted.resolve(await prepare(transaction))
                await lockProxyRuntimeSettings(transaction)
                return reconcileDiscoveryInTransaction(
                    transaction,
                    await readDiscoveryRegistry(transaction),
                    snapshot,
                )
            })
            const discoveryPid = await discoveryStarted.promise
            await waitForBlock(discoveryPid, blockerPid)
            const [audit] = await database.execute<{ held: boolean }>(
                sql`select exists (select 1 from pg_locks where pid = ${discoveryPid} and locktype = 'advisory' and objid = ${AUDIT_RETENTION_LOCK_ID}::oid and granted) held`,
            )
            expect(audit?.held).toBe(true)

            notifications = database.transaction(async (transaction) => {
                notificationStarted.resolve(await prepare(transaction))
                await requirePermissionInTransaction(
                    transaction,
                    actor,
                    PERMISSIONS.NOTIFICATIONS_MANAGE,
                )
                const current = await lockNotificationConfiguration(transaction)
                expect(current.revision).toBe(configuration.revision)
                await persistNotificationConfigurationInTransaction(
                    {
                        lockRuntime: lockNotificationRuntime,
                        writeConfiguration: writeNotificationConfiguration,
                        writeRuntime: writeNotificationRuntime,
                        audit: appendAuditEventInTransactionService,
                        now: Date.now,
                        makeId: crypto.randomUUID,
                    },
                    transaction,
                    {
                        actorId: actor,
                        configuration: { ...current, revision: current.revision + 1 },
                        effective: current,
                        newIds: new Set(),
                        channelCount: current.channels.length,
                    },
                )
            })
            await waitForBlock(await notificationStarted.promise, discoveryPid)
            releaseHost.resolve()
            const results = await Promise.allSettled([discovery, notifications])
            const errors = results.flatMap((result) => {
                if (result.status !== 'rejected') return []
                const cause = result.reason instanceof Error ? result.reason.cause : null
                return [
                    {
                        message: String(result.reason),
                        code: cause instanceof Error && 'code' in cause ? cause.code : null,
                        sqlstate: cause instanceof Error && 'errno' in cause ? cause.errno : null,
                        detail: cause instanceof Error ? cause.message : null,
                    },
                ]
            })
            expect(
                results.map((result) => result.status),
                JSON.stringify(errors),
            ).toEqual(['fulfilled', 'fulfilled'])

            const registry = await database.transaction(readDiscoveryRegistry)
            expect(registry.owners).toHaveLength(2)
            expect(registry.results).toEqual([])
            const [host] = await database
                .select({ address: proxyHosts.forwardHost })
                .from(proxyHosts)
                .where(eq(proxyHosts.id, hostId))
            expect(host?.address).toBe('172.22.0.6')
            const [updatedPolicy] = await database
                .select()
                .from(hostCheckPolicies)
                .where(eq(hostCheckPolicies.id, policy.id))
            expect(updatedPolicy?.version).not.toBe(policy.version)
            expect(updatedPolicy?.state).toBe('unknown')
            expect(updatedPolicy?.leaseToken).toBeNull()
            const [sourceIncident] = await database
                .select()
                .from(hostCheckIncidents)
                .where(eq(hostCheckIncidents.policyId, policy.id))
            expect(sourceIncident?.closedReason).toBe('policy_changed')
            const runtime = await database.transaction(readNotificationRuntime)
            expect(
                runtime.engine.incidents.some((incident) => incident.resourceId === hostId),
            ).toBe(false)
            expect(
                runtime.engine.deliveries
                    .filter((delivery) => delivery.payload.resourceId === hostId)
                    .map((delivery) => delivery.status),
            ).toEqual(['cancelled'])
            expect((await database.transaction(readNotificationConfiguration)).revision).toBe(2)
            const audits = await database
                .select()
                .from(auditEvents)
                .where(and(eq(auditEvents.actorUserId, actor), eq(auditEvents.result, 'success')))
            expect(
                audits
                    .filter((event) => event.resource === 'proxy-host')
                    .map((event) => event.action)
                    .toSorted(),
            ).toEqual(['create', 'update'])
            expect(
                audits.filter((event) => event.metadata.setting === 'notifications'),
            ).toHaveLength(1)
        } finally {
            releaseHost.resolve()
            await Promise.allSettled([blocker, discovery, notifications])
        }
    },
)

async function waitForBlockedTransaction(blocker: number): Promise<number> {
    const deadline = performance.now() + 1500
    while (performance.now() < deadline) {
        const [row] = await database.execute<{ pid: number }>(sql`
            select pid from pg_stat_activity
            where datname = current_database() and pid <> pg_backend_pid()
                and ${blocker} = any(pg_blocking_pids(pid))
            limit 1
        `)
        if (row) return row.pid
        await Bun.sleep(5)
    }
    throw new Error('Expected production transaction did not enter the competing lock wait')
}

integrationTest.each(['claim', 'acknowledge'] as const)(
    'monitoring recovery and notification %s commit while holding their competing lock chains',
    async (operation) => {
        const { hostId, policy, context } = await monitoringFixture()
        const delivery = operation === 'acknowledge' ? await claimNextNotificationDelivery() : null
        if (operation === 'acknowledge' && !delivery)
            throw new Error('Owned notification claim unavailable')
        await database
            .update(hostCheckPolicies)
            .set({ nextCheckAt: new Date(0) })
            .where(eq(hostCheckPolicies.id, policy.id))
        const claim = await claimDueHostMonitoringPolicy()
        expect(claim?.policy.id).toBe(policy.id)
        if (!claim) throw new Error('Owned recovery claim unavailable')
        const releaseRuntime = Promise.withResolvers<void>()
        const runtimeLocked = Promise.withResolvers<number>()
        const blocker = database.transaction(async (transaction) => {
            const pid = await prepare(transaction)
            await lockNotificationRuntime(transaction)
            runtimeLocked.resolve(pid)
            await releaseRuntime.promise
        })
        let notification:
            | ReturnType<typeof claimNextNotificationDelivery>
            | ReturnType<typeof acknowledgeNotificationDelivery>
            | undefined
        let completion: Promise<boolean> | undefined
        try {
            const blockerPid = await runtimeLocked.promise
            notification =
                operation === 'claim'
                    ? claimNextNotificationDelivery()
                    : acknowledgeNotificationDelivery(delivery!.leaseToken, { success: true })
            const notificationPid = await waitForBlockedTransaction(blockerPid)
            completion = completeHostMonitoringClaim(
                claim,
                { status: 'healthy', code: 'expected_status', durationMs: 1, httpStatus: 200 },
                async (transaction) => {
                    const current = await readHostMonitoringContextInTransaction(transaction, {
                        hostType: 'proxy',
                        hostId,
                        domain: policy.domain,
                    })
                    return current.proof === context.proof && current.revision === context.revision
                },
                context.availability,
            )
            const completionPid = await waitForBlockedTransaction(notificationPid)
            const [monitoring] = await database.execute<{ held: boolean }>(sql`
                select exists (select 1 from pg_locks where pid = ${completionPid}
                    and locktype = 'advisory' and objid = ${HOST_MONITORING_LIMITS.advisoryLockId}::oid and granted) held
            `)
            expect(monitoring?.held).toBe(true)
            releaseRuntime.resolve()
            const [result, completed] = await Promise.all([notification, completion])
            expect(completed).toBe(true)
            if (operation === 'claim') expect(result).not.toBeNull()
            const runtime = await database.transaction(readNotificationRuntime)
            const outage = runtime.engine.deliveries.find(
                (item) => item.payload.resourceId === hostId && item.payload.type === 'incident',
            )
            expect(outage?.status).toBe(operation === 'claim' ? 'inflight' : 'succeeded')
            if (operation === 'acknowledge') expect(outage?.leaseToken).toBeNull()
            expect(
                runtime.engine.incidents.find((item) => item.resourceId === hostId)?.recoveredAt ??
                    0,
            ).toBeGreaterThan(0)
            const [source] = await database
                .select()
                .from(hostCheckIncidents)
                .where(eq(hostCheckIncidents.policyId, policy.id))
            expect(source?.closedReason).toBe('recovered')
            const [finished] = await database
                .select()
                .from(hostCheckPolicies)
                .where(eq(hostCheckPolicies.id, policy.id))
            expect(finished?.leaseToken).toBeNull()
            expect(finished?.state).toBe('healthy')
        } finally {
            releaseRuntime.resolve()
            await Promise.allSettled([blocker, notification, completion])
        }
    },
)
