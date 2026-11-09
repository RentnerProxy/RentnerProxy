// oxlint-disable-next-line import/no-unassigned-import -- Keeps durable monitoring operations on the server.
import '@tanstack/react-start/server-only'

import { randomUUID } from 'node:crypto'
import { and, asc, desc, eq, gte, inArray, isNull, lt, or, sql } from 'drizzle-orm'
import { HOST_MONITORING_LIMITS } from '@/config/host-monitoring.config.ts'
import {
    hostCheckIncidents,
    hostCheckObservations,
    hostCheckPolicies,
    hostDomains,
    proxyHosts,
} from '@/db/schema.ts'
import { AuthDomainError } from '@/server/Auth/Core/errors.server.ts'
import type { AuthTransaction } from '@/server/Auth/Core/Types/database.types.ts'
import { cancelHostAvailabilityForHostsInTransaction } from '@/server/Notifications/host-availability.repository.ts'
import type {
    HostMonitoringHistory,
    HostMonitoringHistoryOptions,
    HostMonitoringPolicy,
    SaveHostMonitoringPolicyInput,
} from './Types/host-monitoring.repository.types.ts'
import { lockMonitoring, pruneHistoryInTransaction } from './host-monitoring.storage.ts'
export {
    claimDueHostMonitoringPolicy,
    claimDueHostMonitoringPolicies,
    completeHostMonitoringClaim,
    completeHostMonitoringClaims,
    readHostMonitoringClaimContexts,
    releaseHostMonitoringClaim,
} from './host-monitoring.execution.ts'
export { pruneHostMonitoringHistory } from './host-monitoring.storage.ts'

export async function readHostMonitoringPolicy(
    transaction: AuthTransaction,
    proxyHostId: string,
): Promise<HostMonitoringPolicy | null> {
    const [policy] = await transaction
        .select()
        .from(hostCheckPolicies)
        .where(eq(hostCheckPolicies.proxyHostId, proxyHostId))
        .limit(1)
    return policy ?? null
}

export async function readHostMonitoringSummaries(
    transaction: AuthTransaction,
    proxyHostIds: readonly string[],
) {
    if (!proxyHostIds.length) return []
    return transaction
        .select({
            proxyHostId: proxyHosts.id,
            enabled: proxyHosts.enabled,
            policy: hostCheckPolicies,
            domain: hostDomains.domain,
        })
        .from(proxyHosts)
        .leftJoin(hostCheckPolicies, eq(hostCheckPolicies.proxyHostId, proxyHosts.id))
        .leftJoin(
            hostDomains,
            and(
                eq(hostDomains.proxyHostId, proxyHosts.id),
                eq(hostDomains.domain, hostCheckPolicies.domain),
            ),
        )
        .where(inArray(proxyHosts.id, [...proxyHostIds]))
        .orderBy(asc(proxyHosts.id))
}

// The caller holds the runtime-settings lock before taking this lock.
export async function lockHostMonitoringPolicy(
    transaction: AuthTransaction,
    proxyHostId: string,
): Promise<HostMonitoringPolicy | null> {
    await lockMonitoring(transaction)
    const [policy] = await transaction
        .select()
        .from(hostCheckPolicies)
        .where(eq(hostCheckPolicies.proxyHostId, proxyHostId))
        .for('update')
    return policy ?? null
}

// The caller holds the runtime-settings lock, before monitoring and notification locks.
export async function invalidateHostMonitoringForHostsInTransaction(
    transaction: AuthTransaction,
    proxyHostIds: readonly string[],
    reason: 'policy_changed' | 'monitoring_disabled' = 'policy_changed',
): Promise<void> {
    if (proxyHostIds.length === 0) return
    await lockMonitoring(transaction)
    const policies = await transaction
        .update(hostCheckPolicies)
        .set({
            version: sql`uuidv7()`,
            state: 'unknown',
            failureCount: 0,
            recoveryCount: 0,
            failureStartedAt: null,
            recoveryStartedAt: null,
            contextProof: null,
            contextRevision: null,
            leaseToken: null,
            leaseExpiresAt: null,
            lastCheckedAt: null,
            lastOutcome: null,
            lastCode: null,
            lastDurationMs: null,
            lastHttpStatus: null,
            nextCheckAt: sql`clock_timestamp()`,
            updatedAt: sql`clock_timestamp()`,
        })
        .where(inArray(hostCheckPolicies.proxyHostId, [...proxyHostIds]))
        .returning({ id: hostCheckPolicies.id })
    if (policies.length > 0)
        await transaction
            .update(hostCheckIncidents)
            .set({
                closedReason: reason,
                closedAt: sql`clock_timestamp()`,
                recoveryStartedAt: null,
            })
            .where(
                and(
                    inArray(
                        hostCheckIncidents.policyId,
                        policies.map((policy) => policy.id),
                    ),
                    isNull(hostCheckIncidents.closedReason),
                ),
            )
    await cancelHostAvailabilityForHostsInTransaction(transaction, proxyHostIds)
}

export async function saveHostMonitoringPolicy(
    transaction: AuthTransaction,
    input: SaveHostMonitoringPolicyInput,
): Promise<HostMonitoringPolicy> {
    await lockMonitoring(transaction)
    if (input.existing === null) {
        const [count] = await transaction
            .select({ count: sql<number>`count(*)::integer` })
            .from(hostCheckPolicies)
        if ((count?.count ?? 0) >= HOST_MONITORING_LIMITS.maxPolicies)
            throw new AuthDomainError(
                'invalid_input',
                'The monitoring policy limit has been reached.',
            )
    } else {
        await transaction
            .update(hostCheckIncidents)
            .set({
                closedReason: input.settings.enabled ? 'policy_changed' : 'monitoring_disabled',
                closedAt: sql`clock_timestamp()`,
                recoveryStartedAt: null,
            })
            .where(
                and(
                    eq(hostCheckIncidents.policyId, input.existing.id),
                    isNull(hostCheckIncidents.closedReason),
                ),
            )
    }
    const changes = {
        ...input.settings,
        version: randomUUID(),
        contextProof: null,
        contextRevision: null,
        configuredBy: input.configuredBy,
        state: 'unknown' as const,
        failureCount: 0,
        recoveryCount: 0,
        failureStartedAt: null,
        recoveryStartedAt: null,
        lastCheckedAt: null,
        lastOutcome: null,
        lastCode: null,
        lastDurationMs: null,
        lastHttpStatus: null,
        leaseToken: null,
        leaseExpiresAt: null,
        nextCheckAt: sql`clock_timestamp()`,
        updatedAt: sql`clock_timestamp()`,
    }
    const rows = input.existing
        ? await transaction
              .update(hostCheckPolicies)
              .set(changes)
              .where(eq(hostCheckPolicies.id, input.existing.id))
              .returning()
        : await transaction
              .insert(hostCheckPolicies)
              .values({ proxyHostId: input.proxyHostId, ...changes })
              .returning()
    const policy = rows.at(0)
    if (!policy) throw new Error('Monitoring policy could not be saved.')
    await pruneHistoryInTransaction(transaction)
    return policy
}

export async function readHostMonitoringHistory(
    transaction: AuthTransaction,
    policyId: string,
    options: HostMonitoringHistoryOptions,
): Promise<HostMonitoringHistory> {
    const limit = Math.max(
        1,
        Math.min(HOST_MONITORING_LIMITS.historyLimit, Math.trunc(options.limit)),
    )
    const cursor = options.before
    const observations = await transaction
        .select()
        .from(hostCheckObservations)
        .where(
            and(
                eq(hostCheckObservations.policyId, policyId),
                options.status && options.status !== 'all'
                    ? eq(hostCheckObservations.status, options.status)
                    : undefined,
                options.since ? gte(hostCheckObservations.checkedAt, options.since) : undefined,
                cursor
                    ? or(
                          lt(hostCheckObservations.checkedAt, cursor.checkedAt),
                          and(
                              eq(hostCheckObservations.checkedAt, cursor.checkedAt),
                              lt(hostCheckObservations.id, cursor.id),
                          ),
                      )
                    : undefined,
            ),
        )
        .orderBy(desc(hostCheckObservations.checkedAt), desc(hostCheckObservations.id))
        .limit(limit + 1)
    const incidents = await transaction
        .select()
        .from(hostCheckIncidents)
        .where(
            and(
                eq(hostCheckIncidents.policyId, policyId),
                options.since
                    ? or(
                          isNull(hostCheckIncidents.closedAt),
                          gte(hostCheckIncidents.closedAt, options.since),
                      )
                    : undefined,
            ),
        )
        .orderBy(
            desc(isNull(hostCheckIncidents.closedAt)),
            desc(hostCheckIncidents.startedAt),
            desc(hostCheckIncidents.id),
        )
        .limit(limit)
    return {
        observations: observations.slice(0, limit),
        incidents,
        hasMore: observations.length > limit,
    }
}
