// oxlint-disable-next-line import/no-unassigned-import -- Keeps durable monitoring operations on the server.
import '@tanstack/react-start/server-only'

import { and, asc, eq, gt, inArray, isNull, lte, or, sql } from 'drizzle-orm'
import { alias } from 'drizzle-orm/pg-core'
import { HOST_MONITORING_LIMITS } from '@/config/host-monitoring.config.ts'
import {
    hostCheckIncidents,
    hostCheckObservations,
    hostCheckPolicies,
    hostDomains,
    proxyHosts,
} from '@/db/schema.ts'
import { advanceHostMonitoringState } from '@/lib/HostMonitoring/state.ts'
import type {
    HostMonitoringContext,
    HostMonitoringOutcome,
} from '@/lib/HostMonitoring/Types/host-monitoring.types.ts'
import { getAuthDatabase } from '@/server/Auth/Core/database.server.ts'
import type { AuthTransaction } from '@/server/Auth/Core/Types/database.types.ts'
import { readHostMonitoringContextsInTransaction } from '@/server/RuntimeDiagnostics/domain-diagnostics-data.ts'
import { recordHostAvailabilityTransitionInTransaction } from '@/server/Notifications/host-availability.repository.ts'
import type { HostAvailabilityIncidentSource } from '@/server/Notifications/Types/notification-internal.types.ts'
import type {
    HostMonitoringClaim,
    HostMonitoringCompletion,
    HostMonitoringContextMatches,
    HostMonitoringPolicy,
} from './Types/host-monitoring.repository.types.ts'
import type { HostMonitoringProbeContext } from './Types/host-monitoring-probe.types.ts'
import {
    databaseNow,
    prepareWorkerTransaction,
    prunePolicyHistoryInTransaction,
} from './host-monitoring.storage.ts'
class ExpiredHostMonitoringClaim extends Error {}

export async function claimDueHostMonitoringPolicy(): Promise<HostMonitoringClaim | null> {
    return (await claimDueHostMonitoringPolicies(1))[0] ?? null
}

export async function claimDueHostMonitoringPolicies(
    limit: number,
): Promise<HostMonitoringClaim[]> {
    if (!Number.isInteger(limit) || limit < 1 || limit > HOST_MONITORING_LIMITS.claimBatchSize)
        throw new Error('Invalid monitoring claim batch size.')
    return getAuthDatabase().transaction(async (transaction) => {
        await prepareWorkerTransaction(transaction)
        const [active] = await transaction
            .select({ count: sql<number>`count(*)::integer` })
            .from(hostCheckPolicies)
            .where(gt(hostCheckPolicies.leaseExpiresAt, sql`clock_timestamp()`))
        const available = HOST_MONITORING_LIMITS.maxActiveClaims - (active?.count ?? 0)
        if (available <= 0) return []
        // PostgreSQL's locking OF clause requires an unqualified relation name.
        const candidatePolicy = alias(hostCheckPolicies, 'monitoring_policy')
        const candidates = await transaction
            .select({ policy: candidatePolicy })
            .from(candidatePolicy)
            .innerJoin(proxyHosts, eq(proxyHosts.id, candidatePolicy.proxyHostId))
            .innerJoin(
                hostDomains,
                and(
                    eq(hostDomains.proxyHostId, candidatePolicy.proxyHostId),
                    eq(hostDomains.domain, candidatePolicy.domain),
                ),
            )
            .where(
                and(
                    eq(candidatePolicy.enabled, true),
                    eq(proxyHosts.enabled, true),
                    lte(candidatePolicy.nextCheckAt, sql`clock_timestamp()`),
                    or(
                        isNull(candidatePolicy.leaseExpiresAt),
                        lte(candidatePolicy.leaseExpiresAt, sql`clock_timestamp()`),
                    ),
                ),
            )
            .orderBy(asc(candidatePolicy.nextCheckAt), asc(candidatePolicy.id))
            .limit(Math.min(limit, available))
            .for('update', { of: candidatePolicy, skipLocked: true })
        if (candidates.length === 0) return []
        const policies = await transaction
            .update(hostCheckPolicies)
            .set({
                leaseToken: sql`gen_random_uuid()`,
                leaseExpiresAt: sql`clock_timestamp() + ${HOST_MONITORING_LIMITS.leaseMs} * interval '1 millisecond'`,
            })
            .where(
                inArray(
                    hostCheckPolicies.id,
                    candidates.map((candidate) => candidate.policy.id),
                ),
            )
            .returning()
        const admitted = new Map(policies.map((policy) => [policy.id, policy]))
        return candidates.flatMap((candidate) => {
            const policy = admitted.get(candidate.policy.id)
            return policy?.leaseToken ? [{ policy, leaseToken: policy.leaseToken }] : []
        })
    })
}

export async function readHostMonitoringClaimContexts(
    claims: readonly HostMonitoringClaim[],
): Promise<readonly (HostMonitoringProbeContext | null)[]> {
    if (claims.length === 0) return []
    if (claims.length > HOST_MONITORING_LIMITS.claimBatchSize)
        throw new Error('Invalid monitoring context batch size.')
    return getAuthDatabase().transaction(
        async (transaction) => {
            const timeout = `${HOST_MONITORING_LIMITS.transactionTimeoutMs}ms`
            await transaction.execute(
                sql`select set_config('statement_timeout', ${timeout}, true), set_config('lock_timeout', ${timeout}, true), set_config('transaction_timeout', ${timeout}, true)`,
            )
            const rows = await transaction
                .select({
                    id: hostCheckPolicies.id,
                    version: hostCheckPolicies.version,
                    leaseToken: hostCheckPolicies.leaseToken,
                })
                .from(hostCheckPolicies)
                .where(
                    and(
                        eq(hostCheckPolicies.enabled, true),
                        gt(hostCheckPolicies.leaseExpiresAt, sql`clock_timestamp()`),
                        or(
                            ...claims.map(({ policy, leaseToken }) =>
                                and(
                                    eq(hostCheckPolicies.id, policy.id),
                                    eq(hostCheckPolicies.version, policy.version),
                                    eq(hostCheckPolicies.leaseToken, leaseToken),
                                ),
                            ),
                        ),
                    ),
                )
            const valid = new Map(rows.map((row) => [row.id, row]))
            const admitted = (claim: HostMonitoringClaim) => {
                const current = valid.get(claim.policy.id)
                return (
                    current?.version === claim.policy.version &&
                    current.leaseToken === claim.leaseToken
                )
            }
            const contexts = await readHostMonitoringContextsInTransaction(
                transaction,
                claims.filter(admitted).map(({ policy }) => ({
                    hostType: 'proxy',
                    hostId: policy.proxyHostId,
                    domain: policy.domain,
                })),
            )
            return claims.map((claim) =>
                admitted(claim) ? (contexts.get(claim.policy.proxyHostId) ?? null) : null,
            )
        },
        { isolationLevel: 'repeatable read', accessMode: 'read only' },
    )
}

async function currentClaim(
    transaction: AuthTransaction,
    claim: HostMonitoringClaim,
): Promise<HostMonitoringPolicy | null> {
    const [policy] = await transaction
        .select()
        .from(hostCheckPolicies)
        .where(
            and(
                eq(hostCheckPolicies.id, claim.policy.id),
                eq(hostCheckPolicies.version, claim.policy.version),
                eq(hostCheckPolicies.leaseToken, claim.leaseToken),
                gt(hostCheckPolicies.leaseExpiresAt, sql`clock_timestamp()`),
            ),
        )
        .for('update')
    return policy ?? null
}

async function clearLease(
    transaction: AuthTransaction,
    policy: HostMonitoringPolicy,
): Promise<void> {
    await transaction
        .update(hostCheckPolicies)
        .set({
            leaseToken: null,
            leaseExpiresAt: null,
            nextCheckAt: nextScheduledCheck(policy),
            updatedAt: sql`clock_timestamp()`,
        })
        .where(eq(hostCheckPolicies.id, policy.id))
}

function nextScheduledCheck(policy: HostMonitoringPolicy) {
    return sql`date_bin(${policy.intervalSeconds} * interval '1 second', clock_timestamp(), ${policy.nextCheckAt}::timestamptz) + ${policy.intervalSeconds} * interval '1 second'`
}

export async function releaseHostMonitoringClaim(claim: HostMonitoringClaim): Promise<boolean> {
    return getAuthDatabase().transaction(async (transaction) => {
        await prepareWorkerTransaction(transaction)
        const policy = await currentClaim(transaction, claim)
        if (!policy) return false
        await clearLease(transaction, policy)
        return true
    })
}

export async function completeHostMonitoringClaim(
    claim: HostMonitoringClaim,
    outcome: HostMonitoringOutcome,
    contextMatches: HostMonitoringContextMatches,
    context: HostMonitoringContext,
): Promise<boolean> {
    return getAuthDatabase()
        .transaction(async (transaction) => {
            await prepareWorkerTransaction(transaction)
            return completeHostMonitoringClaimInTransaction(
                transaction,
                claim,
                outcome,
                contextMatches,
                context,
            )
        })
        .catch((error: unknown) => {
            if (error instanceof ExpiredHostMonitoringClaim) return false
            throw error
        })
}

async function completeHostMonitoringClaimInTransaction(
    transaction: AuthTransaction,
    claim: HostMonitoringClaim,
    outcome: HostMonitoringOutcome,
    contextMatches: HostMonitoringContextMatches,
    context: HostMonitoringContext,
    signal?: AbortSignal,
): Promise<boolean> {
    if (signal?.aborted) return false
    const policy = await currentClaim(transaction, claim)
    if (!policy) return false
    const [eligible] = await transaction
        .select({ id: proxyHosts.id })
        .from(proxyHosts)
        .innerJoin(
            hostDomains,
            and(eq(hostDomains.proxyHostId, proxyHosts.id), eq(hostDomains.domain, policy.domain)),
        )
        .where(and(eq(proxyHosts.id, policy.proxyHostId), eq(proxyHosts.enabled, true)))
        .limit(1)
    // Context comparison is local database work under the runtime configuration lock.
    if (!policy.enabled || !eligible || !(await contextMatches(transaction, policy))) {
        await clearLease(transaction, policy)
        return false
    }
    const now = await databaseNow(transaction)
    // Recheck expiry after the callback as well as before it.
    if (!policy.leaseExpiresAt || policy.leaseExpiresAt <= now || signal?.aborted) return false
    const contextChanged =
        policy.contextProof !== context.proof || policy.contextRevision !== context.revision
    if (contextChanged) {
        await transaction
            .update(hostCheckIncidents)
            .set({
                closedReason: 'policy_changed',
                closedAt: now,
                recoveryStartedAt: null,
            })
            .where(
                and(
                    eq(hostCheckIncidents.policyId, policy.id),
                    isNull(hostCheckIncidents.closedReason),
                ),
            )
    }
    const previous = contextChanged
        ? {
              state: 'unknown' as const,
              failureCount: 0,
              recoveryCount: 0,
              failureStartedAt: null,
              recoveryStartedAt: null,
          }
        : policy
    const change = advanceHostMonitoringState(previous, outcome, policy, now)
    await transaction.insert(hostCheckObservations).values({
        policyId: policy.id,
        policyVersion: policy.version,
        checkedAt: now,
        status: outcome.status,
        code: outcome.code,
        durationMs: outcome.durationMs,
        httpStatus: outcome.httpStatus,
    })
    let notificationIncident: HostAvailabilityIncidentSource | undefined
    if (change.transition?.kind === 'outage') {
        const [incident] = await transaction
            .insert(hostCheckIncidents)
            .values({
                policyId: policy.id,
                startedAt: change.transition.firstObservedAt,
                detectedAt: change.transition.confirmedAt,
                failureCode: change.transition.code,
            })
            .returning()
        notificationIncident = incident
    } else if (change.transition?.kind === 'recovery') {
        const [incident] = await transaction
            .update(hostCheckIncidents)
            .set({
                recoveryStartedAt: change.transition.firstObservedAt,
                recoveredAt: change.transition.confirmedAt,
                recoveryCode: change.transition.code,
                closedReason: 'recovered',
                closedAt: change.transition.confirmedAt,
            })
            .where(
                and(
                    eq(hostCheckIncidents.policyId, policy.id),
                    isNull(hostCheckIncidents.closedReason),
                ),
            )
            .returning()
        notificationIncident = incident
    } else if (previous.state === 'unhealthy') {
        await transaction
            .update(hostCheckIncidents)
            .set({
                recoveryStartedAt: change.snapshot.recoveryStartedAt,
            })
            .where(
                and(
                    eq(hostCheckIncidents.policyId, policy.id),
                    isNull(hostCheckIncidents.closedReason),
                ),
            )
    }
    await prunePolicyHistoryInTransaction(transaction, policy)
    const updated = await transaction
        .update(hostCheckPolicies)
        .set({
            ...change.snapshot,
            contextProof: context.proof,
            contextRevision: context.revision,
            lastCheckedAt: now,
            lastOutcome: outcome.status,
            lastCode: outcome.code,
            lastDurationMs: outcome.durationMs,
            lastHttpStatus: outcome.httpStatus,
            leaseToken: null,
            leaseExpiresAt: null,
            nextCheckAt: nextScheduledCheck(policy),
            updatedAt: sql`clock_timestamp()`,
        })
        .where(
            and(
                eq(hostCheckPolicies.id, policy.id),
                eq(hostCheckPolicies.version, claim.policy.version),
                eq(hostCheckPolicies.leaseToken, claim.leaseToken),
                gt(hostCheckPolicies.leaseExpiresAt, sql`clock_timestamp()`),
            ),
        )
        .returning({ id: hostCheckPolicies.id })
    if (updated.length === 0) throw new ExpiredHostMonitoringClaim()
    if (signal?.aborted) throw new ExpiredHostMonitoringClaim()
    if (notificationIncident && change.transition)
        await recordHostAvailabilityTransitionInTransaction(
            transaction,
            policy,
            context,
            notificationIncident,
            change.transition.kind,
        )
    if (signal?.aborted) throw new ExpiredHostMonitoringClaim()
    return true
}

export async function completeHostMonitoringClaims(
    items: readonly HostMonitoringCompletion[],
): Promise<readonly PromiseSettledResult<boolean>[]> {
    if (items.length === 0) return []
    if (items.length > HOST_MONITORING_LIMITS.persistenceBatchSize)
        throw new Error('Invalid monitoring completion batch size.')
    return getAuthDatabase().transaction(async (transaction) => {
        await prepareWorkerTransaction(transaction)
        const contexts = await readHostMonitoringContextsInTransaction(
            transaction,
            items.map(({ claim }) => ({
                hostType: 'proxy',
                hostId: claim.policy.proxyHostId,
                domain: claim.policy.domain,
            })),
        )
        const results: PromiseSettledResult<boolean>[] = []
        for (const item of items) {
            try {
                const after = contexts.get(item.claim.policy.proxyHostId)
                // oxlint-disable-next-line no-await-in-loop -- Savepoints share one transaction connection and must run sequentially.
                const value = await transaction.transaction((nested) =>
                    completeHostMonitoringClaimInTransaction(
                        nested,
                        item.claim,
                        item.outcome,
                        async () =>
                            Boolean(
                                after?.enabled &&
                                after.proof === item.before.proof &&
                                after.revision === item.before.revision,
                            ),
                        item.before.availability,
                        item.signal,
                    ),
                )
                results.push({ status: 'fulfilled', value })
            } catch (reason) {
                results.push(
                    reason instanceof ExpiredHostMonitoringClaim
                        ? { status: 'fulfilled', value: false }
                        : { status: 'rejected', reason },
                )
            }
        }
        if (
            items.some(
                (item, index) =>
                    item.signal.aborted &&
                    results[index]?.status === 'fulfilled' &&
                    results[index].value,
            )
        )
            throw new ExpiredHostMonitoringClaim()
        return results
    })
}
