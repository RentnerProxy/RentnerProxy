// oxlint-disable-next-line import/no-unassigned-import -- Keeps durable monitoring operations on the server.
import '@tanstack/react-start/server-only'

import { sql } from 'drizzle-orm'
import { HOST_MONITORING_LIMITS } from '@/config/host-monitoring.config.ts'
import { getBackgroundDatabase } from '@/server/Foundation/background-database.server.ts'
import type { AuthTransaction } from '@/server/Auth/Core/Types/database.types.ts'
import { lockProxyRuntimeSettings } from '@/server/ProxyRuntime/proxy-runtime-settings.ts'
import type { HostMonitoringPolicy } from './Types/host-monitoring.repository.types.ts'
export async function lockMonitoring(transaction: AuthTransaction): Promise<void> {
    await transaction.execute(
        sql`select pg_advisory_xact_lock(${HOST_MONITORING_LIMITS.advisoryLockId})`,
    )
}

/** Bounds SQL/lock time and preserves settings-before-monitoring lock order when settings are needed. */
export async function prepareWorkerTransaction(
    transaction: AuthTransaction,
    runtimeSettings = true,
): Promise<void> {
    const timeout = `${HOST_MONITORING_LIMITS.transactionTimeoutMs}ms`
    await transaction.execute(
        sql`select set_config('lock_timeout', ${timeout}, true), set_config('statement_timeout', ${timeout}, true), set_config('transaction_timeout', ${timeout}, true)`,
    )
    if (runtimeSettings) await lockProxyRuntimeSettings(transaction)
    await lockMonitoring(transaction)
}

/** Uses the live database clock for lease decisions, including time elapsed inside the transaction. */
export async function databaseNow(transaction: AuthTransaction): Promise<Date> {
    const [row] = await transaction.execute<{ now: string }>(
        sql`select clock_timestamp()::text as now`,
    )
    if (!row) throw new Error('Monitoring transaction clock is unavailable.')
    return new Date(row.now)
}

export async function prunePolicyHistoryInTransaction(
    transaction: AuthTransaction,
    policy: HostMonitoringPolicy,
): Promise<void> {
    // Keep separate capped pages and savepoint rollback in one command.
    // Incident eligibility reads no observations; the shared command snapshot is intentional.
    await transaction.execute(sql`
        with pruned_observations as (
            delete from "rentnerproxy"."host_check_observations" as observation
            using (
                select id from "rentnerproxy"."host_check_observations"
                where policy_id = ${policy.id}::uuid
                    and checked_at < clock_timestamp() - ${policy.retentionDays} * interval '1 day'
                union
                (select id from "rentnerproxy"."host_check_observations"
                 where policy_id = ${policy.id}::uuid
                 order by checked_at desc, id desc offset ${policy.sampleLimit})
                limit ${HOST_MONITORING_LIMITS.retentionDeleteLimit}
            ) expired where observation.id = expired.id
            returning observation.id
        )
        delete from "rentnerproxy"."host_check_incidents" as incident
        using (
            select id from "rentnerproxy"."host_check_incidents"
            where policy_id = ${policy.id}::uuid and closed_reason is not null
                and closed_at < clock_timestamp() - ${policy.retentionDays} * interval '1 day'
            union
            (select id from "rentnerproxy"."host_check_incidents"
             where policy_id = ${policy.id}::uuid and closed_reason is not null
             order by closed_at desc, id desc
             offset greatest(${policy.incidentLimit} - (
                 select count(*) from "rentnerproxy"."host_check_incidents"
                 where policy_id = ${policy.id}::uuid and closed_reason is null
             ), 0))
            limit ${HOST_MONITORING_LIMITS.retentionDeleteLimit}
        ) expired where incident.id = expired.id
            and (select count(*) from pruned_observations) >= 0
    `)
}

export async function pruneHistoryInTransaction(transaction: AuthTransaction): Promise<void> {
    // Fix each policy's cutoff and overflow boundary before scanning its oldest page.
    // The extra time bound keeps a settled policy from scanning its retained samples.
    // A bounded ID array lets deletion use the primary key instead of joining the whole history.
    await transaction.execute(sql`
        with policy as materialized (
            select budget.id, budget.policy_count,
                clock_timestamp() - budget.retention_days * interval '1 day' as cutoff,
                boundary.checked_at as boundary_at, boundary.id as boundary_id
            from (
                select id, retention_days, count(*) over () as policy_count,
                    least(sample_limit, greatest(${HOST_MONITORING_LIMITS.maxObservations} / count(*) over (), 1)) as sample_budget
                from "rentnerproxy"."host_check_policies"
            ) budget
            left join lateral (
                select checked_at, id
                from "rentnerproxy"."host_check_observations"
                where policy_id = budget.id
                order by checked_at desc, id desc
                offset budget.sample_budget limit 1
            ) boundary on true
        )
        delete from "rentnerproxy"."host_check_observations" as observation
        where observation.id = any(array(
            select expired.id
            from policy
            cross join lateral (
                select o.id, row_number() over (order by o.checked_at, o.id) as slot
                from "rentnerproxy"."host_check_observations" o
                where o.policy_id = policy.id
                    and o.checked_at <= greatest(policy.cutoff, policy.boundary_at)
                    and (
                        o.checked_at < policy.cutoff
                        or (o.checked_at, o.id) <= (policy.boundary_at, policy.boundary_id)
                    )
                order by o.checked_at, o.id
                limit greatest(${HOST_MONITORING_LIMITS.retentionPolicyPageSize}, ${HOST_MONITORING_LIMITS.retentionDeleteLimit} / policy.policy_count)
            ) expired
            order by expired.slot, policy.id
            limit ${HOST_MONITORING_LIMITS.retentionDeleteLimit}
        ))
    `)
    // Open incidents consume their policy's share but are never selected for deletion.
    await transaction.execute(sql`
        delete from "rentnerproxy"."host_check_incidents" as incident
        using (
            select id from (
                select i.id, i.policy_id, i.closed_at, p.retention_days,
                    row_number() over (partition by i.policy_id order by i.closed_at desc, i.id desc) as policy_rank,
                    row_number() over (partition by i.policy_id order by i.closed_at, i.id) as slot,
                    greatest(least(p.incident_limit, greatest(${HOST_MONITORING_LIMITS.maxIncidents} / (
                        select count(*) from "rentnerproxy"."host_check_policies"
                    ), 1)) - case when opened.id is null then 0 else 1 end, 0) as incident_budget
                from "rentnerproxy"."host_check_incidents" i
                join "rentnerproxy"."host_check_policies" p on p.id = i.policy_id
                left join "rentnerproxy"."host_check_incidents" opened
                    on opened.policy_id = i.policy_id and opened.closed_reason is null
                where i.closed_reason is not null
            ) ranked
            where closed_at < clock_timestamp() - retention_days * interval '1 day'
                or policy_rank > incident_budget
            order by slot, policy_id
            limit ${HOST_MONITORING_LIMITS.retentionDeleteLimit}
        ) expired where incident.id = expired.id
    `)
}

export async function pruneHostMonitoringHistory(): Promise<void> {
    await getBackgroundDatabase('housekeeping').transaction(async (transaction) => {
        await prepareWorkerTransaction(transaction, false)
        await pruneHistoryInTransaction(transaction)
    })
}
