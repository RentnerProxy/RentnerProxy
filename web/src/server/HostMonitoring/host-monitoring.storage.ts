// oxlint-disable-next-line import/no-unassigned-import -- Keeps durable monitoring operations on the server.
import '@tanstack/react-start/server-only'

import { sql } from 'drizzle-orm'
import { HOST_MONITORING_LIMITS } from '@/config/host-monitoring.config.ts'
import { getAuthDatabase } from '@/server/Auth/Core/database.server.ts'
import type { AuthTransaction } from '@/server/Auth/Core/Types/database.types.ts'
import { lockProxyRuntimeSettings } from '@/server/ProxyRuntime/proxy-runtime-settings.ts'
import type { HostMonitoringPolicy } from './Types/host-monitoring.repository.types.ts'
export async function lockMonitoring(transaction: AuthTransaction): Promise<void> {
    await transaction.execute(
        sql`select pg_advisory_xact_lock(${HOST_MONITORING_LIMITS.advisoryLockId})`,
    )
}

export async function prepareWorkerTransaction(transaction: AuthTransaction): Promise<void> {
    const timeout = `${HOST_MONITORING_LIMITS.transactionTimeoutMs}ms`
    await transaction.execute(
        sql`select set_config('lock_timeout', ${timeout}, true), set_config('statement_timeout', ${timeout}, true), set_config('transaction_timeout', ${timeout}, true)`,
    )
    await lockProxyRuntimeSettings(transaction)
    await lockMonitoring(transaction)
}

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
    await transaction.execute(sql`
        delete from "rentnerproxy"."host_check_observations" as observation
        using (
            select id from "rentnerproxy"."host_check_observations"
            where policy_id = ${policy.id}::uuid
                and checked_at < clock_timestamp() - ${policy.retentionDays} * interval '1 day'
            union
            (select id from "rentnerproxy"."host_check_observations"
             where policy_id = ${policy.id}::uuid
             order by checked_at desc, id desc offset ${policy.sampleLimit})
        ) expired where observation.id = expired.id
    `)
    await transaction.execute(sql`
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
        ) expired where incident.id = expired.id
    `)
}

// The periodic sweep and policy edits reconcile global retention targets.
export async function pruneHistoryInTransaction(transaction: AuthTransaction): Promise<void> {
    await transaction.execute(sql`
        delete from "rentnerproxy"."host_check_observations" as observation
        using (
            select id from (
                select o.id, o.checked_at, p.retention_days, p.sample_limit,
                    row_number() over (partition by o.policy_id order by o.checked_at desc, o.id desc) as policy_rank,
                    row_number() over (order by o.checked_at desc, o.id desc) as global_rank
                from "rentnerproxy"."host_check_observations" o
                join "rentnerproxy"."host_check_policies" p on p.id = o.policy_id
            ) ranked
            where checked_at < clock_timestamp() - retention_days * interval '1 day'
                or policy_rank > sample_limit or global_rank > ${HOST_MONITORING_LIMITS.maxObservations}
        ) expired where observation.id = expired.id
    `)
    // Open incidents survive both retention and count caps. At most one can exist per policy.
    await transaction.execute(sql`
        delete from "rentnerproxy"."host_check_incidents" as incident
        using (
            select id from (
                select i.id, i.closed_at, p.retention_days, p.incident_limit,
                    row_number() over (partition by i.policy_id order by i.closed_at desc, i.id desc) as policy_rank,
                    row_number() over (order by i.closed_at desc, i.id desc) as global_rank,
                    case when opened.id is null then 0 else 1 end as policy_open,
                    (select count(*) from "rentnerproxy"."host_check_incidents" opened where opened.closed_reason is null) as global_open
                from "rentnerproxy"."host_check_incidents" i
                join "rentnerproxy"."host_check_policies" p on p.id = i.policy_id
                left join "rentnerproxy"."host_check_incidents" opened on opened.policy_id = i.policy_id and opened.closed_reason is null
                where i.closed_reason is not null
            ) ranked
            where closed_at < clock_timestamp() - retention_days * interval '1 day'
                or policy_rank > greatest(incident_limit - policy_open, 0)
                or global_rank > greatest(${HOST_MONITORING_LIMITS.maxIncidents} - global_open, 0)
        ) expired where incident.id = expired.id
    `)
}

export async function pruneHostMonitoringHistory(): Promise<void> {
    await getAuthDatabase().transaction(async (transaction) => {
        await prepareWorkerTransaction(transaction)
        await pruneHistoryInTransaction(transaction)
    })
}
