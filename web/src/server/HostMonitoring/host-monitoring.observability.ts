// oxlint-disable-next-line import/no-unassigned-import -- Aggregate policy reads remain server-side.
import '@tanstack/react-start/server-only'
import { and, eq, sql } from 'drizzle-orm'

import { hostCheckPolicies, hostDomains, proxyHosts } from '@/db/schema.ts'
import { getAuthDatabase } from '@/server/Auth/Core/database.server.ts'
import { workerObservability } from '@/server/Foundation/worker-observability.ts'

/** Read only enabled, existing policies under a two-second SQL deadline; unavailable queue evidence becomes null rather than zero. */
export async function sampleHostMonitoringBacklog(): Promise<void> {
    try {
        const row = await getAuthDatabase().transaction(
            async (transaction) => {
                await transaction.execute(sql`set local statement_timeout = '2000ms'`)
                const [counts] = await transaction
                    .select({
                        backlog:
                            sql<number>`count(*) filter (where ${hostCheckPolicies.nextCheckAt} <= clock_timestamp() and (${hostCheckPolicies.leaseExpiresAt} is null or ${hostCheckPolicies.leaseExpiresAt} <= clock_timestamp()))`.mapWith(
                                Number,
                            ),
                        oldestPendingAt: sql<
                            number | null
                        >`extract(epoch from min(${hostCheckPolicies.nextCheckAt}) filter (where ${hostCheckPolicies.nextCheckAt} <= clock_timestamp() and (${hostCheckPolicies.leaseExpiresAt} is null or ${hostCheckPolicies.leaseExpiresAt} <= clock_timestamp()))) * 1000`.mapWith(
                            Number,
                        ),
                        nextDueAt: sql<
                            number | null
                        >`extract(epoch from min(${hostCheckPolicies.nextCheckAt})) * 1000`.mapWith(
                            Number,
                        ),
                    })
                    .from(hostCheckPolicies)
                    .innerJoin(proxyHosts, eq(proxyHosts.id, hostCheckPolicies.proxyHostId))
                    .innerJoin(
                        hostDomains,
                        and(
                            eq(hostDomains.proxyHostId, hostCheckPolicies.proxyHostId),
                            eq(hostDomains.domain, hostCheckPolicies.domain),
                        ),
                    )
                    .where(and(eq(hostCheckPolicies.enabled, true), eq(proxyHosts.enabled, true)))
                return counts
            },
            { accessMode: 'read only' },
        )
        workerObservability.sample(
            'host_monitoring',
            row ?? { backlog: null, oldestPendingAt: null, nextDueAt: null },
        )
    } catch {
        workerObservability.sample('host_monitoring', {
            backlog: null,
            oldestPendingAt: null,
            nextDueAt: null,
        })
    }
}
