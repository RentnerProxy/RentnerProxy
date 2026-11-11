import type { getAuthDatabase } from '../../web/src/server/Auth/Core/database.server.ts'
import type { AuthTransaction } from '../../web/src/server/Auth/Core/Types/database.types.ts'

/** Direct fixture policy changes use the original production settings/monitoring lock order. */
export async function monitoringB2PolicyTransaction<T>(
    database: ReturnType<typeof getAuthDatabase>,
    mutation: (transaction: AuthTransaction) => Promise<T>,
) {
    if (process.env.RENTNERPROXY_MONITORING_B2 !== '1')
        throw new Error('B2 policy fixture opt-in required')
    const { prepareWorkerTransaction } =
        await import('../../web/src/server/HostMonitoring/host-monitoring.storage.ts')
    return database.transaction(async (transaction) => {
        await prepareWorkerTransaction(transaction)
        return mutation(transaction)
    })
}

const diagnostics = new Set([
    '42P01',
    '42501',
    '40P01',
    '23505',
    '23503',
    '23514',
    '42601',
    '42703',
    '42704',
    '08006',
    '57P01',
    '55P03',
    '57014',
    '25P02',
    'EACCES',
    'EPERM',
    'ENOENT',
])

/** Bun can wrap PostgreSQL SQLSTATE in errno/cause. Only fixed codes leave the private fixture. */
export function monitoringB2FixtureDiagnostic(error: unknown) {
    const seen = new Set<unknown>()
    let current = error
    for (
        let depth = 0;
        depth < 4 && current && typeof current === 'object' && !seen.has(current);
        depth++
    ) {
        seen.add(current)
        const value = current as { code?: unknown; errno?: unknown; cause?: unknown }
        for (const field of [value.code, value.errno])
            if (typeof field === 'string' && diagnostics.has(field)) return field
        current = value.cause
    }
    return 'unclassified'
}
