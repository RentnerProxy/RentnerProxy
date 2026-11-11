import type { getAuthDatabase } from '../../web/src/server/Auth/Core/database.server.ts'
import type { AuthTransaction } from '../../web/src/server/Auth/Core/Types/database.types.ts'
import { Database } from 'bun:sqlite'
import { lstat } from 'node:fs/promises'

/** Only a missing store before every native launch attempt can omit replay drain. */
export async function openMonitoringB2OutboxForDrain(path: string, nativeStartAttempted: boolean) {
    try {
        const metadata = await lstat(path)
        if (!metadata.isFile()) throw new Error('B2 native outbox must be an ordinary file')
    } catch (error) {
        if (!nativeStartAttempted && (error as { code?: unknown })?.code === 'ENOENT')
            return undefined
        throw error
    }
    return new Database(path, { readonly: true, strict: true })
}

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
    'role-identity-invalid',
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

/** Only generated disposable roles and 256-bit hexadecimal fixture credentials may reach psql. */
export function validateMonitoringB2Role(
    action: 'create' | 'drop',
    role: string,
    password?: string,
) {
    if (
        !/^monitor_b2_role_[a-f0-9]{32}$/u.test(role) ||
        (action === 'create' && !/^[a-f0-9]{64}$/u.test(password ?? ''))
    )
        throw Object.assign(new Error('B2 disposable role identity invalid'), {
            code: 'role-identity-invalid',
        })
}

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
