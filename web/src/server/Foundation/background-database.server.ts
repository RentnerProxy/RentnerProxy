import '@tanstack/react-start/server-only'
import { sql } from 'drizzle-orm'
import { BACKGROUND_DATABASE_POLICY } from '@/config/background-resources.config.ts'
import { getAuthDatabase } from '@/server/Auth/Core/database.server.ts'
import { createResourceAdmission } from './resource-admission.ts'
import type { BackgroundDatabaseClass } from './Types/resource-admission.types.ts'

type Database = ReturnType<typeof getAuthDatabase>
type Transaction = Parameters<Parameters<Database['transaction']>[0]>[0]

const classes = BACKGROUND_DATABASE_POLICY.classes
const admissions = {
    monitoring: createResourceAdmission(classes.monitoring),
    notifications: createResourceAdmission(classes.notifications),
    discovery: createResourceAdmission(classes.discovery),
    housekeeping: createResourceAdmission(classes.housekeeping),
    critical: createResourceAdmission(classes.critical),
}
if (
    Object.values(classes).reduce((sum, value) => sum + value.running, 0) !==
    BACKGROUND_DATABASE_POLICY.ceiling
)
    throw new Error('Background database partitions must match the connection budget')

/** Only SQL belongs inside this boundary; network calls and lease ownership never occupy a waiting connection. */
export function getBackgroundDatabase(resource: BackgroundDatabaseClass) {
    return {
        /** Acquire the class permit before opening SQL and apply transaction-local statement/lock/transaction deadlines before invoking the caller. */
        transaction<T>(
            operation: (transaction: Transaction) => Promise<T>,
            options?: Parameters<Database['transaction']>[1],
        ): Promise<T> {
            return admissions[resource].run(() =>
                getAuthDatabase().transaction(async (transaction) => {
                    const timeout = `${classes[resource].transactionMs}ms`
                    await transaction.execute(
                        sql`select set_config('statement_timeout', ${timeout}, true), set_config('lock_timeout', ${timeout}, true), set_config('transaction_timeout', ${timeout}, true)`,
                    )
                    return operation(transaction)
                }, options),
            )
        },
    }
}

/** Fixed class keys and saturated counters keep overload inspection independent of payloads/cardinality. */
export function getBackgroundDatabaseResourceSnapshot() {
    return Object.fromEntries(
        Object.entries(admissions).map(([resource, admission]) => [resource, admission.snapshot()]),
    )
}
