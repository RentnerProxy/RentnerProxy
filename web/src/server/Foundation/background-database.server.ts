import '@tanstack/react-start/server-only'
import { getAuthDatabase } from '@/server/Auth/Core/database.server.ts'
import {
    withBackgroundDatabaseContext,
    backgroundDatabaseResourceSnapshot,
} from '@/db/background-admission.ts'
import type { BackgroundDatabaseClass } from './Types/resource-admission.types.ts'

type Database = ReturnType<typeof getAuthDatabase>
type Transaction = Parameters<Parameters<Database['transaction']>[0]>[0]

/** Only SQL belongs inside this boundary; network calls and lease ownership never occupy a waiting connection. */
export function getBackgroundDatabase(resource: BackgroundDatabaseClass) {
    return {
        /** Select this transaction's class; the shared database boundary acquires its sole permit and applies local SQL deadlines. */
        transaction<T>(
            operation: (transaction: Transaction) => Promise<T>,
            options?: Parameters<Database['transaction']>[1],
        ): Promise<T> {
            return withBackgroundDatabaseContext(
                () => getAuthDatabase().transaction(operation, options),
                undefined,
                resource,
            )
        },
    }
}

/** Fixed class keys and saturated counters keep overload inspection independent of payloads/cardinality. */
export function getBackgroundDatabaseResourceSnapshot() {
    return backgroundDatabaseResourceSnapshot()
}
