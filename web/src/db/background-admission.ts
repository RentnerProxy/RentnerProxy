import { AsyncLocalStorage } from 'node:async_hooks'
import { DATABASE_CONNECTION_BUDGET } from '../config/database.config.ts'
import { createResourceAdmission } from '../server/Foundation/resource-admission.ts'

const background = new AsyncLocalStorage<{ signal: AbortSignal | undefined }>()
const admission = createResourceAdmission({
    running: DATABASE_CONNECTION_BUDGET.backgroundTransactions,
    pending: DATABASE_CONNECTION_BUDGET.backgroundPending,
    waitMs: DATABASE_CONNECTION_BUDGET.backgroundWaitMs,
})

/** Mark worker-owned work without reserving SQL during waits; explicit null detaches inherited cancellation for admitted finalization. */
export function withBackgroundDatabaseContext<T>(
    operation: () => Promise<T>,
    signal?: AbortSignal | null,
): Promise<T> {
    return background.run(
        { signal: signal === null ? undefined : (signal ?? background.getStore()?.signal) },
        operation,
    )
}

/** Acquire only for a real top-level background transaction; foreground SQL and nested savepoints retain their normal behavior. */
export function admitDatabaseTransaction<T>(operation: () => Promise<T>): Promise<T> {
    const context = background.getStore()
    return context ? admission.run(operation, context.signal) : operation()
}
