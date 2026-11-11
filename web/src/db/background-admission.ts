import { AsyncLocalStorage } from 'node:async_hooks'
import { DATABASE_CONNECTION_BUDGET } from '../config/database.config.ts'
import { createResourceAdmission } from '../server/Foundation/resource-admission.ts'

const background = new AsyncLocalStorage<{ signal: AbortSignal | undefined }>()
const admission = createResourceAdmission({
    running: DATABASE_CONNECTION_BUDGET.backgroundTransactions,
    pending: DATABASE_CONNECTION_BUDGET.backgroundPending,
    waitMs: DATABASE_CONNECTION_BUDGET.backgroundWaitMs,
})

/** Mark worker-owned async work without reserving a SQL connection during network calls or timer waits. */
export function withBackgroundDatabaseContext<T>(
    operation: () => Promise<T>,
    signal?: AbortSignal,
): Promise<T> {
    return background.run({ signal: signal ?? background.getStore()?.signal }, operation)
}

/** Acquire only for a real top-level background transaction; foreground SQL and nested savepoints retain their normal behavior. */
export function admitDatabaseTransaction<T>(operation: () => Promise<T>): Promise<T> {
    const context = background.getStore()
    return context ? admission.run(operation, context.signal) : operation()
}
