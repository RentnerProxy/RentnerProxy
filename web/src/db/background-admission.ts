import { AsyncLocalStorage } from 'node:async_hooks'
import { BACKGROUND_DATABASE_POLICY } from '../config/background-resources.config.ts'
import { createResourceAdmission } from '../server/Foundation/resource-admission.ts'
import type { BackgroundDatabaseClass } from '../server/Foundation/Types/resource-admission.types.ts'

const background = new AsyncLocalStorage<{
    signal: AbortSignal | undefined
    resource: BackgroundDatabaseClass
}>()
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

/** Mark worker-owned async work without reserving a SQL connection during network calls or timer waits. */
export function withBackgroundDatabaseContext<T>(
    operation: () => Promise<T>,
    signal?: AbortSignal,
    resource?: BackgroundDatabaseClass,
): Promise<T> {
    const parent = background.getStore()
    return background.run(
        { signal: signal ?? parent?.signal, resource: resource ?? parent?.resource ?? 'critical' },
        operation,
    )
}

/** Acquire only for a real top-level background transaction; foreground SQL and nested savepoints retain their normal behavior. */
export function admitDatabaseTransaction<T>(
    operation: (resource?: BackgroundDatabaseClass) => Promise<T>,
): Promise<T> {
    const context = background.getStore()
    return context
        ? admissions[context.resource].run(() => operation(context.resource), context.signal)
        : operation()
}

/** Read the same class singletons that admit SQL, without reserving capacity or exposing worker payloads. */
export function backgroundDatabaseResourceSnapshot() {
    return Object.fromEntries(
        Object.entries(admissions).map(([resource, admission]) => [resource, admission.snapshot()]),
    )
}
