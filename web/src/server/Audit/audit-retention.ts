import { sql } from 'drizzle-orm'

import type { AuthTransaction } from '@/server/Auth/Core/Types/database.types.ts'

export const AUDIT_RETENTION_DAYS = 90
export const AUDIT_RETENTION_LOCK_ID = 7_421_908
const AUDIT_MAX_ROWS = 100_000
const AUDIT_RETENTION_DELETE_BATCH = 128n

type RetentionTransaction = {
    dirty: boolean
    lock?: Promise<unknown>
}

// This tracks transaction lifecycle only, never a cached row count or retention boundary.
const retentionTransactions = new WeakMap<object, RetentionTransaction>()

/** Reconcile once before root commit; promote savepoint work only after successful release. */
export async function withAuditRetentionTransaction<TResult>(
    commit: (operation: (transaction: AuthTransaction) => Promise<TResult>) => Promise<TResult>,
    operation: (transaction: AuthTransaction) => Promise<TResult>,
    parent?: AuthTransaction,
): Promise<TResult> {
    let current: AuthTransaction | undefined
    const state: RetentionTransaction = { dirty: false }
    try {
        const result = await commit(async (transaction) => {
            current = transaction
            retentionTransactions.set(transaction, state)
            const operationResult = await operation(transaction)
            if (state.dirty && !parent) {
                await lockAuditRetentionInTransaction(transaction)
                await pruneAuditEventsInTransaction(transaction)
            }
            return operationResult
        })
        const parentState = parent && retentionTransactions.get(parent)
        if (state.dirty && parentState) parentState.dirty = true
        return result
    } finally {
        if (current) retentionTransactions.delete(current)
    }
}

/** Match raw SQL's table / advisory / rows / counter order, including concurrent TRUNCATE. */
export async function lockAuditRetentionInTransaction(transaction: AuthTransaction): Promise<void> {
    const state = retentionTransactions.get(transaction)
    if (state) {
        // Drizzle's query is lazy and re-executes on every await; cache the settled Promise instead.
        state.lock ??= acquireAuditLock(transaction)
        await state.lock
    } else {
        await acquireAuditLock(transaction)
    }
}

async function acquireAuditLock(transaction: AuthTransaction): Promise<void> {
    await transaction.execute(sql`lock table "rentnerproxy"."audit_events" in row exclusive mode`)
    await transaction.execute(sql`select pg_advisory_xact_lock(${AUDIT_RETENTION_LOCK_ID})`)
}

export async function retainAuditEventsInTransaction(transaction: AuthTransaction): Promise<void> {
    const state = retentionTransactions.get(transaction)
    if (state) state.dirty = true
    // Unmanaged transactions retain the immediate append contract, including raw SQL fixtures.
    else await pruneAuditEventsInTransaction(transaction)
}

async function pruneAuditEventsInTransaction(transaction: AuthTransaction): Promise<void> {
    const cutoff = new Date(Date.now() - AUDIT_RETENTION_DAYS * 24 * 60 * 60 * 1_000)
    await transaction.execute(sql`
        delete from "rentnerproxy"."audit_events"
        where "created_at" < ${cutoff.toISOString()}::timestamptz
    `)
    const [state] = await transaction.execute<{ excess: string }>(sql`
        select greatest("row_count" - ${AUDIT_MAX_ROWS}, 0)::text as "excess"
        from "rentnerproxy"."audit_retention_state" where "singleton"
    `)
    if (!state) throw new Error('Audit retention cardinality state is missing')
    let excess = BigInt(state.excess)
    while (excess > 0n) {
        // Bounded ID batches avoid tuple-range selectivity estimates that can scan all retained
        // rows when many events share one timestamp. Only the obsolete prefix is traversed.
        const limit = Number(excess < AUDIT_RETENTION_DELETE_BATCH ? excess : AUDIT_RETENTION_DELETE_BATCH)
        // oxlint-disable-next-line no-await-in-loop -- Delete each oldest batch before advancing within the same locked transaction.
        const obsolete = await transaction.execute<{ id: string }>(sql`
            select "id" from "rentnerproxy"."audit_events"
            order by "created_at", "id" limit ${limit}
        `)
        if (obsolete.length !== limit)
            throw new Error('Audit retention cardinality does not match stored events')
        // oxlint-disable-next-line no-await-in-loop -- Statement triggers atomically decrement the exact count for this batch.
        await transaction.execute(sql`
            delete from "rentnerproxy"."audit_events"
            where "id" in (${sql.join(obsolete.map(({ id }) => sql`${id}::uuid`), sql`, `)})
        `)
        excess -= BigInt(obsolete.length)
    }
}
