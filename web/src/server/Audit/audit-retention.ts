import { sql } from 'drizzle-orm'

import type { AuthTransaction } from '@/server/Auth/Core/Types/database.types.ts'

export const AUDIT_RETENTION_DAYS = 90
export const AUDIT_RETENTION_LOCK_ID = 7_421_908
const AUDIT_MAX_ROWS = 100_000

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

/** Acquire before inserting, preserving the established application-lock / audit-lock order. */
export async function lockAuditRetentionInTransaction(transaction: AuthTransaction): Promise<void> {
    const state = retentionTransactions.get(transaction)
    if (state) {
        // Drizzle's query is lazy and re-executes on every await; cache the settled Promise instead.
        state.lock ??= Promise.resolve(
            transaction.execute(sql`select pg_advisory_xact_lock(${AUDIT_RETENTION_LOCK_ID})`),
        )
        await state.lock
    } else {
        await transaction.execute(sql`select pg_advisory_xact_lock(${AUDIT_RETENTION_LOCK_ID})`)
    }
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
    // Find one boundary in the ordering index, then delete its suffix by index range.
    // Exact reconciliation still visits up to 100001 index entries, once per logical transaction.
    await transaction.execute(sql`
        delete from "rentnerproxy"."audit_events"
        where ("created_at", "id") <= (
            select "created_at", "id" from "rentnerproxy"."audit_events"
            order by "created_at" desc, "id" desc
            offset ${AUDIT_MAX_ROWS} limit 1
        )
    `)
}
