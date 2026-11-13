import { describe, expect, test } from 'bun:test'
import { PgDialect } from 'drizzle-orm/pg-core'
import type { SQL } from 'drizzle-orm'

import type { AuthTransaction } from '@/server/Auth/Core/Types/database.types.ts'
import {
    lockAuditRetentionInTransaction,
    retainAuditEventsInTransaction,
    withAuditRetentionTransaction,
} from '@/server/Audit/audit-retention.ts'

function fixture(excess = '1') {
    const statements: string[] = []
    const transaction = {
        // Match Drizzle's lazy query, which executes anew whenever its then method is called.
        execute: (query: SQL) => ({
            // oxlint-disable-next-line unicorn/no-thenable -- Exercise the driver's actual repeatable-thenable contract.
            then: (resolve: (rows: unknown[]) => unknown) =>
                Promise.resolve().then(() => {
                    const statement = new PgDialect().sqlToQuery(query).sql
                    statements.push(statement)
                    if (statement.includes('audit_retention_state')) return resolve([{ excess }])
                    if (statement.includes('order by')) return resolve([{
                        id: '00000000-0000-4000-8000-000000000001',
                    }])
                    return resolve([])
                }),
        }),
    } as unknown as AuthTransaction
    return { statements, transaction }
}

async function append(transaction: AuthTransaction) {
    await lockAuditRetentionInTransaction(transaction)
    await retainAuditEventsInTransaction(transaction)
}

describe('audit transaction retention lifecycle', () => {
    test('many appends acquire one lock and reconcile once before root commit', async () => {
        const { transaction, statements } = fixture()
        let committed = false
        const result = await withAuditRetentionTransaction(
            async (run) => {
                const operationResult = await run(transaction)
                expect(statements.filter((query) => query.includes('delete from'))).toHaveLength(2)
                committed = true
                return operationResult
            },
            async (current) => {
                await Promise.all(Array.from({ length: 50 }, () => append(current)))
                expect(statements).toHaveLength(2)
                expect(statements[0]).toContain('lock table')
                expect(statements[1]).toContain('pg_advisory_xact_lock')
                expect(committed).toBe(false)
                return 'imported'
            },
        )
        expect(result).toBe('imported')
        expect(committed).toBe(true)
        expect(statements).toHaveLength(6)
    })

    test('released savepoints promote work and failed releases or callbacks do not', async () => {
        const outer = fixture()
        const released = fixture()
        const failedRelease = fixture()
        const failedCallback = fixture()
        await withAuditRetentionTransaction(
            (run) => run(outer.transaction),
            async (parent) => {
                for (const child of [failedRelease, failedCallback]) {
                    // oxlint-disable-next-line no-await-in-loop -- Savepoint rollback precedes the next sibling.
                    await expect(
                        withAuditRetentionTransaction(
                            async (run) => {
                                await run(child.transaction)
                                throw new Error('release failed')
                            },
                            async (current) => {
                                await append(current)
                                if (child === failedCallback) throw new Error('callback failed')
                            },
                            parent,
                        ),
                    ).rejects.toThrow()
                }
                expect(outer.statements).toHaveLength(0)
                await withAuditRetentionTransaction(
                    (run) => run(released.transaction),
                    append,
                    parent,
                )
                expect(released.statements).toHaveLength(2)
            },
        )
        expect(outer.statements).toHaveLength(6)
        expect(failedRelease.statements).toHaveLength(2)
        expect(failedCallback.statements).toHaveLength(2)
    })

    test('rolled-back savepoints alone leave no root retention work', async () => {
        const outer = fixture()
        const child = fixture()
        await withAuditRetentionTransaction(
            (run) => run(outer.transaction),
            async (parent) => {
                await expect(
                    withAuditRetentionTransaction(
                        async (run) => {
                            await run(child.transaction)
                            throw new Error('rollback savepoint')
                        },
                        append,
                        parent,
                    ),
                ).rejects.toThrow('rollback savepoint')
            },
        )
        expect(outer.statements).toHaveLength(0)
    })

    test('unmanaged appends reconcile immediately, without cached state', async () => {
        const { transaction, statements } = fixture()
        await append(transaction)
        expect(statements).toHaveLength(6)
        await append(transaction)
        expect(statements).toHaveLength(12)
    })

    test('failed root callbacks discard lifecycle state before the object can be reused', async () => {
        const { transaction, statements } = fixture()
        await expect(
            withAuditRetentionTransaction(
                (run) => run(transaction),
                async (current) => {
                    await append(current)
                    throw new Error('rollback mutation')
                },
            ),
        ).rejects.toThrow('rollback mutation')
        expect(statements).toHaveLength(2)
        await append(transaction)
        expect(statements).toHaveLength(8)
    })

    test('no overflow skips all event ordering traversal and overflow deletion', async () => {
        const { transaction, statements } = fixture('0')
        await append(transaction)
        expect(statements).toHaveLength(4)
        expect(statements.some((statement) => statement.includes('order by'))).toBe(false)
        expect(statements.filter((statement) => statement.includes('delete from'))).toHaveLength(1)
    })

    test('retention errors reject the mutation before commit', async () => {
        const original = fixture()
        const transaction = {
            execute: async (query: SQL) => {
                if (new PgDialect().sqlToQuery(query).sql.includes('delete from'))
                    throw new Error('retention failed')
                return original.transaction.execute(query)
            },
        } as unknown as AuthTransaction
        let committed = false
        await expect(
            withAuditRetentionTransaction(async (run) => {
                await run(transaction)
                committed = true
            }, append),
        ).rejects.toThrow('retention failed')
        expect(committed).toBe(false)
    })
})
