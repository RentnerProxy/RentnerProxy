import '@tanstack/react-start/server-only'

import { db } from '@/db/index.ts'
import { withCommittedApplicationChanges } from '@/server/WebSockets/committedChanges.ts'
import { withAuditRetentionTransaction } from '@/server/Audit/audit-retention.ts'

type DatabaseTransaction = Parameters<Parameters<typeof db.transaction>[0]>[0]

function wrapTransaction(current: DatabaseTransaction): DatabaseTransaction {
    const wrapped: DatabaseTransaction = new Proxy(current, {
        get(target, property, receiver): unknown {
            if (property !== 'transaction') return Reflect.get(target, property, receiver)
            return <TResult>(operation: (nested: DatabaseTransaction) => Promise<TResult>) =>
                withCommittedApplicationChanges(
                    (run) =>
                        withAuditRetentionTransaction(
                            (retain) => target.transaction((nested) => retain(wrapTransaction(nested))),
                            run,
                            wrapped,
                        ),
                    operation,
                    wrapped,
                )
        },
    })
    return wrapped
}

function transaction<TResult>(
    operation: (transaction: DatabaseTransaction) => Promise<TResult>,
    options?: Parameters<typeof db.transaction>[1],
): Promise<TResult> {
    return withCommittedApplicationChanges(
        (run) =>
            withAuditRetentionTransaction(
                (retain) => db.transaction((current) => retain(wrapTransaction(current)), options),
                run,
            ),
        operation,
    )
}

const database = new Proxy(db, {
    get(target, property, receiver): unknown {
        return property === 'transaction' ? transaction : Reflect.get(target, property, receiver)
    },
})

export function getAuthDatabase() {
    return database
}
