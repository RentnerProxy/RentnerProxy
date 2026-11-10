import type { AuditEventInput } from '@/lib/Admin/AuditLogs/Types/audit-events.types.ts'
import { publishApplicationChange } from './applicationChanges.ts'
import { affectsDockerDiscovery, publishDiscoveryChange } from './discoveryChanges.ts'

const changedTransactions = new WeakSet<object>()
const discoveryTransactions = new WeakSet<object>()
const discoveryReconciliations = new WeakSet<object>()

/** Discovery already schedules its bounded continuation; its own writes must not re-invalidate itself. */
export function markDiscoveryReconciliation(transaction: object): void {
    discoveryReconciliations.add(transaction)
}

export function markAuditedApplicationMutation(transaction: object, event: AuditEventInput): void {
    if (affectsDockerDiscovery(event)) discoveryTransactions.add(transaction)
    if (
        (event.actorKind !== 'system' && (event.actorKind !== 'user' || !event.actorUserId)) ||
        event.result !== 'success' ||
        event.resource === 'config-export' ||
        event.action === 'login' ||
        event.metadata?.changedFields?.length === 0
    )
        return
    changedTransactions.add(transaction)
}

export async function withCommittedApplicationChanges<TTransaction extends object, TResult>(
    commit: (operation: (transaction: TTransaction) => Promise<TResult>) => Promise<TResult>,
    operation: (transaction: TTransaction) => Promise<TResult>,
    parent?: object,
): Promise<TResult> {
    let current: TTransaction | undefined
    try {
        const result = await commit(async (transaction) => {
            current = transaction
            return operation(transaction)
        })
        if (current && changedTransactions.has(current)) {
            if (parent) changedTransactions.add(parent)
            else publishApplicationChange()
        }
        if (
            current &&
            discoveryTransactions.has(current) &&
            !discoveryReconciliations.has(current)
        ) {
            if (parent) discoveryTransactions.add(parent)
            else publishDiscoveryChange()
        }
        return result
    } finally {
        if (current) changedTransactions.delete(current)
        if (current) discoveryTransactions.delete(current)
        if (current) discoveryReconciliations.delete(current)
    }
}
