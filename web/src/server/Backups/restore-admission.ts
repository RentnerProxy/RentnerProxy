import type { readRestoreMaintenanceObservation } from './restore-maintenance-status.server.ts'

type RestoreObservation = Awaited<ReturnType<typeof readRestoreMaintenanceObservation>>
type RestoreStatus =
    RestoreObservation['maintenance'] | RestoreObservation['restoreResults'][number] | undefined

/** Destructive progress cannot be undone by a failed preflight or an unrelated operation. */
export const isDestructiveRestore = (status: RestoreStatus) =>
    status?.operation === 'restore' && !['queued', 'preparing', 'validating'].includes(status.phase)

/** Wait for active restores and unresolved destructive failures; terminal preflight failures permit admission. */
export const isRestoreAdmissionBlocked = (status: RestoreStatus) =>
    status?.operation === 'restore' &&
    (status.state === 'queued' ||
        status.state === 'running' ||
        ((status.state === 'interrupted' || status.state === 'failed') &&
            isDestructiveRestore(status)))

/** Pure bounded admission decision over the root journal's append order; IDs and wall clocks are not ordering proof. */
export function advanceRestoreAdmission(
    previous: ReadonlySet<string>,
    observation: RestoreObservation,
): Set<string> {
    const waiting = new Set(previous)
    const receipts = observation.restoreResults
    const recovery = receipts.findLastIndex(
        (receipt) => receipt.state === 'succeeded' && receipt.phase === 'complete',
    )
    for (const restoreId of waiting) {
        const index = receipts.findIndex((receipt) => receipt.id === restoreId)
        const result =
            observation.maintenance?.id === restoreId ? observation.maintenance : receipts[index]
        if (observation.maintenance?.id === restoreId && isRestoreAdmissionBlocked(result)) continue
        if (
            (index >= 0 && index < recovery) ||
            (result?.operation === 'restore' &&
                !['queued', 'running'].includes(result.state) &&
                !isRestoreAdmissionBlocked(result))
        )
            waiting.delete(restoreId)
    }
    const retain = (restoreId: string) => {
        // Missing receipts remain unresolved; the bounded publication cannot prove older history safe.
        if (!waiting.has(restoreId) && waiting.size === 100)
            throw new Error('restore_admission_history_exhausted')
        waiting.add(restoreId)
    }
    for (let index = recovery + 1; index < receipts.length; index++)
        if (isRestoreAdmissionBlocked(receipts[index])) retain(receipts[index]!.id)
    if (isRestoreAdmissionBlocked(observation.maintenance)) retain(observation.maintenance!.id)
    return waiting
}
