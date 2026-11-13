import useAsyncAction from '@/shared/Feedback/Hooks/useAsyncAction.ts'
import { useCallback, useEffect, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useRouter } from '@tanstack/react-router'
import { toast } from '@rentnerkev/toasts'
import { RESTORE_COMPLETION_VISIBLE_MS } from '@/config/backups.config.ts'
import useTranslationStore from '@/shared/Language/Hooks/useTranslationStore.ts'
import { backupRestoreSnapshotSchema } from '@/lib/Backups/validation.ts'
import type { RestoreMaintenanceDisplay } from '@/lib/Backups/Types/backups.types.ts'
import useLiveQuery from '@/shared/Live/Hooks/useLiveQuery.ts'

/** Retain restore blockers through disconnects and retire them only after verified recovery or a safe preflight dismissal. */
export default function useRestoreProgress(beforeHide?: () => void) {
    const runAction = useAsyncAction()
    const queryClient = useQueryClient()
    const router = useRouter()
    const { t } = useTranslationStore()
    const [maintenance, setMaintenance] = useState<RestoreMaintenanceDisplay | null>(null)
    const current = useRef<RestoreMaintenanceDisplay | null>(null)
    const completing = useRef<string | null>(null)
    const refreshRequired = useRef(false)
    const [refreshPending, setRefreshPending] = useState(false)
    const historicalBlocker = useRef<string | null>(null)
    const retryTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
    const retryDelay = useRef(1_000)
    const retryOwner = useRef<{
        target: RestoreMaintenanceDisplay
        owner: RestoreMaintenanceDisplay | null
    } | null>(null)
    const cancelRefreshRetry = useCallback((resetDelay = true) => {
        if (retryTimer.current !== null) clearTimeout(retryTimer.current)
        retryTimer.current = null
        retryOwner.current = null
        if (resetDelay) retryDelay.current = 1_000
    }, [])
    const retiredOperations = useRef(new Set<string>())
    const retireOperation = useCallback((id: string) => {
        const retired = retiredOperations.current
        retired.add(id)
        // Bound this to the same recent-operation window as maintenance receipts.
        if (retired.size > 100) retired.delete(retired.values().next().value!)
    }, [])
    const mounted = useRef(true)
    const refreshRestoredRoutes = useCallback(async () => {
        await router.invalidate({ sync: true })
        // Router invalidation can resolve with failed matches after a transient
        // restart response. Only a settled successful route confirms recovery.
        const state = router.state
        if (
            state.status !== 'idle' ||
            state.isLoading ||
            (state.resolvedLocation && state.resolvedLocation.href !== state.location.href) ||
            !state.matches.some((match) => match.routeId === '/_authenticated') ||
            state.matches.some(
                (match) => match.status !== 'success' || match.invalid || match.error,
            )
        )
            throw new Error('Restored routes have not recovered')
    }, [router])
    useEffect(() => {
        mounted.current = true
        return () => {
            mounted.current = false
            cancelRefreshRetry()
        }
    }, [cancelRefreshRetry])
    const handleSnapshot = useCallback(
        async function handleRestoreSnapshot(value: unknown): Promise<void> {
            const parsed = backupRestoreSnapshotSchema.safeParse(value)
            if (!parsed.success) return
            const next = parsed.data.maintenance
            if (!next || next.operation !== 'restore') return
            if (retiredOperations.current.has(next.id)) return
            const previous = current.current
            const historical = 'detailsUnavailable' in next
            if (
                (completing.current !== null || retryTimer.current !== null) &&
                historical &&
                previous &&
                'detailsUnavailable' in previous &&
                previous.id === next.id &&
                previous.state === next.state &&
                previous.phase === next.phase
            )
                return
            if (previous?.id === next.id && !('detailsUnavailable' in previous)) {
                if (historical || next.progressPercent < previous.progressPercent) return
            }
            // A fresh server can report completed recovery after this client missed its active stages.
            // A destructively quiesced server retains its blocker instead of publishing completion.
            const blockedByHistory = previous !== null && historicalBlocker.current === previous.id
            const completedRecovery =
                blockedByHistory &&
                !historical &&
                next.state === 'succeeded' &&
                next.phase === 'complete'
            // A fenced server retains its destructive blocker instead of exposing a later preflight failure.
            const preflightRecovery =
                !historical &&
                (next.state === 'failed' || next.state === 'interrupted') &&
                ['queued', 'preparing', 'validating'].includes(next.phase) &&
                ((blockedByHistory && previous?.id !== next.id) || refreshRequired.current)
            if (
                !historical &&
                !completedRecovery &&
                !preflightRecovery &&
                next.state !== 'queued' &&
                next.state !== 'running' &&
                current.current?.id !== next.id
            )
                return
            const pendingRetry = retryOwner.current
            if (pendingRetry) {
                if (
                    pendingRetry.owner === previous &&
                    JSON.stringify(pendingRetry.target) === JSON.stringify(next)
                )
                    return
                if (previous?.id !== next.id && pendingRetry.target.id !== next.id)
                    retireOperation(pendingRetry.target.id)
                cancelRefreshRetry(pendingRetry.target.id !== next.id)
            }
            // One timer owns one failed refresh. Identical server snapshots need not be republished.
            const scheduleRefreshRetry = (owner: RestoreMaintenanceDisplay | null) => {
                if (!mounted.current || current.current !== owner || retryTimer.current !== null)
                    return
                const waitMs = retryDelay.current
                retryDelay.current = Math.min(waitMs * 2, 30_000)
                retryOwner.current = { target: next, owner }
                retryTimer.current = setTimeout(() => {
                    const retry = retryOwner.current
                    retryTimer.current = null
                    retryOwner.current = null
                    if (
                        retry &&
                        mounted.current &&
                        current.current === retry.owner &&
                        !retiredOperations.current.has(retry.target.id)
                    )
                        runAction(() => handleRestoreSnapshot({ maintenance: retry.target }))
                }, waitMs)
            }
            if (
                previous?.id !== next.id &&
                completing.current !== null &&
                completing.current !== next.id
            )
                retireOperation(completing.current)
            if (preflightRecovery) {
                refreshRequired.current = true
                setRefreshPending(true)
                if (completing.current === next.id) return
                completing.current = next.id
                try {
                    await queryClient.cancelQueries()
                    queryClient.clear()
                    await refreshRestoredRoutes()
                    // Keep the blocker until refresh succeeds; a newer ordered snapshot owns the screen now.
                    if (
                        mounted.current &&
                        current.current === previous &&
                        completing.current === next.id
                    ) {
                        if (previous && previous.id !== next.id) retireOperation(previous.id)
                        cancelRefreshRetry()
                        historicalBlocker.current = null
                        refreshRequired.current = false
                        setRefreshPending(false)
                        current.current = next
                        setMaintenance(next)
                    }
                } catch {
                    if (completing.current === next.id) scheduleRefreshRetry(previous)
                } finally {
                    if (completing.current === next.id) completing.current = null
                }
                return
            }
            // Socket snapshots are ordered and fenced to the active connection.
            // Remember replaced IDs instead of comparing adjustable wall clocks.
            if (previous && previous.id !== next.id) retireOperation(previous.id)
            // Preserve the route and its component state below the screen. A disconnect
            // or empty snapshot cannot confirm that destructive replacement finished.
            if (!previous) beforeHide?.()
            if (historical) {
                historicalBlocker.current = next.id
                refreshRequired.current = true
                setRefreshPending(true)
            } else if (previous?.id !== next.id) historicalBlocker.current = null
            current.current = next
            setMaintenance(next)
            if (next.state !== 'succeeded' || completing.current === next.id) return
            refreshRequired.current = true
            setRefreshPending(true)
            completing.current = next.id
            try {
                await queryClient.cancelQueries()
                queryClient.clear()
                await Promise.all([
                    refreshRestoredRoutes(),
                    new Promise<void>((resolve) =>
                        setTimeout(resolve, RESTORE_COMPLETION_VISIBLE_MS),
                    ),
                ])
                if (mounted.current && current.current?.id === next.id) {
                    cancelRefreshRetry()
                    refreshRequired.current = false
                    setRefreshPending(false)
                    toast.success(t('admin.backups.messages.restored'), {
                        title: t('toast.titles.success'),
                        dedupeKey: `backup-restore:${next.id}`,
                    })
                    current.current = null
                    retireOperation(next.id)
                    setMaintenance(null)
                }
            } catch {
                if (completing.current === next.id && current.current?.id === next.id) {
                    scheduleRefreshRetry(current.current)
                    completing.current = null
                }
            }
        },
        [
            beforeHide,
            queryClient,
            refreshRestoredRoutes,
            t,
            retireOperation,
            cancelRefreshRetry,
            runAction,
        ],
    )
    const status = useLiveQuery<unknown>({
        topic: 'backup-restore',
        query: {},
        enabled: true,
        onData: handleSnapshot,
        onUnauthorized: () => {
            void router.invalidate({ sync: true }).catch(() => undefined)
        },
    })
    const canDismiss =
        maintenance !== null &&
        !refreshPending &&
        !('detailsUnavailable' in maintenance) &&
        (maintenance.state === 'failed' || maintenance.state === 'interrupted') &&
        ['queued', 'preparing', 'validating'].includes(maintenance.phase)
    /** Retire only a known non-destructive terminal failure so stale snapshots cannot reopen its screen. */
    function dismiss() {
        if (!canDismiss) return
        cancelRefreshRetry()
        historicalBlocker.current = null
        retireOperation(maintenance.id)
        current.current = null
        setMaintenance(null)
    }
    return { maintenance, status, active: maintenance !== null, canDismiss, dismiss }
}
