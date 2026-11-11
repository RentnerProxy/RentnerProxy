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
    const queryClient = useQueryClient()
    const router = useRouter()
    const { t } = useTranslationStore()
    const [maintenance, setMaintenance] = useState<RestoreMaintenanceDisplay | null>(null)
    const current = useRef<RestoreMaintenanceDisplay | null>(null)
    const completing = useRef<string | null>(null)
    const refreshRequired = useRef(false)
    const [refreshPending, setRefreshPending] = useState(false)
    const retiredOperations = useRef(new Set<string>())
    const retireOperation = useCallback((id: string) => {
        const retired = retiredOperations.current
        retired.add(id)
        // Bound this to the same recent-operation window as maintenance receipts.
        if (retired.size > 100) retired.delete(retired.values().next().value!)
    }, [])
    const mounted = useRef(true)
    useEffect(() => {
        mounted.current = true
        return () => {
            mounted.current = false
        }
    }, [])
    const handleSnapshot = useCallback(
        async (value: unknown) => {
            const parsed = backupRestoreSnapshotSchema.safeParse(value)
            if (!parsed.success) return
            const next = parsed.data.maintenance
            if (!next || next.operation !== 'restore') return
            if (retiredOperations.current.has(next.id)) return
            const previous = current.current
            const historical = 'detailsUnavailable' in next
            if (completing.current !== null && historical && previous &&
                'detailsUnavailable' in previous && previous.id === next.id &&
                previous.state === next.state && previous.phase === next.phase) return
            if (previous?.id === next.id && !('detailsUnavailable' in previous)) {
                if (historical || next.progressPercent < previous.progressPercent) return
            }
            // A fresh server can report completed recovery after this client missed its active stages.
            // A destructively quiesced server retains its blocker instead of publishing completion.
            const completedRecovery =
                previous && 'detailsUnavailable' in previous &&
                !historical && next.state === 'succeeded' && next.phase === 'complete'
            // A fenced server retains its destructive blocker instead of exposing a later preflight failure.
            const preflightRecovery = !historical &&
                (next.state === 'failed' || next.state === 'interrupted') &&
                ['queued', 'preparing', 'validating'].includes(next.phase) &&
                ((previous && 'detailsUnavailable' in previous && previous.id !== next.id) || refreshRequired.current)
            if (
                !historical && !completedRecovery && !preflightRecovery &&
                next.state !== 'queued' &&
                next.state !== 'running' &&
                current.current?.id !== next.id
            )
                return
            if (previous?.id !== next.id && completing.current !== null && completing.current !== next.id)
                retireOperation(completing.current)
            if (preflightRecovery) {
                refreshRequired.current = true
                setRefreshPending(true)
                if (completing.current === next.id) return
                completing.current = next.id
                try {
                    await queryClient.cancelQueries()
                    queryClient.clear()
                    await router.invalidate({ sync: true })
                    // Keep the blocker until refresh succeeds; a newer ordered snapshot owns the screen now.
                    if (mounted.current && current.current === previous && completing.current === next.id) {
                        if (previous && previous.id !== next.id) retireOperation(previous.id)
                        refreshRequired.current = false
                        setRefreshPending(false)
                        current.current = next
                        setMaintenance(next)
                    }
                } catch {
                    // Retain the original blocker and let the same terminal snapshot retry the refresh.
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
            current.current = next
            setMaintenance(next)
            if (next.state !== 'succeeded' || completing.current === next.id) return
            completing.current = next.id
            try {
                await queryClient.cancelQueries()
                queryClient.clear()
                await Promise.all([
                    router.invalidate({ sync: true }),
                    new Promise<void>((resolve) =>
                        setTimeout(resolve, RESTORE_COMPLETION_VISIBLE_MS),
                    ),
                ])
                if (mounted.current && current.current?.id === next.id) {
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
                if (completing.current === next.id) completing.current = null
            }
        },
        [beforeHide, queryClient, router, t, retireOperation],
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
        retireOperation(maintenance.id)
        current.current = null
        setMaintenance(null)
    }
    return { maintenance, status, active: maintenance !== null, canDismiss, dismiss }
}
