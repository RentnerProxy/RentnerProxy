import useAsyncAction from '@/shared/Feedback/Hooks/useAsyncAction.ts'
import type {
    LiveRevisionSnapshot,
    UseLiveInvalidationOptions,
} from '../Types/live-invalidation.types.ts'
import { useCallback, useEffect, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'

import useLiveQuery from './useLiveQuery.ts'

export default function useLiveInvalidation({
    topic,
    query,
    enabled,
    queryKeys,
}: UseLiveInvalidationOptions): ReturnType<typeof useLiveQuery> {
    const runAction = useAsyncAction()
    const queryClient = useQueryClient()
    const revisionRef = useRef<string | undefined>(undefined)
    const onData = useCallback(
        (snapshot: LiveRevisionSnapshot) => {
            if (!snapshot || typeof snapshot.revision !== 'string') return
            const previousRevision = revisionRef.current
            revisionRef.current = snapshot.revision
            if (previousRevision === snapshot.revision) return
            runAction(() =>
                Promise.all(
                    queryKeys.map((queryKey) => queryClient.invalidateQueries({ queryKey })),
                ),
            )
        },
        [queryClient, queryKeys, runAction],
    )
    const onResume = useCallback(() => {
        runAction(() =>
            Promise.all(queryKeys.map((queryKey) => queryClient.invalidateQueries({ queryKey }))),
        )
    }, [queryClient, queryKeys, runAction])

    const status = useLiveQuery<LiveRevisionSnapshot>({
        topic,
        query,
        enabled,
        onData,
        onResume,
    })
    useEffect(() => {
        // A reconnect must retry a failed HTTP read even when server state did not change.
        if (status === 'disconnected' || status === 'inactive') revisionRef.current = undefined
    }, [status])
    return status
}
