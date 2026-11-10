import { NOTIFICATION_LIMITS as limits } from '@/config/notifications.config.ts'
import type {
    NotificationDispatch,
    NotificationEngineConfiguration,
    NotificationQueuedDelivery,
    NotificationState,
} from './Types/notification-state.types.ts'

function dispatchFor(
    delivery: NotificationQueuedDelivery,
    configuration: NotificationEngineConfiguration,
    now: number,
): NotificationDispatch | null {
    const channel = configuration.channels.find(
        (item) => item.id === delivery.channelId && item.revision === delivery.channelRevision,
    )
    if (!channel) return null
    return {
        channelId: channel.id,
        ownerUserId: channel.ownerUserId ?? null,
        kind: channel.kind,
        category: delivery.payload.category,
        at: now,
    }
}

function capacity(
    candidate: NotificationDispatch,
    dispatches: readonly NotificationDispatch[],
    concurrency: boolean,
) {
    return (
        dispatches.length < (concurrency ? limits.concurrency : limits.globalRate) &&
        dispatches.filter((item) => item.channelId === candidate.channelId).length <
            (concurrency ? limits.channelConcurrency : limits.channelRate) &&
        dispatches.filter((item) => sameOwner(item, candidate)).length <
            (concurrency ? limits.ownerConcurrency : limits.ownerRate) &&
        dispatches.filter((item) => item.kind === candidate.kind).length <
            (concurrency ? limits.transportConcurrency : limits.transportRate) &&
        dispatches.filter((item) => item.category === candidate.category).length <
            (concurrency ? limits.categoryConcurrency : limits.categoryRate)
    )
}

function sameOwner(left: NotificationDispatch, right: NotificationDispatch) {
    return left.ownerUserId === null && right.ownerUserId === null
        ? left.channelId === right.channelId
        : left.ownerUserId === right.ownerUserId
}

function activeDispatches(
    state: NotificationState,
    configuration: NotificationEngineConfiguration,
    now: number,
) {
    return state.deliveries.flatMap((delivery) => {
        if (delivery.status !== 'inflight' || (delivery.leaseExpiresAt ?? 0) <= now) return []
        // Keep a revoked/revised channel's admitted work in the global budget until its lease ends.
        return [
            delivery.dispatch ??
                dispatchFor(delivery, configuration, now) ?? {
                    channelId: delivery.channelId,
                    ownerUserId: null,
                    kind: 'webhook' as const,
                    category: delivery.payload.category,
                    at: now,
                },
        ]
    })
}

export function selectNotificationDelivery(
    state: NotificationState,
    configuration: NotificationEngineConfiguration,
    now: number,
    deliveryId?: string,
): NotificationQueuedDelivery | null {
    const recent = (state.dispatches ?? []).filter((item) => item.at > now - limits.rateWindowMs)
    const active = activeDispatches(state, configuration, now)
    const candidates = state.deliveries.flatMap((delivery) => {
        if (
            delivery.status !== 'pending' ||
            delivery.nextAttemptAt > now ||
            (deliveryId && delivery.id !== deliveryId)
        )
            return []
        const dispatch = dispatchFor(delivery, configuration, now)
        if (!dispatch || !capacity(dispatch, active, true) || !capacity(dispatch, recent, false))
            return []
        return [{ delivery, dispatch }]
    })
    // Least recently served owner, then channel, then category. Receipt order breaks same-ms ties.
    const rank = (dispatch: NotificationDispatch) => [
        recent.findLastIndex((item) => sameOwner(item, dispatch)),
        recent.findLastIndex((item) => item.channelId === dispatch.channelId),
        recent.findLastIndex((item) => item.category === dispatch.category),
    ]
    candidates.sort((left, right) => {
        const a = rank(left.dispatch)
        const b = rank(right.dispatch)
        for (let index = 0; index < a.length; index++) {
            const difference = a[index]! - b[index]!
            if (difference) return difference
        }
        return left.delivery.createdAt - right.delivery.createdAt
    })
    const selected = candidates[0]
    if (!selected) return null
    // These bounded receipts survive terminal-history eviction and multi-process claims.
    state.dispatches = [...recent, selected.dispatch]
    selected.delivery.dispatch = selected.dispatch
    return selected.delivery
}

export function nextNotificationDeliveryAt(
    state: NotificationState,
    configuration: NotificationEngineConfiguration,
    now: number,
): number | null {
    if (!configuration.enabled || configuration.pausedAfterRestore) return null
    const pending = state.deliveries.filter((item) => item.status === 'pending')
    const expiries = state.deliveries.flatMap((item) =>
        item.status === 'inflight' && item.leaseExpiresAt !== null ? [item.leaseExpiresAt] : [],
    )
    const active = activeDispatches(state, configuration, now)
    const recent = (state.dispatches ?? []).filter((item) => item.at > now - limits.rateWindowMs)
    const times = [...expiries]
    for (const delivery of pending) {
        const dispatch = dispatchFor(delivery, configuration, now)
        if (!dispatch) {
            times.push(now)
            continue
        }
        if (!capacity(dispatch, active, true)) continue
        let due = Math.max(now, delivery.nextAttemptAt)
        for (const receipt of recent) {
            if (
                capacity(
                    dispatch,
                    recent.filter((item) => item.at + limits.rateWindowMs > due),
                    false,
                )
            )
                break
            due = Math.max(due, receipt.at + limits.rateWindowMs)
        }
        times.push(due)
    }
    return times.length ? Math.max(now, Math.min(...times)) : null
}
