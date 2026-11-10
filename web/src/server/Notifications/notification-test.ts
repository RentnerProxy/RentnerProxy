import { NOTIFICATION_LIMITS } from '@/config/notifications.config.ts'
import { reserveNotificationDeliverySlot } from './incident-engine.ts'
import { NotificationDomainError } from './notification.errors.ts'
import { selectNotificationDelivery } from './notification-scheduling.ts'
import type {
    NotificationClaimedDelivery,
    NotificationEngineConfiguration,
    NotificationState,
} from './Types/notification-state.types.ts'

export function claimNotificationTestDelivery(
    state: NotificationState,
    configuration: NotificationEngineConfiguration,
    channelId: string,
    nowMs: number,
    makeId: () => string,
): { state: NotificationState; delivery: NotificationClaimedDelivery } {
    const channel = configuration.channels.find((item) => item.id === channelId)
    const category = channel?.events[0]
    if (
        !configuration.enabled ||
        configuration.pausedAfterRestore ||
        !channel?.enabled ||
        !category
    )
        throw new NotificationDomainError('configuration_unavailable')
    const next = structuredClone(state)
    if (!reserveNotificationDeliverySlot(next))
        throw new NotificationDomainError('configuration_unavailable')
    const id = makeId()
    const leaseToken = makeId()
    const timestamp = new Date(nowMs).toISOString()
    const payload = {
        schemaVersion: 1 as const,
        id,
        incidentId: makeId(),
        type: 'test' as const,
        category,
        severity: 'info' as const,
        resourceId: 'test',
        code: 'notification_test' as const,
        occurredAt: timestamp,
        startedAt: timestamp,
        failures: 0,
    }
    next.deliveries.push({
        id,
        channelId,
        channelRevision: channel.revision,
        payload,
        status: 'pending',
        attempts: 0,
        createdAt: nowMs,
        updatedAt: nowMs,
        nextAttemptAt: nowMs,
        leaseToken: null,
        leaseExpiresAt: null,
        error: null,
    })
    const admitted = selectNotificationDelivery(next, configuration, nowMs, id)
    if (!admitted) throw new NotificationDomainError('configuration_unavailable')
    admitted.status = 'inflight'
    admitted.attempts = 1
    admitted.leaseToken = leaseToken
    admitted.leaseExpiresAt = nowMs + NOTIFICATION_LIMITS.leaseMs
    if (new TextEncoder().encode(JSON.stringify(next)).byteLength > NOTIFICATION_LIMITS.stateBytes)
        throw new NotificationDomainError('configuration_unavailable')
    return {
        state: next,
        delivery: { id, channelId, channelRevision: channel.revision, payload, leaseToken },
    }
}
