import { pendingNotificationAdmission } from '../../web/src/server/Notifications/notification-admission.ts'
import type { NotificationState, NotificationEngineConfiguration } from '../../web/src/server/Notifications/Types/notification-state.types.ts'

/** Identify unallocated obligations using the production admission policy, never inventing delivery IDs before allocation. */
export function backgroundDeferredNotifications(
    state: NotificationState,
    configuration: NotificationEngineConfiguration,
    now: number,
) {
    if (!configuration.enabled || configuration.pausedAfterRestore) return []
    return state.incidents.flatMap((incident) => configuration.channels.flatMap((channel) => {
        const type = pendingNotificationAdmission(incident, channel, now)
        return type ? [{ incidentId: incident.id, channelId: channel.id, channelRevision: channel.revision, type }] : []
    }))
}
