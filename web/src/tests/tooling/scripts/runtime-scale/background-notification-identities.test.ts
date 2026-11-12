import { expect, test } from 'bun:test'
import { backgroundDeferredNotifications } from '../../../../../../scripts/runtime-scale/background-notification-identities.ts'
import {
    createNotificationState,
    observeNotificationSignals,
} from '../../../../server/Notifications/incident-engine.ts'
import type { NotificationEngineConfiguration } from '../../../../server/Notifications/Types/notification-state.types.ts'

test('saturation exports the exact unallocated incident/channel obligations without inventing delivery IDs', () => {
    const configuration: NotificationEngineConfiguration = {
        enabled: true,
        pausedAfterRestore: false,
        channels: Array.from({ length: 8 }, () => ({
            id: Bun.randomUUIDv7(),
            revision: 1,
            name: 'fixture',
            kind: 'webhook',
            enabled: true,
            events: ['ddns'],
            minimumSeverity: 'error',
            thresholdSeconds: 0,
            recovery: true,
        })),
    }
    const state = observeNotificationSignals(
        createNotificationState(),
        configuration,
        Array.from({ length: 40 }, (_, index) => ({
            category: 'ddns',
            source: 'event',
            resourceId: 'fixture-' + index,
            health: 'failing',
            severity: 'error',
            occurredAt: new Date(1000).toISOString(),
            evidence: 'fixture',
            failures: 1,
        })),
        1000,
        () => Bun.randomUUIDv7(),
    )
    const deferred = backgroundDeferredNotifications(state, configuration, 1000)
    expect(state.deliveries).toHaveLength(256)
    expect(deferred).toHaveLength(64)
    expect(deferred).toHaveLength(state.deferredCount)
    expect(new Set(deferred.map((item) => JSON.stringify(item))).size).toBe(64)
    expect(
        deferred.every(
            (item) =>
                !state.deliveries.some(
                    (delivery) =>
                        delivery.payload.incidentId === item.incidentId &&
                        delivery.channelId === item.channelId &&
                        delivery.channelRevision === item.channelRevision &&
                        delivery.payload.type === item.type,
                ),
        ),
    ).toBe(true)
    expect(deferred.every((item) => !('deliveryId' in item))).toBe(true)
    expect(
        backgroundDeferredNotifications(
            state,
            { ...configuration, pausedAfterRestore: true },
            1000,
        ),
    ).toEqual([])
})
