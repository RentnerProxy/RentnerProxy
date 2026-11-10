import '@tanstack/react-start/server-only'

import {
    publishApplicationChange,
    subscribeToApplicationChanges,
} from '@/server/WebSockets/applicationChanges.ts'
import { collectNotificationObservations } from './collector.ts'
import { deliverAuthorizedNotificationClaim } from './notification-delivery.server.ts'
import {
    acknowledgeNotificationDelivery,
    claimNextNotificationDelivery,
    claimNotificationCollection,
    commitNotificationCollection,
    readNextNotificationDeliveryAt,
} from './notification-repository.ts'
import { createNotificationWorker } from './notification-worker.ts'
import { observeWorkerOperation } from '@/server/Foundation/worker-operation.ts'
import { workerObservability } from '@/server/Foundation/worker-observability.ts'
import { NOTIFICATION_LIMITS } from '@/config/notifications.config.ts'

export const notificationWorker = createNotificationWorker({
    claimCollection: () =>
        observeWorkerOperation(
            'notifications',
            claimNotificationCollection,
            undefined,
            () => 'deferred',
            'admission',
        ),
    collect: (cursor, signal) =>
        observeWorkerOperation(
            'notifications',
            () => collectNotificationObservations(cursor, signal),
            signal,
            undefined,
            'collection',
        ),
    commitCollection: (claim, batch) =>
        observeWorkerOperation(
            'notifications',
            () => commitNotificationCollection(claim, batch),
            undefined,
            (changed) => (changed ? 'success' : 'deferred'),
            'persistence',
        ),
    claimDelivery: () =>
        observeWorkerOperation(
            'notifications',
            claimNextNotificationDelivery,
            undefined,
            () => 'deferred',
            'admission',
        ),
    acknowledge: (token, result) =>
        observeWorkerOperation(
            'notifications',
            () => acknowledgeNotificationDelivery(token, result),
            undefined,
            undefined,
            'persistence',
        ),
    deliver: (claim, signal) =>
        observeWorkerOperation(
            'notifications',
            () => deliverAuthorizedNotificationClaim(claim, signal),
            signal,
            (result) =>
                result.success ? 'success' : result.code === 'cancelled' ? 'cancelled' : 'failure',
            'delivery',
        ),
    changed: publishApplicationChange,
    nextDeliveryAt: readNextNotificationDeliveryAt,
    subscribe: subscribeToApplicationChanges,
    unavailable: () => console.warn('[notifications] Delivery worker temporarily unavailable'),
})

workerObservability.sample('notifications', { concurrency: NOTIFICATION_LIMITS.concurrency })
