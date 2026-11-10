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

export const notificationWorker = createNotificationWorker({
    claimCollection: claimNotificationCollection,
    collect: collectNotificationObservations,
    commitCollection: commitNotificationCollection,
    claimDelivery: claimNextNotificationDelivery,
    acknowledge: acknowledgeNotificationDelivery,
    deliver: deliverAuthorizedNotificationClaim,
    changed: publishApplicationChange,
    nextDeliveryAt: readNextNotificationDeliveryAt,
    subscribe: subscribeToApplicationChanges,
    unavailable: () => console.warn('[notifications] Delivery worker temporarily unavailable'),
})
