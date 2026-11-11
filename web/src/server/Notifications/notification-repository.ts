// oxlint-disable-next-line import/no-unassigned-import -- Queue claims and acknowledgements are privileged server operations.
import '@tanstack/react-start/server-only'

import { randomUUID } from 'node:crypto'
import type { NotificationDeliveryResult } from '@/lib/Notifications/Types/notifications.types.ts'
import { PERMISSIONS } from '@/config/permissions.config.ts'
import { DEFAULT_ACCENT_COLOR } from '@/config/appearance.config.ts'
import {
    canUsePrivateNotificationDestinations,
    getAllowedNotificationCategories,
} from '@/lib/Notifications/notification-access.ts'
import { resolveActiveUserAccessInTransaction } from '@/server/Auth/Access/rbac.service.ts'
import { getBackgroundDatabase } from '@/server/Foundation/background-database.server.ts'
import { resolveEffectiveNotificationConfigurationInTransaction } from './notification-access.ts'
import { claimNotificationDelivery, finishNotificationDelivery } from './incident-engine.ts'
import {
    commitNotificationCollectedBatch,
    leaseNotificationCollection,
} from './notification-collection.ts'
import {
    lockNotificationConfiguration,
    lockNotificationRuntime,
    readNotificationConfiguration,
    readNotificationRuntime,
    writeNotificationRuntime,
} from './notification-storage.ts'
import type {
    NotificationCollectionClaim,
    NotificationDeliveryClaim,
    NotificationDeliveryAuthorization,
} from './Types/notification-internal.types.ts'
import type { NotificationCollectedBatch } from './Types/notification-worker.types.ts'
import {
    authorizeHostAvailabilityDeliveryInTransaction,
    reconcileHostAvailabilityInTransaction,
} from './host-availability.repository.ts'
import { nextNotificationDeliveryAt } from './notification-scheduling.ts'
import { workerObservability } from '@/server/Foundation/worker-observability.ts'

/** Read scheduling and pending counts in one read-only notification-partition snapshot; disabled/restored queues retain unavailable evidence. */
export async function readNextNotificationDeliveryAt(): Promise<number | null> {
    const sample = await getBackgroundDatabase('notifications').transaction(
        async (transaction) => {
            const configuration = await readNotificationConfiguration(transaction)
            if (!configuration.enabled || configuration.pausedAfterRestore)
                return { nextDueAt: null, backlog: null, oldestPendingAt: null }
            const runtime = await readNotificationRuntime(transaction)
            const pending = runtime.engine.deliveries.filter(
                (delivery) => delivery.status === 'pending' || delivery.status === 'inflight',
            )
            return {
                nextDueAt: nextNotificationDeliveryAt(runtime.engine, configuration, Date.now()),
                backlog: pending.length,
                oldestPendingAt:
                    pending.length > 0
                        ? Math.min(...pending.map((delivery) => delivery.createdAt))
                        : null,
            }
        },
        { isolationLevel: 'repeatable read', accessMode: 'read only' },
    )
    workerObservability.sample('notifications', sample)
    return sample.nextDueAt
}

/** Use notification capacity for read-only disabled checks and configuration-before-runtime locking before durable collection admission. */
export async function claimNotificationCollection(): Promise<NotificationCollectionClaim | null> {
    // Keep the disabled default read-only, including before the first channel is configured.
    const snapshot = await getBackgroundDatabase('notifications').transaction(
        readNotificationConfiguration,
        {
            accessMode: 'read only',
        },
    )
    if (
        !snapshot.enabled ||
        snapshot.pausedAfterRestore ||
        !snapshot.channels.some((channel) => channel.enabled)
    )
        return null
    return getBackgroundDatabase('notifications').transaction(async (transaction) => {
        // Always lock configuration before runtime, matching administrative updates and delivery acknowledgements.
        const configuration = await resolveEffectiveNotificationConfigurationInTransaction(
            transaction,
            await lockNotificationConfiguration(transaction),
        )
        if (!configuration.enabled || configuration.pausedAfterRestore) return null
        const runtime = await lockNotificationRuntime(transaction)
        const leased = leaseNotificationCollection(runtime, configuration, Date.now(), randomUUID())
        if (!leased) return null
        await writeNotificationRuntime(transaction, leased.runtime)
        return leased.claim
    })
}

/** Consume the collection fence under notification SQL admission and atomically persist reconciled availability and notification effects. */
export async function commitNotificationCollection(
    claim: NotificationCollectionClaim,
    batch: NotificationCollectedBatch,
): Promise<boolean> {
    return getBackgroundDatabase('notifications').transaction(async (transaction) => {
        const configuration = await resolveEffectiveNotificationConfigurationInTransaction(
            transaction,
            await lockNotificationConfiguration(transaction),
        )
        const runtime = await lockNotificationRuntime(transaction)
        const engine = await reconcileHostAvailabilityInTransaction(
            transaction,
            runtime.engine,
            Date.now(),
        )
        const committed = commitNotificationCollectedBatch(
            engine === runtime.engine ? runtime : { ...runtime, engine },
            configuration,
            claim,
            batch,
            Date.now(),
            randomUUID,
        )
        if (committed.runtime !== runtime)
            await writeNotificationRuntime(transaction, committed.runtime)
        return committed.changed || engine !== runtime.engine
    })
}

/** Claim durable work under the notification partition and return only a channel matching its admitted revision; transport runs after SQL release. */
export async function claimNextNotificationDelivery(): Promise<NotificationDeliveryClaim | null> {
    const snapshot = await getBackgroundDatabase('notifications').transaction(
        readNotificationConfiguration,
        {
            accessMode: 'read only',
        },
    )
    if (!snapshot.enabled || snapshot.pausedAfterRestore) return null
    return getBackgroundDatabase('notifications').transaction(async (transaction) => {
        const configuration = await resolveEffectiveNotificationConfigurationInTransaction(
            transaction,
            await lockNotificationConfiguration(transaction),
        )
        const runtime = await lockNotificationRuntime(transaction)
        const engine = await reconcileHostAvailabilityInTransaction(
            transaction,
            runtime.engine,
            Date.now(),
        )
        const result = claimNotificationDelivery(engine, configuration, Date.now(), randomUUID)
        if (JSON.stringify(runtime.engine) !== JSON.stringify(result.state)) {
            runtime.engine = result.state
            await writeNotificationRuntime(transaction, runtime)
        }
        if (!result.delivery) return null
        const channel = configuration.channels.find(
            (item) =>
                item.id === result.delivery!.channelId &&
                item.revision === result.delivery!.channelRevision,
        )
        if (!channel) return null
        return { ...result.delivery, channel }
    })
}

/** Apply a delivery result under its lease token and current configuration within notification SQL capacity, writing only changed durable state. */
export async function acknowledgeNotificationDelivery(
    leaseToken: string,
    result: NotificationDeliveryResult,
): Promise<void> {
    await getBackgroundDatabase('notifications').transaction(async (transaction) => {
        const configuration = await resolveEffectiveNotificationConfigurationInTransaction(
            transaction,
            await lockNotificationConfiguration(transaction),
        )
        const runtime = await lockNotificationRuntime(transaction)
        const state = finishNotificationDelivery(
            runtime.engine,
            configuration,
            leaseToken,
            result,
            Date.now(),
        )
        if (JSON.stringify(runtime.engine) !== JSON.stringify(state)) {
            runtime.engine = state
            await writeNotificationRuntime(transaction, runtime)
        }
    })
}

/** Recheck live lease, channel revision, resource access, and owner permissions in a read-only notification transaction before transport. */
export async function authorizeNotificationDelivery(
    claim: NotificationDeliveryClaim,
): Promise<NotificationDeliveryAuthorization | null> {
    return getBackgroundDatabase('notifications').transaction(
        async (transaction) => {
            const configuration = await readNotificationConfiguration(transaction)
            if (!configuration.enabled || configuration.pausedAfterRestore) return null
            const runtime = await readNotificationRuntime(transaction)
            if (
                !runtime.engine.deliveries.some(
                    (delivery) =>
                        delivery.id === claim.id &&
                        delivery.status === 'inflight' &&
                        delivery.leaseToken === claim.leaseToken &&
                        (delivery.leaseExpiresAt ?? 0) > Date.now(),
                )
            )
                return null
            const channel = configuration.channels.find(
                (candidate) =>
                    candidate.id === claim.channelId &&
                    candidate.revision === claim.channelRevision &&
                    candidate.ownerUserId === claim.channel.ownerUserId &&
                    candidate.enabled &&
                    candidate.events.includes(claim.payload.category),
            )
            if (!channel) return null
            if (!(await authorizeHostAvailabilityDeliveryInTransaction(transaction, claim)))
                return null
            if (!channel.ownerUserId) return { channel, allowPrivateDestinations: true }
            const owner = await resolveActiveUserAccessInTransaction(
                transaction,
                channel.ownerUserId,
            )
            if (
                !owner?.permissions.includes(PERMISSIONS.APP_ACCESS) ||
                !getAllowedNotificationCategories(owner.permissions).includes(
                    claim.payload.category,
                )
            )
                return null
            return {
                channel,
                allowPrivateDestinations: canUsePrivateNotificationDestinations(owner.permissions),
                mailAppearance: {
                    themeMode: owner.themeMode,
                    accentColor: owner.accentColor ?? DEFAULT_ACCENT_COLOR,
                },
            }
        },
        { accessMode: 'read only' },
    )
}
