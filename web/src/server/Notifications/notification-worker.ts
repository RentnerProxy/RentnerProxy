// oxlint-disable no-await-in-loop -- Durable admission is serialized; transports run outside the claim transaction.
import { NOTIFICATION_LIMITS as limits } from '@/config/notifications.config.ts'
import type { NotificationDeliveryResult } from '@/lib/Notifications/Types/notifications.types.ts'
import type { NotificationDeliveryClaim } from './Types/notification-internal.types.ts'
import type {
    NotificationCollectedBatch,
    NotificationWorkerDependencies,
} from './Types/notification-worker.types.ts'

export function createNotificationWorker(dependencies: NotificationWorkerDependencies) {
    let collectionTimer: ReturnType<typeof setTimeout> | null = null
    let deliveryTimer: ReturnType<typeof setTimeout> | null = null
    let deliveryScheduledAt = 0
    let collecting: Promise<void> | null = null
    let dispatching: Promise<void> | null = null
    let running: Promise<void> | null = null
    let controller = new AbortController()
    const active = new Set<Promise<void>>()
    let unsubscribe: (() => void) | undefined
    let started = false
    let stopped = false
    const unavailable = new Set<'collection' | 'delivery'>()

    function failed(lane: 'collection' | 'delivery') {
        if (!controller.signal.aborted && !unavailable.has(lane)) dependencies.unavailable()
        unavailable.add(lane)
    }

    function scheduleDelivery(delayMs: number) {
        if (!started || stopped) return
        const delay = Math.max(limits.wakeCoalesceMs, Math.min(limits.pollIntervalMs, delayMs))
        const at = Date.now() + delay
        if (deliveryTimer && deliveryScheduledAt <= at) return
        if (deliveryTimer) clearTimeout(deliveryTimer)
        deliveryScheduledAt = at
        deliveryTimer = setTimeout(() => {
            deliveryTimer = null
            void dispatch()
        }, delay)
        deliveryTimer.unref()
    }

    async function collect() {
        const claim = await dependencies.claimCollection()
        if (!claim) return
        const signal = AbortSignal.any([
            controller.signal,
            AbortSignal.timeout(limits.collectionTimeoutMs),
        ])
        let batch: NotificationCollectedBatch = {
            observations: [],
            certificateCursor: claim.certificateCursor,
        }
        try {
            if (!signal.aborted) batch = await dependencies.collect(claim.certificateCursor, signal)
        } finally {
            const committed = signal.aborted
                ? { observations: [], certificateCursor: claim.certificateCursor }
                : batch
            if (await dependencies.commitCollection(claim, committed)) {
                dependencies.changed()
                scheduleDelivery(0)
            }
        }
    }

    function collectOnce(): Promise<void> {
        if (stopped) return Promise.resolve()
        if (collecting) return collecting
        if (collectionTimer) clearTimeout(collectionTimer)
        collectionTimer = null
        collecting = collect()
            .then(
                () => {
                    unavailable.delete('collection')
                },
                () => {
                    failed('collection')
                },
            )
            .finally(() => {
                collecting = null
                if (!started || stopped) return
                collectionTimer = setTimeout(() => {
                    collectionTimer = null
                    void collectOnce()
                }, limits.pollIntervalMs)
                collectionTimer.unref()
            })
        return collecting
    }

    async function send(claim: NotificationDeliveryClaim) {
        let result: NotificationDeliveryResult
        try {
            result = controller.signal.aborted
                ? { success: false, code: 'cancelled', retryable: true }
                : await dependencies.deliver(claim, controller.signal)
        } catch {
            result = { success: false, code: 'destination_unavailable', retryable: true }
        }
        // Accepted sends and permanent failures remain truthful when cancellation races their result.
        if (!result.success && result.code === 'cancelled' && controller.signal.aborted)
            result = { success: false, code: 'cancelled', retryable: true }
        await dependencies.acknowledge(claim.leaseToken, result)
        dependencies.changed()
    }

    function dispatch(): Promise<void> {
        if (stopped) return Promise.resolve()
        if (dispatching) return dispatching
        dispatching = (async () => {
            try {
                for (let count = 0; count < limits.deliveriesPerTick; count++) {
                    if (controller.signal.aborted || active.size >= limits.concurrency) break
                    const claim = await dependencies.claimDelivery()
                    if (!claim) break
                    let task: Promise<void>
                    task = send(claim)
                        .catch(() => {
                            failed('delivery')
                        })
                        .finally(() => {
                            active.delete(task)
                            scheduleDelivery(0)
                        })
                    active.add(task)
                }
                const due =
                    active.size >= limits.concurrency ? null : await dependencies.nextDeliveryAt?.()
                scheduleDelivery(due == null ? limits.pollIntervalMs : due - Date.now())
                unavailable.delete('delivery')
            } catch {
                failed('delivery')
                scheduleDelivery(limits.pollIntervalMs)
            }
        })().finally(() => {
            dispatching = null
        })
        return dispatching
    }

    function runOnce(): Promise<void> {
        if (stopped) return Promise.resolve()
        if (running) return running
        running = (async () => {
            await Promise.all([
                collectOnce(),
                (async () => {
                    await dispatch()
                    await Promise.all(active)
                })(),
            ])
        })().finally(() => {
            running = null
        })
        return running
    }

    return {
        runOnce,
        wake() {
            scheduleDelivery(0)
        },
        start() {
            if (started) return
            started = true
            stopped = false
            controller = new AbortController()
            unsubscribe = dependencies.subscribe?.(() => scheduleDelivery(0))
            void collectOnce()
            void dispatch()
        },
        async stop() {
            stopped = true
            started = false
            unsubscribe?.()
            unsubscribe = undefined
            if (collectionTimer) clearTimeout(collectionTimer)
            if (deliveryTimer) clearTimeout(deliveryTimer)
            collectionTimer = null
            deliveryTimer = null
            controller.abort()
            // Drain late claims, cancelled transports and their durable acknowledgements before restore.
            await Promise.all([collecting, dispatching, running])
            await Promise.all(active)
        },
    }
}
