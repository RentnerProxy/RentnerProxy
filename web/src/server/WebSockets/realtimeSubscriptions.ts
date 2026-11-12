import type {
    LiveConnectionData,
    LiveTopicSubscription,
    SubscriptionManagerOptions,
    LiveSubscriptionManager,
} from './Types/realtime-subscriptions.types.ts'

import { LIVE_INTERVALS, LIVE_MAX_SUBSCRIPTIONS } from '@/config/realtime.config.ts'
import { BACKGROUND_RESOURCE_ENVELOPE } from '@/config/background-resources.config.ts'
import {
    createResourceAdmission,
    ResourceAdmissionError,
    releaseAfterSettled,
} from '@/server/Foundation/resource-admission.ts'
import type { LiveTopic } from '@/lib/Live/Types/realtime.types.ts'
import { comparisonKey, snapshotMessage } from '@/lib/Live/snapshot.ts'
import { runDetachedTask } from '@/lib/Async/runDetachedTask.ts'

function unsubscribe(connection: LiveConnectionData, topic: LiveTopic): void {
    const subscription = connection.subscriptions.get(topic)
    if (!subscription) return
    subscription.active = false
    subscription.pending = false
    subscription.abortController?.abort()
    subscription.abortController = null
    if (subscription.abortTimer !== null) clearTimeout(subscription.abortTimer)
    subscription.abortTimer = null
    if (subscription.timer !== null) clearInterval(subscription.timer)
    subscription.timer = null
    connection.subscriptions.delete(topic)
}

/** Own globally bounded realtime permits across handshakes and snapshots, retaining capacity until tracked underlying reads actually settle. */
export function createSubscriptionManager(options: SubscriptionManagerOptions) {
    const admission = createResourceAdmission(BACKGROUND_RESOURCE_ENVELOPE.realtime)
    const outstanding = new Set<Promise<void>>()
    let pressureLoggedAt = -Infinity
    /** Track each admitted permit through idempotent release and emit rate-limited numeric pressure diagnostics without swallowing admission errors. */
    async function acquireResource(signal?: AbortSignal) {
        try {
            const release = await admission.acquire(signal)
            const completion = Promise.withResolvers<void>()
            outstanding.add(completion.promise)
            let released = false
            return () => {
                if (released) return
                released = true
                release()
                outstanding.delete(completion.promise)
                completion.resolve()
            }
        } catch (error) {
            if (
                error instanceof ResourceAdmissionError &&
                error.reason !== 'cancelled' &&
                Date.now() - pressureLoggedAt >=
                    BACKGROUND_RESOURCE_ENVELOPE.realtime.pressureLogIntervalMs
            ) {
                pressureLoggedAt = Date.now()
                console.warn('[realtime] Snapshot admission deferred', admission.snapshot())
            }
            throw error
        }
    }
    /** Coalesce per-subscription reads and defer overloaded admission to the periodic sampler; hold capacity past response timeout for tracked work. */
    async function sample(subscription: LiveTopicSubscription): Promise<void> {
        const { connection } = subscription
        if (!subscription.active || connection.closed) return
        if (subscription.inflight) {
            subscription.pending = true
            return
        }
        subscription.inflight = true
        subscription.pending = false
        const controller = new AbortController()
        subscription.abortController = controller
        let release: (() => void) | undefined
        let deferred = false
        const actualWork = new Set<Promise<unknown>>()
        try {
            release = await acquireResource(controller.signal)
            if (!subscription.active || connection.closed) return
            subscription.abortTimer = setTimeout(
                () => controller.abort(),
                options.snapshotTimeoutMs,
            )
            const result = await options.readSnapshot(subscription, controller.signal, (work) =>
                actualWork.add(work),
            )
            if (!subscription.active || connection.closed) return
            if (result.kind === 'failure') {
                if (result.status === 401 || result.status === 403) {
                    options.closeUnauthorized(connection)
                } else {
                    options.closeUnavailable(connection, result.status ?? 503)
                }
                return
            }
            if (
                result.data &&
                typeof result.data === 'object' &&
                'maintenanceSuspended' in result.data &&
                result.data.maintenanceSuspended === true
            )
                return

            let comparison: string
            let message: string
            try {
                comparison = comparisonKey(result.data)
                message = snapshotMessage(
                    subscription.topic,
                    subscription.query,
                    result.data,
                    options.maxPayloadBytes,
                )
            } catch {
                options.closeUnavailable(connection, 413)
                return
            }
            if (comparison === subscription.lastComparison) return
            const ws = connection.ws
            if (!ws || ws.sendText(message) <= 0) {
                options.closeBackpressured(connection)
                return
            }
            subscription.lastComparison = comparison
        } catch (error) {
            if (!(error instanceof ResourceAdmissionError)) throw error
            // The existing periodic sampler retries; no additional callback/retry timer is queued.
            deferred = true
        } finally {
            releaseAfterSettled(release, actualWork)
            subscription.inflight = false
            if (subscription.abortTimer !== null) clearTimeout(subscription.abortTimer)
            subscription.abortTimer = null
            subscription.abortController = null
            if (!deferred && subscription.pending && subscription.active && !connection.closed) {
                subscription.pending = false
                queueMicrotask(() => requestSample(subscription))
            }
        }
    }

    function requestSample(subscription: LiveTopicSubscription): void {
        runDetachedTask(
            () => sample(subscription),
            () => options.closeUnavailable(subscription.connection, 503),
        )
    }

    function subscribe(
        connection: LiveConnectionData,
        topic: LiveTopic,
        query: unknown,
        queryText: string,
    ): boolean {
        const existing = connection.subscriptions.get(topic)
        if (!existing && connection.subscriptions.size >= LIVE_MAX_SUBSCRIPTIONS) return false
        if (existing) unsubscribe(connection, topic)
        const subscription: LiveTopicSubscription = {
            connection,
            topic,
            query,
            queryText,
            intervalMs: LIVE_INTERVALS[topic],
            timer: null,
            abortController: null,
            abortTimer: null,
            inflight: false,
            pending: false,
            lastComparison: null,
            active: true,
        }
        connection.subscriptions.set(topic, subscription)
        subscription.timer = setInterval(() => requestSample(subscription), subscription.intervalMs)
        requestSample(subscription)
        return true
    }

    function dispose(connection: LiveConnectionData): void {
        for (const topic of connection.subscriptions.keys()) unsubscribe(connection, topic)
        connection.handshakeAbortController?.abort()
        connection.handshakeAbortController = null
    }

    function sampleAll(): void {
        for (const connection of connections) {
            for (const subscription of connection.subscriptions.values())
                requestSample(subscription)
        }
    }

    const connections = new Set<LiveConnectionData>()

    const manager: LiveSubscriptionManager = {
        /** Wait until every admitted underlying read releases its permit; callers must stop admission before claiming a completed drain. */
        async drainResources() {
            while (outstanding.size > 0) {
                // oxlint-disable-next-line no-await-in-loop -- All admitted reads must settle before claiming a completed drain.
                await Promise.allSettled(outstanding)
            }
        },
        connections,
        sample,
        sampleAll,
        subscribe,
        unsubscribe,
        dispose,
        resourceUsage: admission.snapshot,
        acquireResource,
    }
    return manager
}
