import { runDetachedTask } from '@/lib/Async/runDetachedTask.ts'
import { drainWorker } from '@/server/Foundation/worker-lifecycle.server.ts'
import { DOCKER_DISCOVERY_WORKER as limits } from '@/config/docker-discovery.config.ts'

/** One flight plus one coalesced dirty bit; an in-flight invalidation always gets a follow-up. */
export function createDiscoveryWorker(
    step: (signal: AbortSignal, force: boolean) => Promise<void>,
    intervalMs: number = limits.fallbackMs,
    subscribe?: (invalidate: () => void) => () => void,
) {
    let timer: ReturnType<typeof setInterval> | null = null
    let queued: ReturnType<typeof setTimeout> | null = null
    let flight: Promise<void> | null = null
    let stopped = false
    let dirty = false
    let forced = false
    let requestedDelay: number | null = null
    let queuedAt = 0
    let unavailable = false
    let unsubscribe: (() => void) | undefined
    const controller = new AbortController()

    function request(force = false, delay: number = limits.coalesceMs) {
        if (stopped) return
        dirty = true
        forced ||= force
        requestedDelay = Math.min(requestedDelay ?? delay, delay)
        if (flight) return
        const at = Date.now() + delay
        if (queued && queuedAt <= at) return
        if (queued) clearTimeout(queued)
        queuedAt = at
        queued = setTimeout(() => {
            queued = null
            runDetachedTask(
                () => trigger(false),
                () => console.error('[docker-discovery] Background operation failed'),
            )
        }, delay)
        queued.unref?.()
    }

    function trigger(force = true): Promise<void> {
        if (stopped) return Promise.resolve()
        dirty = true
        forced ||= force
        if (flight) return flight
        if (queued) clearTimeout(queued)
        queued = null
        const nextForce = forced
        dirty = false
        forced = false
        requestedDelay = null
        flight = step(controller.signal, nextForce)
            .then(() => {
                unavailable = false
            })
            .catch(() => {
                if (!unavailable && !controller.signal.aborted)
                    console.warn('[docker-discovery] reconciliation unavailable; retrying')
                unavailable = true
            })
            .finally(() => {
                flight = null
                if (dirty) request(forced, requestedDelay ?? limits.coalesceMs)
            })
        return flight
    }
    return {
        trigger,
        request,
        start(): Promise<void> {
            if (timer || stopped) return flight ?? Promise.resolve()
            unsubscribe = subscribe?.(() => request(true))
            timer = setInterval(() => {
                runDetachedTask(
                    () => trigger(),
                    () => console.error('[docker-discovery] Background operation failed'),
                )
            }, intervalMs)
            timer.unref?.()
            return trigger()
        },
        /** Cancel subscriptions, queued triggers, and controller I/O before waiting for the actual reconciliation flight to drain. */
        async stop() {
            stopped = true
            dirty = false
            forced = false
            unsubscribe?.()
            if (timer) clearInterval(timer)
            if (queued) clearTimeout(queued)
            timer = null
            queued = null
            controller.abort()
            await drainWorker(Promise.resolve(flight))
        },
    }
}
