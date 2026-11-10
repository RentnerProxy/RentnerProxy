// oxlint-disable no-await-in-loop -- One bounded long-poll consumer with cancellation and reconnect backoff.
import { DOCKER_DISCOVERY_WORKER as limits } from '@/config/docker-discovery.config.ts'
import type { DockerDiscoveryCursor } from '@/lib/DockerDiscovery/Types/docker-discovery.types.ts'
import type {
    DiscoveryObservation,
    DiscoveryObserverDependencies,
} from './Types/discovery-observer.types.ts'
import { createDiscoveryWorker } from './discovery-scheduler.ts'

export class DiscoverySuperseded extends Error {}

function equal(left: DockerDiscoveryCursor | null, right: DockerDiscoveryCursor | null) {
    return left?.runtimeId === right?.runtimeId && left?.generation === right?.generation
}

function pause(ms: number, signal: AbortSignal): Promise<void> {
    if (signal.aborted) return Promise.resolve()
    return new Promise((resolve) => {
        const done = () => {
            clearTimeout(timer)
            signal.removeEventListener('abort', done)
            resolve()
        }
        const timer = setTimeout(done, ms)
        timer.unref?.()
        signal.addEventListener('abort', done, { once: true })
        if (signal.aborted) done()
    })
}

/** Cursor acknowledgement is only a hint receipt: failed reconciliation retains its snapshot for replay. */
export function createDiscoveryObserver(dependencies: DiscoveryObserverDependencies) {
    let enabled = false
    let started = false
    let stopping = false
    let epoch = 0
    let cursor: DockerDiscoveryCursor | null = null
    let pending: DiscoveryObservation | null = null
    let active: DiscoveryObservation | null = null
    let watchController: AbortController | null = null
    let watching: Promise<void> | null = null
    let lastReconciled: DockerDiscoveryCursor | null = null
    let watchUnavailable = false
    const retired = new Map<string, number>()

    function accept(observation: DiscoveryObservation, requestEpoch: number): boolean {
        if (!observation.cursor) return cursor === null
        for (const [identity, until] of retired) if (until <= Date.now()) retired.delete(identity)
        if (retired.has(observation.cursor.runtimeId)) return false
        if (cursor?.runtimeId === observation.cursor.runtimeId) {
            if (observation.cursor.generation < cursor.generation) return false
        } else {
            if (requestEpoch !== epoch) return false
            if (retired.size >= limits.runtimeIdentities) return false
            if (cursor) retired.set(cursor.runtimeId, Date.now() + limits.cursorHorizonMs)
            epoch++
        }
        cursor = observation.cursor
        return true
    }

    function stopWatch() {
        watchController?.abort()
    }

    function startWatch() {
        if (!started || stopping || !enabled || !cursor || watching) return
        const controller = new AbortController()
        watchController = controller
        watching = (async () => {
            while (!controller.signal.aborted) {
                if (!enabled || !cursor) break
                const requested = cursor
                const requestEpoch = epoch
                const began = Date.now()
                try {
                    const reply = await dependencies.watch(requested, controller.signal)
                    if (controller.signal.aborted || stopping) break
                    if (reply.snapshot && !equal(cursor, reply.cursor)) {
                        const observation = { cursor: reply.cursor, snapshot: reply.snapshot }
                        if (accept(observation, requestEpoch)) {
                            pending = observation
                            scheduler.request()
                        }
                    }
                    watchUnavailable = false
                    await pause(
                        Math.max(0, limits.minimumWatchMs - (Date.now() - began)),
                        controller.signal,
                    )
                } catch {
                    if (controller.signal.aborted) break
                    if (!watchUnavailable)
                        console.warn(
                            '[docker-discovery] change subscription unavailable; reconnecting',
                        )
                    watchUnavailable = true
                    await pause(limits.reconnectMs, controller.signal)
                }
            }
        })().finally(() => {
            watching = null
            watchController = null
            startWatch()
        })
    }

    const scheduler = createDiscoveryWorker(
        async (signal, force) => {
            try {
                if (force) enabled = await dependencies.enabled()
                if (!enabled || signal.aborted) {
                    stopWatch()
                    pending = null
                    active = null
                    cursor = null
                    lastReconciled = null
                    epoch++
                    return
                }
                let observation = force ? null : (pending ?? active)
                pending = null
                if (!observation) {
                    const requestEpoch = epoch
                    observation = await dependencies.read(signal)
                    if (!accept(observation, requestEpoch)) throw new DiscoverySuperseded()
                }
                const expected = observation.cursor
                const assertCurrent = () => {
                    if (signal.aborted || !equal(cursor, expected)) throw new DiscoverySuperseded()
                }
                assertCurrent()
                active = observation
                const result = await dependencies.reconcile(
                    observation.snapshot,
                    signal,
                    assertCurrent,
                    force || !equal(lastReconciled, observation.cursor),
                )
                lastReconciled = observation.cursor
                enabled = result.enabled
                if (!enabled) stopWatch()
                else startWatch()
                if (result.again) scheduler.request(false, limits.continuationMs)
            } catch (error) {
                if (error instanceof DiscoverySuperseded || signal.aborted) {
                    if (!signal.aborted) scheduler.request(pending === null)
                    return
                }
                try {
                    await dependencies.failed()
                } finally {
                    // Collection/admission can fail before a subscription exists. Retry that
                    // resync too, rather than leaving startup recovery to the safety timer.
                    if (!signal.aborted) scheduler.request(true, limits.reconnectMs)
                }
                throw error
            }
        },
        limits.fallbackMs,
        dependencies.subscribe,
    )

    return {
        trigger: scheduler.trigger,
        start() {
            if (stopping) return Promise.resolve()
            started = true
            return scheduler.start()
        },
        async stop() {
            stopping = true
            started = false
            stopWatch()
            await scheduler.stop()
            await watching
        },
    }
}
