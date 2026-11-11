import { subscribeToApplicationChanges } from './applicationChanges.ts'
import {
    LIVE_IDLE_TIMEOUT_SECONDS,
    LIVE_MAX_CONNECTIONS,
    LIVE_MAX_PAYLOAD_BYTES,
    LIVE_MAX_QUERY_BYTES,
    LIVE_SNAPSHOT_PATH,
    LIVE_SNAPSHOT_TIMEOUT_MS,
    REALTIME_WS_PATH,
} from '@/config/realtime.config.ts'
import {
    isWebSocketUpgrade,
    normalizeOrigin,
    snapshotUrl,
    statusResponse,
} from '@/lib/Live/snapshot.ts'
import { readLiveHandshake, readSubscriptionSnapshot } from './liveReader.ts'
import type {
    LiveSocketData,
    LiveUpgradeResult,
    LiveWebSocketRuntime,
    LiveWebSocketRuntimeOptions,
} from './Types/bun.types.ts'
import { createSubscriptionManager } from './realtimeSubscriptions.ts'
import type { LiveConnectionData } from './Types/realtime-subscriptions.types.ts'
import { createRealtimeLifecycle } from './realtimeLifecycle.ts'
import { createRealtimeHandler } from './realtimeHandler.ts'
import {
    ResourceAdmissionError,
    releaseAfterSettled,
} from '@/server/Foundation/resource-admission.ts'
import { drainWorker } from '@/server/Foundation/worker-drain.ts'

export function createLiveWebSocketRuntime(
    options: LiveWebSocketRuntimeOptions,
): LiveWebSocketRuntime {
    const snapshotPath = options.snapshotPath ?? LIVE_SNAPSHOT_PATH
    const maxConnections = options.maxConnections ?? LIVE_MAX_CONNECTIONS
    const maxPayloadBytes = options.maxPayloadBytes ?? LIVE_MAX_PAYLOAD_BYTES
    const maxQueryBytes = options.maxQueryBytes ?? LIVE_MAX_QUERY_BYTES
    const idleTimeoutSeconds = options.idleTimeoutSeconds ?? LIVE_IDLE_TIMEOUT_SECONDS
    const snapshotTimeoutMs = options.snapshotTimeoutMs ?? LIVE_SNAPSHOT_TIMEOUT_MS
    const pending = new Set<LiveConnectionData>()
    let pendingReservations = 0
    let shuttingDown = false
    let shutdownResult: Promise<void> | null = null

    async function expectedOrigin(request: Request): Promise<string | null> {
        const value =
            typeof options.allowedOrigin === 'function'
                ? await options.allowedOrigin(request)
                : options.allowedOrigin
        return normalizeOrigin(value)
    }

    let lifecycle: ReturnType<typeof createRealtimeLifecycle>
    const manager = createSubscriptionManager({
        maxPayloadBytes,
        snapshotTimeoutMs,
        readSnapshot: (subscription, signal, trackWork) =>
            readSubscriptionSnapshot(
                subscription,
                signal,
                options.readSnapshot,
                (current) =>
                    snapshotUrl(
                        current.connection.requestUrl,
                        snapshotPath,
                        current.topic,
                        current.queryText,
                    ),
                maxPayloadBytes,
                trackWork,
            ),
        closeUnauthorized: (connection) => lifecycle.unauthorized(connection),
        closeUnavailable: (connection, status) => {
            lifecycle.close(
                connection,
                status === 413 ? 1009 : status === 429 || status === 503 ? 1013 : 1011,
                'Live snapshot unavailable',
            )
        },
        closeBackpressured: (connection) =>
            lifecycle.close(connection, 1013, 'Live client is backpressured'),
    })
    lifecycle = createRealtimeLifecycle({ manager, pending })

    const websocket = createRealtimeHandler({
        manager,
        lifecycle,
        maxQueryBytes,
        maxPayloadBytes,
        idleTimeoutSeconds,
    })

    async function handle(
        request: Request,
        server: Bun.Server<LiveSocketData>,
    ): Promise<LiveUpgradeResult> {
        const url = new URL(request.url)
        if (url.pathname !== REALTIME_WS_PATH || !isWebSocketUpgrade(request)) {
            return { handled: false }
        }
        if (request.method.toUpperCase() !== 'GET')
            return { handled: true, response: statusResponse(405) }
        if (shuttingDown) return { handled: true, response: statusResponse(503) }
        if (manager.connections.size + pending.size + pendingReservations >= maxConnections) {
            return { handled: true, response: statusResponse(429) }
        }

        pendingReservations += 1
        let pendingReservation = true
        let reservation: LiveConnectionData | null = null
        let upgraded = false
        try {
            const origin = normalizeOrigin(request.headers.get('origin'))
            const configuredOrigin = await expectedOrigin(request)
            if (shuttingDown) return { handled: true, response: statusResponse(503) }
            if (!origin || !configuredOrigin || origin !== configuredOrigin) {
                return { handled: true, response: statusResponse(403) }
            }
            reservation = {
                requestUrl: request.url,
                cookie: request.headers.get('cookie'),
                origin,
                ws: null,
                closed: false,
                handshakeAbortController: null,
                subscriptions: new Map(),
                messageWindowStarted: Date.now(),
                messageCount: 0,
            }
            pending.add(reservation)
            pendingReservations -= 1
            pendingReservation = false
            const waiting = new AbortController()
            reservation.handshakeAbortController = waiting
            let release: (() => void) | undefined
            let authStatus: number
            const actualWork = new Set<Promise<unknown>>()
            try {
                release = await manager.acquireResource(waiting.signal)
                if (reservation.closed || shuttingDown)
                    return { handled: true, response: statusResponse(503) }
                authStatus = await readLiveHandshake(
                    reservation,
                    snapshotPath,
                    snapshotTimeoutMs,
                    maxPayloadBytes,
                    options.readSnapshot,
                    (work) => actualWork.add(work),
                )
            } catch (error) {
                if (!(error instanceof ResourceAdmissionError)) throw error
                return { handled: true, response: statusResponse(503) }
            } finally {
                releaseAfterSettled(release, actualWork)
                if (reservation.handshakeAbortController === waiting)
                    reservation.handshakeAbortController = null
            }
            if (shuttingDown) return { handled: true, response: statusResponse(503) }
            if (authStatus !== 200) return { handled: true, response: statusResponse(authStatus) }
            try {
                upgraded = server.upgrade(request, { data: { connection: reservation } })
            } catch {
                upgraded = false
            }
            if (!upgraded) return { handled: true, response: statusResponse(400) }
            pending.delete(reservation)
            manager.connections.add(reservation)
            return { handled: true }
        } finally {
            if (pendingReservation) pendingReservations -= 1
            if (reservation && !upgraded) lifecycle.dispose(reservation)
        }
    }

    const unsubscribeFromChanges = subscribeToApplicationChanges(() => manager.sampleAll())

    function shutdown(): Promise<void> {
        if (shutdownResult) return shutdownResult
        shuttingDown = true
        unsubscribeFromChanges()
        for (const connection of [...pending, ...manager.connections]) {
            lifecycle.close(connection, 1012, 'Server restarting')
        }
        pending.clear()
        manager.connections.clear()
        shutdownResult = drainWorker(manager.drainResources())
        return shutdownResult
    }

    return {
        websocket,
        handle,
        shutdown,
        get connectionCount() {
            return manager.connections.size + pending.size + pendingReservations
        },
        get samplingGroupCount() {
            let count = 0
            for (const connection of manager.connections) count += connection.subscriptions.size
            return count
        },
    }
}
