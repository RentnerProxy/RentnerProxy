import type {
    LiveStatus,
    RealtimeSubscriptionOptions,
    Listener,
    Subscription,
    RealtimeResumeMetadata,
} from './Types/realtime-events-client.types.ts'
import { LIVE_WEBSOCKET_PROTOCOL, LIVE_SESSION_PROTOCOL_PREFIX } from '@/config/realtime.config.ts'
import { realtimeEventSchema } from './events.ts'
import { runDetachedTask } from '@/lib/Async/runDetachedTask.ts'
import type { LiveTopic, SnapshotEvent } from './Types/events.types.ts'

const MIN_RECONNECT_DELAY_MS = 1000
const MAX_RECONNECT_DELAY_MS = 30_000
const listeners = new Map<number, Listener>()
const subscriptions = new Map<string, Subscription>()

let websocket: WebSocket | null = null
let reconnectTimeout: ReturnType<typeof setTimeout> | null = null
let reconnectDelay = MIN_RECONNECT_DELAY_MS
let connectionStatus: LiveStatus = 'inactive'
let nextListenerId = 1
let shouldReconnect = false
let isConnecting = false
let isPageSuspended = false
let lifecycleListenersRegistered = false
let needsReconnectResync = false

function getRealtimeUrl(): URL {
    const url = new URL('/api/live', window.location.href)
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
    url.search = ''
    return url
}

function notifyListenerStatus(listener: Listener, status: LiveStatus): void {
    if (!listener.onStatus) return
    runDetachedTask(
        () => listener.onStatus?.(status),
        () => console.warn('[realtime] Status listener failed'),
    )
}

function notifyStatus(status: LiveStatus): void {
    connectionStatus = status
    for (const listener of listeners.values()) notifyListenerStatus(listener, status)
}

function notifyLifecycleListeners(
    event: 'onResume' | 'onUnauthorized',
    metadata?: RealtimeResumeMetadata,
): void {
    for (const listener of listeners.values()) {
        if (!listener[event]) continue
        runDetachedTask(
            () =>
                event === 'onResume' ? listener.onResume?.(metadata) : listener.onUnauthorized?.(),
            () => console.warn('[realtime] Lifecycle listener failed'),
        )
    }
}

function resumeSessionVersion(): string | undefined {
    let expected: string | undefined
    for (const subscription of subscriptions.values()) {
        if (subscription.topic !== 'app-events') continue
        for (const listener of subscription.listeners) {
            let version: string | undefined
            try {
                version = listener.getResumeSessionVersion?.()
            } catch {
                return undefined
            }
            if (version === undefined) continue
            if (
                !/^[a-f0-9]{64}$/u.test(version) ||
                (expected !== undefined && expected !== version)
            )
                return undefined
            expected = version
        }
    }
    return expected
}

function notifyData(event: SnapshotEvent): void {
    const queryText = JSON.stringify(event.query)
    for (const subscription of subscriptions.values()) {
        if (subscription.topic !== event.topic || subscription.queryText !== queryText) continue
        for (const listener of subscription.listeners) {
            runDetachedTask(
                () => listener.onData(event.payload),
                () => console.warn('[realtime] Data listener failed'),
            )
        }
    }
}

function clearReconnectTimeout(): void {
    if (reconnectTimeout === null) return
    clearTimeout(reconnectTimeout)
    reconnectTimeout = null
}

function sendMessage(message: object): boolean {
    if (!websocket || websocket.readyState !== WebSocket.OPEN) return false
    try {
        websocket.send(JSON.stringify(message))
        return true
    } catch {
        websocket.close()
        return false
    }
}

function sendSubscription(subscription: Subscription): void {
    sendMessage({ type: 'subscribe', topic: subscription.topic, query: subscription.query })
}

function sendAllSubscriptions(): void {
    for (const subscription of subscriptions.values()) sendSubscription(subscription)
}

function hasTopicSubscription(topic: LiveTopic): boolean {
    for (const subscription of subscriptions.values()) {
        if (subscription.topic === topic) return true
    }
    return false
}

function closeCurrentWebSocket(): void {
    clearReconnectTimeout()
    const current = websocket
    websocket = null
    if (!current) return
    try {
        current.close()
    } catch {
        return
    }
}

function scheduleReconnect(): void {
    if (!shouldReconnect || isPageSuspended || listeners.size === 0 || reconnectTimeout !== null)
        return
    notifyStatus('disconnected')
    const delay = reconnectDelay
    reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY_MS)
    reconnectTimeout = setTimeout(() => {
        reconnectTimeout = null
        connect()
    }, delay)
}

function handleUnauthorized(): void {
    needsReconnectResync = false
    shouldReconnect = false
    clearReconnectTimeout()
    notifyStatus('disconnected')
    notifyLifecycleListeners('onUnauthorized')
    closeCurrentWebSocket()
}

function connect(): void {
    if (isConnecting || websocket || !shouldReconnect || isPageSuspended || listeners.size === 0)
        return
    isConnecting = true
    notifyStatus('connecting')
    try {
        const expectedSessionVersion = resumeSessionVersion()
        const protocols = [LIVE_WEBSOCKET_PROTOCOL]
        if (expectedSessionVersion)
            protocols.push(LIVE_SESSION_PROTOCOL_PREFIX + expectedSessionVersion)
        const current = new WebSocket(getRealtimeUrl(), protocols)
        websocket = current
        current.addEventListener('open', () => {
            if (websocket !== current) return
            const resuming = needsReconnectResync
            needsReconnectResync = false
            reconnectDelay = MIN_RECONNECT_DELAY_MS
            notifyStatus('connected')
            sendAllSubscriptions()
            if (resuming) {
                const confirmed =
                    expectedSessionVersion !== undefined &&
                    current.protocol === LIVE_SESSION_PROTOCOL_PREFIX + expectedSessionVersion
                notifyLifecycleListeners(
                    'onResume',
                    confirmed ? { sessionVersion: expectedSessionVersion } : undefined,
                )
            }
        })
        current.addEventListener('message', (event) => {
            if (websocket !== current) return
            let value: unknown
            try {
                value = JSON.parse(String(event.data))
            } catch {
                return
            }
            const parsed = realtimeEventSchema.safeParse(value)
            if (!parsed.success) return
            if (parsed.data.type === 'unauthorized') {
                handleUnauthorized()
                return
            }
            notifyStatus('connected')
            notifyData(parsed.data)
        })
        current.addEventListener('error', () => {
            if (websocket === current) current.close()
        })
        current.addEventListener('close', () => {
            if (websocket !== current) return
            websocket = null
            if (shouldReconnect && !isPageSuspended) {
                needsReconnectResync = true
                scheduleReconnect()
            } else notifyStatus(isPageSuspended ? 'inactive' : 'disconnected')
        })
    } catch {
        scheduleReconnect()
    } finally {
        isConnecting = false
    }
}

function setSuspended(suspended: boolean): void {
    isPageSuspended = suspended
    if (suspended) {
        needsReconnectResync = false
        closeCurrentWebSocket()
        notifyStatus('inactive')
    }
}

function resume(): void {
    if (document.visibilityState === 'hidden') {
        setSuspended(true)
        return
    }
    const wasSuspended = isPageSuspended
    isPageSuspended = false
    if (wasSuspended) notifyLifecycleListeners('onResume')
    reconnectDelay = MIN_RECONNECT_DELAY_MS
    if (listeners.size > 0) connect()
}

function setupLifecycleListeners(): void {
    if (lifecycleListenersRegistered || typeof window === 'undefined') return
    lifecycleListenersRegistered = true
    document.addEventListener('visibilitychange', resume)
    window.addEventListener('pagehide', () => setSuspended(true))
    window.addEventListener('pageshow', resume)
}

export function subscribeToRealtimeEvents<T>({
    topic,
    query,
    onData,
    onStatus,
    onUnauthorized,
    onResume,
    getResumeSessionVersion,
}: RealtimeSubscriptionOptions<T>): () => void {
    if (
        listeners.size === 0 &&
        !isPageSuspended &&
        typeof document !== 'undefined' &&
        document.visibilityState === 'hidden'
    ) {
        isPageSuspended = true
    }
    setupLifecycleListeners()
    const queryText = JSON.stringify(query)
    const key = `${topic}\u0000${queryText}`
    let subscription = subscriptions.get(key)
    if (!subscription) {
        subscription = { key, topic, queryText, query, listeners: new Set() }
        subscriptions.set(key, subscription)
    }
    const listener: Listener = {
        id: nextListenerId++,
        onData: onData as (data: unknown) => void | Promise<void>,
        subscriptionKey: key,
        ...(onStatus ? { onStatus } : {}),
        ...(onUnauthorized ? { onUnauthorized } : {}),
        ...(onResume ? { onResume } : {}),
        ...(getResumeSessionVersion ? { getResumeSessionVersion } : {}),
    }
    listeners.set(listener.id, listener)
    subscription.listeners.add(listener)
    notifyListenerStatus(listener, isPageSuspended ? 'inactive' : connectionStatus)
    shouldReconnect = true
    if (websocket?.readyState === WebSocket.OPEN && subscription.listeners.size === 1) {
        sendSubscription(subscription)
    }
    if (!isPageSuspended) connect()
    return () => {
        if (!listeners.delete(listener.id)) return
        const current = subscriptions.get(listener.subscriptionKey)
        if (!current) return
        current.listeners.delete(listener)
        if (current.listeners.size > 0) return
        subscriptions.delete(current.key)
        if (!hasTopicSubscription(current.topic))
            sendMessage({ type: 'unsubscribe', topic: current.topic })
        if (listeners.size > 0) return
        shouldReconnect = false
        needsReconnectResync = false
        closeCurrentWebSocket()
        notifyStatus('inactive')
    }
}
