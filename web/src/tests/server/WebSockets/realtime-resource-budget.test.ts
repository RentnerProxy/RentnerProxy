import { expect, test } from 'bun:test'
import { createSubscriptionManager } from '@/server/WebSockets/realtimeSubscriptions.ts'
import { createLiveWebSocketRuntime } from '@/server/WebSockets/realtimeWebSocket.ts'
import type { LiveSocketData } from '@/server/WebSockets/Types/bun.types.ts'
import type {
    LiveConnectionData,
    LiveTopicSubscription,
} from '@/server/WebSockets/Types/realtime-subscriptions.types.ts'

function subscription(index: number): LiveTopicSubscription {
    const connection: LiveConnectionData = {
        requestUrl: 'https://proxy.example/api/live',
        cookie: null,
        origin: 'https://proxy.example',
        ws: { sendText: () => 1 } as unknown as LiveConnectionData['ws'],
        closed: false,
        handshakeAbortController: null,
        subscriptions: new Map(),
        messageWindowStarted: 0,
        messageCount: 0,
    }
    const result: LiveTopicSubscription = {
        connection,
        topic: 'app-events',
        query: index,
        queryText: '{}',
        intervalMs: 60_000,
        timer: null,
        abortController: null,
        abortTimer: null,
        inflight: false,
        pending: false,
        lastComparison: null,
        active: true,
    }
    connection.subscriptions.set(result.topic, result)
    return result
}

test('a detached snapshot failure closes its connection and releases admission', async () => {
    const closed = Promise.withResolvers<number>()
    const manager = createSubscriptionManager({
        snapshotTimeoutMs: 1_000,
        maxPayloadBytes: 65_536,
        readSnapshot: async () => {
            throw new Error('private transport details')
        },
        closeUnavailable: (_connection, status) => closed.resolve(status),
        closeUnauthorized: () => {},
        closeBackpressured: () => {},
    })
    const connection = subscription(0).connection
    try {
        expect(manager.subscribe(connection, 'app-events', {}, '{}')).toBe(true)
        expect(await closed.promise).toBe(503)
        await manager.drainResources()
        expect(manager.resourceUsage().running).toBe(0)
        expect(manager.resourceUsage().pending).toBe(0)
    } finally {
        manager.dispose(connection)
    }
})

test.each(['subscription replacement', 'connection closure'] as const)(
    'ignores a late snapshot failure after %s',
    async (change) => {
        const entered = Promise.withResolvers<void>()
        const delayed = Promise.withResolvers<never>()
        const closed: number[] = []
        let reads = 0
        const manager = createSubscriptionManager({
            snapshotTimeoutMs: 1_000,
            maxPayloadBytes: 65_536,
            readSnapshot: async () => {
                reads++
                if (reads === 1) {
                    entered.resolve()
                    await delayed.promise
                }
                return { kind: 'success', data: { revision: 'current' } }
            },
            closeUnavailable: (_connection, status) => {
                closed.push(status)
            },
            closeUnauthorized: () => {},
            closeBackpressured: () => {},
        })
        const connection = subscription(0).connection
        try {
            manager.subscribe(connection, 'app-events', {}, '{}')
            await entered.promise
            if (change === 'subscription replacement') {
                manager.subscribe(connection, 'app-events', {}, '{}')
            } else {
                connection.closed = true
            }
            delayed.reject(new Error('late transport failure'))
            await manager.drainResources()
            await Bun.sleep(0)
            expect(closed).toEqual([])
            expect(manager.resourceUsage().running).toBe(0)
            expect(manager.resourceUsage().pending).toBe(0)
            if (change === 'subscription replacement') {
                expect(reads).toBe(2)
                expect(connection.subscriptions.get('app-events')?.active).toBe(true)
            }
        } finally {
            manager.dispose(connection)
            if (reads > 0) delayed.reject(new Error('test cleanup'))
            await manager.drainResources()
        }
    },
)

test('handshake storms share the sampling budget and shutdown aborts queued admission before authorization reads', async () => {
    let reads = 0
    const finish = Promise.withResolvers<void>()
    const runtime = createLiveWebSocketRuntime({
        allowedOrigin: 'https://proxy.example',
        readSnapshot: async () => {
            reads++
            await finish.promise
            return Response.json({})
        },
    })
    const requests = Array.from({ length: 40 }, () =>
        runtime.handle(
            new Request('https://proxy.example/api/live', {
                headers: { upgrade: 'websocket', origin: 'https://proxy.example' },
            }),
            { upgrade: () => false } as unknown as Bun.Server<LiveSocketData>,
        ),
    )
    await Bun.sleep(0)
    expect(reads).toBe(4)
    expect(runtime.connectionCount).toBe(36)
    let drained = false
    const stopping = runtime.shutdown().then(() => {
        drained = true
    })
    const results = await Promise.all(requests)
    expect(drained).toBe(false)
    finish.resolve()
    await stopping
    expect(reads).toBe(4)
    expect(results.every((result) => result.response?.status === 503)).toBe(true)
    expect(runtime.connectionCount).toBe(0)
})

test('timed-out hydration retains its permit and shutdown waits for actual reader settlement', async () => {
    const finish = Promise.withResolvers<void>()
    let reads = 0
    let active = 0
    let peak = 0
    const runtime = createLiveWebSocketRuntime({
        allowedOrigin: 'https://proxy.example',
        snapshotTimeoutMs: 10,
        readSnapshot: async () => {
            reads++
            active++
            peak = Math.max(peak, active)
            try {
                await finish.promise
                return Response.json({})
            } finally {
                active--
            }
        },
    })
    const wave = () =>
        Promise.all(
            Array.from({ length: 4 }, () =>
                runtime.handle(
                    new Request('https://proxy.example/api/live', {
                        headers: { upgrade: 'websocket', origin: 'https://proxy.example' },
                    }),
                    { upgrade: () => false } as unknown as Bun.Server<LiveSocketData>,
                ),
            ),
        )
    try {
        expect((await wave()).every((result) => result.response?.status === 503)).toBe(true)
        expect((await wave()).every((result) => result.response?.status === 503)).toBe(true)
        expect(reads).toBe(4)
        expect(active).toBe(4)
        expect(peak).toBe(4)
        let drained = false
        const stopping = runtime.shutdown().then(() => {
            drained = true
        })
        await Bun.sleep(0)
        expect(drained).toBe(false)
        finish.resolve()
        await stopping
        expect(active).toBe(0)
        expect(drained).toBe(true)
    } finally {
        finish.resolve()
        await runtime.shutdown()
    }
})

test('realtime storms are admitted before snapshot hydration with bounded FIFO waits and coalesced pending hints', async () => {
    const finish = Promise.withResolvers<void>()
    const entered: number[] = []
    const manager = createSubscriptionManager({
        maxPayloadBytes: 4096,
        snapshotTimeoutMs: 1000,
        readSnapshot: async (item) => {
            entered.push(item.query as number)
            await finish.promise
            return { kind: 'success', data: {} }
        },
        closeUnauthorized: () => {},
        closeUnavailable: () => {},
        closeBackpressured: () => {},
    })
    const subscriptions = Array.from({ length: 40 }, (_, index) => subscription(index))
    const reads = subscriptions.map((item) => manager.sample(item))
    await Bun.sleep(0)
    expect(entered).toEqual([0, 1, 2, 3])
    expect(manager.resourceUsage()).toMatchObject({ running: 4, pending: 32, busy: 4 })
    // Repeated invalidations are only a bit on the existing subscription.
    await Promise.all(Array.from({ length: 100 }, () => manager.sample(subscriptions[4]!)))
    expect(manager.resourceUsage().pending).toBe(32)
    manager.unsubscribe(subscriptions[5]!.connection, 'app-events')
    await Bun.sleep(0)
    expect(manager.resourceUsage()).toMatchObject({ pending: 31, cancelled: 1 })
    finish.resolve()
    await Promise.all(reads)
    expect(entered.slice(0, 7)).toEqual([0, 1, 2, 3, 4, 6, 7])
    expect(entered).not.toContain(5)
    expect(manager.resourceUsage()).toMatchObject({ running: 0, pending: 0 })
    // Existing periodic sampling can recover a previously deferred subscription.
    await manager.sample(subscriptions[39]!)
    expect(entered.at(-1)).toBe(39)
    for (const item of subscriptions) manager.dispose(item.connection)
})
