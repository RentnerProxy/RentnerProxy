import { describe, expect, test } from 'bun:test'

import { createLiveWebSocketRuntime } from '@/server/WebSockets/realtimeWebSocket.ts'
import { publishApplicationChange } from '@/server/WebSockets/applicationChanges.ts'
import { BACKGROUND_RESOURCE_ENVELOPE } from '@/config/background-resources.config.ts'
import { LIVE_CHANGE_COALESCE_MS } from '@/config/realtime.config.ts'
import type { LiveSocketData as RuntimeSocketData } from '@/server/WebSockets/Types/bun.types.ts'

type SnapshotReader = (request: Request) => Response | Promise<Response>

function startRuntime(readSnapshot: SnapshotReader) {
    const options = {
        allowedOrigin: 'http://allowed.test',
        readSnapshot: (request: Request) => readSnapshot(request),
    }
    const runtime = createLiveWebSocketRuntime({
        ...options,
    })
    const server = Bun.serve({
        hostname: '127.0.0.1',
        port: 0,
        websocket: runtime.websocket,
        fetch: async (request, bunServer) => {
            const result = await runtime.handle(request, bunServer)
            if (result.handled) return result.response
            return new Response(null, { status: 404 })
        },
    })
    return { runtime, server }
}

function openSocket(url: URL): WebSocket {
    const Constructor = globalThis.WebSocket as unknown as new (
        url: string,
        options?: { headers?: Record<string, string> },
    ) => WebSocket
    return new Constructor(url.toString(), { headers: { Origin: 'http://allowed.test' } })
}

function nextMessage(socket: WebSocket, timeoutMs = 1_000): Promise<unknown> {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            socket.removeEventListener('message', onMessage)
            reject(new Error('Timed out waiting for a WebSocket message.'))
        }, timeoutMs)
        const onMessage = (event: MessageEvent) => {
            clearTimeout(timer)
            socket.removeEventListener('message', onMessage)
            resolve(JSON.parse(String(event.data)) as unknown)
        }
        socket.addEventListener('message', onMessage)
    })
}

function socketOpened(socket: WebSocket): Promise<void> {
    return new Promise((resolve, reject) => {
        socket.addEventListener('open', () => resolve(), { once: true })
        socket.addEventListener('error', () => reject(new Error('WebSocket failed to open.')), {
            once: true,
        })
    })
}

function socketClosed(socket: WebSocket): Promise<CloseEvent> {
    return new Promise((resolve) => {
        socket.addEventListener('close', (event) => resolve(event), { once: true })
    })
}

describe('live WebSocket transport', () => {
    test('rejects cross-origin upgrades before reading a snapshot', async () => {
        let reads = 0
        const runtime = createLiveWebSocketRuntime({
            allowedOrigin: 'http://allowed.test',
            readSnapshot: () => {
                reads += 1
                return Response.json({ value: 1 })
            },
        })
        let upgrades = 0
        const request = new Request('http://localhost/api/live', {
            headers: { upgrade: 'websocket', origin: 'http://evil.test' },
        })
        const result = await runtime.handle(request, {
            upgrade: () => {
                upgrades += 1
                return false
            },
        } as unknown as Bun.Server<RuntimeSocketData>)

        expect(result.response?.status).toBe(403)
        expect(reads).toBe(0)
        expect(upgrades).toBe(0)
        await runtime.shutdown()
    })

    test('rejects an unauthorized initial snapshot', async () => {
        const runtime = createLiveWebSocketRuntime({
            allowedOrigin: 'http://allowed.test',
            readSnapshot: () => new Response(null, { status: 401 }),
        })
        const result = await runtime.handle(
            new Request('http://localhost/api/live', {
                headers: { upgrade: 'websocket', origin: 'http://allowed.test' },
            }),
            { upgrade: () => true } as unknown as Bun.Server<RuntimeSocketData>,
        )

        expect(result.response?.status).toBe(401)
        expect(runtime.connectionCount).toBe(0)
        await runtime.shutdown()
    })

    test('releases a handshake reservation but drains timed-out snapshot work before shutdown', async () => {
        const reader = Promise.withResolvers<Response>()
        const runtime = createLiveWebSocketRuntime({
            allowedOrigin: 'http://allowed.test',
            snapshotTimeoutMs: 5,
            readSnapshot: () => reader.promise,
        })
        const result = await runtime.handle(
            new Request('http://localhost/api/live', {
                headers: { upgrade: 'websocket', origin: 'http://allowed.test' },
            }),
            { upgrade: () => true } as unknown as Bun.Server<RuntimeSocketData>,
        )

        expect(result.response?.status).toBe(503)
        expect(runtime.connectionCount).toBe(0)
        let drained = false
        const shutdown = runtime.shutdown().then(() => {
            drained = true
        })
        await new Promise<void>((resolve) => setImmediate(resolve))
        expect(drained).toBe(false)
        reader.resolve(Response.json({ revision: 'fixture' }))
        await shutdown
        expect(drained).toBe(true)
    })

    test('sends changed snapshots, ignores volatile fields, and stops sampling after close', async () => {
        const snapshots = [
            { revision: 'auth' },
            { revision: '1', snapshot: 'token', snapshotExpiresAt: 'first' },
            { revision: '1', snapshot: 'token', snapshotExpiresAt: 'second' },
            { revision: '2', snapshot: 'token-2', snapshotExpiresAt: 'third' },
        ]
        let reads = 0
        const { runtime, server } = startRuntime(() => {
            const snapshot = snapshots[Math.min(reads, snapshots.length - 1)]!
            reads += 1
            return Response.json(snapshot)
        })
        const socket = openSocket(new URL('/api/live', server.url))

        try {
            await socketOpened(socket)
            socket.send(JSON.stringify({ type: 'subscribe', topic: 'app-events', query: {} }))
            await expect(nextMessage(socket)).resolves.toEqual({
                type: 'snapshot',
                topic: 'app-events',
                query: {},
                payload: { revision: '1', snapshot: 'token', snapshotExpiresAt: 'first' },
            })
            publishApplicationChange()
            await new Promise((resolve) => setTimeout(resolve, LIVE_CHANGE_COALESCE_MS + 30))
            publishApplicationChange()
            await expect(nextMessage(socket)).resolves.toEqual({
                type: 'snapshot',
                topic: 'app-events',
                query: {},
                payload: { revision: '2', snapshot: 'token-2', snapshotExpiresAt: 'third' },
            })
            socket.close()
            await socketClosed(socket)
            await new Promise((resolve) => setTimeout(resolve, 20))
            const readsAtClose = reads
            publishApplicationChange()
            await new Promise((resolve) => setTimeout(resolve, LIVE_CHANGE_COALESCE_MS + 30))
            expect(reads).toBe(readsAtClose)
            expect(runtime.connectionCount).toBe(0)
        } finally {
            if (socket.readyState !== WebSocket.CLOSED) socket.close()
            await runtime.shutdown()
            await server.stop(true)
        }
    })

    test('sends unauthorized and closes when authorization is revoked', async () => {
        let reads = 0
        const runtime = createLiveWebSocketRuntime({
            allowedOrigin: 'http://allowed.test',
            readSnapshot: () => {
                reads += 1
                return reads < 3
                    ? Response.json({ revision: String(reads) })
                    : new Response(null, { status: 403 })
            },
        })
        let upgradedData: RuntimeSocketData | undefined
        const result = await runtime.handle(
            new Request('http://localhost/api/live', {
                headers: { upgrade: 'websocket', origin: 'http://allowed.test' },
            }),
            {
                upgrade: (_request: Request, options: { data?: RuntimeSocketData }) => {
                    upgradedData = options?.data
                    return true
                },
            } as unknown as Bun.Server<RuntimeSocketData>,
        )
        expect(result.handled).toBe(true)
        const messages: string[] = []
        let closeCode: number | undefined
        const socket = {
            data: upgradedData!,
            sendText(message: string) {
                messages.push(message)
                return message.length
            },
            close(code: number) {
                closeCode = code
            },
        } as unknown as Bun.ServerWebSocket<RuntimeSocketData>

        try {
            runtime.websocket.open?.(socket)
            runtime.websocket.message?.(
                socket,
                JSON.stringify({ type: 'subscribe', topic: 'app-events', query: {} }),
            )
            await new Promise((resolve) => setTimeout(resolve, 5))
            publishApplicationChange()
            await new Promise((resolve) => setTimeout(resolve, LIVE_CHANGE_COALESCE_MS + 30))
            expect(messages.at(-1)).toBe(JSON.stringify({ type: 'unauthorized' }))
            expect(closeCode).toBe(4001)
            expect(runtime.connectionCount).toBe(0)
        } finally {
            await runtime.shutdown()
        }
    })
})

test.each(['resolve', 'reject'] as const)(
    'late response body cancellation retains all permits and shutdown ownership until cancel %s',
    async (outcome) => {
        const slots = BACKGROUND_RESOURCE_ENVELOPE.realtime.running
        const responses = Array.from({ length: slots }, () => Promise.withResolvers<Response>())
        const cancel = Promise.withResolvers<void>()
        const cancelled = Promise.withResolvers<void>()
        let reads = 0,
            cancellations = 0,
            nextSettled = false,
            drained = false
        const runtime = createLiveWebSocketRuntime({
            allowedOrigin: 'http://allowed.test',
            snapshotTimeoutMs: 5,
            readSnapshot: () =>
                responses[reads++]?.promise ?? Response.json({ revision: 'unexpected fresh read' }),
        })
        const request = () =>
            runtime.handle(
                new Request('http://localhost/api/live', {
                    headers: { upgrade: 'websocket', origin: 'http://allowed.test' },
                }),
                { upgrade: () => false } as unknown as Bun.Server<RuntimeSocketData>,
            )
        const initial = await Promise.all(Array.from({ length: slots }, request))
        expect(initial.every((result) => result.response?.status === 503)).toBe(true)
        try {
            for (const response of responses)
                response.resolve(
                    new Response(
                        new ReadableStream({
                            cancel() {
                                if (++cancellations === slots) cancelled.resolve()
                                return cancel.promise
                            },
                        }),
                    ),
                )
            await cancelled.promise
            const next = request().then((result) => {
                nextSettled = true
                return result
            })
            await new Promise<void>((resolve) => setImmediate(resolve))
            await new Promise<void>((resolve) => setImmediate(resolve))
            expect(reads).toBe(slots)
            expect(nextSettled).toBe(false)
            const shutdown = runtime.shutdown().then(() => {
                drained = true
            })
            expect((await next).response?.status).toBe(503)
            await new Promise<void>((resolve) => setImmediate(resolve))
            expect(drained).toBe(false)
            if (outcome === 'reject') cancel.reject(new Error('ignored body cancellation failure'))
            else cancel.resolve()
            await shutdown
            expect(drained).toBe(true)
            expect(cancellations).toBe(slots)
        } finally {
            cancel.resolve()
            for (const response of responses) response.resolve(new Response(null))
            await runtime.shutdown()
        }
    },
)
