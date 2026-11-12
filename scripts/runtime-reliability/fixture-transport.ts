import { fixtureTransport } from './fixture-transport.config.ts'
import type { FixturePorts } from './Types/fixture-transport.types.ts'

/** Own controlled upstream/auth/TLS listeners and a bounded background receipt buffer; explicit overflow preserves missing-evidence semantics. */
export function startFixtureServers(ports: FixturePorts) {
    let upstreamFailed = false
    let authMode: 'allow' | 'deny' | 'unavailable' = 'allow'
    let tls: ReturnType<typeof Bun.serve> | undefined
    const backgroundReceipts: {
        index: number
        latency: string
        startedAt: number
        finishedAt: number | null
    }[] = []
    let receiptsOverflowed = false
    /** Create a named upstream whose controlled HEAD workload records real start/finish times independently from normal proxy GET traffic. */
    const backend = (name: string, port: number) =>
        Bun.serve({
            hostname: '0.0.0.0',
            port,
            idleTimeout: fixtureTransport.idleTimeout,
            /** Delay only recognized background HEADs, cap receipts at 10000, and release timers/listeners on abort before recording completion. */
            async fetch(request) {
                if (upstreamFailed) return new Response(null, { status: 503 })
                const background = /^bg-(\d{1,3})-(healthy|slow|timeout|failure)-/u.exec(
                    request.headers.get('host') ?? '',
                )
                if (request.method === 'HEAD' && background) {
                    const receipt = {
                        index: Number(background[1]),
                        latency: background[2]!,
                        startedAt: Date.now(),
                        finishedAt: null as number | null,
                    }
                    if (backgroundReceipts.length < 10000) backgroundReceipts.push(receipt)
                    else receiptsOverflowed = true
                    const delay =
                        receipt.latency === 'timeout' ? 4000 : receipt.latency === 'slow' ? 800 : 10
                    await new Promise<void>((resolve) => {
                        const finish = () => {
                            clearTimeout(timer)
                            request.signal.removeEventListener('abort', finish)
                            resolve()
                        }
                        const timer = setTimeout(finish, delay)
                        request.signal.addEventListener('abort', finish, { once: true })
                        if (request.signal.aborted) finish()
                    })
                    receipt.finishedAt = Date.now()
                    return new Response(null, { status: receipt.latency === 'failure' ? 503 : 200 })
                }
                return Response.json({
                    backend: name,
                    user: request.headers.get('Remote-User'),
                    forwardedFor: request.headers.get('X-Forwarded-For'),
                })
            },
        })
    const primary = backend('a', ports.primaryPort)
    const secondary = backend('b', ports.secondaryPort)
    const auth = Bun.serve({
        hostname: '0.0.0.0',
        port: ports.authPort,
        idleTimeout: fixtureTransport.idleTimeout,
        fetch() {
            if (authMode === 'unavailable') return new Response(null, { status: 503 })
            if (authMode === 'deny') return new Response(null, { status: 401 })
            return new Response(null, {
                status: 200,
                headers: { 'Remote-User': 'reliability-user' },
            })
        },
    })
    // Control is reachable only through exec inside this owned fixture container.
    const control = Bun.serve({
        hostname: '127.0.0.1',
        port: ports.controlPort,
        /** Expose receipt reads and single-field fault controls only on the fixture loopback listener; reject malformed or combined mutations. */
        async fetch(request) {
            if (
                request.method === 'GET' &&
                new URL(request.url).pathname === '/background-receipts'
            )
                return Response.json({
                    overflowed: receiptsOverflowed,
                    receipts: backgroundReceipts,
                })
            if (request.method === 'GET') return new Response(null, { status: 200 })
            if (request.method !== 'POST') return new Response(null, { status: 405 })
            let input: unknown
            try {
                input = await request.json()
            } catch {
                return new Response(null, { status: 400 })
            }
            if (!input || typeof input !== 'object' || Array.isArray(input))
                return new Response(null, { status: 400 })
            const record = input as Record<string, unknown>
            if (Object.keys(record).length !== 1) return new Response(null, { status: 400 })
            if (typeof record.upstreamFailed === 'boolean') upstreamFailed = record.upstreamFailed
            else if (
                record.authMode === 'allow' ||
                record.authMode === 'deny' ||
                record.authMode === 'unavailable'
            )
                authMode = record.authMode
            else if (record.startTls === true && !tls) {
                tls = Bun.serve({
                    hostname: '0.0.0.0',
                    port: ports.tlsPort,
                    idleTimeout: fixtureTransport.idleTimeout,
                    tls: {
                        key: Bun.file('/tmp/fixture-leaf.key'),
                        cert: Bun.file('/tmp/fixture-leaf.pem'),
                    },
                    fetch: () => Response.json({ backend: 'tls' }),
                })
            } else return new Response(null, { status: 400 })
            return new Response(null, { status: 200 })
        },
    })
    return {
        primary,
        secondary,
        auth,
        control,
        async stop() {
            await Promise.all([
                primary.stop(true),
                secondary.stop(true),
                auth.stop(true),
                control.stop(true),
                tls?.stop(true),
            ])
        },
    }
}

if (import.meta.main) startFixtureServers(fixtureTransport)
