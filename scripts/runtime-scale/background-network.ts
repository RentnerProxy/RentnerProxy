export async function withInFlightBackgroundHead(operation: () => Promise<void>) {
    const entered = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    const server = Bun.serve({
        hostname: '127.0.0.1',
        port: 0,
        async fetch() {
            entered.resolve()
            await release.promise
            return new Response(null, { status: 503 })
        },
    })
    const start = performance.now()
    const request = fetch(`http://127.0.0.1:${server.port}/`, {
        method: 'HEAD',
        signal: AbortSignal.timeout(3000),
    })
    try {
        await Promise.race([
            entered.promise,
            request.then(() => {
                throw new Error('Fixture HEAD completed before mutation barrier')
            }),
        ])
        await operation()
        release.resolve()
        const response = await request
        await response.body?.cancel()
        return { status: response.status, durationMs: Math.ceil(performance.now() - start) }
    } finally {
        release.resolve()
        await request.catch(() => undefined)
        server.stop(true)
    }
}
