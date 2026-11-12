import { startBackgroundNotificationReceivers } from './background-receivers.ts'
import { startBackgroundDockerEngine } from './background-discovery.ts'

/** Bound the abandoned-process fallback; the runner supplies its measured window plus setup/recovery/drain allowance. */
export function parseContentionReceiverLifetime(value: string | undefined): number {
    if (!value || !/^\d{1,7}$/u.test(value)) throw new Error('Invalid contention receiver lifetime')
    const lifetimeMs = Number(value)
    if (!Number.isSafeInteger(lifetimeMs) || lifetimeMs < 1 || lifetimeMs > 1_200_000)
        throw new Error('Invalid contention receiver lifetime')
    return lifetimeMs
}

/** Own slow private notification receivers and a paused deterministic Docker source; the control server arms the burst and shuts down all owned listeners. */
export async function startContentionReceivers(
    seed: number,
    ports = { smtp: 2525, webhook: 9016, docker: 9017, control: 9018 },
    lifetimeMs = 1_200_000,
) {
    parseContentionReceiverLifetime(String(lifetimeMs))
    const notifications = await startBackgroundNotificationReceivers({
        smtpPort: ports.smtp,
        webhookPort: ports.webhook,
        delayMs: 1500,
    })
    const discovery = startBackgroundDockerEngine(seed, {
        port: ports.docker,
        initiallyPaused: true,
        mutateDetachedInventory: true,
    })
    let timer: ReturnType<typeof setTimeout> | undefined
    let stopping: Promise<void> | undefined
    /** Stop discovery/control admission before awaiting notification receiver closure. */
    const stop = () => {
        if (stopping) return stopping
        clearTimeout(timer)
        discovery.stop()
        control.stop(true)
        stopping = notifications.close()
        return stopping
    }
    const control = Bun.serve({
        hostname: '127.0.0.1',
        port: ports.control,
        /** Expose only fixture status, one burst-arm action, and delayed shutdown acknowledgement on the loopback control listener. */
        async fetch(request) {
            const path = new URL(request.url).pathname
            if (request.method === 'GET' && path === '/status')
                return Response.json({
                    notifications: notifications.stats(),
                    discovery: discovery.stats(),
                })
            if (request.method === 'POST' && path === '/arm') {
                discovery.arm()
                return new Response(null, { status: 204 })
            }
            if (request.method === 'POST' && path === '/stop') {
                setTimeout(() => {
                    void stop()
                }, 50)
                return new Response(null, { status: 204 })
            }
            return new Response(null, { status: 404 })
        },
    })
    timer = setTimeout(() => {
        void stop()
    }, lifetimeMs)
    timer.unref()
    return { notifications, discovery, control, stop }
}

if (import.meta.main) {
    const seed = Number(process.argv[2])
    if (!Number.isInteger(seed) || seed < 0 || seed > 0xffffffff)
        throw new Error('Invalid contention seed')
    const lifetimeMs = parseContentionReceiverLifetime(process.argv[3])
    await startContentionReceivers(seed, undefined, lifetimeMs)
}
