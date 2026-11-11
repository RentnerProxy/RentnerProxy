import { startBackgroundNotificationReceivers } from './background-receivers.ts'
import { startBackgroundDockerEngine } from './background-discovery.ts'

export async function startContentionReceivers(
    seed: number,
    ports = { smtp: 2525, webhook: 9016, docker: 9017, control: 9018 },
) {
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
    const stop = async () => {
        discovery.stop()
        control.stop(true)
        await notifications.close()
    }
    const control = Bun.serve({
        hostname: '127.0.0.1',
        port: ports.control,
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
    return { notifications, discovery, control, stop }
}

if (import.meta.main) {
    const seed = Number(process.argv[2])
    if (!Number.isInteger(seed) || seed < 0 || seed > 0xffffffff)
        throw new Error('Invalid contention seed')
    const receivers = await startContentionReceivers(seed)
    const timer = setTimeout(() => {
        void receivers.stop()
    }, 360000)
    timer.unref()
}
