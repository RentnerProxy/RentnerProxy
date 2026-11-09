import type { MonitoringScaleOptions } from './Types/monitoring.types.ts'
import type { ProxyRuntimeSnapshot } from '../../web/src/server/ProxyRuntime/Types/proxy-runtime.types.ts'
import { monitoringLatency } from './monitoring.ts'

export function assertNativeMonitoringFixture(
    flag: string | undefined,
    controllerUrl: string | undefined,
    token: string | undefined,
) {
    if (
        flag !== '1' ||
        controllerUrl !== 'http://127.0.0.1:55441' ||
        token !== 'synthetic-monitoring-scale-token-392'
    )
        throw new Error('Native monitoring requires the explicitly isolated controller fixture.')
}

export async function createNativeMonitoringFixture(
    options: MonitoringScaleOptions,
    domains: readonly string[],
) {
    assertNativeMonitoringFixture(
        process.env.RENTNERPROXY_MONITORING_NATIVE_FIXTURE,
        process.env.RENTNERPROXY_CONTROLLER_URL,
        process.env.RENTNERPROXY_CONTROLLER_TOKEN,
    )
    const [{ inspectControllerDomain }, { applyProxyRuntimeConfiguration }] = await Promise.all([
        import('../../web/src/server/Controller/domain-diagnostics.server.ts'),
        import('../../web/src/server/Controller/proxy.server.ts'),
    ])
    const hosts = new Map(domains.map((domain, index) => [domain, index]))
    let active = 0
    const upstream = Bun.serve({
        hostname: '0.0.0.0',
        port: 0,
        idleTimeout: 5,
        async fetch(request) {
            const index = hosts.get(request.headers.get('host') ?? '')
            if (
                index !== undefined &&
                request.method === 'GET' &&
                new URL(request.url).pathname === '/traffic'
            )
                return new Response('monitoring-capacity-traffic')
            if (request.method !== 'HEAD' || index === undefined)
                return new Response(null, { status: 404 })
            if (active >= 128) return new Response(null, { status: 503 })
            active++
            try {
                const latency = monitoringLatency(options.profile, index, options.timeoutMs)
                const delayMs = latency.timeout ? latency.durationMs + 500 : latency.durationMs
                await new Promise<void>((resolve) => {
                    const finish = () => {
                        clearTimeout(timer)
                        request.signal.removeEventListener('abort', finish)
                        resolve()
                    }
                    const timer = setTimeout(finish, delayMs)
                    request.signal.addEventListener('abort', finish, { once: true })
                    if (request.signal.aborted) finish()
                })
                return new Response(null, { status: 200 })
            } finally {
                active--
            }
        },
    })
    return {
        forwardHost: 'host.docker.internal',
        forwardPort: upstream.port,
        inspect: inspectControllerDomain,
        async traffic() {
            const response = await fetch('http://127.0.0.1:55442/traffic', {
                headers: { host: domains[0]! },
                signal: AbortSignal.timeout(1000),
                redirect: 'error',
            })
            if (
                response.status !== 200 ||
                (await response.text()) !== 'monitoring-capacity-traffic'
            )
                throw new Error('Isolated Caddy traffic failed during monitoring load.')
        },
        async apply(snapshot: ProxyRuntimeSnapshot) {
            if (!(await applyProxyRuntimeConfiguration(snapshot)))
                throw new Error('Isolated native monitoring configuration was not activated.')
        },
        stop: () => upstream.stop(true),
    }
}
