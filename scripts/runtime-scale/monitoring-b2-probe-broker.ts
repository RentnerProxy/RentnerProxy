import { timingSafeEqual } from 'node:crypto'
import { z } from 'zod'
import type { SQL } from 'bun'

const requestSchema = z.strictObject({
    hostType: z.literal('proxy'),
    hostId: z.uuid(),
    domain: z.string().regex(/^[a-z0-9.-]{1,253}$/u),
    expectedRevision: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
    upstreamOnly: z.literal(true),
    timeoutMs: z.number().int().min(1000).max(10000),
})
export type MonitoringB2ProbeRequest = z.infer<typeof requestSchema>
interface BrokerOptions {
    probeKey: string
    admit: (request: MonitoringB2ProbeRequest, attempt: string) => Promise<number | null>
    forward: (request: MonitoringB2ProbeRequest, signal: AbortSignal) => Promise<Response>
}

/** No management method/path, arbitrary URL, interactive mode, or caller-supplied headers are forwarded. */
export function monitoringB2ProbeHandler(options: BrokerOptions) {
    if (!/^[a-f0-9]{64}$/u.test(options.probeKey))
        throw new Error('B2 probe capability format invalid')
    let active = 0
    let accepting = true
    const forwards = new AbortController(),
        pending = new Set<Promise<Response>>()
    const hosts = new Set<string>()
    /** Validate the probe-only request, consume fresh claim authority, and bound its forwarded response. */
    const serve = async (request: Request) => {
        if (!accepting) return new Response(null, { status: 503 })
        const supplied = Buffer.from(request.headers.get('authorization') ?? ''),
            expected = Buffer.from('Bearer ' + options.probeKey)
        if (supplied.length !== expected.length || !timingSafeEqual(supplied, expected))
            return new Response(null, { status: 403 })
        if (new URL(request.url).pathname !== '/internal/v1/diagnostics/domain')
            return new Response(null, { status: 403 })
        if (request.method !== 'POST') return new Response(null, { status: 405 })
        const attempt = z.uuid().safeParse(request.headers.get('x-b2-attempt'))
        if (!attempt.success) return new Response(null, { status: 400 })
        try {
            const body = await Promise.race([
                request.text().catch(() => null),
                Bun.sleep(250).then(() => null),
            ])
            if (body === null) return new Response(null, { status: 408 })
            if (Buffer.byteLength(body) > 4096) return new Response(null, { status: 413 })
            const parsed = requestSchema.safeParse(JSON.parse(body))
            if (!parsed.success) return new Response(null, { status: 400 })
            if (!accepting) return new Response(null, { status: 503 })
            if (active >= 64 || hosts.has(parsed.data.hostId))
                return new Response(null, { status: 503 })
            active++
            hosts.add(parsed.data.hostId)
            try {
                const leaseExpiresAt = await options.admit(parsed.data, attempt.data)
                if (!accepting) return new Response(null, { status: 503 })
                if (
                    leaseExpiresAt === null ||
                    !Number.isSafeInteger(leaseExpiresAt) ||
                    Date.now() >= leaseExpiresAt
                )
                    return new Response(null, { status: 409 })
                const response = await options.forward(parsed.data, forwards.signal)
                const chunks: Uint8Array[] = []
                let bytes = 0
                if (response.body)
                    for await (const chunk of response.body) {
                        bytes += chunk.byteLength
                        if (bytes > 2048)
                            throw new Error('B2 native probe response exceeds raw bound')
                        chunks.push(chunk)
                    }
                return new Response(Buffer.concat(chunks), {
                    status: response.status,
                    headers: { 'content-type': 'application/json' },
                })
            } finally {
                active--
                hosts.delete(parsed.data.hostId)
            }
        } catch {
            return new Response(null, { status: 503 })
        }
    }
    return {
        fetch(request: Request) {
            const work = serve(request).finally(() => pending.delete(work))
            pending.add(work)
            return work
        },
        closeAdmission: () => {
            accepting = false
        },
        active: () => pending.size,
        admissionsClosed: () => !accepting,
        /** Close admission and wait for admitted RPCs; a timeout aborts forwarding and rejects the ACK. */
        async drain(timeoutMs = 12000) {
            accepting = false
            let timer: ReturnType<typeof setTimeout> | undefined
            try {
                const settled = await Promise.race([
                    Promise.allSettled(pending).then(() => true),
                    new Promise<boolean>((resolve) => {
                        timer = setTimeout(() => resolve(false), timeoutMs)
                    }),
                ])
                if (!settled || pending.size !== 0) {
                    forwards.abort()
                    throw new Error('B2 scoped broker tasks failed to quiesce')
                }
            } finally {
                clearTimeout(timer)
            }
        },
    }
}

/** Root records one actual RPC admission against the immutable, live policy claim before network work. */
export async function admitMonitoringB2Probe(
    database: SQL,
    namespace: string,
    request: MonitoringB2ProbeRequest,
    attempt: string,
) {
    if (!/^monitor_b2_[a-f0-9]{32}$/u.test(namespace)) throw new Error('B2 probe namespace invalid')
    return database.begin(async (transaction) => {
        await transaction.unsafe(
            "SET LOCAL lock_timeout='2000ms'; SET LOCAL statement_timeout='2000ms'; SET LOCAL transaction_timeout='2000ms'",
        )
        await transaction.unsafe(
            "SELECT 1 FROM rentnerproxy.system_settings WHERE key='proxy_runtime_editor_v1' FOR SHARE",
        )
        await transaction.unsafe('SELECT pg_advisory_xact_lock(7421916)')
        const rows = await transaction.unsafe<{ expires: string }[]>(
            `SELECT a.lease_token,(a.original_claim->>'leaseExpiresAtMs')::bigint::text expires FROM ${namespace}.admissions a
            JOIN rentnerproxy.host_check_policies p ON p.lease_token=a.lease_token AND p.id=(a.original_claim->>'policyId')::uuid AND p.version=(a.original_claim->>'policyVersion')::uuid
            JOIN rentnerproxy.proxy_hosts h ON h.id=p.proxy_host_id AND h.enabled
            JOIN rentnerproxy.host_domains d ON d.proxy_host_id=h.id AND d.domain=p.domain
            JOIN ${namespace}.contexts c ON c.policy_id=p.id AND c.policy_version=p.version
            JOIN ${namespace}.identity i ON i.id=1
            WHERE a.lease_token=$1 AND a.admitted_ms IS NOT NULL AND a.probe_started_ms IS NULL AND p.enabled AND p.lease_expires_at>clock_timestamp()
            AND p.proxy_host_id=$2 AND p.domain=$3 AND p.timeout_ms=$5 AND a.original_claim->>'hostId'=$2::text AND a.original_claim->>'domain'=$3 AND (a.original_claim->>'timeoutMs')::integer=$5
            AND a.original_claim->'before'->>'revision'=$4 AND a.original_claim->'before'=c.before
            AND (a.original_claim->>'leaseExpiresAtMs')::bigint=floor(extract(epoch FROM p.lease_expires_at)*1000)::bigint
            AND i.store_id=a.original_claim->>'storeId' AND i.epoch=a.original_claim->>'controllerEpoch' AND i.generation=(a.original_claim->>'generation')::bigint
            AND i.generation=i.published_generation AND NOT i.paused AND NOT i.resync_required
            FOR SHARE OF i,p,h,d,c FOR UPDATE OF a`,
            [attempt, request.hostId, request.domain, request.expectedRevision, request.timeoutMs],
        )
        if (rows.length !== 1) return null
        const changed = await transaction.unsafe(
            `UPDATE ${namespace}.admissions SET probe_started_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint WHERE lease_token=$1 AND probe_started_ms IS NULL RETURNING lease_token`,
            [attempt],
        )
        return changed.length === 1 ? Number(rows[0]!.expires) : null
    })
}

/** Bind the scoped broker only inside the root Linux fixture; quiescence also closes its listener. */
export function startMonitoringB2ProbeBroker(options: BrokerOptions) {
    if (
        process.platform !== 'linux' ||
        process.getuid?.() !== 0 ||
        process.env.RENTNERPROXY_MONITORING_B2 !== '1'
    )
        throw new Error('B2 probe broker requires the root Linux fixture')
    const handler = monitoringB2ProbeHandler(options)
    const server = Bun.serve({
        hostname: '127.0.0.1',
        port: 9024,
        maxRequestBodySize: 4096,
        idleTimeout: 12,
        fetch: handler.fetch,
    })
    return {
        ...handler,
        stop: (close: boolean) => server.stop(close),
        async quiesce() {
            handler.closeAdmission()
            server.stop(true)
            await handler.drain()
        },
    }
}
