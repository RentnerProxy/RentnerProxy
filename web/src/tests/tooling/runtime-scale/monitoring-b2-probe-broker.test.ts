import { describe, expect, test } from 'bun:test'
import { randomUUID } from 'node:crypto'
import {
    monitoringB2ProbeHandler,
    type MonitoringB2ProbeRequest,
} from '../../../../../scripts/runtime-scale/monitoring-b2-probe-broker.ts'

const probeKey = 'a'.repeat(64)
const input = (): MonitoringB2ProbeRequest => ({
    hostType: 'proxy',
    hostId: randomUUID(),
    domain: 'probe.fixture.test',
    expectedRevision: 'sha256:' + 'b'.repeat(64),
    upstreamOnly: true,
    timeoutMs: 3000,
})
function request(body: unknown = input(), path = '/internal/v1/diagnostics/domain') {
    return new Request('http://127.0.0.1:9024' + path, {
        method: 'POST',
        headers: {
            authorization: 'Bearer ' + probeKey,
            'content-type': 'application/json',
            'x-b2-attempt': randomUUID(),
        },
        body: JSON.stringify(body),
    })
}

describe('B2 native probe capability boundary', () => {
    test('rejects wrong credentials, management paths and malformed or interactive selectors before authority', async () => {
        let admissions = 0,
            forwards = 0
        const broker = monitoringB2ProbeHandler({
            probeKey,
            admit: async () => {
                admissions++
                return Date.now() + 10000
            },
            forward: async () => {
                forwards++
                return Response.json({ ok: true })
            },
        })
        const credentials = ['', 'Bearer ' + 'c'.repeat(64), 'Bearer ' + 'ä'.repeat(64)]
        const forbidden = ['/internal/v1/proxy/config', '/internal/v1/certificates', '/quiesce']
        const malformed = [
            { ...input(), upstreamOnly: false },
            { ...input(), hostType: 'redirect' },
            { ...input(), url: 'http://unconfigured.invalid/' },
            { ...input(), headers: { authorization: 'arbitrary' } },
            { ...input(), timeoutMs: 10001 },
            { ...input(), hostId: 'invalid' },
        ]
        const credentialResults = await Promise.all(
            credentials.map(async (value) => {
                const call = request()
                call.headers.set('authorization', value)
                return (await broker.fetch(call)).status
            }),
        )
        expect(credentialResults).toEqual([403, 403, 403])
        expect(
            await Promise.all(
                forbidden.map(async (path) => (await broker.fetch(request({}, path))).status),
            ),
        ).toEqual([403, 403, 403])
        expect(
            await Promise.all(
                malformed.map(async (body) => (await broker.fetch(request(body))).status),
            ),
        ).toEqual(malformed.map(() => 400))
        const missingAttempt = request()
        missingAttempt.headers.delete('x-b2-attempt')
        expect((await broker.fetch(missingAttempt)).status).toBe(400)
        expect(
            (
                await broker.fetch(
                    new Request('http://127.0.0.1:9024/internal/v1/diagnostics/domain', {
                        headers: { authorization: 'Bearer ' + probeKey },
                    }),
                )
            ).status,
        ).toBe(405)
        expect(admissions).toBe(0)
        expect(forwards).toBe(0)
        await broker.drain()
    })

    test('forwards only the admitted strict selector and never forwards expired or rejected authority', async () => {
        const admitted: { selector: MonitoringB2ProbeRequest; attempt: string }[] = [],
            forwarded: MonitoringB2ProbeRequest[] = []
        let expiry: number | null = Date.now() + 10000
        const broker = monitoringB2ProbeHandler({
            probeKey,
            admit: async (selector, attempt) => {
                admitted.push({ selector, attempt })
                return expiry
            },
            forward: async (selector) => {
                forwarded.push(selector)
                return Response.json({ ok: true }, { headers: { 'x-private-fixture': 'hidden' } })
            },
        })
        const selector = input(),
            call = request(selector),
            attempt = call.headers.get('x-b2-attempt')!
        call.headers.set('x-forwarded-arbitrary', 'ignored')
        const result = await broker.fetch(call)
        expect(result.status).toBe(200)
        expect(await result.json()).toEqual({ ok: true })
        expect(result.headers.get('x-private-fixture')).toBeNull()
        expect(admitted).toEqual([{ selector, attempt }])
        expect(forwarded).toEqual([selector])
        expiry = null
        expect((await broker.fetch(request())).status).toBe(409)
        expiry = Date.now() - 1
        expect((await broker.fetch(request())).status).toBe(409)
        expect(forwarded).toHaveLength(1)
        await broker.drain()
    })

    test('limits live host execution and scheduled capacity before database or network work', async () => {
        const gate = Promise.withResolvers<void>()
        let admissions = 0,
            forwards = 0
        const broker = monitoringB2ProbeHandler({
            probeKey,
            admit: async () => {
                admissions++
                await gate.promise
                return Date.now() + 10000
            },
            forward: async () => {
                forwards++
                return Response.json({ ok: true })
            },
        })
        const first = input(),
            calls = [broker.fetch(request(first))]
        try {
            await Bun.sleep(0)
            expect((await broker.fetch(request(first))).status).toBe(503)
            calls.push(...Array.from({ length: 63 }, () => broker.fetch(request())))
            await Bun.sleep(0)
            expect((await broker.fetch(request())).status).toBe(503)
            expect(admissions).toBe(64)
        } finally {
            gate.resolve()
            await Promise.all(calls)
            await broker.drain()
        }
        expect(forwards).toBe(64)
        expect(broker.active()).toBe(0)
    })

    test('maintenance waits for pending authority and never admits its later network request', async () => {
        const entered = Promise.withResolvers<void>(),
            release = Promise.withResolvers<void>()
        let forwards = 0,
            drained = false
        const broker = monitoringB2ProbeHandler({
            probeKey,
            admit: async () => {
                entered.resolve()
                await release.promise
                return Date.now() + 10000
            },
            forward: async () => {
                forwards++
                return Response.json({ ok: true })
            },
        })
        const pending = broker.fetch(request())
        await entered.promise
        const drain = broker.drain().then(() => {
            drained = true
        })
        expect((await broker.fetch(request())).status).toBe(503)
        await Promise.resolve()
        expect(drained).toBe(false)
        release.resolve()
        expect((await pending).status).toBe(503)
        await drain
        expect(forwards).toBe(0)
        expect(broker.active()).toBe(0)
    })

    test('response pressure and unsuccessful drain cannot acknowledge safe maintenance', async () => {
        const broker = monitoringB2ProbeHandler({
            probeKey,
            admit: async () => Date.now() + 10000,
            forward: async () => new Response('x'.repeat(2049)),
        })
        expect((await broker.fetch(request())).status).toBe(503)
        await broker.drain()
        const entered = Promise.withResolvers<void>()
        const stuck = monitoringB2ProbeHandler({
            probeKey,
            admit: async () => Date.now() + 10000,
            forward: async (_, signal) => {
                entered.resolve()
                await new Promise<void>((resolve) =>
                    signal.addEventListener('abort', () => resolve(), { once: true }),
                )
                throw new Error('fixture aborted')
            },
        })
        const pending = stuck.fetch(request())
        await entered.promise
        await expect(stuck.drain(1)).rejects.toThrow('failed to quiesce')
        expect((await pending).status).toBe(503)
        await stuck.drain()
        expect(stuck.active()).toBe(0)
    })
})
