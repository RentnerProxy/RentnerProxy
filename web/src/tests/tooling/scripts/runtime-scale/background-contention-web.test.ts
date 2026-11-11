import { expect, test } from 'bun:test'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
    decodeSupportRpcResponse,
    readSupportRpcMetadata,
    requestSupportRpc,
    supportProbeDiagnosticSchema,
} from '../../../../../../scripts/runtime-scale/background-contention-web.ts'
import { contentionWebSchema } from '../../../../../../scripts/runtime-scale/background-contention.ts'
import { createWorkerObservability } from '../../../../server/Foundation/worker-observability.ts'

const privateValue = 'private-fixture-token-must-never-leave-probe'
const id = 'a'.repeat(64)
const metadata = (value: string) =>
    `createServerRpc({id:"${value}",name:"exportRuntimeSupportReportHandler"})`
function supportReport() {
    const usage = {
        running: 0,
        pending: 0,
        oldestPendingAgeMs: 0,
        busy: 0,
        deadline: 0,
        cancelled: 0,
        admitted: 1,
    }
    const workers = createWorkerObservability({ now: () => 1000 })
    workers.eventLoopDelay(2)
    return {
        privateValue,
        backgroundWorkers: {
            resources: {
                state: 'available',
                data: {
                    sampledAt: 1000,
                    database: {
                        monitoring: { ...usage },
                        notifications: { ...usage },
                        discovery: { ...usage },
                        housekeeping: { ...usage },
                        critical: { ...usage },
                    },
                    privateValue,
                },
            },
            bun: { state: 'available', data: workers.snapshot() },
        },
    }
}

test('metadata scan includes the matching handler beyond the former first 64 files and fails closed on ambiguity', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'background-rpc-metadata-'))
    try {
        await Promise.all(
            Array.from({ length: 65 }, (_, index) =>
                writeFile(
                    join(directory, 'middleware-' + String(index).padStart(3, '0') + '.js'),
                    index === 64 ? metadata(id) : '',
                ),
            ),
        )
        expect(await readSupportRpcMetadata(directory)).toEqual({ id })
        await writeFile(
            join(directory, 'middleware-064.js'),
            metadata(id) + metadata('b'.repeat(64)),
        )
        expect(await readSupportRpcMetadata(directory)).toMatchObject({
            diagnostic: { stage: 'metadata', reason: 'handler-ambiguous' },
        })
        await writeFile(join(directory, 'middleware-064.js'), '')
        expect(await readSupportRpcMetadata(directory)).toMatchObject({
            diagnostic: { stage: 'metadata', reason: 'handler-not-found' },
        })
        await writeFile(join(directory, 'middleware-064.js'), 'x'.repeat(2 * 1024 ** 2 + 1))
        expect(await readSupportRpcMetadata(directory)).toMatchObject({
            diagnostic: { stage: 'metadata', reason: 'asset-limit' },
        })
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
    expect(await readSupportRpcMetadata(join(directory, privateValue))).toEqual({
        source: 'unavailable',
        resources: null,
        bun: null,
        diagnostic: { stage: 'metadata', reason: 'asset-read-failed' },
    })
})

test('direct and SDK enveloped reports preserve actual numeric snapshots and strip unrelated report content', () => {
    const direct = decodeSupportRpcResponse(supportReport())
    expect(direct).toMatchObject({
        source: 'authenticated actual web support RPC',
        resources: { database: { monitoring: { admitted: 1 } } },
        bun: { eventLoopLag: { count: 1, maximumMs: 2 } },
    })
    expect(
        decodeSupportRpcResponse({ result: supportReport(), context: { privateValue } }),
    ).toEqual(direct)
    expect(decodeSupportRpcResponse({ result: supportReport(), error: null })).toEqual(direct)
    expect(JSON.stringify(direct)).not.toContain(privateValue)
})

test('SDK errors and malformed reports become fixed diagnostics with unknown snapshots', () => {
    for (const error of [new Error(privateValue), { message: privateValue }, false, 0, '']) {
        const result = decodeSupportRpcResponse({ result: supportReport(), error })
        expect(result).toEqual({
            source: 'unavailable',
            resources: null,
            bun: null,
            diagnostic: { stage: 'protocol', reason: 'remote-error' },
        })
        expect(JSON.stringify(result)).not.toContain(privateValue)
    }
    expect(decodeSupportRpcResponse({ result: undefined })).toMatchObject({
        diagnostic: { stage: 'schema', reason: 'invalid-report' },
    })
    const report = supportReport()
    report.backgroundWorkers.resources.data.database.monitoring.admitted = -1
    expect(decodeSupportRpcResponse(report)).toMatchObject({
        diagnostic: { stage: 'schema', reason: 'invalid-resources' },
    })
    const invalidWorkers = supportReport()
    invalidWorkers.backgroundWorkers.bun.data.workers = []
    expect(decodeSupportRpcResponse(invalidWorkers)).toMatchObject({
        diagnostic: { stage: 'schema', reason: 'invalid-workers' },
    })
})

test('standalone SDK actually reaches HTTP with private session and a matching same-origin header', async () => {
    let calls = 0
    const result = await requestSupportRpc(id, privateValue, async (url, init) => {
        calls++
        expect(url).toBe('http://127.0.0.1:3000/_serverFn/' + id)
        const headers = new Headers(init.headers)
        expect(headers.get('cookie') === 'rentnerproxy_session=' + privateValue).toBe(true)
        expect(headers.get('origin')).toBe(new URL(url).origin)
        expect(headers.get('x-tsr-serverFn')).toBe('true')
        return Response.json({ result: supportReport() })
    })
    expect(calls).toBe(1)
    expect(result).toMatchObject({
        source: 'authenticated actual web support RPC',
        resources: { sampledAt: 1000 },
    })
    expect(JSON.stringify(result)).not.toContain(privateValue)
})

test('HTTP permission/protocol failures cancel private bodies and retain no response or error text', async () => {
    await Promise.all(
        (
            [
                [401, 'application/json', 'permission', 'unauthorized'],
                [403, 'text/html', 'permission', 'forbidden'],
                [503, 'text/html', 'protocol', 'http-error'],
                [200, 'text/html', 'protocol', 'unexpected-content-type'],
            ] as const
        ).map(async ([status, contentType, stage, reason]) => {
            let cancelled = false
            const body = new ReadableStream({
                cancel() {
                    cancelled = true
                },
            })
            const result = await requestSupportRpc(
                id,
                privateValue,
                async () =>
                    new Response(body, { status, headers: { 'content-type': contentType } }),
            )
            expect(cancelled).toBe(true)
            expect(result).toEqual({
                source: 'unavailable',
                resources: null,
                bun: null,
                diagnostic: { stage, reason },
            })
            expect(JSON.stringify(result)).not.toContain(privateValue)
        }),
    )
    const malformed = await requestSupportRpc(
        id,
        privateValue,
        async () => new Response(privateValue, { headers: { 'content-type': 'application/json' } }),
    )
    expect(malformed).toMatchObject({ diagnostic: { stage: 'protocol', reason: 'decode-failed' } })
    await Promise.all(
        [new Error(privateValue), null, undefined, false].map(async (error) => {
            expect(await requestSupportRpc(id, privateValue, () => Promise.reject(error))).toEqual({
                source: 'unavailable',
                resources: null,
                bun: null,
                diagnostic: { stage: 'transport', reason: 'request-failed' },
            })
        }),
    )
})

test('report validation accepts fixed probe diagnostics and rejects arbitrary messages or categories', () => {
    const failure = {
        source: 'unavailable',
        resources: null,
        bun: null,
        diagnostic: { stage: 'permission', reason: 'forbidden' },
    } as const
    expect(contentionWebSchema.parse(failure)).toEqual(failure)
    expect(
        supportProbeDiagnosticSchema.safeParse({ stage: 'protocol', reason: privateValue }).success,
    ).toBe(false)
    expect(
        supportProbeDiagnosticSchema.safeParse({
            stage: 'protocol',
            reason: 'remote-error',
            message: privateValue,
        }).success,
    ).toBe(false)
})
