import { readdir, readFile, stat } from 'node:fs/promises'
import { createClientRpc } from '@tanstack/react-start/client-rpc'
import { runWithStartContext } from '@tanstack/start-storage-context'
import { z } from 'zod'
import { readBackgroundResourceReport } from '../../web/src/lib/RuntimeDiagnostics/backgroundResourceReport.ts'
import { readBunWorkerReport } from '../../web/src/lib/RuntimeDiagnostics/backgroundWorkerReport.ts'

export const supportProbeDiagnosticSchema = z.strictObject({
    stage: z.enum(['metadata', 'session', 'transport', 'permission', 'protocol', 'schema']),
    reason: z.enum([
        'asset-read-failed',
        'asset-limit',
        'handler-not-found',
        'handler-ambiguous',
        'session-unavailable',
        'request-failed',
        'request-timeout',
        'unauthorized',
        'forbidden',
        'http-error',
        'unexpected-content-type',
        'decode-failed',
        'remote-error',
        'invalid-report',
        'invalid-resources',
        'invalid-workers',
    ]),
})
type Diagnostic = z.output<typeof supportProbeDiagnosticSchema>
class ProbeFailure {
    constructor(readonly diagnostic: Diagnostic) {}
}
const unavailable = (diagnostic: Diagnostic) => ({
    source: 'unavailable' as const,
    resources: null,
    bun: null,
    diagnostic,
})

const reportSchema = z.object({
    backgroundWorkers: z.object({
        resources: z.object({ state: z.literal('available'), data: z.unknown() }),
        bun: z.object({ state: z.literal('available'), data: z.unknown() }),
    }),
})

/** Only allowlisted numeric snapshots leave this process, never the private support report. */
export function decodeSupportRpcResponse(response: unknown) {
    const envelope = z.record(z.string(), z.unknown()).safeParse(response)
    if (envelope.success && envelope.data.error !== undefined && envelope.data.error !== null)
        return unavailable({ stage: 'protocol', reason: 'remote-error' })
    const value =
        envelope.success && Object.hasOwn(envelope.data, 'result') ? envelope.data.result : response
    const report = reportSchema.safeParse(value)
    if (!report.success) return unavailable({ stage: 'schema', reason: 'invalid-report' })
    const resources = readBackgroundResourceReport(report.data.backgroundWorkers.resources.data)
    if (!resources) return unavailable({ stage: 'schema', reason: 'invalid-resources' })
    const bun = readBunWorkerReport(report.data.backgroundWorkers.bun.data)
    if (!bun) return unavailable({ stage: 'schema', reason: 'invalid-workers' })
    return { source: 'authenticated actual web support RPC' as const, resources, bun }
}

/** The installed SDK owns serialization; the wrapper inspects only HTTP protocol metadata. */
export async function requestSupportRpc(
    id: string,
    session: string,
    transport: (url: string, init: RequestInit) => Promise<Response> = fetch,
) {
    const start = performance.now()
    const signal = AbortSignal.timeout(4000)
    try {
        process.env.TSS_SERVER_FN_BASE = 'http://127.0.0.1:3000/_serverFn/'
        // The standalone Bun SDK selects its server implementation of getStartOptions.
        // Supply only transport options, never a local service/authentication context.
        // Every measured snapshot still comes from the authenticated HTTP request below.
        const response: unknown = await runWithStartContext(
            {
                getRouter: () => {
                    throw new ProbeFailure({ stage: 'protocol', reason: 'decode-failed' })
                },
                request: new Request('http://127.0.0.1:3000/_serverFn/' + id),
                startOptions: {},
                contextAfterGlobalMiddlewares: {},
                handlerType: 'serverFn',
            },
            () =>
                createClientRpc(id)({
                    method: 'GET',
                    headers: {
                        cookie: 'rentnerproxy_session=' + session,
                        origin: 'http://127.0.0.1:3000',
                    },
                    signal,
                    fetch: async (url: string, init: RequestInit) => {
                        let httpResponse: Response
                        try {
                            httpResponse = await transport(url, init)
                        } catch {
                            throw new ProbeFailure({
                                stage: 'transport',
                                reason: signal.aborted ? 'request-timeout' : 'request-failed',
                            })
                        }
                        const contentType = httpResponse.headers.get('content-type') ?? ''
                        const reason =
                            httpResponse.status === 401
                                ? 'unauthorized'
                                : httpResponse.status === 403
                                  ? 'forbidden'
                                  : !httpResponse.ok
                                    ? 'http-error'
                                    : null
                        if (
                            reason ||
                            !/^(application\/json|application\/x-ndjson|application\/x-tss-framed)(?:;|$)/u.test(
                                contentType,
                            )
                        ) {
                            await httpResponse.body?.cancel().catch(() => {})
                            throw new ProbeFailure({
                                stage:
                                    reason === 'unauthorized' || reason === 'forbidden'
                                        ? 'permission'
                                        : 'protocol',
                                reason: reason ?? 'unexpected-content-type',
                            })
                        }
                        return httpResponse
                    },
                }),
        )
        return { ...decodeSupportRpcResponse(response), rpcMs: performance.now() - start }
    } catch (error) {
        return unavailable(
            error instanceof ProbeFailure
                ? error.diagnostic
                : {
                      stage: 'protocol',
                      reason: signal.aborted ? 'request-timeout' : 'decode-failed',
                  },
        )
    }
}

function supportRpcIds(sources: readonly string[]) {
    return new Set(
        sources.flatMap((source) =>
            [
                ...source.matchAll(
                    /createServerRpc\(\{\s*id:\s*["']([a-f0-9]{64})["']\s*,\s*name:\s*["']exportRuntimeSupportReportHandler["']/gu,
                ),
            ].map((match) => match[1]!),
        ),
    )
}

export function findSupportRpcId(sources: readonly string[]): string | null {
    const ids = supportRpcIds(sources)
    return ids.size === 1 ? [...ids][0]! : null
}

export async function readSupportRpcMetadata(directory: string) {
    try {
        const names = (await readdir(directory))
            .filter((name) => /^middleware-[a-zA-Z0-9_-]+\.js$/u.test(name))
            .toSorted()
        if (names.length > 4096) return unavailable({ stage: 'metadata', reason: 'asset-limit' })
        let totalBytes = 0
        const sources = await Promise.all(
            names.map(async (name) => {
                const size = (await stat(directory + '/' + name)).size
                totalBytes += size
                if (size > 2 * 1024 ** 2 || totalBytes > 64 * 1024 ** 2)
                    throw new ProbeFailure({ stage: 'metadata', reason: 'asset-limit' })
                const source = await readFile(directory + '/' + name, 'utf8')
                if (Buffer.byteLength(source) > 2 * 1024 ** 2)
                    throw new ProbeFailure({ stage: 'metadata', reason: 'asset-limit' })
                return source
            }),
        )
        const ids = supportRpcIds(sources)
        if (ids.size !== 1)
            return unavailable({
                stage: 'metadata',
                reason: ids.size > 1 ? 'handler-ambiguous' : 'handler-not-found',
            })
        return { id: [...ids][0]! }
    } catch (error) {
        return unavailable(
            error instanceof ProbeFailure
                ? error.diagnostic
                : { stage: 'metadata', reason: 'asset-read-failed' },
        )
    }
}

export async function probeActualWebWorkers() {
    const metadata = await readSupportRpcMetadata('/opt/rentnerproxy/web/web/dist/server/assets')
    if (!('id' in metadata)) return metadata
    try {
        const state = z
            .object({ session: z.string().min(1).max(256) })
            .parse(JSON.parse(await readFile('/tmp/rentnerproxy-reliability-fixture.json', 'utf8')))
        return await requestSupportRpc(metadata.id, state.session)
    } catch {
        return unavailable({ stage: 'session', reason: 'session-unavailable' })
    }
}

if (import.meta.main) process.stdout.write(JSON.stringify(await probeActualWebWorkers()))
