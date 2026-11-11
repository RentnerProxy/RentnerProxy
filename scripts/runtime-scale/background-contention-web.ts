import { readdir, readFile, stat } from 'node:fs/promises'
import { createClientRpc } from '@tanstack/react-start/client-rpc'
import { z } from 'zod'
import { readBackgroundResourceReport } from '../../web/src/lib/RuntimeDiagnostics/backgroundResourceReport.ts'
import { readBunWorkerReport } from '../../web/src/lib/RuntimeDiagnostics/backgroundWorkerReport.ts'

export function findSupportRpcId(sources: readonly string[]): string | null {
    const ids = new Set(
        sources.flatMap((source) =>
            [
                ...source.matchAll(
                    /createServerRpc\(\{\s*id:\s*["']([a-f0-9]{64})["']\s*,\s*name:\s*["']exportRuntimeSupportReportHandler["']/gu,
                ),
            ].map((match) => match[1]!),
        ),
    )
    return ids.size === 1 ? [...ids][0]! : null
}

export async function probeActualWebWorkers() {
    try {
        const directory = '/opt/rentnerproxy/web/web/dist/server/assets'
        const names = (await readdir(directory))
            .filter((name) => /^middleware-[a-zA-Z0-9_-]+\.js$/u.test(name))
            .slice(0, 64)
        const sources = await Promise.all(
            names.map(async (name) =>
                (await stat(directory + '/' + name)).size <= 2 * 1024 ** 2
                    ? readFile(directory + '/' + name, 'utf8')
                    : '',
            ),
        )
        const id = findSupportRpcId(sources)
        if (!id) return { source: 'unavailable' as const, resources: null, bun: null }
        const state = z
            .object({ session: z.string().min(1).max(256) })
            .parse(JSON.parse(await readFile('/tmp/rentnerproxy-reliability-fixture.json', 'utf8')))
        process.env.TSS_SERVER_FN_BASE = 'http://127.0.0.1:3000/_serverFn/'
        const start = performance.now()
        const response: unknown = await createClientRpc(id)({
            method: 'GET',
            headers: { cookie: 'rentnerproxy_session=' + state.session },
            signal: AbortSignal.timeout(4000),
        })
        const envelope = z.object({ result: z.unknown() }).safeParse(response)
        const report = z
            .object({
                backgroundWorkers: z.object({
                    resources: z.object({ data: z.unknown() }),
                    bun: z.object({ data: z.unknown() }),
                }),
            })
            .parse(envelope.success ? envelope.data.result : response)
        return {
            source: 'authenticated actual web support RPC' as const,
            rpcMs: performance.now() - start,
            resources: readBackgroundResourceReport(report.backgroundWorkers.resources.data),
            bun: readBunWorkerReport(report.backgroundWorkers.bun.data),
        }
    } catch {
        return { source: 'unavailable' as const, resources: null, bun: null }
    }
}

if (import.meta.main) process.stdout.write(JSON.stringify(await probeActualWebWorkers()))
