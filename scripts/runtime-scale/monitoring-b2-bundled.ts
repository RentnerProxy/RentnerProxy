import { readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

/** Resolves named original imports rather than assuming unstable Rollup export aliases. */
export function monitoringB2OriginalImport(source: string, name: string) {
    const matches = [...source.matchAll(/import\s*\{([^}]+)\}\s*from\s*["']([^"']+)["']/gu)]
    for (const match of matches)
        for (const item of match[1]!.split(',')) {
            const pair = item.trim().split(/\s+as\s+/u)
            if ((pair[1] ?? pair[0]) === name && /^\.[./][a-zA-Z0-9_.-]+\.js$/u.test(match[2]!))
                return { path: match[2]!, exported: pair[0]! }
        }
    throw new Error('B2 original application import unavailable')
}

/** Resolve one relative original bundle binding and load its existing singleton rather than a replacement. */
async function dependency(owner: string, contents: string, name: string) {
    const selector = monitoringB2OriginalImport(contents, name),
        path = resolve(dirname(owner), selector.path)
    const loaded: Record<string, unknown> = await import(pathToFileURL(path).href)
    return { path, value: loaded[selector.exported], source: await readFile(path, 'utf8') }
}

/** Loads the actual application's already shared modules and pool; the fixture introduces no second implementation. */
export async function loadMonitoringB2Application(workerPath: string) {
    const source = await readFile(workerPath, 'utf8')
    const completion = await dependency(workerPath, source, 'completeHostMonitoringClaims')
    const background = await dependency(workerPath, source, 'getBackgroundDatabase')
    const database = await dependency(background.path, background.source, 'getAuthDatabase')
    const policy = await dependency(completion.path, completion.source, 'hostCheckPolicies')
    const contexts = await dependency(workerPath, source, 'readHostMonitoringClaimContexts')
    const classify = await dependency(workerPath, source, 'classifyHostMonitoringOutcome')
    const unknown = await dependency(workerPath, source, 'unknownHostMonitoringOutcome')
    const resultSchema = await dependency(workerPath, source, 'domainDiagnosticsResultSchema')
    if (
        [completion, database, background, contexts, classify, unknown].some(
            (item) => typeof item.value !== 'function',
        )
    )
        throw new Error('B2 original application export shape mismatch')
    return {
        complete:
            completion.value as typeof import('../../web/src/server/HostMonitoring/host-monitoring.execution.ts').completeHostMonitoringClaims,
        getDatabase:
            database.value as typeof import('../../web/src/server/Auth/Core/database.server.ts').getAuthDatabase,
        background: background.value as (
            resource: 'monitoring',
        ) => Pick<
            ReturnType<
                typeof import('../../web/src/server/Auth/Core/database.server.ts').getAuthDatabase
            >,
            'transaction'
        >,
        policies: policy.value as typeof import('../../web/src/db/schema.ts').hostCheckPolicies,
        contexts:
            contexts.value as typeof import('../../web/src/server/HostMonitoring/host-monitoring.execution.ts').readHostMonitoringClaimContexts,
        classify:
            classify.value as typeof import('../../web/src/lib/HostMonitoring/state.ts').classifyHostMonitoringOutcome,
        unknown:
            unknown.value as typeof import('../../web/src/lib/HostMonitoring/state.ts').unknownHostMonitoringOutcome,
        resultSchema:
            resultSchema.value as typeof import('../../web/src/lib/RuntimeDiagnostics/domainDiagnostics.ts').domainDiagnosticsResultSchema,
        publication: async () => {
            const { loadMonitoringB2Publication } = await import('./monitoring-b2-publisher.ts')
            return loadMonitoringB2Publication(completion.path, completion.source)
        },
    }
}
