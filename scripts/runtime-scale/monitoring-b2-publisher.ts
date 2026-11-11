import { createHash } from 'node:crypto'
import { dirname, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { monitoringB2OriginalImport } from './monitoring-b2-bundled.ts'

const declarations = [
    'function contextHash(value)',
    'function createHostMonitoringContext(host, snapshot)',
    'async function readHostMonitoringContextsInTransaction(transaction, inputs)',
    'async function lockMonitoring(transaction)',
    'async function prepareWorkerTransaction(transaction, runtimeSettings = true)',
] as const

/** The pinned original bundle keeps these canonical helpers private. Export their exact declarations,
 * with original singleton dependencies, instead of maintaining another proof/RBAC implementation. */
export function monitoringB2PublicationFragment(owner: string, source: string) {
    const fragments = declarations.map((declaration) => {
        const escaped = declaration.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')
        const matches = [
            ...source.matchAll(new RegExp('^' + escaped + ' \\{[\\s\\S]*?^\\}', 'gmu')),
        ]
        if (matches.length !== 1) throw new Error('B2 original canonical declaration unavailable')
        return matches[0]![0]
    })
    const exported = (name: string) => {
        const clauses = [...source.matchAll(/export\s*\{([^}]+)\}/gu)]
        const matches = clauses
            .flatMap((clause) =>
                clause[1]!.split(',').map((item) => item.trim().split(/\s+as\s+/u)),
            )
            .filter((pair) => pair[0] === name)
        if (matches.length !== 1 || !/^[A-Za-z_$][A-Za-z0-9_$]*$/u.test(matches[0]![1] ?? name))
            throw new Error('B2 original canonical export unavailable')
        return matches[0]![1] ?? name
    }
    const original = (name: string) => {
        const selector = monitoringB2OriginalImport(source, name)
        return `import { ${selector.exported} as ${name} } from ${JSON.stringify(pathToFileURL(resolve(dirname(owner), selector.path)).href)};`
    }
    const imports = [
        `import { ${exported('readProxyHostSummariesInTransaction')} as readProxyHostSummariesInTransaction, ${exported('DomainDiagnosticsError')} as DomainDiagnosticsError } from ${JSON.stringify(pathToFileURL(owner).href)};`,
        original('readProxyRuntimeSnapshot'),
        original('HOST_MONITORING_LIMITS'),
        original('lockProxyRuntimeSettings'),
        `import { sql } from ${JSON.stringify(import.meta.resolve('drizzle-orm'))};`,
    ]
    const body = fragments.join('\n')
    return {
        code: [
            ...imports,
            body,
            'export { readHostMonitoringContextsInTransaction as contexts, prepareWorkerTransaction as prepare };',
        ].join('\n'),
        proof: {
            originalChunkSha256: createHash('sha256').update(source).digest('hex'),
            fragmentSha256: createHash('sha256').update(body).digest('hex'),
            functions: declarations.length,
            originalSingletonImports: true,
        },
    }
}

/** Load the exact validated canonical fragment from a data URL, outside the native-writable store tree. */
export async function loadMonitoringB2Publication(owner: string, source: string) {
    const fragment = monitoringB2PublicationFragment(owner, source)
    // A data URL avoids importing executable code from the shared, lower-trust SQLite directory.
    const module = (await import(
        'data:text/javascript;base64,' + Buffer.from(fragment.code).toString('base64')
    )) as Record<string, unknown>
    if (typeof module.contexts !== 'function' || typeof module.prepare !== 'function')
        throw new Error('B2 original canonical module shape invalid')
    return {
        contexts:
            module.contexts as typeof import('../../web/src/server/RuntimeDiagnostics/domain-diagnostics-data.ts').readHostMonitoringContextsInTransaction,
        prepare:
            module.prepare as typeof import('../../web/src/server/HostMonitoring/host-monitoring.storage.ts').prepareWorkerTransaction,
        proof: fragment.proof,
    }
}
