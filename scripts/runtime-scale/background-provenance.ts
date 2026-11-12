import { createHash } from 'node:crypto'
import { readFile, readdir, realpath } from 'node:fs/promises'
import { dirname, extname, isAbsolute, join, relative, resolve } from 'node:path'
import { BACKGROUND_FIXTURE_SOURCE_PATHS } from './background-source.config.ts'
import {
    RELIABILITY_FIXTURE_ARCHIVE_PATHS,
    RELIABILITY_FIXTURE_SOURCE_PATHS,
} from '../runtime-reliability/fixture-source.config.ts'

async function collect(directory: string): Promise<string[]> {
    const entries = await readdir(directory, { withFileTypes: true })
    const groups = await Promise.all(
        entries.map((entry) => {
            if (entry.isSymbolicLink()) throw new Error('Fixture source must not contain symlinks')
            const path = join(directory, entry.name)
            return entry.isDirectory() ? collect(path) : Promise.resolve([path])
        }),
    )
    return groups.flat()
}

/** Hash only the explicitly staged source tree; paths stay relative and no environment or runtime state is inspected. */
export async function backgroundSourceManifest(root: string) {
    return manifestForPaths(root, await collect(root))
}

/** Independently rebuild the expected fixture manifest from the checked-out archive inputs and staged source allowlist. */
export async function backgroundFixtureSourceManifest(root: string) {
    const paths = await Promise.all(
        RELIABILITY_FIXTURE_ARCHIVE_PATHS.map(async (path) =>
            path === 'web/src' ? collect(join(root, path)) : [join(root, path)],
        ),
    )
    return manifestForPaths(root, [
        ...paths.flat(),
        ...BACKGROUND_FIXTURE_SOURCE_PATHS.map((path) => join(root, path)),
        ...RELIABILITY_FIXTURE_SOURCE_PATHS.map((path) => join(root, path)),
    ])
}

async function manifestForPaths(root: string, sourcePaths: readonly string[]) {
    const paths = [...new Set(sourcePaths)].toSorted()
    if (paths.length > 10_000) throw new Error('Fixture source manifest exceeds bound')
    const files = await Promise.all(
        paths.map(async (path) => ({
            path: relative(root, path).replaceAll('\\', '/'),
            sha256: createHash('sha256')
                .update(await readFile(path))
                .digest('hex'),
        })),
    )
    return { sha256: createHash('sha256').update(JSON.stringify(files)).digest('hex'), files }
}

/** Bind immutable image/source artifacts and predeclared workload inputs without asserting that one cadence proves capacity. */
export function backgroundReportBinding(value: unknown) {
    return createHash('sha256').update(JSON.stringify(value)).digest('hex')
}

/** Follow literal executable imports, including dynamic imports, without executing modules or resolving external packages. */
export async function backgroundAutomationManifest(root: string) {
    const rootPath = await realpath(root)
    const pending = [
        'scripts/runtime-scale/background-run.ts',
        'scripts/runtime-reliability/harness.ts',
        'package.json',
        'bun.lock',
        'tsconfig.json',
        'tsconfig.scripts.json',
    ].map((path) => resolve(rootPath, path))
    const visited = new Set<string>()
    const files: { path: string; sha256: string }[] = []
    while (pending.length) {
        const path = pending.pop()!
        if (visited.has(path)) continue
        if (visited.size >= 10_000) throw new Error('Automation import graph exceeds bound')
        const relativePath = relative(rootPath, path).replaceAll('\\', '/')
        if (
            isAbsolute(relativePath) ||
            relativePath.startsWith('../') ||
            relativePath.split('/').includes('node_modules') ||
            (!['.ts', '.tsx', '.json'].includes(extname(path)) && relativePath !== 'bun.lock')
        )
            throw new Error('Automation input is outside the source boundary')
        // oxlint-disable-next-line no-await-in-loop -- Resolve each newly discovered local source before reading it.
        if ((await realpath(path)) !== path)
            throw new Error('Automation source must not contain symlinks')
        visited.add(path)
        // oxlint-disable-next-line no-await-in-loop -- The exact hashed bytes also supply this module's import scan.
        const contents = await readFile(path)
        files.push({
            path: relativePath,
            sha256: createHash('sha256').update(contents).digest('hex'),
        })
        if (!['.ts', '.tsx'].includes(extname(path))) continue
        const imports = new Bun.Transpiler({
            loader: path.endsWith('.tsx') ? 'tsx' : 'ts',
        }).scanImports(contents)
        for (const item of imports) {
            if (item.path.startsWith('.')) pending.push(resolve(dirname(path), item.path))
            else if (item.path.startsWith('@/'))
                pending.push(resolve(rootPath, 'web/src', item.path.slice(2)))
        }
    }
    files.sort((a, b) => a.path.localeCompare(b.path, 'en'))
    return { sha256: backgroundReportBinding(files), files }
}

/** A source disappearing after cleanup invalidates provenance but must not discard completed measurements or suppress the report. */
export async function recheckBackgroundAutomationManifest(root: string, expectedSha256: string) {
    try {
        const current = await backgroundAutomationManifest(root)
        return {
            unchanged: current.sha256 === expectedSha256,
            sha256: current.sha256,
            status: current.sha256 === expectedSha256 ? ('matched' as const) : ('changed' as const),
        }
    } catch {
        return { unchanged: false, sha256: null, status: 'unavailable' as const }
    }
}
