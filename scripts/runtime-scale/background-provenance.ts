import { createHash } from 'node:crypto'
import { readFile, readdir } from 'node:fs/promises'
import { join, relative } from 'node:path'

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
    const paths = (await collect(root)).toSorted()
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

/** Record the host runner graph separately from archived application inputs; only source files and lock/config files are read. */
export async function backgroundAutomationManifest(root: string) {
    const paths: string[] = ['package.json', 'bun.lock', 'tsconfig.json', 'tsconfig.scripts.json']
    for (const directory of ['scripts/runtime-scale', 'scripts/runtime-reliability']) {
        // oxlint-disable-next-line no-await-in-loop -- Two fixed source owners are enumerated, never runtime state.
        const entries = await readdir(join(root, directory), { withFileTypes: true })
        paths.push(
            ...entries
                .filter((entry) => entry.isFile() && entry.name.endsWith('.ts'))
                .map((entry) => directory + '/' + entry.name),
        )
        // oxlint-disable-next-line no-await-in-loop -- Type sources belong to their preceding fixed owner.
        const types = await readdir(join(root, directory, 'Types'))
        paths.push(
            ...types
                .filter((name) => name.endsWith('.ts'))
                .map((name) => directory + '/Types/' + name),
        )
    }
    const files = await Promise.all(
        paths.toSorted().map(async (path) => ({
            path,
            sha256: createHash('sha256')
                .update(await readFile(join(root, path)))
                .digest('hex'),
        })),
    )
    return { sha256: backgroundReportBinding(files), files }
}
