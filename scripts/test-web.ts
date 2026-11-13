import { readdir } from 'node:fs/promises'
import { relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

type TestMode = '--unit' | '--database' | '--fuzz'

const repositoryRoot = fileURLToPath(new URL('..', import.meta.url))
const testRoot = resolve(repositoryRoot, 'web/src/tests')
const testCommand = [process.execPath, '--no-env-file', 'test', '--max-concurrency=1', '--isolate']
// Windows CreateProcess allows 32,767 UTF-16 code units, including the terminating NUL.
// Count every argument as fully quoted with every character escaped: a conservative
// bound even for executable/path names containing spaces, quotes or backslashes.
const maxCommandLength = 24_000
const argumentLength = (value: string) => value.length * 2 + 3

export function selectWebTests(paths: readonly string[], mode: TestMode): string[] {
    return paths.filter((path) => {
        if (!/\.test\.tsx?$/u.test(path)) return false
        if (mode === '--database') return path.endsWith('.db.test.ts')
        if (mode === '--fuzz') return path.endsWith('.fuzz.test.ts')
        return !path.endsWith('.db.test.ts') && !path.endsWith('.fuzz.test.ts')
    }).toSorted()
}

async function collectTests(directory: string): Promise<string[]> {
    const entries = await readdir(directory, { withFileTypes: true })
    const files = await Promise.all(
        entries.map((entry) => {
            const path = resolve(directory, entry.name)
            return entry.isDirectory() ? collectTests(path) : Promise.resolve([path])
        }),
    )
    return files.flat()
}

/** Run isolated suites sequentially without exceeding the Windows command-line limit. */
export async function runWebTestBatches(
    tests: readonly string[],
    execute: (command: string[]) => Promise<number>,
    command: readonly string[] = testCommand,
): Promise<number> {
    if (tests.length === 0) throw new Error('No matching web tests found.')
    const prefixLength = 1 + command.reduce((length, part) => length + argumentLength(part), 0)
    const batches: string[][] = []
    let batch: string[] = []
    let length = prefixLength
    for (const path of tests) {
        const addedLength = argumentLength(path)
        if (prefixLength + addedLength > maxCommandLength)
            throw new Error('A web test path exceeds the command-line limit.')
        if (length + addedLength > maxCommandLength) {
            batches.push(batch)
            batch = []
            length = prefixLength
        }
        batch.push(path)
        length += addedLength
    }
    batches.push(batch)
    for (const paths of batches) {
        // oxlint-disable-next-line no-await-in-loop -- Preserve isolation and stop before launching another batch after failure.
        const exitCode = await execute([...command, ...paths])
        if (exitCode !== 0) return exitCode
    }
    return 0
}

if (import.meta.main) {
    const mode = process.argv[2] ?? '--unit'
    if (mode !== '--unit' && mode !== '--database' && mode !== '--fuzz') {
        throw new Error('Expected --unit, --database, or --fuzz.')
    }
    if (mode === '--database' && process.env.RENTNERPROXY_DATABASE_INTEGRATION !== '1') {
        throw new Error('Database tests require an explicitly configured isolated integration database.')
    }
    const tests = selectWebTests(await collectTests(testRoot), mode)
        .map((path) => `./${relative(repositoryRoot, path).replaceAll('\\', '/')}`)
    process.exitCode = await runWebTestBatches(tests, async (command) => {
        const child = Bun.spawn(command, {
            cwd: repositoryRoot,
            env:
                mode === '--unit' && !process.env.DATABASE_URL
                    ? {
                          ...process.env,
                          // Unit modules construct a lazy SQL client; their probes are injected.
                          DATABASE_URL: 'postgresql://unit:unit@127.0.0.1:1/rentnerproxy_unit',
                      }
                    : process.env,
            stdin: 'inherit',
            stdout: 'inherit',
            stderr: 'inherit',
        })
        return child.exited
    })
}
