import { expect, test } from 'bun:test'
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

test('interrupted child snapshot writes never publish partial JSON or prevent later complete snapshots', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'rp-coverage-snapshot-'))
    const output = join(directory, 'coverage'),
        manifest = join(directory, 'manifest.json'),
        script = join(directory, 'writer.ts')
    const collector = fileURLToPath(
        new URL('../../../../../../scripts/coverage/collector.ts', import.meta.url),
    )
    try {
        await writeFile(
            manifest,
            JSON.stringify({
                repositoryRoot: directory,
                cacheDirectory: join(directory, 'instrumented'),
                outputDirectory: output,
                files: [],
            }),
        )
        await writeFile(
            script,
            [
                'import { syncBuiltinESMExports } from "node:module"',
                'const fs = require("node:fs")',
                'const write = fs.writeFileSync',
                'if (process.argv[2] === "interrupt") {',
                '    fs.writeFileSync = (path, data, options) => {',
                '        write(path, String(data).slice(0, 1), options)',
                '        process.exit(23)',
                '    }',
                '    syncBuiltinESMExports()',
                '}',
                `const { installCoverageCollector, writeCoverageSnapshot } = await import(${JSON.stringify(collector)})`,
                'if (!installCoverageCollector()) throw new Error("Missing collector")',
                'writeCoverageSnapshot()',
            ].join('\n'),
        )
        const run = async (mode: string) => {
            const child = Bun.spawn([process.execPath, '--no-env-file', script, mode], {
                env: {
                    ...process.env,
                    BUN_OPTIONS: '',
                    RENTNERPROXY_COVERAGE_MANIFEST: manifest,
                },
                stdin: 'ignore',
                stdout: 'ignore',
                stderr: 'pipe',
            })
            const [code, stderr] = await Promise.all([
                child.exited,
                new Response(child.stderr).text(),
            ])
            return { code, stderr }
        }
        expect(await run('interrupt')).toEqual({ code: 23, stderr: '' })
        const raw = join(output, 'raw'),
            interrupted = await readdir(raw)
        expect(interrupted).toHaveLength(1)
        expect(await readFile(join(raw, interrupted[0]!), 'utf8')).toBe('{')
        expect(interrupted.filter((name) => name.endsWith('.json'))).toEqual([])
        expect(await run('complete')).toEqual({ code: 0, stderr: '' })
        const published = (await readdir(raw)).filter((name) => name.endsWith('.json'))
        expect(published).toHaveLength(1)
        expect(JSON.parse(await readFile(join(raw, published[0]!), 'utf8'))).toEqual({})
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})
