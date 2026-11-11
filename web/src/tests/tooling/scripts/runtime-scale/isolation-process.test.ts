import { describe, expect, spyOn, test } from 'bun:test'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
    command,
    createIsolationLineReader,
} from '../../../../../../scripts/runtime-scale/isolation-run.ts'
import { isolationReadySchema } from '../../../../../../scripts/runtime-scale/isolation.validation.ts'

const ready = { ready: true, port: 12345, pid: 123, uid: 20004 }

describe('isolation command ownership and readiness streams', () => {
    test.skipIf(process.platform === 'win32')(
        'a descendant holding both pipes cannot extend command cleanup indefinitely',
        async () => {
            const directory = await mkdtemp(join(tmpdir(), 'isolation-command-'))
            const pidFile = join(directory, 'descendant.pid')
            const spawned = spyOn(Bun, 'spawn')
            let deadline: ReturnType<typeof setTimeout> | undefined
            const started = performance.now()
            const operation = command(
                [
                    process.execPath,
                    '--no-env-file',
                    '-e',
                    "const descendant=Bun.spawn([process.execPath,'--no-env-file','-e','setInterval(()=>{},1000)'],{stdin:'ignore',stdout:'inherit',stderr:'inherit'});await Bun.write(process.argv[1],String(descendant.pid));setInterval(()=>{},1000)",
                    pidFile,
                ],
                750,
            )
            try {
                await expect(
                    Promise.race([
                        operation,
                        new Promise<never>((_, reject) => {
                            deadline = setTimeout(
                                () => reject(new Error('Unbounded cleanup')),
                                3_000,
                            )
                        }),
                    ]),
                ).rejects.toThrow('Isolation fixture deadline')
                expect(performance.now() - started).toBeLessThan(2_500)
                const child = spawned.mock.results[0]!.value as Bun.Subprocess<
                    'ignore',
                    'pipe',
                    'pipe'
                >
                expect(await child.exited).not.toBe(0)
                expect(child.signalCode).toBe('SIGKILL')
                const descendant = Number(await readFile(pidFile, 'utf8'))
                expect(Number.isInteger(descendant) && descendant > 0).toBe(true)
                expect(process.kill(descendant, 0)).toBe(true)
            } finally {
                clearTimeout(deadline)
                spawned.mockRestore()
                const descendant = Number(await readFile(pidFile, 'utf8').catch(() => ''))
                if (Number.isInteger(descendant) && descendant > 0) {
                    try {
                        process.kill(descendant, 'SIGKILL')
                    } catch {}
                }
                await operation.catch(() => {})
                await rm(directory, { recursive: true, force: true })
            }
        },
    )
    test('bounds stalled reader cleanup while still reaping the direct child', async () => {
        const spawned = spyOn(Bun, 'spawn')
        const originalText = Response.prototype.text
        const release = Promise.withResolvers<void>()
        const text = spyOn(Response.prototype, 'text').mockImplementation(async function (
            this: Response,
        ) {
            const result = await originalText.call(this)
            await release.promise
            return result
        })
        try {
            const started = performance.now()
            await expect(
                command([process.execPath, '--no-env-file', '-e', 'setInterval(()=>{},1000)'], 50),
            ).rejects.toThrow('Isolation fixture deadline')
            expect(performance.now() - started).toBeLessThan(1_500)
            const child = spawned.mock.results[0]!.value as Bun.Subprocess<'ignore', 'pipe', 'pipe'>
            expect(await child.exited).not.toBe(0)
            expect(child.signalCode).toBe('SIGKILL')
        } finally {
            release.resolve()
            text.mockRestore()
            spawned.mockRestore()
        }
    })
    test('a deadline kills and reaps the owned child and waits for both output readers', async () => {
        const spawned = spyOn(Bun, 'spawn')
        const originalText = Response.prototype.text
        const drained = Promise.withResolvers<void>()
        let readers = 0
        const text = spyOn(Response.prototype, 'text').mockImplementation(function (
            this: Response,
        ) {
            const reading = originalText.call(this)
            return ++readers === 2
                ? reading.then(async (value) => {
                      await drained.promise
                      return value
                  })
                : reading
        })
        try {
            const operation = command(
                [
                    process.execPath,
                    '--no-env-file',
                    '-e',
                    "process.stdout.write('fixture stdout');process.stderr.write('fixture stderr');process.on('SIGTERM',()=>{});setInterval(()=>{},1000)",
                ],
                50,
            )
            let settled = false
            void operation.then(
                () => {
                    settled = true
                },
                () => {
                    settled = true
                },
            )
            await Bun.sleep(120)
            const child = spawned.mock.results[0]!.value as Bun.Subprocess<'ignore', 'pipe', 'pipe'>
            expect(await child.exited).not.toBe(0)
            expect(child.signalCode).toBe('SIGKILL')
            expect(settled).toBe(false)
            drained.resolve()
            await expect(operation).rejects.toThrow('Isolation fixture deadline')
            expect(settled).toBe(true)
        } finally {
            drained.resolve()
            text.mockRestore()
            const child = spawned.mock.results[0]?.value as
                Bun.Subprocess<'ignore', 'pipe', 'pipe'> | undefined
            spawned.mockRestore()
            if (child && child.exitCode === null && child.signalCode === null) {
                child.kill('SIGKILL')
                await child.exited
            }
        }
    })
    test('preserves successful output and rejects unsuccessful commands', async () => {
        expect(
            await command([
                process.execPath,
                '--no-env-file',
                '-e',
                `process.stdout.write('  fixture result  \\n');process.stderr.write('diagnostic')`,
            ]),
        ).toBe('fixture result')
        expect(
            (
                await command([
                    process.execPath,
                    '--no-env-file',
                    '-e',
                    "await Promise.all([Bun.write(Bun.stdout,'x'.repeat(1048576)),Bun.write(Bun.stderr,'y'.repeat(1048576))])",
                ])
            ).length,
        ).toBe(1048576)
        await expect(
            command([process.execPath, '--no-env-file', '-e', 'process.exit(2)']),
        ).rejects.toThrow('Isolation fixture command failed')
        await expect(
            command([process.execPath, '--no-env-file', '-e', 'process.exit(0)'], 0),
        ).rejects.toThrow('Invalid isolation command timeout')
    })
    test('processes every complete line in one chunk so logs cannot hide readiness', () => {
        const records: unknown[] = []
        const reader = createIsolationLineReader((line) => {
            try {
                records.push(isolationReadySchema.parse(JSON.parse(line)))
            } catch {}
        })
        reader.write(
            new TextEncoder().encode(
                'startup diagnostic\nnot JSON\n' + JSON.stringify(ready) + '\n',
            ),
        )
        expect(records).toEqual([ready])
    })
    test('retains UTF8 characters and JSON across arbitrary byte boundaries and flushes EOF', () => {
        const lines: string[] = []
        const reader = createIsolationLineReader((line) => lines.push(line))
        const expected = ['Gude Büro 👴', JSON.stringify(ready)]
        const bytes = new TextEncoder().encode(expected.join('\n'))
        for (let index = 0; index < bytes.length; index++)
            reader.write(bytes.subarray(index, index + 1))
        reader.finish()
        expect(lines).toEqual(expected)
    })
    test('discards oversized records through their newline without accepting a ready-shaped suffix', () => {
        const lines: string[] = []
        const reader = createIsolationLineReader((line) => lines.push(line))
        reader.write(new TextEncoder().encode('x'.repeat(4097)))
        reader.write(
            new TextEncoder().encode(
                JSON.stringify(ready) + '\n' + JSON.stringify(ready) + '\npartial',
            ),
        )
        expect(lines).toEqual([JSON.stringify(ready)])
        reader.finish()
        expect(lines).toEqual([JSON.stringify(ready), 'partial'])
    })
})
