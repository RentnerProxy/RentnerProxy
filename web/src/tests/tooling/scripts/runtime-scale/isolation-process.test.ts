// oxlint-disable no-await-in-loop -- PostgreSQL readiness and isolated cleanup failures are checked in order.
import { describe, expect, spyOn, test } from 'bun:test'
import { chown, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
    command,
    readIsolationCompletion,
    createIsolationLineReader,
    IsolationCommandError,
    stopIsolationPostgres,
    finishIsolationFixture,
    waitForIsolationPostgres,
    withIsolationCancellation,
} from '../../../../../../scripts/runtime-scale/isolation-run.ts'
import { isolationReadySchema } from '../../../../../../scripts/runtime-scale/isolation.validation.ts'

const ready = { ready: true, port: 12345, pid: 123, uid: 20004 }

describe('isolation PostgreSQL cleanup ownership', () => {
    test('readiness requires the owned PID, data directory, launch time, port and live child', async () => {
        const directory = await mkdtemp(join(tmpdir(), 'isolation-ready-'))
        await mkdir(directory + '/pg')
        const launchedAt = Date.now(),
            epoch = Math.floor(launchedAt / 1_000)
        const child = { pid: 123, exitCode: null, signalCode: null }
        const record = [
            '123',
            directory + '/pg',
            String(epoch),
            '55439',
            '/tmp',
            '127.0.0.1',
            '0',
            'ready',
        ]
        try {
            await writeFile(directory + '/pg/postmaster.pid', record.join('\n'))
            await waitForIsolationPostgres(directory, child, launchedAt)
            for (const [index, value] of [
                [0, '999'],
                [1, '/foreign/pg'],
                [2, String(epoch - 1)],
                [2, String(epoch + 60)],
                [3, '55438'],
            ] as const) {
                const invalid = [...record]
                invalid[index] = value
                await writeFile(directory + '/pg/postmaster.pid', invalid.join('\n'))
                await expect(
                    waitForIsolationPostgres(directory, child, launchedAt),
                ).rejects.toThrow('identity mismatch')
            }
            await writeFile(directory + '/pg/postmaster.pid', record.join('\n'))
            await expect(
                waitForIsolationPostgres(directory, { ...child, exitCode: 1 }, launchedAt),
            ).rejects.toThrow('exited before readiness')
        } finally {
            await rm(directory, { recursive: true, force: true })
        }
    })
    test('a failed launch before a process exists accepts only explicit stopped status', async () => {
        let removed = false
        await finishIsolationFixture(
            [
                () =>
                    stopIsolationPostgres('/tmp/fixture', undefined, async (args) => {
                        expect(args.at(-1)).toBe('status')
                        throw new IsolationCommandError(3)
                    }),
            ],
            async () => {
                removed = true
            },
            undefined,
        )
        expect(removed).toBe(true)
    })
    test('unknown status or a live cluster retains data and preserves both failures', async () => {
        for (const outcome of [0, 4, 'timeout'] as const) {
            const primary = new Error('readiness failed after start')
            let removed = false,
                laterStage = false
            let failure: unknown
            try {
                await finishIsolationFixture(
                    [
                        () =>
                            stopIsolationPostgres('/tmp/fixture', undefined, async () => {
                                if (outcome === 0) return 'server is running'
                                if (outcome === 'timeout') throw new Error('status deadline')
                                throw new IsolationCommandError(outcome)
                            }),
                        async () => {
                            laterStage = true
                        },
                    ],
                    async () => {
                        removed = true
                    },
                    { error: primary },
                )
            } catch (error) {
                failure = error
            }
            expect(removed).toBe(false)
            expect(laterStage).toBe(true)
            expect(failure).toBeInstanceOf(AggregateError)
            expect((failure as AggregateError).errors[0]).toBe(primary)
            expect((failure as AggregateError).errors[1]).toBeInstanceOf(AggregateError)
        }
    })
    test('a stop failure still reaps the owned child, retains data and does not hide the primary error', async () => {
        const child = Bun.spawn(
            [process.execPath, '--no-env-file', '-e', 'setInterval(()=>{},1000)'],
            { stdout: 'ignore', stderr: 'ignore' },
        )
        const primary = new Error('readiness timeout'),
            stopError = new Error('stop failed')
        let removed = false,
            failure: unknown
        try {
            await finishIsolationFixture(
                [
                    async () => {
                        throw new Error('network cleanup failed')
                    },
                    () =>
                        stopIsolationPostgres(
                            '/tmp/fixture',
                            {
                                get exitCode() {
                                    return child.exitCode
                                },
                                get signalCode() {
                                    return child.signalCode
                                },
                                exited: child.exited,
                                kill() {
                                    child.kill('SIGKILL')
                                    throw stopError
                                },
                            },
                            async () => {
                                throw new IsolationCommandError(3)
                            },
                        ),
                ],
                async () => {
                    removed = true
                },
                { error: primary },
            )
        } catch (error) {
            failure = error
        } finally {
            if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
            await child.exited
        }
        expect(removed).toBe(false)
        expect(await child.exited).not.toBe(0)
        expect((failure as AggregateError).errors[0]).toBe(primary)
        expect(((failure as AggregateError).errors[2] as AggregateError).errors).toContain(
            stopError,
        )
    })
    test.skipIf(
        process.platform !== 'linux' || process.env.RENTNERPROXY_ISOLATION_POSTGRES_TEST !== '1',
    )(
        'rejects a foreign PostgreSQL listener and reaps only owned postmasters after failed readiness',
        async () => {
            const directory = await mkdtemp('/tmp/rentnerproxy-isolation-pg-cleanup-')
            const uid = Number(await command(['id', '-u', 'postgres']))
            const gid = Number(await command(['id', '-g', 'postgres']))
            await chown(directory, uid, gid)
            let postgres: Bun.Subprocess<'ignore', 'ignore', 'ignore'> | undefined
            let contender: Bun.Subprocess<'ignore', 'ignore', 'ignore'> | undefined
            const other = await mkdtemp('/tmp/rentnerproxy-isolation-pg-contender-')
            await chown(other, uid, gid)
            let removed = false
            try {
                await command([
                    'gosu',
                    'postgres',
                    'initdb',
                    '-D',
                    directory + '/pg',
                    '--auth=trust',
                    '--no-instructions',
                ])
                const launchedAt = Date.now()
                postgres = Bun.spawn(
                    [
                        'gosu',
                        'postgres',
                        'postgres',
                        '-D',
                        directory + '/pg',
                        '-h',
                        '127.0.0.1',
                        '-p',
                        '55439',
                    ],
                    { stdin: 'ignore', stdout: 'ignore', stderr: 'ignore' },
                )
                await waitForIsolationPostgres(directory, postgres, launchedAt)
                await command(['pg_isready', '-h', '127.0.0.1', '-p', '55439', '-t', '1'])
                await command([
                    'gosu',
                    'postgres',
                    'initdb',
                    '-D',
                    other + '/pg',
                    '--auth=trust',
                    '--no-instructions',
                ])
                const contenderLaunchedAt = Date.now()
                contender = Bun.spawn(
                    [
                        'gosu',
                        'postgres',
                        'postgres',
                        '-D',
                        other + '/pg',
                        '-h',
                        '127.0.0.1',
                        '-p',
                        '55439',
                    ],
                    { stdin: 'ignore', stdout: 'ignore', stderr: 'ignore' },
                )
                let mutationsAllowed = false
                await expect(
                    waitForIsolationPostgres(other, contender, contenderLaunchedAt).then(() => {
                        mutationsAllowed = true
                    }),
                ).rejects.toThrow('exited before readiness')
                expect(mutationsAllowed).toBe(false)
                await stopIsolationPostgres(other, contender)
                contender = undefined
                await rm(other, { recursive: true, force: true })
                expect(
                    await command([
                        'psql',
                        '-h',
                        '127.0.0.1',
                        '-p',
                        '55439',
                        '-U',
                        'postgres',
                        '-d',
                        'postgres',
                        '-At',
                        '-c',
                        "select count(*) from pg_database where datname='rentnerproxy_upstream_test'",
                    ]),
                ).toBe('0')
                await waitForIsolationPostgres(directory, postgres, launchedAt)
                const started = await command([
                    'gosu',
                    'postgres',
                    'pg_ctl',
                    '-D',
                    directory + '/pg',
                    'status',
                ])
                expect(started).toContain(String(postgres.pid))
                const primary = new Error('injected readiness failure after actual server start')
                await finishIsolationFixture(
                    [() => stopIsolationPostgres(directory, postgres)],
                    async () => {
                        expect(await postgres!.exited).toBe(0)
                        await expect(
                            command([
                                'gosu',
                                'postgres',
                                'pg_ctl',
                                '-D',
                                directory + '/pg',
                                'status',
                            ]),
                        ).rejects.toMatchObject({ exitCode: 3 })
                        await rm(directory, { recursive: true, force: true })
                        removed = true
                    },
                    { error: primary },
                )
                expect(removed).toBe(true)
            } finally {
                if (contender) await stopIsolationPostgres(other, contender)
                await rm(other, { recursive: true, force: true })
                if (!removed) {
                    if (postgres) await stopIsolationPostgres(directory, postgres)
                    await rm(directory, { recursive: true, force: true })
                }
            }
        },
        20_000,
    )
})

describe('isolation command ownership and readiness streams', () => {
    test('drain waits for a late durable commit and reports its original latency', async () => {
        const commit = Promise.withResolvers<void>()
        const metrics = {
            pid: 123,
            uid: 20004,
            rssBytes: 1024,
            cpuMicroseconds: 100,
            fds: 4,
            lag: { count: 1, p95: 0, p99: 0, max: 0 },
            failures: 0,
            committed: 0,
            commitLagMaxMs: null as number | null,
        }
        let rows = 0,
            read = false
        const operation = readIsolationCompletion(
            async () => {
                await commit.promise
                rows++
                metrics.committed++
                metrics.commitLagMaxMs = 25
                return Response.json({ drained: true, ...metrics })
            },
            async () => {
                read = true
                return rows
            },
        )
        let settled = false
        void operation.then(() => {
            settled = true
        })
        await Bun.sleep(30)
        expect(settled).toBe(false)
        expect(read).toBe(false)
        commit.resolve()
        const { drained, progress } = await operation
        expect(read).toBe(true)
        expect(drained.metrics.committed).toBe(progress)
        expect(progress).toBe(1)
        expect(drained.metrics.commitLagMaxMs).toBe(25)
        expect(drained.elapsedMs).toBeGreaterThanOrEqual(25)
    })
    test('wall expiry reaches bounded cleanup even when evaluation and one cleanup stage hang', async () => {
        const abort = new AbortController()
        const primary = new Error('wall budget exhausted')
        const hung = Promise.withResolvers<void>()
        let stopped = false,
            removed = false
        const operation = (async () => {
            let failure: { error: unknown } | undefined
            try {
                await withIsolationCancellation(hung.promise, abort.signal)
            } catch (error) {
                failure = { error }
                throw error
            } finally {
                await finishIsolationFixture(
                    [
                        () => hung.promise,
                        async () => {
                            stopped = true
                        },
                    ],
                    async () => {
                        removed = true
                    },
                    failure,
                    25,
                )
            }
        })()
        abort.abort(primary)
        try {
            await expect(operation).rejects.toBeInstanceOf(AggregateError)
            await operation.catch((error: AggregateError) => {
                expect(error.errors[0]).toBe(primary)
            })
            expect(stopped).toBe(true)
            expect(removed).toBe(false)
        } finally {
            hung.resolve()
        }
    })
    test.skipIf(process.platform === 'win32')(
        'a descendant holding both pipes cannot extend command cleanup indefinitely',
        async () => {
            const directory = await mkdtemp(join(tmpdir(), 'isolation-command-'))
            const pidFile = join(directory, 'descendant.pid')
            const spawned = spyOn(Bun, 'spawn')
            let deadline: ReturnType<typeof setTimeout> | undefined
            let descendantExited = false
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
                const exitDeadline = performance.now() + 2_000
                while (!descendantExited && performance.now() < exitDeadline) {
                    try {
                        process.kill(descendant, 0)
                        await Bun.sleep(25)
                    } catch (error) {
                        if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH'))
                            throw error
                        descendantExited = true
                    }
                }
                expect(descendantExited).toBe(true)
            } finally {
                clearTimeout(deadline)
                spawned.mockRestore()
                const descendant = Number(await readFile(pidFile, 'utf8').catch(() => ''))
                if (!descendantExited && Number.isInteger(descendant) && descendant > 0) {
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
