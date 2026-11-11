// oxlint-disable no-await-in-loop -- Matched repetitions, owned child cleanup and maintenance checkpoints are ordered.
import { SQL } from 'bun'
import { createHash } from 'node:crypto'
import { chmod, chown, mkdtemp, open, readdir, readFile, rm } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { isolationContract as contract } from './isolation.config.ts'
import {
    parseIsolationOptions,
    isolationDistribution,
    assessIsolation,
    assertIsolationRole,
} from './isolation.ts'
import { isolationReadySchema, isolationRoleMetricsSchema } from './isolation.validation.ts'
import type {
    IsolationMode,
    IsolationLoad,
    IsolationMeasurement,
    IsolationOptions,
    IsolationChild,
} from './Types/isolation.types.ts'

const root = resolve(import.meta.dir, '../..')
const controlHeaders = { 'x-isolation-control': 'fixture' }
/** Reject at the deadline and clear the timer on either outcome; the caller retains responsibility for cancelling the operation. */
async function bounded<T>(operation: Promise<T>, milliseconds: number): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
        return await Promise.race([
            operation,
            new Promise<never>((_, reject) => {
                timer = setTimeout(
                    () => reject(new Error('Isolation fixture deadline')),
                    milliseconds,
                )
            }),
        ])
    } finally {
        clearTimeout(timer)
    }
}
export class IsolationCommandError extends Error {
    constructor(readonly exitCode: number) {
        super('Isolation fixture command failed')
    }
}
/** Run an owned tool with bounded output completion; failures kill and reap the child and bound inherited-pipe cleanup before rethrowing. */
export async function command(args: string[], timeoutMs = 30_000): Promise<string> {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0)
        throw new Error('Invalid isolation command timeout')
    const child = Bun.spawn(args, { stdout: 'pipe', stderr: 'pipe' })
    const output = new AbortController()
    const read = (stream: ReadableStream<Uint8Array>) =>
        new Response(
            stream.pipeThrough(new TransformStream<Uint8Array, Uint8Array>(), {
                signal: output.signal,
            }),
        ).text()
    const completions = [child.exited, read(child.stdout), read(child.stderr)] as const
    try {
        const [code, out] = await bounded(Promise.all(completions), timeoutMs)
        if (code !== 0) throw new IsolationCommandError(code)
        return out.trim()
    } catch (error) {
        if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
        // Reap the owned process even when a descendant keeps an inherited pipe open.
        await child.exited
        try {
            await bounded(Promise.allSettled(completions), 250)
        } catch {
            output.abort()
            await bounded(Promise.allSettled(completions), 250).catch(() => {})
        }
        throw error
    }
}

/** Reap the directly owned postmaster and require pg_ctl's explicit stopped result; unknown state retains the data directory. */
export async function stopIsolationPostgres(
    directory: string,
    postgres: Pick<Bun.Subprocess, 'exitCode' | 'signalCode' | 'kill' | 'exited'> | undefined,
    run = command,
) {
    const errors: unknown[] = []
    if (postgres && postgres.exitCode === null && postgres.signalCode === null) {
        try {
            // PostgreSQL's immediate shutdown signal, sent only to our owned handle.
            // Never let an unexpected PID file choose a process to stop.
            postgres.kill('SIGQUIT')
        } catch (error) {
            errors.push(error)
        }
    }
    if (postgres) {
        try {
            await bounded(postgres.exited, 5_000)
        } catch (error) {
            errors.push(error)
            try {
                postgres.kill('SIGKILL')
            } catch (killError) {
                errors.push(killError)
            } finally {
                await postgres.exited
            }
        }
        if (postgres.signalCode !== null)
            errors.push(
                new Error(
                    'Isolation PostgreSQL terminated by signal; cluster shutdown is unconfirmed',
                ),
            )
    }
    try {
        await run(['gosu', 'postgres', 'pg_ctl', '-D', directory + '/pg', 'status'])
        errors.push(new Error('Isolation PostgreSQL is still running'))
    } catch (error) {
        if (!(error instanceof IsolationCommandError && error.exitCode === 3)) errors.push(error)
    }
    if (errors.length)
        throw new AggregateError(errors, 'Isolation PostgreSQL shutdown was not confirmed')
}

/** Accept readiness only from this live postmaster's own identity file, never another listener on the same port. */
export async function waitForIsolationPostgres(
    directory: string,
    postgres: Pick<Bun.Subprocess, 'pid' | 'exitCode' | 'signalCode'>,
    launchedAt: number,
    port = 55439,
) {
    const deadline = performance.now() + 10_000
    while (performance.now() < deadline) {
        if (postgres.exitCode !== null || postgres.signalCode !== null)
            throw new Error('Isolation PostgreSQL exited before readiness')
        const text = await readFile(directory + '/pg/postmaster.pid', 'utf8').catch(
            (error: unknown) => {
                if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return ''
                throw error
            },
        )
        const lines = text.trimEnd().split('\n')
        if (lines.length >= 4) {
            if (
                Number(lines[0]) !== postgres.pid ||
                lines[1] !== directory + '/pg' ||
                Number(lines[2]) < Math.floor(launchedAt / 1_000) ||
                Number(lines[2]) > Math.floor(Date.now() / 1_000) ||
                !Number.isSafeInteger(Number(lines[2])) ||
                Number(lines[3]) !== port
            )
                throw new Error('Isolation PostgreSQL identity mismatch')
            if (lines[7]?.trim() === 'ready') {
                if (postgres.exitCode !== null || postgres.signalCode !== null)
                    throw new Error('Isolation PostgreSQL exited before readiness')
                return
            }
        }
        await Bun.sleep(25)
    }
    throw new Error('Isolation PostgreSQL readiness deadline')
}

/** Attempt every cleanup stage; failed cleanup retains owned fixture data and preserves the original failure. */
export async function finishIsolationFixture(
    stages: (() => Promise<unknown>)[],
    remove: () => Promise<unknown>,
    primary: { error: unknown } | undefined,
) {
    const errors: unknown[] = []
    for (const stage of stages) {
        try {
            await stage()
        } catch (error) {
            errors.push(error)
        }
    }
    if (!errors.length) {
        try {
            await remove()
        } catch (error) {
            errors.push(error)
        }
    }
    if (errors.length)
        throw new AggregateError(
            primary ? [primary.error, ...errors] : errors,
            'Isolation fixture cleanup failed; disposable state retained',
        )
}

/** Decode streaming UTF-8 and emit complete lines; discard an entire overlong line above 4096 characters until its newline. */
export function createIsolationLineReader(onLine: (line: string) => void) {
    const decoder = new TextDecoder()
    let pending = ''
    let discarding = false
    /** Accumulate fragments across chunks, dropping oversized lines without treating their suffix as a fresh readiness message. */
    function read(text: string) {
        let offset = 0
        while (offset < text.length) {
            const end = text.indexOf('\n', offset)
            const fragment = text.slice(offset, end < 0 ? text.length : end)
            if (!discarding) {
                if (pending.length + fragment.length > 4096) {
                    pending = ''
                    discarding = true
                } else pending += fragment
            }
            if (end < 0) break
            if (!discarding) onLine(pending)
            pending = ''
            discarding = false
            offset = end + 1
        }
    }
    return {
        /** Preserve partial UTF-8 sequences between chunks before framing newline-delimited messages. */
        write(chunk: Uint8Array) {
            read(decoder.decode(chunk, { stream: true }))
        },
        /** Flush decoder state and emit a final unterminated line only when it stayed within the framing bound. */
        finish() {
            read(decoder.decode())
            if (!discarding && pending.length > 0) onLine(pending)
            pending = ''
            discarding = false
        },
    }
}

/** Hash sorted TypeScript source paths and contents plus package metadata so reports identify the measured source snapshot. */
async function sourceHash() {
    const hash = createHash('sha256')
    /** Traverse directories in sorted order and hash only TypeScript files, including each relative path to distinguish identical contents. */
    async function add(directory: string) {
        const entries = await readdir(join(root, directory), { withFileTypes: true })
        for (const entry of entries.toSorted((a, b) => a.name.localeCompare(b.name))) {
            const relative = directory + '/' + entry.name
            if (entry.isDirectory()) await add(relative)
            else if (/\.tsx?$/u.test(entry.name)) {
                hash.update(relative)
                hash.update(await readFile(join(root, relative)))
            }
        }
    }
    for (const dir of [
        'web/src/server',
        'web/src/lib',
        'web/src/config',
        'web/src/db',
        'web/src/shared',
        'web/src/features',
        'scripts/runtime-scale',
    ])
        await add(dir)
    for (const file of ['package.json', 'bun.lock']) {
        hash.update(file)
        hash.update(await readFile(join(root, file)))
    }
    return hash.digest('hex')
}
/** Read current fixture resource usage and reject missing finite CPU, memory, or PID enforcement beyond the declared envelope. */
async function cgroup() {
    const [memory, pids, cpu, limit, quota, pidLimit] = await Promise.all(
        ['memory.current', 'pids.current', 'cpu.stat', 'memory.max', 'cpu.max', 'pids.max'].map(
            (name) => readFile('/sys/fs/cgroup/' + name, 'utf8'),
        ),
    )
    if (
        Number(limit) > contract.envelope.memoryBytes ||
        Number(pidLimit) > contract.envelope.pids ||
        limit!.trim() === 'max' ||
        pidLimit!.trim() === 'max'
    )
        throw new Error('Isolation resource envelope not enforced')
    const [allowed, period] = quota!.trim().split(' ')
    if (allowed === 'max' || Number(allowed) / Number(period) > contract.envelope.cpuCores)
        throw new Error('Isolation CPU envelope not enforced')
    return {
        memory: Number(memory),
        pids: Number(pids),
        cpu: Number(cpu!.match(/^usage_usec (\d+)/mu)?.[1] ?? NaN),
    }
}
type Child = IsolationChild

/** Start an unprivileged role with fixture-only environment; readiness must arrive within ten seconds or the owned child is killed and reaped. */
async function spawnRole(
    role: 'foreground' | 'combined' | 'background',
    networkPort: number,
    denied = false,
): Promise<Child> {
    const ready = Promise.withResolvers<{ port: number; pid: number; uid: number }>()
    const started = performance.now()
    const user = denied
        ? 'isolation_denied'
        : role === 'background'
          ? 'isolation_worker'
          : 'isolation_foreground'
    const child = Bun.spawn(
        [
            'gosu',
            role === 'background' ? '20004:20004' : '20002:20002',
            process.execPath,
            '--no-env-file',
            import.meta.path,
            'role',
            role,
            String(networkPort),
        ],
        {
            cwd: root,
            env: {
                PATH: process.env.PATH!,
                DATABASE_URL:
                    'postgresql://' + user + '@127.0.0.1:55439/rentnerproxy_upstream_test',
                RENTNERPROXY_DATABASE_INTEGRATION: '1',
                RENTNERPROXY_ISOLATION_FIXTURE: '1',
                NODE_ENV: 'test',
            },
            stdin: 'ignore',
            stdout: 'pipe',
            stderr: 'pipe',
        },
    )
    const lines = createIsolationLineReader((line) => {
        try {
            ready.resolve(isolationReadySchema.parse(JSON.parse(line)))
        } catch {}
    })
    void (async () => {
        for await (const data of child.stdout) lines.write(data)
        lines.finish()
    })().catch((error: unknown) => ready.reject(error))
    void new Response(child.stderr).text()
    try {
        return {
            process: child,
            ready: await bounded(
                Promise.race([
                    ready.promise,
                    child.exited.then(() => {
                        throw new Error('Isolation role failed before readiness')
                    }),
                ]),
                10_000,
            ),
            startupMs: performance.now() - started,
        }
    } catch (error) {
        child.kill('SIGKILL')
        await child.exited
        throw error
    }
}
/** Send a control-marked request only to the owned role loopback port, with a sixteen-second deadline and redirects disabled. */
async function request(child: Child, path: string, method = 'GET') {
    return fetch('http://127.0.0.1:' + child.ready.port + path, {
        method,
        headers: controlHeaders,
        signal: AbortSignal.timeout(16_000),
        redirect: 'error',
    })
}
/** Require successful drain and a zero exit after SIGTERM; always kill and reap a still-running child when shutdown fails. */
async function stopChild(child: Child) {
    const start = performance.now()
    try {
        const response = await request(child, '/stop', 'POST')
        if (!response.ok) throw new Error('Isolation role did not drain')
        child.process.kill('SIGTERM')
        if ((await bounded(child.process.exited, 15_000)) !== 0)
            throw new Error('Isolation child exit failed')
        return performance.now() - start
    } finally {
        if (child.process.exitCode === null) {
            child.process.kill('SIGKILL')
            await child.process.exited
        }
    }
}

/** Refuse cleanup outside the fixture temporary-directory prefix before recursively removing disposable state. */
async function removeFixtureDirectory(directory: string) {
    if (!directory.startsWith('/tmp/rentnerproxy-isolation-'))
        throw new Error('Unsafe isolation fixture cleanup')
    await rm(directory, { recursive: true, force: true })
}

/** Create and migrate a fresh disposable database with connection-limited roles; return the admin connection that the caller must close. */
async function prepareDatabase(url: string) {
    const admin = new SQL('postgresql://postgres@127.0.0.1:55439/postgres', { max: 4 })
    await admin.unsafe('create database rentnerproxy_upstream_test')
    await admin.unsafe('create role isolation_foreground login connection limit 16')
    await admin.unsafe('create role isolation_worker login connection limit 8')
    await admin.close()
    process.env.DATABASE_URL = url
    const { runMigrate } = await import('../../web/src/db/migrate.ts')
    await runMigrate({ databaseUrl: url, migrationsFolder: join(root, 'web/drizzle') })
    const database = new SQL(url, { max: 4 })
    await database.unsafe(
        'grant connect on database rentnerproxy_upstream_test to isolation_foreground,isolation_worker',
    )
    await database.unsafe(
        'grant usage on schema rentnerproxy to isolation_foreground,isolation_worker',
    )
    await database.unsafe(
        'grant select,insert,update,delete on all tables in schema rentnerproxy to isolation_foreground,isolation_worker',
    )
    await database.unsafe(
        'grant usage,select on all sequences in schema rentnerproxy to isolation_foreground,isolation_worker',
    )
    return database
}
/** Replace fixture hosts and insert the selected load atomically with deterministic identities and a shared future due time. */
async function seed(load: IsolationLoad, database: SQL) {
    await database.unsafe('delete from rentnerproxy.proxy_hosts')
    const [{ db }, schema] = await Promise.all([
        import('../../web/src/db/index.ts'),
        import('../../web/src/db/schema.ts'),
    ])
    const ids = Array.from(
        { length: load },
        (_, index) => '00000000-0000-4000-8000-' + String(index + 1).padStart(12, '0'),
    )
    await db.transaction(async (transaction) => {
        await transaction.insert(schema.proxyHosts).values(
            ids.map((id) => ({
                id,
                forwardScheme: 'http' as const,
                forwardHost: '127.0.0.1',
                forwardPort: 9,
            })),
        )
        await transaction.insert(schema.hostDomains).values(
            ids.map((id, index) => ({
                proxyHostId: id,
                domain: 'isolation-' + index + '.example.test',
            })),
        )
        const due = Date.now() + 1_000
        await transaction.insert(schema.hostCheckPolicies).values(
            ids.map((id, index) => ({
                proxyHostId: id,
                domain: 'isolation-' + index + '.example.test',
                enabled: true,
                intervalSeconds: 60,
                timeoutMs: 1_000,
                nextCheckAt: new Date(due),
            })),
        )
    })
}

/** Measure one freshly seeded topology under bounded traffic, drain pending samples, and reap owned roles even if measurement fails. */
async function measure(
    mode: IsolationMode,
    load: IsolationLoad,
    repetition: number,
    options: IsolationOptions,
    database: SQL,
    networkPort: number,
    networkStats: { probes: number; accepted: number; invalid: number; snapshots: number },
): Promise<IsolationMeasurement> {
    await seed(load, database)
    const foreground = await spawnRole(
        mode === 'in-process' ? 'combined' : 'foreground',
        networkPort,
    )
    let background: Child | null = null
    try {
        if (mode === 'separate-bun') background = await spawnRole('background', networkPort)
        const networkBefore = { ...networkStats }
        const owner = background ?? foreground
        const idleBefore = await Promise.all(
            [foreground, ...(background ? [background] : [])].map(async (child) =>
                isolationRoleMetricsSchema.parse(await (await request(child, '/metrics')).json()),
            ),
        )
        const idleStarted = performance.now()
        await Bun.sleep(250)
        const idleAfter = await Promise.all(
            [foreground, ...(background ? [background] : [])].map(async (child) =>
                isolationRoleMetricsSchema.parse(await (await request(child, '/metrics')).json()),
            ),
        )
        const idleCpuPercentagePoints =
            (idleAfter.reduce((sum, item) => sum + item.cpuMicroseconds, 0) -
                idleBefore.reduce((sum, item) => sum + item.cpuMicroseconds, 0)) /
            ((performance.now() - idleStarted) * 10)
        const idleBunRssBytes = idleAfter.reduce((sum, item) => sum + item.rssBytes, 0)
        await database.unsafe(
            "update rentnerproxy.host_check_policies set next_check_at=clock_timestamp()+interval '0.5 seconds' returning extract(epoch from next_check_at)*1000 as due",
        )
        await request(owner, '/start', 'POST')
        const latencies: number[] = []
        let rejectedTraffic = 0,
            ssrFailures = 0,
            active = 0,
            telemetryFailures = 0,
            sampling = false
        let maxMemory = 0,
            maxPids = 0,
            maxConnections = 0
        const pending = new Set<Promise<void>>()
        const begin = await cgroup(),
            started = performance.now()
        const sampler = setInterval(() => {
            if (sampling) return
            sampling = true
            const task = (async () => {
                const sample = await cgroup()
                maxMemory = Math.max(maxMemory, sample.memory)
                maxPids = Math.max(maxPids, sample.pids)
                const rows =
                    await database`select count(*)::integer as count from pg_stat_activity where datname=current_database()`
                maxConnections = Math.max(maxConnections, Number(rows[0]?.count))
            })()
            pending.add(task)
            void task.then(
                () => {
                    pending.delete(task)
                    sampling = false
                },
                () => {
                    pending.delete(task)
                    sampling = false
                    telemetryFailures++
                },
            )
        }, 250)
        const traffic = setInterval(() => {
            if (active >= contract.requestConcurrency) {
                rejectedTraffic++
                return
            }
            active++
            const task = (async () => {
                const start = performance.now()
                try {
                    const response = await request(foreground, '/ssr')
                    if (!response.ok || !(await response.text()).includes('Proxy hosts'))
                        ssrFailures++
                } catch {
                    ssrFailures++
                } finally {
                    latencies.push(performance.now() - start)
                    active--
                }
            })()
            pending.add(task)
            void task.finally(() => pending.delete(task))
        }, 1000 / contract.requestsPerSecond)
        try {
            await Bun.sleep(options.durationSeconds * 1000)
        } finally {
            clearInterval(traffic)
            clearInterval(sampler)
            await Promise.allSettled(pending)
        }
        const elapsedMs = performance.now() - started,
            ending = await cgroup()
        const foregroundMetrics = isolationRoleMetricsSchema.parse(
                await (await request(foreground, '/metrics')).json(),
            ),
            backgroundMetrics = background
                ? isolationRoleMetricsSchema.parse(
                      await (await request(background, '/metrics')).json(),
                  )
                : null
        const progress = await database.unsafe(
            "select count(*)::integer as count,count(distinct policy_id)::integer as distinct_policies,count(*) filter(where status='healthy' and code='expected_status')::integer as healthy from rentnerproxy.host_check_observations",
        )
        const observations = Number(progress[0]?.count),
            unique = Number(progress[0]?.distinct_policies)
        const progressMetrics = backgroundMetrics ?? foregroundMetrics
        const dueToCommitMaxMs =
            progressMetrics.committed === observations ? progressMetrics.commitLagMaxMs : null
        const stopMs = await stopChild(owner)
        if (background) await stopChild(foreground)
        return {
            mode,
            load,
            repetition,
            elapsedMs,
            ssr: isolationDistribution(latencies),
            ssrFailures,
            rejectedTraffic,
            telemetryFailures,
            foreground: foregroundMetrics,
            background: backgroundMetrics,
            bunRssBytes: foregroundMetrics.rssBytes + (backgroundMetrics?.rssBytes ?? 0),
            idleBunRssBytes,
            idleCpuPercentagePoints,
            totalMemoryBytes: maxMemory,
            totalCpuPercentagePoints: (ending.cpu - begin.cpu) / (elapsedMs * 10),
            postgresConnections: maxConnections,
            pids: maxPids,
            observations,
            healthyObservations: Number(progress[0]?.healthy),
            network: {
                probes: networkStats.probes - networkBefore.probes,
                accepted: networkStats.accepted - networkBefore.accepted,
                invalid: networkStats.invalid - networkBefore.invalid,
                snapshots: networkStats.snapshots - networkBefore.snapshots,
            },
            duplicates: observations - unique,
            dueToCommitMaxMs,
            startupMs: foreground.startupMs + (background?.startupMs ?? 0),
            stopMs,
        }
    } finally {
        for (const child of [foreground, background])
            if (child && child.process.exitCode === null) {
                child.process.kill('SIGKILL')
                await child.process.exited
            }
    }
}

/** Require the explicit root Linux fixture and enforced envelope; own the disposable database/network lifecycle and emit source-bound evidence. */
export async function runIsolationEvaluation(options: IsolationOptions) {
    if (
        process.platform !== 'linux' ||
        process.env.RENTNERPROXY_ISOLATION_FIXTURE !== '1' ||
        process.getuid?.() !== 0
    )
        throw new Error('Explicit disposable Linux isolation fixture required')
    const wallClock = setTimeout(() => {
        console.error('Isolation fixture wall-clock budget exhausted')
        process.exit(1)
    }, contract.wallClockBudgetMs)
    wallClock.unref()
    await cgroup()
    const codeHash = await sourceHash()
    console.log(
        JSON.stringify({
            predeclaredContract: contract,
            options,
            codeHash,
            bunVersion: Bun.version,
        }),
    )
    const directory = await mkdtemp('/tmp/rentnerproxy-isolation-')
    const postgresUid = Number(await command(['id', '-u', 'postgres'])),
        postgresGid = Number(await command(['id', '-g', 'postgres']))
    await chown(directory, postgresUid, postgresGid)
    await chmod(directory, 0o700)
    let database: SQL | undefined
    let postgresStartAttempted = false
    let postgres: Bun.Subprocess<'ignore', number, number> | undefined
    let primary: { error: unknown } | undefined
    const networkStats = { probes: 0, accepted: 0, invalid: 0, snapshots: 0 }
    const network = Bun.serve({
        hostname: '127.0.0.1',
        port: 0,
        maxRequestBodySize: 32 * 1024 ** 2,
        /** Serve delayed loopback probes and revision-checked snapshots while counting accepted and invalid fixture traffic. */
        async fetch(incoming) {
            const path = new URL(incoming.url).pathname
            if (incoming.method === 'HEAD' && /^\/probe\/\d{1,4}$/u.test(path)) {
                networkStats.probes++
                const index = Number(path.split('/').at(-1))
                await Bun.sleep(contract.workload.networkDelaysMs[index % 4]!)
                networkStats.accepted++
                return new Response(null, { status: 200 })
            }
            if (incoming.method === 'POST' && path === '/snapshot') {
                const value: unknown = JSON.parse(await incoming.text())
                if (
                    !value ||
                    typeof value !== 'object' ||
                    !('revision' in value) ||
                    typeof value.revision !== 'string' ||
                    !/^sha256:[a-f0-9]{64}$/u.test(value.revision)
                )
                    return new Response(null, { status: 400 })
                networkStats.snapshots++
                return Response.json({ accepted: true })
            }
            networkStats.invalid++
            return new Response(null, { status: 404 })
        },
    })
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
        const log = await open(directory + '/postgres.log', 'a')
        const postgresLaunchedAt = Date.now()
        try {
            // Own the postmaster directly: a timed-out readiness tool cannot orphan a daemonized server.
            postgresStartAttempted = true
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
                    '-c',
                    'max_connections=100',
                    '-c',
                    'shared_buffers=64MB',
                ],
                { stdin: 'ignore', stdout: log.fd, stderr: log.fd },
            )
        } finally {
            await log.close()
        }
        await waitForIsolationPostgres(directory, postgres, postgresLaunchedAt)
        database = await prepareDatabase(
            'postgresql://postgres@127.0.0.1:55439/rentnerproxy_upstream_test',
        )
        const measurements: IsolationMeasurement[] = []
        for (const load of options.faultsOnly ? [] : options.loads)
            for (let repetition = 1; repetition <= options.repetitions; repetition++) {
                const modes =
                    repetition % 2 === 1 ? contract.modes : [...contract.modes].toReversed()
                for (const mode of modes) {
                    const result = await measure(
                        mode,
                        load,
                        repetition,
                        options,
                        database,
                        network.port!,
                        networkStats,
                    )
                    measurements.push(result)
                    console.log(JSON.stringify({ measurement: result }))
                }
            }
        const faults = options.faults
            ? await (
                  await import('./isolation-faults.ts')
              ).runIsolationFaults({
                  database,
                  seed: (load) => seed(load, database!),
                  spawnRole: (role, denied) => spawnRole(role, network.port!, denied),
                  request,
                  stopChild,
                  bounded,
                  command,
                  snapshotPath: join(directory, 'active-writers.dump'),
              })
            : null
        const report = {
            kind: 'bounded-bun-isolation-fixture',
            codeHash,
            bunVersion: Bun.version,
            contract,
            options,
            measurements,
            assessment: assessIsolation(measurements, false),
            faults,
        }
        console.log(JSON.stringify({ isolationReport: report }))
        return report
    } catch (error) {
        primary = { error }
        throw error
    } finally {
        try {
            await finishIsolationFixture(
                [
                    () => network.stop(true),
                    async () => database?.close(),
                    async () => {
                        if (
                            process.env.DATABASE_URL ===
                            'postgresql://postgres@127.0.0.1:55439/rentnerproxy_upstream_test'
                        ) {
                            const { closeDatabaseForRestoreMaintenance } =
                                await import('../../web/src/db/index.ts')
                            await closeDatabaseForRestoreMaintenance()
                        }
                    },
                    async () => {
                        if (postgresStartAttempted) await stopIsolationPostgres(directory, postgres)
                    },
                ],
                () => removeFixtureDirectory(directory),
                primary,
            )
        } finally {
            clearTimeout(wallClock)
        }
    }
}

if (import.meta.main) {
    try {
        if (process.argv[2] === 'role') {
            const role = process.argv[3],
                port = Number(process.argv[4])
            if (
                !['foreground', 'combined', 'background'].includes(role ?? '') ||
                !Number.isInteger(port) ||
                port < 1 ||
                port > 65535
            )
                throw new Error('Invalid isolation role')
            assertIsolationRole(
                process.env.RENTNERPROXY_ISOLATION_FIXTURE,
                process.platform,
                process.getuid?.(),
                process.env.DATABASE_URL,
                role as 'foreground' | 'combined' | 'background',
            )
            const { runIsolationRole } = await import('./isolation-role.ts')
            await runIsolationRole(role as 'foreground' | 'combined' | 'background', port)
        } else await runIsolationEvaluation(parseIsolationOptions(process.argv.slice(2)))
    } catch {
        console.error('Isolation evaluation failed; only disposable fixture state was used.')
        process.exitCode = 1
    }
}
