// oxlint-disable no-await-in-loop -- Each bounded operation owns a separate PostgreSQL statistics window.
import { SQL } from 'bun'
import { expect, spyOn, test } from 'bun:test'
import * as crypto from 'node:crypto'
import { open, readFile } from 'node:fs/promises'
import { sql } from 'drizzle-orm'
import { postgresCpuDelta, readPostgresCpuSnapshot } from '@/tests/Helpers/postgresCpu.ts'

type Sample = {
    name: string
    queries: number
    queryWallMs: number
    explicitLockStatementWallMs: number[]
    lockStatementToTransactionSettlementMs: number[]
    sqlstates: string[]
    rootTransactionAcquireAndBeginMs: number[]
    rootTransactionCallbackMs: number[]
    nestedSavepointEnterMs: number[]
    nestedSavepointCallbackMs: number[]
    returnedRows: number
    affectedRows: number
    rows: unknown[]
    jsonCalls: number
    jsonCpuUs: number
    hashCalls: number
    hashCpuUs: number
    lockWaitSamples: number
    connectionSamples: number[]
}

let active: Sample | undefined

/** Run only source-identity Git reads and require a successful result. */
function git(...args: string[]): string {
    const result = Bun.spawnSync(['git', ...args], { stdout: 'pipe', stderr: 'pipe' })
    if (result.exitCode !== 0) throw new Error('Profile source identity unavailable')
    return result.stdout.toString().trim()
}

/** Stamp the actual checked-out production bytes, including uncommitted changes, independently of the requested Git revision. */
async function sourceStamp() {
    const paths = git('ls-files', 'web/src/server', 'web/src/db', 'web/src/config', 'web/src/lib')
        .split('\n')
        .filter((path) => path.endsWith('.ts'))
    const digest = crypto.createHash('sha256')
    for (let offset = 0; offset < paths.length; offset += 32) {
        const batch = paths.slice(offset, offset + 32)
        const contents = await Promise.all(batch.map((path) => readFile(path)))
        for (const [index, path] of batch.entries()) digest.update(path).update(contents[index]!)
    }
    return {
        head: git('rev-parse', 'HEAD'),
        productionSha256: digest.digest('hex'),
        dirtyProductionPaths: git(
            'diff',
            '--name-only',
            'HEAD',
            '--',
            'web/src/server',
            'web/src/db',
            'web/src/config',
            'web/src/lib',
        ),
        helperSha256: crypto
            .createHash('sha256')
            .update(await readFile(import.meta.filename))
            .digest('hex'),
        cpuHelperSha256: crypto.createHash('sha256')
            .update(await readFile(new URL('../../Helpers/postgresCpu.ts', import.meta.url)))
            .digest('hex'),
    }
}

/** Recover only the two historical DELETE templates from the pinned original implementation and verify its numeric policy before binding them. */
function historicalRetention() {
    const revision = '3ec345555b1a0b9e545609ee49415f6869f1b513'
    const path = 'web/src/server/HostMonitoring/host-monitoring.repository.ts'
    const source = git('show', revision + ':' + path)
    const config = git('show', revision + ':web/src/config/host-monitoring.config.ts')
    if (!config.includes('maxObservations: 100_000') || !config.includes('maxIncidents: 10_000'))
        throw new Error('Historical retention policy no longer matches the reviewed baseline')
    const start = source.indexOf('async function pruneHistoryInTransaction(')
    const end = source.indexOf('export async function pruneHostMonitoringHistory(', start)
    if (start < 0 || end < start) throw new Error('Historical retention source unavailable')
    const implementation = source.slice(start, end)
    const statements = Array.from(
        implementation.matchAll(/transaction\.execute\(sql`([\s\S]*?)`\)/gu),
        ([, statement]) =>
            statement!.replace(
                /\$\{HOST_MONITORING_LIMITS\.(maxObservations|maxIncidents)\}/gu,
                (_match, policy: string) => (policy === 'maxObservations' ? '100000' : '10000'),
            ),
    )
    if (
        statements.length !== 2 ||
        statements.some(
            (statement) =>
                statement.includes('${') || !statement.trimStart().startsWith('delete from'),
        )
    )
        throw new Error('Unsupported historical retention SQL')
    return {
        revision,
        path,
        implementationSha256: crypto.createHash('sha256').update(implementation).digest('hex'),
        statements,
    }
}

/** Observe lazy query settlement without replacing the native query, result mode, SQL text, parameters, or transaction callbacks. */
function instrumentQuery(
    query: SQL.Query<unknown>,
    lockCompletions?: number[],
    explicitLock = false,
): SQL.Query<unknown> {
    let observed = false
    return new Proxy(query, {
        get(target, property) {
            if (property === 'then')
                return (
                    fulfilled?: (value: unknown) => unknown,
                    rejected?: (error: unknown) => unknown,
                ) => {
                    const sample = !observed ? active : undefined
                    observed = true
                    const start = performance.now()
                    if (sample) sample.queries++
                    return target.then(
                        (rows) => {
                            if (sample) {
                                sample.queryWallMs += performance.now() - start
                                if (explicitLock) {
                                    sample.explicitLockStatementWallMs.push(
                                        performance.now() - start,
                                    )
                                    lockCompletions?.push(performance.now())
                                }
                                sample.returnedRows += Array.isArray(rows) ? rows.length : 0
                                const metadata = rows as SQL.ResultMetadata
                                sample.affectedRows += metadata.affectedRows ?? 0
                                if (sample.rows.length >= 2048)
                                    throw new Error('Profile result bound exceeded: ' + sample.name)
                                sample.rows.push(rows)
                            }
                            return fulfilled ? fulfilled(rows) : rows
                        },
                        (error: unknown) => {
                            if (sample) {
                                sample.queryWallMs += performance.now() - start
                                if (
                                    error &&
                                    typeof error === 'object' &&
                                    'errno' in error &&
                                    /^[0-9A-Z]{5}$/u.test(String(error.errno))
                                )
                                    sample.sqlstates.push(String(error.errno))
                            }
                            if (rejected) return rejected(error)
                            throw error
                        },
                    )
                }
            const value: unknown = Reflect.get(target, property)
            if (typeof value !== 'function') return value
            if (['values', 'raw', 'simple'].includes(String(property)))
                return (...args: unknown[]) =>
                    instrumentQuery(
                        Reflect.apply(value, target, args),
                        lockCompletions,
                        explicitLock,
                    )
            return value.bind(target)
        },
    })
}

/** Patch only this fixture's real pool and its transaction/savepoint clients, retaining production admission and restoring all methods afterward. */
function instrumentClient(
    client: SQL,
    restores: Array<() => void>,
    seen = new WeakSet<object>(),
    inheritedLockCompletions?: number[],
) {
    if (seen.has(client)) return
    seen.add(client)
    for (const key of ['unsafe', 'begin', 'savepoint'] as const) {
        const descriptor = Object.getOwnPropertyDescriptor(client, key)
        if (!descriptor) continue
        if (typeof descriptor.value !== 'function' || !descriptor.writable)
            throw new Error('Native SQL instrumentation contract changed')
        const original: (...args: unknown[]) => unknown = descriptor.value
        Object.defineProperty(client, key, {
            ...descriptor,
            value: (...args: unknown[]) => {
                if (key === 'unsafe')
                    return instrumentQuery(
                        Reflect.apply(original, client, args) as SQL.Query<unknown>,
                        inheritedLockCompletions,
                        /pg_advisory_xact_lock|for\s+update/iu.test(String(args[0])),
                    )
                const sample = active
                const lockCompletions = key === 'begin' ? [] : inheritedLockCompletions
                const started = performance.now()
                const callbacks = args.map((argument) => {
                    if (typeof argument !== 'function') return argument
                    return async (transaction: SQL) => {
                        instrumentClient(transaction, restores, seen, lockCompletions)
                        const entered = performance.now()
                        if (sample) {
                            const entry =
                                key === 'begin'
                                    ? sample.rootTransactionAcquireAndBeginMs
                                    : sample.nestedSavepointEnterMs
                            entry.push(entered - started)
                        }
                        try {
                            return await argument(transaction)
                        } finally {
                            if (sample) {
                                const callbackDurations =
                                    key === 'begin'
                                        ? sample.rootTransactionCallbackMs
                                        : sample.nestedSavepointCallbackMs
                                callbackDurations.push(performance.now() - entered)
                            }
                        }
                    }
                })
                return Promise.resolve(Reflect.apply(original, client, callbacks)).finally(() => {
                    if (sample && key === 'begin')
                        for (const acquired of lockCompletions ?? [])
                            sample.lockStatementToTransactionSettlementMs.push(
                                performance.now() - acquired,
                            )
                })
            },
        })
        restores.push(() => Object.defineProperty(client, key, descriptor))
    }
}

/** Count only JavaScript JSON calls and crypto Hash update/digest calls during an operation; native SQL JSON decoding remains unattributed. */
function instrumentSynchronousWork(restores: Array<() => void>) {
    for (const key of ['parse', 'stringify'] as const) {
        const original = JSON[key]
        const descriptor = Object.getOwnPropertyDescriptor(JSON, key)!
        Object.defineProperty(JSON, key, {
            ...descriptor,
            value: (...args: unknown[]) => {
                const sample = active
                const cpu = process.cpuUsage()
                try {
                    return Reflect.apply(original, JSON, args)
                } finally {
                    if (sample) {
                        const used = process.cpuUsage(cpu)
                        sample.jsonCalls++
                        sample.jsonCpuUs += used.user + used.system
                    }
                }
            },
        })
        restores.push(() => Object.defineProperty(JSON, key, descriptor))
    }
    for (const key of ['update', 'digest'] as const) {
        const prototype = Bun.CryptoHasher.prototype
        const descriptor = Object.getOwnPropertyDescriptor(prototype, key)!
        const original: (...args: unknown[]) => unknown = descriptor.value
        Object.defineProperty(prototype, key, {
            ...descriptor,
            value: function (this: Bun.CryptoHasher, ...args: unknown[]) {
                const sample = active
                const cpu = process.cpuUsage()
                try {
                    return Reflect.apply(original, this, args)
                } finally {
                    if (sample) {
                        const used = process.cpuUsage(cpu)
                        sample.hashCalls++
                        sample.hashCpuUs += used.user + used.system
                    }
                }
            },
        })
        restores.push(() => Object.defineProperty(prototype, key, descriptor))
    }
    const original = crypto.createHash
    const hashSpy = spyOn(crypto, 'createHash').mockImplementation((...args) => {
        const hash = original(...args)
        const wrapped = new Proxy(hash, {
            get(target, property) {
                const value: unknown = Reflect.get(target, property)
                if (typeof value !== 'function') return value
                return (...parameters: unknown[]) => {
                    const sample = active
                    const cpu = process.cpuUsage()
                    try {
                        const result: unknown = Reflect.apply(value, target, parameters)
                        return result === target ? wrapped : result
                    } finally {
                        if (sample && (property === 'update' || property === 'digest')) {
                            const used = process.cpuUsage(cpu)
                            sample.hashCalls++
                            sample.hashCpuUs += used.user + used.system
                        }
                    }
                }
            },
        })
        return wrapped
    })
    restores.push(() => hashSpy.mockRestore())
}

/** Record exact logical UTF-8 JSONB text bytes for every JSONB column on each committed inserted/updated row; this explicitly adds trigger overhead. */
async function installWriteVolumeObserver(observer: SQL) {
    await observer
        .unsafe(
            `create schema worker_profile;
        create table worker_profile.phase (name text not null);
        insert into worker_profile.phase values ('setup');
        create table worker_profile.writes (operation text, bytes bigint, jsonb_columns integer);
        create function worker_profile.capture() returns trigger language plpgsql as $$
        declare document jsonb := to_jsonb(new); attribute text; total bigint := 0; columns integer := 0;
        begin
            foreach attribute in array TG_ARGV loop
                if document->attribute <> 'null'::jsonb then
                    total := total + octet_length((document->attribute)::text);
                    columns := columns + 1;
                end if;
            end loop;
            insert into worker_profile.writes select name, total, columns from worker_profile.phase;
            return new;
        end $$;`,
        )
        .simple()
    const tables = await observer<{ table_name: string; columns: string[] }[]>`
        select table_name, array_agg(column_name::text order by ordinal_position) columns
        from information_schema.columns where table_schema='rentnerproxy' and data_type='jsonb'
        group by table_name`
    for (const table of tables) {
        if (
            !/^[a-z_]+$/u.test(table.table_name) ||
            table.columns.some((name) => !/^[a-z_]+$/u.test(name))
        )
            throw new Error('Unexpected profile schema identifier')
        await observer.unsafe(`create trigger worker_profile_volume after insert or update on rentnerproxy."${table.table_name}"
            for each row execute function worker_profile.capture(${table.columns.map((name) => `'${name}'`).join(',')})`)
    }
}

const enabled = process.env.RENTNERPROXY_POSTGRES_PROFILE === '1'

;(enabled ? test : test.skip)(
    'opt-in actual worker PostgreSQL operation profile',
    async () => {
        const url = process.env.DATABASE_URL
        if (
            !url ||
            process.env.RENTNERPROXY_DATABASE_INTEGRATION !== '1' ||
            new URL(url).hostname !== '127.0.0.1' ||
            new URL(url).pathname !== '/rentnerproxy_upstream_test'
        )
            throw new Error('Profile requires its explicitly owned disposable loopback database')
        const before = await sourceStamp()
        const expectedSource = process.env.RENTNERPROXY_POSTGRES_PROFILE_SOURCE
        if (expectedSource) {
            expect(before.head).toBe(expectedSource)
            expect(before.dirtyProductionPaths).toBe('')
        }
        const { db } = await import('@/db/index.ts')
        const observer = new SQL(url, {
            max: 1,
            connection: { application_name: 'worker-profile-observer' },
        })
        const restores: Array<() => void> = []
        const reports: unknown[] = []
        const cpuProfile = process.env.RENTNERPROXY_POSTGRES_PROFILE_CPU === '1'
        const runQueueCountersEnabled = cpuProfile &&
            (await readFile('/proc/sys/kernel/sched_schedstats', 'utf8')).trim() === '1'
        const scopeDatabase = 'worker_profile_scope_' + crypto.randomBytes(6).toString('hex')
        let scopeCreated = false
        let scopeClient: SQL | undefined
        try {
            const [server] = await observer<{ version: string; max_connections: string; track_io_timing: string }[]>`select current_setting('server_version') version,
                current_setting('max_connections') max_connections, current_setting('track_io_timing') track_io_timing`
            if (!server) throw new Error('PostgreSQL server identity unavailable')
            expect(server.track_io_timing).toBe('on')
            const [occupied] = await observer<{ count: number }[]>`select
            (select count(*) from rentnerproxy.proxy_hosts) + (select count(*) from rentnerproxy.users) +
            (select count(*) from rentnerproxy.certificate_binding_jobs) as count`
            expect(Number(occupied!.count)).toBe(0)
            await observer`create extension if not exists pg_stat_statements`
            const [statisticsIdentity] = await observer<
                { database_oid: number; reset_arguments: string }[]
            >`
                select (select oid::int from pg_database where datname=current_database()) database_oid,
                    pg_get_function_identity_arguments(oid) reset_arguments
                from pg_proc where proname='pg_stat_statements_reset'`
            if (
                !statisticsIdentity ||
                !statisticsIdentity.reset_arguments.startsWith(
                    'userid oid, dbid oid, queryid bigint',
                )
            )
                throw new Error('Unsupported PostgreSQL statistics reset API')
            const databaseOid = statisticsIdentity.database_oid
            await observer.unsafe('create database "' + scopeDatabase + '"')
            scopeCreated = true
            const scopeUrl = new URL(url)
            scopeUrl.pathname = '/' + scopeDatabase
            const scope = new SQL(scopeUrl.href, {
                max: 1,
                connection: { application_name: 'worker-profile-stat-sentinel' },
            })
            scopeClient = scope
            await scope`create table profile_scope_sentinel (value integer not null)`
            await scope`insert into profile_scope_sentinel values (1), (2), (3)`
            await scope`select sum(value)::integer as sentinel from profile_scope_sentinel`
            const [scopeIdentity] = await observer<
                { oid: number }[]
            >`select oid::int oid from pg_database where datname=${scopeDatabase}`
            if (!scopeIdentity) throw new Error('Owned statistics sentinel database unavailable')
            /** Read only the freshly owned second database's shared statistics; never reset its counters. */
            const scopeStatistics = () => observer`
                select queryid::text queryid, calls::float8 calls, rows::float8 rows, total_exec_time
                from pg_stat_statements where dbid=${scopeIdentity.oid}::oid order by queryid`
            expect((await scopeStatistics()).length).toBeGreaterThan(0)
            await installWriteVolumeObserver(observer)
            instrumentClient(db.$client, restores)
            instrumentSynchronousWork(restores)
            const network = spyOn(globalThis, 'fetch').mockRejectedValue(
                new Error('Profile forbids network transports'),
            )
            restores.push(() => network.mockRestore())

            /** Run one real production operation with no overlapping producer, then read numeric observer evidence outside its wall/CPU window. */
            async function measure<T>(name: string, operation: () => Promise<T>): Promise<T> {
                await observer`update worker_profile.phase set name=${name}`
                const scopeBeforeReset = await scopeStatistics()
                await observer`select pg_stat_statements_reset(0::oid, ${databaseOid}::oid, 0::bigint)`
                expect(await scopeStatistics()).toEqual(scopeBeforeReset)
                await scope`select sum(value)::integer as sentinel from profile_scope_sentinel`
                const scopeBeforeOperation = await scopeStatistics()
                const sample: Sample = {
                    name,
                    queries: 0,
                    queryWallMs: 0,
                    explicitLockStatementWallMs: [],
                    lockStatementToTransactionSettlementMs: [],
                    sqlstates: [],
                    rootTransactionAcquireAndBeginMs: [],
                    rootTransactionCallbackMs: [],
                    nestedSavepointEnterMs: [],
                    nestedSavepointCallbackMs: [],
                    returnedRows: 0,
                    affectedRows: 0,
                    rows: [],
                    jsonCalls: 0,
                    jsonCpuUs: 0,
                    hashCalls: 0,
                    hashCpuUs: 0,
                    lockWaitSamples: 0,
                    connectionSamples: [],
                }
                let sampling: Promise<unknown> | undefined
                let samplingFailed = false
                const timer = setInterval(() => {
                    if (sampling) return
                    sampling = observer<
                        { connections: number; lock_waiters: number }[]
                    >`select count(*)::int connections,
                    count(*) filter(where wait_event_type='Lock')::int lock_waiters from pg_stat_activity
                    where datname=current_database() and application_name <> 'worker-profile-observer'`
                        .then(([row]) => {
                            sample.connectionSamples.push(row!.connections)
                            if (row!.lock_waiters) sample.lockWaitSamples++
                        })
                        .catch(() => { samplingFailed = true })
                        .finally(() => {
                            sampling = undefined
                        })
                }, 5)
                const cpuBefore = cpuProfile ? await readPostgresCpuSnapshot(observer) : null
                const start = performance.now(),
                    cpu = process.cpuUsage()
                active = sample
                let value: T
                try {
                    value = await operation()
                } finally {
                    active = undefined
                    clearInterval(timer)
                }
                const wallMs = performance.now() - start
                const used = process.cpuUsage(cpu)
                await sampling
                if (samplingFailed) throw new Error('PostgreSQL profile sampling failed')
                const cpuAfter = cpuProfile ? await readPostgresCpuSnapshot(observer) : null
                const backendCpu = cpuBefore && cpuAfter ? postgresCpuDelta(cpuBefore, cpuAfter) : null
                if (backendCpu) expect(backendCpu.status).toBe('complete')
                const [pg] = await observer`select coalesce(sum(calls),0)::float8 calls,
                coalesce(sum(total_exec_time),0)::float8 exec_ms, coalesce(sum(rows),0)::float8 rows,
                coalesce(sum(wal_bytes),0)::float8 wal_bytes,
                coalesce(sum(shared_blks_hit),0)::float8 shared_blks_hit,
                coalesce(sum(shared_blks_read),0)::float8 shared_blks_read,
                coalesce(sum(shared_blks_written),0)::float8 shared_blks_written,
                coalesce(sum(temp_blks_read),0)::float8 temp_blks_read,
                coalesce(sum(temp_blks_written),0)::float8 temp_blks_written,
                coalesce(sum(shared_blk_read_time),0)::float8 shared_read_ms,
                coalesce(sum(shared_blk_write_time),0)::float8 shared_write_ms,
                coalesce(sum(temp_blk_read_time),0)::float8 temp_read_ms,
                coalesce(sum(temp_blk_write_time),0)::float8 temp_write_ms,
                count(distinct dbid)::int database_count from pg_stat_statements
                where dbid=${databaseOid}::oid and query not like '%pg_stat%' and query not like '%worker_profile%'
                and query not in ('BEGIN', 'COMMIT', 'ROLLBACK')`
                expect(pg!.database_count).toBe(1)
                expect(await scopeStatistics()).toEqual(scopeBeforeOperation)
                if (name === 'monitoring-complete16') {
                    expect(sample.rootTransactionAcquireAndBeginMs).toHaveLength(1)
                    expect(sample.rootTransactionCallbackMs).toHaveLength(1)
                    expect(sample.nestedSavepointEnterMs).toHaveLength(16)
                    expect(sample.nestedSavepointCallbackMs).toHaveLength(16)
                }
                const [writes] =
                    await observer`select coalesce(sum(bytes),0)::float8 jsonb_text_bytes_written,
                coalesce(sum(jsonb_columns),0)::int jsonb_column_write_occurrences from worker_profile.writes where operation=${name}`
                const { rows, ...numeric } = sample
                reports.push({
                    ...numeric,
                    wallMs,
                    processCpuMs: (used.user + used.system) / 1000,
                    postgresBackendCpu: backendCpu ? {
                        ...backendCpu,
                        runQueueMs: runQueueCountersEnabled ? backendCpu.runQueueMs : null,
                        runQueueCountersEnabled,
                    } : null,
                    hydratedJsonUtf8Bytes: Buffer.byteLength(
                        JSON.stringify(rows, (_key, item: unknown) =>
                            typeof item === 'bigint' ? item.toString() : item,
                        ),
                    ),
                    postgres: pg,
                    writes,
                    clientMinusPostgresMs: wallMs - Number(pg!.exec_ms),
                })
                return value!
            }

            const controller = await import('@/server/Controller/certificates.server.ts')
            const eventsSpy = spyOn(controller, 'getControllerCertificateEvents').mockResolvedValue(
                {
                    events: [],
                    nextCursor: Bun.randomUUIDv7() + ':0',
                    hasMore: false,
                    resetRequired: false,
                },
            )
            const metadataSpy = spyOn(controller, 'getControllerCertificates').mockResolvedValue([])
            restores.push(
                () => eventsSpy.mockRestore(),
                () => metadataSpy.mockRestore(),
            )
            const monitoring = await import('@/server/HostMonitoring/host-monitoring.repository.ts')
            await db.execute(sql`insert into rentnerproxy.proxy_hosts (id,forward_scheme,forward_host,forward_port)
            select uuidv7(),'http','fixture.invalid',8080 from generate_series(1,1000)`)
            await db.execute(sql`insert into rentnerproxy.host_domains (proxy_host_id,domain)
            select id,'profile-'||row_number() over(order by id)||'.example.test' from rentnerproxy.proxy_hosts`)
            await db.execute(sql`insert into rentnerproxy.host_check_policies (proxy_host_id,domain,enabled,next_check_at)
            select proxy_host_id,domain,true,clock_timestamp() from rentnerproxy.host_domains`)
            await db.execute(sql`insert into rentnerproxy.host_check_observations (policy_id,policy_version,checked_at,status,code)
            select p.id,p.version,clock_timestamp()-n*interval '1 minute','healthy','expected_status'
            from rentnerproxy.host_check_policies p cross join generate_series(1,144) n`)
            const claims = await measure('monitoring-claim16', () =>
                monitoring.claimDueHostMonitoringPolicies(16),
            )
            const contexts = await measure('monitoring-context16', () =>
                monitoring.readHostMonitoringClaimContexts(claims),
            )
            await measure('monitoring-complete16', () =>
                monitoring.completeHostMonitoringClaims(
                    claims.map((claim, index) => ({
                        claim,
                        before: contexts[index]!,
                        outcome: {
                            status: 'healthy',
                            code: 'expected_status',
                            durationMs: 2,
                            httpStatus: 200,
                        },
                        signal: new AbortController().signal,
                    })),
                ),
            )

            const notifications = await import('@/server/Notifications/notification-repository.ts')
            const storage = await import('@/server/Notifications/notification-storage.ts')
            const engine = await import('@/server/Notifications/incident-engine.ts')
            const configuration = {
                version: 1 as const,
                revision: 1,
                enabled: true,
                pausedAfterRestore: false,
                channels: Array.from({ length: 16 }, () => ({
                    id: crypto.randomUUID(),
                    name: 'Profile synthetic',
                    kind: 'webhook' as const,
                    revision: 1,
                    enabled: true,
                    events: ['ddns' as const],
                    minimumSeverity: 'error' as const,
                    thresholdSeconds: 0,
                    recovery: true,
                    destination: { ciphertext: 'a'.repeat(32), iv: 'b'.repeat(16) },
                    hasAuthorization: false,
                })),
            }
            const state = engine.observeNotificationSignals(
                engine.createNotificationState(),
                configuration,
                Array.from({ length: 1024 }, (_, index) => ({
                    source: 'snapshot' as const,
                    category: 'ddns' as const,
                    resourceId: 'profile-' + index + 'x'.repeat(190),
                    health: 'failing' as const,
                    severity: 'error' as const,
                    occurredAt: new Date(Date.now() - 1000).toISOString(),
                    evidence: 'e'.repeat(512),
                    failures: 1,
                })),
                Date.now(),
                crypto.randomUUID,
            )
            await db.transaction(async (transaction) => {
                await storage.lockNotificationConfiguration(transaction)
                await storage.writeNotificationConfiguration(transaction, configuration)
                await storage.lockNotificationRuntime(transaction)
                await storage.writeNotificationRuntime(transaction, {
                    version: 1,
                    engine: state,
                    certificateCursor: null,
                    collectionLease: null,
                })
            })
            const delivery = await measure('notification-claim', () =>
                notifications.claimNextNotificationDelivery(),
            )
            expect(delivery).not.toBeNull()
            await measure('notification-ack', () =>
                notifications.acknowledgeNotificationDelivery(delivery!.leaseToken, {
                    success: true,
                }),
            )
            await measure('notification-idle-due-read', () =>
                notifications.readNextNotificationDeliveryAt(),
            )

            const { createBackendActor } = await import('@/tests/Helpers/backendCoverageFixture.ts')
            const actor = await createBackendActor()
            const discovery =
                await import('@/server/Admin/DockerDiscovery/discovery-reconcile.server.ts')
            const fixtures = await import('@/tests/Helpers/discoveryFixture.ts')
            const { discoveryEndpointHash } =
                await import('@/server/Admin/DockerDiscovery/discovery-plan.ts')
            const { lockProxyRuntimeSettings } =
                await import('@/server/ProxyRuntime/proxy-runtime-settings.ts')
            await measure('discovery-one-candidate', () =>
                db.transaction(async (transaction) => {
                    await lockProxyRuntimeSettings(transaction)
                    return discovery.reconcileDiscoveryInTransaction(
                        transaction,
                        fixtures.discoveryRegistry({
                            enabled: true,
                            actorUserId: actor,
                            connectorRevision: 1,
                            endpointHash: discoveryEndpointHash('http://docker-proxy:2375'),
                        }),
                        fixtures.discoverySnapshot([
                            fixtures.discoveryCandidate({
                                domains: ['profile-discovery.example.test'],
                            }),
                        ]),
                    )
                }),
            )
            await db.execute(sql`insert into rentnerproxy.certificate_binding_jobs
            (actor_user_id,idempotency_key,request_digest,domains,required_permissions,host_revision,desired_enabled,desired_force_https)
            values (${actor}::uuid,uuidv7(),${'a'.repeat(64)},array['profile.example.test'],array['certificates.issue'],${'b'.repeat(64)},true,false)`)
            const certificates =
                await import('@/server/Admin/ProxyHostManagement/certificate-jobs.lease.server.ts')
            expect(
                await measure('certificate-job-claim', () => certificates.claimCertificateJob()),
            ).not.toBeNull()
            const cursorWorker =
                await import('@/server/Admin/CertificateManagement/certificate-events.worker.ts')
            expect(
                await measure('certificate-cursor-empty-page', () =>
                    cursorWorker.synchronizeCertificateEventsOnce(),
                ),
            ).toBe(true)
            const audit = await import('@/server/Audit/audit.service.ts')
            await measure('audit-append-retention', () =>
                db.transaction((transaction) =>
                    audit.appendAuditEventInTransactionService(transaction, {
                        actorUserId: actor,
                        actorKind: 'user',
                        action: 'apply',
                        resource: 'proxy-runtime',
                        targetId: null,
                        result: 'success',
                        metadata: { runtimeStatus: 'applied' },
                    }),
                ),
            )

            const holder = new SQL(url, {
                max: 1,
                connection: { application_name: 'worker-profile-lock-holder' },
            })
            const locked = Promise.withResolvers<void>()
            const release = Promise.withResolvers<void>()
            const holding = holder.begin(async (connection) => {
                await connection`select pg_advisory_xact_lock(${audit.AUDIT_RETENTION_LOCK_ID})`
                locked.resolve()
                await release.promise
            })
            try {
                await locked.promise
                await measure('audit-controlled-lock-timeout', async () => {
                    let timeoutState: unknown
                    try {
                        await db.transaction(async (transaction) => {
                            await transaction.execute(sql`set local lock_timeout='50ms'`)
                            await audit.appendAuditEventInTransactionService(transaction, {
                                actorUserId: actor,
                                actorKind: 'user',
                                action: 'apply',
                                resource: 'proxy-runtime',
                                targetId: null,
                                result: 'success',
                                metadata: { runtimeStatus: 'applied' },
                            })
                        })
                    } catch (error) {
                        const cause =
                            error && typeof error === 'object' && 'cause' in error
                                ? error.cause
                                : error
                        if (cause && typeof cause === 'object' && 'errno' in cause)
                            timeoutState = cause.errno
                    }
                    expect(timeoutState).toBe('55P03')
                })
            } finally {
                release.resolve()
                await holding
                await holder.close()
            }

            const baseline = historicalRetention()
            const { HOST_MONITORING_LIMITS: limits } =
                await import('@/config/host-monitoring.config.ts')
            const { prepareWorkerTransaction } =
                await import('@/server/HostMonitoring/host-monitoring.storage.ts')
            expect(limits.maxObservations).toBe(100_000)
            expect(limits.retentionDeleteLimit).toBe(4096)
            /** Read counts outside operation timing; the optional identity digest proves the rollback sentinel retained the original observation identities. */
            const retentionCounts = async (identity = false) => {
                const [row] = await observer<
                    { observations: number; incidents: number; identity: string | null }[]
                >`
                    select count(*)::int observations,
                    (select count(*)::int from rentnerproxy.host_check_incidents) incidents,
                    case when ${identity} then md5(string_agg(id::text,',' order by id)) else null end identity
                    from rentnerproxy.host_check_observations`
                return row!
            }
            const beforeRetention = await retentionCounts(true)
            expect(beforeRetention.observations).toBeGreaterThan(limits.maxObservations)
            const rollback = new Error('owned retention baseline rollback sentinel')
            await measure('retention-original-rollback', async () => {
                try {
                    await db.transaction(async (transaction) => {
                        // Same 2s deadlines and settings-before-monitoring lock order as the pinned original wrapper.
                        await prepareWorkerTransaction(transaction)
                        for (const statement of baseline.statements)
                            await transaction.execute(sql.raw(statement))
                        throw rollback
                    })
                } catch (error) {
                    if (error !== rollback) throw error
                }
            })
            expect(await retentionCounts(true)).toEqual(beforeRetention)
            const pages: Array<{
                pass: number
                observationsBefore: number
                observationsAfter: number
                observationsDeleted: number
                incidentsDeleted: number
            }> = []
            let count = beforeRetention.observations
            let arrivals = 0
            const retentionBegan = performance.now()
            for (let pass = 0; pass < 24 && count > limits.maxObservations; pass++) {
                if (pass === 2) {
                    await db.execute(sql`insert into rentnerproxy.host_check_observations(policy_id,policy_version,checked_at,status,code)
                        select id,version,clock_timestamp(),'healthy','expected_status' from rentnerproxy.host_check_policies order by id limit 16`)
                    arrivals += 16
                }
                const beforePage = await retentionCounts()
                await measure('retention-current-page-' + (pass + 1), () =>
                    monitoring.pruneHostMonitoringHistory(),
                )
                const afterPage = await retentionCounts()
                const observationsDeleted = beforePage.observations - afterPage.observations
                const incidentsDeleted = beforePage.incidents - afterPage.incidents
                expect(observationsDeleted).toBeGreaterThan(0)
                expect(observationsDeleted).toBeLessThanOrEqual(limits.retentionDeleteLimit)
                expect(incidentsDeleted).toBeGreaterThanOrEqual(0)
                expect(incidentsDeleted).toBeLessThanOrEqual(limits.retentionDeleteLimit)
                pages.push({
                    pass: pass + 1,
                    observationsBefore: beforePage.observations,
                    observationsAfter: afterPage.observations,
                    observationsDeleted,
                    incidentsDeleted,
                })
                count = afterPage.observations
            }
            expect(count).toBeLessThanOrEqual(limits.maxObservations)
            expect(pages.reduce((sum, page) => sum + page.observationsDeleted, 0)).toBe(
                beforeRetention.observations + arrivals - count,
            )
            const retention = {
                originalSource: {
                    revision: baseline.revision,
                    path: baseline.path,
                    implementationSha256: baseline.implementationSha256,
                },
                before: beforeRetention,
                rollbackIdentityVerified: true,
                controlledArrivals: arrivals,
                pages,
                after: count,
                currentDrainWallMs: performance.now() - retentionBegan,
                semantics:
                    'Original two DELETE statements execute once with original lock order/deadlines and then roll back; current real paged API commits every page. Unequal deleted work is reported, not presented as a same-work speedup; 16 arrivals occur once between pages.',
            }

            const after = await sourceStamp()
            expect(after.productionSha256).toBe(before.productionSha256)
            expect(after.head).toBe(before.head)
            const report = {
                    kind: 'worker-postgres-profile-v3',
                    source: before,
                    sourceAfter: after,
                    bun: Bun.version,
                    postgres: server,
                    statisticsScope: {
                        databaseOid,
                        resetArguments: statisticsIdentity.reset_arguments,
                        untouchedSentinelDatabaseOid: scopeIdentity.oid,
                        sentinelStatisticsPreserved: true,
                    },
                    platform: process.platform,
                    architecture: process.arch,
                    semantics: {
                        singlePass: true,
                        warmup: false,
                        sqlQueries:
                            'Actually awaited Drizzle SQL.unsafe queries; native BEGIN/COMMIT are separate',
                        hydratedBytes:
                            'UTF-8 JSON encoding of actual driver result rows, outside timing; not PostgreSQL wire or heap bytes',
                        jsonbWrites:
                            'Logical UTF-8 JSONB text per written row and column; not WAL or physical TOAST bytes',
                        cpu: 'Process CPU includes instrumentation; JSON and node:crypto/Bun CryptoHasher update/digest counters cover JS entrypoints only, excluding native SQL JSON decoding',
                        transactionTiming:
                            'Root BEGIN arrays exclude nested SAVEPOINTs, which have separate entry/callback arrays; explicit lock completion to transaction settlement is a client-observed hold interval, not exact server lock hold',
                        waits: '5ms sampled Lock waits and connection counts; client-minus-PG is a residual, not attributed pool/network wait. Failed SQL execution time is not attributed by pg_stat_statements',
                        postgresCpu: 'Optional collocated Linux /proc backend-generation and schedstat deltas; observer and auxiliary processes excluded. Operation windows include trigger overhead and backend startup. Runqueue time stays unknown when kernel schedstats are disabled; it is never SQL lock wait.',
                        postgresIo: 'pg_stat_statements block counts and PostgreSQL 18 I/O timing with track_io_timing enabled; buffer hits do not imply physical disk reads',
                        observerOverhead:
                            'JSONB AFTER triggers add PostgreSQL CPU/WAL and retain transaction atomicity; observer SQL excluded from counters',
                        controller:
                            'Certificate cursor uses injected empty page/metadata; actual capture, CAS, cursor persistence and retention SQL run',
                        capacity:
                            'insufficient; bounded per-operation profile, no throughput or capacity claim',
                    },
                    operations: reports,
                    retention,
            }
            const text = JSON.stringify(report)
            console.info(text)
            const reportPath = process.env.RENTNERPROXY_POSTGRES_PROFILE_REPORT
            if (reportPath) {
                const file = await open(reportPath, 'wx', 0o600)
                try {
                    await file.writeFile(text + '\n')
                } finally {
                    await file.close()
                }
            }
        } finally {
            active = undefined
            for (const restore of restores.toReversed()) restore()
            await db.$client.close()
            await scopeClient?.close()
            if (scopeCreated) await observer.unsafe('drop database "' + scopeDatabase + '"')
            await observer.close()
        }
    },
    20_000,
)
