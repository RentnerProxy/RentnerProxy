// oxlint-disable no-await-in-loop -- Scope setup, adversarial session state and cleanup are ordered integration boundaries.
import { test, expect } from 'bun:test'
import { SQL } from 'bun'
import { mkdir, writeFile, lstat, chmod, rm, rmdir } from 'node:fs/promises'
import {
    createMonitoringB2WindowCap,
    writeMonitoringB2WindowSeal,
    qualifyMonitoringB2WindowSession,
} from '../../../../../scripts/runtime-scale/monitoring-b2-window-cap.ts'
import { assertIsolatedMonitoringDatabase } from '../../../../../scripts/runtime-scale/monitoring.ts'
let isolated = false
try {
    assertIsolatedMonitoringDatabase(
        process.env.RENTNERPROXY_DATABASE_INTEGRATION,
        process.env.DATABASE_URL,
    )
    isolated =
        process.env.RENTNERPROXY_MONITORING_B2 === '1' &&
        process.platform === 'linux' &&
        process.getuid?.() === 0
} catch {
    /* Ordinary unit runs never open a database. */
}
const integration = isolated ? test : test.skip

integration(
    'restricted LOGIN observer sees its native session and physically closes before measurement',
    async () => {
        const admin = new SQL(process.env.DATABASE_URL!, { max: 1, connectionTimeout: 2 })
        const suffix = Bun.randomUUIDv7().replaceAll('-', ''),
            appRole = 'window_app_' + suffix,
            nativeRole = 'window_native_' + suffix
        let app: SQL | undefined, native: SQL | undefined
        try {
            await admin.unsafe(
                `CREATE ROLE ${appRole} LOGIN NOSUPERUSER NOCREATEROLE NOINHERIT; CREATE ROLE ${nativeRole} LOGIN NOSUPERUSER NOCREATEROLE NOINHERIT CONNECTION LIMIT 2`,
            )
            const appUrl = new URL(process.env.DATABASE_URL!),
                nativeUrl = new URL(process.env.DATABASE_URL!)
            appUrl.username = appRole
            appUrl.password = ''
            nativeUrl.username = nativeRole
            nativeUrl.password = ''
            app = new SQL(appUrl.toString(), { max: 1, connectionTimeout: 2 })
            native = new SQL(nativeUrl.toString(), { max: 1, connectionTimeout: 2 })
            const query = 'SELECT pg_backend_pid() pid',
                [owner] = await native.unsafe<{ pid: number }[]>(query)
            const [hidden] = await app.unsafe<
                { pid: number; state: string | null; query: string }[]
            >('SELECT pid,state,query FROM pg_stat_activity WHERE pid=$1', [owner!.pid])
            expect(hidden!.pid).toBe(owner!.pid)
            expect(hidden!.state).toBeNull()
            expect(hidden!.query).not.toBe(query)
            const [privileges] = await app.unsafe<
                { superuser: boolean; stats: boolean; identity: string }[]
            >(
                "SELECT rolsuper superuser,pg_has_role(current_user,'pg_read_all_stats','MEMBER') stats,current_user identity FROM pg_roles WHERE rolname=current_user",
            )
            expect(privileges).toEqual({ superuser: false, stats: false, identity: appRole })
            let ownerChecks = 0
            await qualifyMonitoringB2WindowSession(app, nativeUrl.toString(), query, () => {
                ownerChecks++
                return Promise.resolve(true)
            })
            expect(ownerChecks).toBe(1)
            const rows = await app.unsafe<{ pid: number }[]>(
                'SELECT pid FROM pg_stat_activity WHERE usename=$1',
                [nativeRole],
            )
            expect(rows.map((row) => row.pid)).toEqual([owner!.pid])
            await expect(
                qualifyMonitoringB2WindowSession(app, nativeUrl.toString(), query, () =>
                    Promise.reject(new Error('owned readiness rejected')),
                ),
            ).rejects.toThrow('owned readiness rejected')
            expect(
                (
                    await app.unsafe<{ pid: number }[]>(
                        'SELECT pid FROM pg_stat_activity WHERE usename=$1',
                        [nativeRole],
                    )
                ).map((row) => row.pid),
            ).toEqual([owner!.pid])
            const [nativePrivileges] = await native.unsafe<
                { superuser: boolean; stats: boolean; identity: string }[]
            >(
                "SELECT rolsuper superuser,pg_has_role(current_user,'pg_read_all_stats','MEMBER') stats,current_user identity FROM pg_roles WHERE rolname=current_user",
            )
            expect(nativePrivileges).toEqual({
                superuser: false,
                stats: false,
                identity: nativeRole,
            })
            await native.close()
            native = undefined
            expect(
                await app.unsafe('SELECT pid FROM pg_stat_activity WHERE usename=$1', [nativeRole]),
            ).toHaveLength(0)
        } finally {
            await app?.close()
            await native?.close()
            await admin.unsafe(`DROP ROLE IF EXISTS ${appRole}; DROP ROLE IF EXISTS ${nativeRole}`)
            await admin.close()
        }
    },
)

integration(
    'private finite workload preserves firsttwo dues, rejects shadow scope and defers only the owned third cadence',
    async () => {
        const database = new SQL(process.env.DATABASE_URL!, { max: 1, connectionTimeout: 2 })
        const runId = Bun.randomUUIDv7().replaceAll('-', '').slice(-12),
            windowId = 'cap-integration'
        const hosts = Array.from({ length: 1001 }, () => Bun.randomUUIDv7()),
            ids = Array.from({ length: 1001 }, () => Bun.randomUUIDv7())
        const domains = hosts.map((host, index) => `cap-${index}-${host}.fixture.test`)
        const scope = {
            enabled: true,
            runId,
            windowId,
            design: 'A' as const,
            policyIds: ids.slice(0, 1000),
        }
        const previousRun = process.env.RENTNERPROXY_RELIABILITY_ISOLATED,
            previousFixture = process.env.B2_APPLIANCE_FIXTURE
        let cap: Awaited<ReturnType<typeof createMonitoringB2WindowCap>>
        let sealCreated = false
        try {
            process.env.RENTNERPROXY_RELIABILITY_ISOLATED = runId
            process.env.B2_APPLIANCE_FIXTURE = runId
            await mkdir('/tmp/monitoring-b2', { mode: 0o755 })
            sealCreated = true
            await writeFile(
                '/tmp/monitoring-b2/mode.json',
                JSON.stringify({ runId, design: 'A' }),
                { mode: 0o644 },
            )
            await database.unsafe(
                "INSERT INTO rentnerproxy.proxy_hosts(id,forward_scheme,forward_host,forward_port) SELECT unnest($1::uuid[]),'http','127.0.0.1',9",
                [hosts],
            )
            await database.unsafe(
                'INSERT INTO rentnerproxy.host_domains(proxy_host_id,domain) SELECT * FROM unnest($1::uuid[],$2::text[])',
                [hosts, domains],
            )
            await database.unsafe(
                "INSERT INTO rentnerproxy.host_check_policies(id,proxy_host_id,domain,enabled,next_check_at) SELECT p,h,d,false,clock_timestamp()+interval '1 hour' FROM unnest($1::uuid[],$2::uuid[],$3::text[]) s(p,h,d)",
                [ids, hosts, domains],
            )
            await writeMonitoringB2WindowSeal(scope)
            expect((await lstat('/tmp/monitoring-b2/mode.json')).mode & 0o777).toBe(0o644)
            expect((await lstat('/tmp/monitoring-b2/measurement.json')).mode & 0o777).toBe(0o600)
            const reader = Bun.spawn(
                [
                    'gosu',
                    '10002:10002',
                    'bun',
                    '--no-env-file',
                    '-e',
                    "const fs=await import('node:fs/promises');await fs.access('/tmp/monitoring-b2/mode.json',4);try{await fs.access('/tmp/monitoring-b2/measurement.json',4);process.exit(1)}catch{process.exit(0)}",
                ],
                {
                    env: { PATH: process.env.PATH },
                    stdin: 'ignore',
                    stdout: 'ignore',
                    stderr: 'ignore',
                },
            )
            expect(await reader.exited).toBe(0)
            await chmod('/tmp/monitoring-b2/measurement.json', 0o644)
            await expect(createMonitoringB2WindowCap(database, scope)).rejects.toThrow('file seal')
            await chmod('/tmp/monitoring-b2/measurement.json', 0o600)
            await expect(
                createMonitoringB2WindowCap(database, { ...scope, policyIds: ids.slice(1) }),
            ).rejects.toThrow('selectors mismatch')
            cap = await createMonitoringB2WindowCap(database, scope)
            expect((await cap!.proof()).policies).toBe(1000)
            const origin = Date.now() + 30000,
                end = origin + 120000,
                deferred = origin + 180000
            const proof = await cap!.arm(origin)
            expect(proof.endMs).toBe(end)
            expect(proof.deferredMs).toBe(deferred)
            await database.unsafe(
                'CREATE TEMP TABLE scope(policy_id uuid); CREATE TEMP TABLE horizon(id integer,origin_ms bigint,end_ms bigint,deferred_ms bigint); INSERT INTO horizon VALUES(1,0,999999999999999,999999999999999)',
            )
            const lease = Bun.randomUUIDv7(),
                expires = Date.now() + 45000
            await database.unsafe(
                'UPDATE rentnerproxy.host_check_policies SET lease_token=$1,lease_expires_at=to_timestamp($2::double precision/1000) WHERE id=$3',
                [lease, expires, ids[0]!],
            )
            const [before] = await database.unsafe(
                'SELECT enabled,version,interval_seconds,timeout_ms,lease_token,lease_expires_at FROM rentnerproxy.host_check_policies WHERE id=$1',
                [ids[0]!],
            )
            for (const due of [origin, origin + 60000]) {
                const [row] = await database.unsafe<{ due: number }[]>(
                    'UPDATE rentnerproxy.host_check_policies SET next_check_at=to_timestamp($1::double precision/1000) WHERE id=$2 RETURNING extract(epoch FROM next_check_at)::float8*1000 due',
                    [due, ids[0]!],
                )
                expect(row!.due).toBe(due)
            }
            const [third] = await database.unsafe<{ due: number }[]>(
                'UPDATE rentnerproxy.host_check_policies SET next_check_at=to_timestamp($1::double precision/1000) WHERE id=$2 RETURNING extract(epoch FROM next_check_at)::float8*1000 due',
                [end, ids[0]!],
            )
            expect(third!.due).toBe(deferred)
            // Matching temp selectors plus a hostile temp horizon must also remain irrelevant.
            await database.unsafe('INSERT INTO pg_temp.scope VALUES($1)', [ids[1]!])
            const [secondShadow] = await database.unsafe<{ due: number }[]>(
                'UPDATE rentnerproxy.host_check_policies SET next_check_at=to_timestamp($1::double precision/1000) WHERE id=$2 RETURNING extract(epoch FROM next_check_at)::float8*1000 due',
                [end, ids[1]!],
            )
            expect(secondShadow!.due).toBe(deferred)
            const [foreign] = await database.unsafe<{ due: number }[]>(
                'UPDATE rentnerproxy.host_check_policies SET next_check_at=to_timestamp($1::double precision/1000) WHERE id=$2 RETURNING extract(epoch FROM next_check_at)::float8*1000 due',
                [end, ids[1000]!],
            )
            expect(foreign!.due).toBe(end)
            const [after] = await database.unsafe(
                'SELECT enabled,version,interval_seconds,timeout_ms,lease_token,lease_expires_at FROM rentnerproxy.host_check_policies WHERE id=$1',
                [ids[0]!],
            )
            expect(after).toEqual(before)
            // Controlled SQL timestamps model a late second completion without a real two-minute wait.
            const late = end + 1
            await database.unsafe(
                "INSERT INTO rentnerproxy.host_check_observations(policy_id,policy_version,checked_at,status,code,duration_ms,http_status) SELECT id,version,to_timestamp($1::double precision/1000),'healthy','expected_status',1,200 FROM rentnerproxy.host_check_policies WHERE id=$2",
                [late, ids[0]!],
            )
            await database.unsafe(
                'UPDATE rentnerproxy.host_check_policies SET next_check_at=to_timestamp($1::double precision/1000),last_checked_at=to_timestamp($2::double precision/1000) WHERE id=$3',
                [end, late, ids[0]!],
            )
            const [observed] = await database.unsafe<{ checked: number }[]>(
                'SELECT extract(epoch FROM checked_at)::float8*1000 checked FROM rentnerproxy.host_check_observations WHERE policy_id=$1',
                [ids[0]!],
            )
            expect(observed!.checked).toBe(late)
            await cap!.drop()
            cap = undefined
            const [removed] = await database.unsafe<{ count: number }[]>(
                'SELECT count(*)::integer count FROM pg_namespace WHERE nspname=$1',
                ['monitor_b2_window_' + runId],
            )
            expect(removed!.count).toBe(0)
            expect(
                await createMonitoringB2WindowCap(database, { ...scope, enabled: false }),
            ).toBeUndefined()
            const [uncapped] = await database.unsafe<{ due: number }[]>(
                'UPDATE rentnerproxy.host_check_policies SET next_check_at=to_timestamp($1::double precision/1000) WHERE id=$2 RETURNING extract(epoch FROM next_check_at)::float8*1000 due',
                [end, ids[0]!],
            )
            expect(uncapped!.due).toBe(end)
        } finally {
            await cap?.drop()
            await database.unsafe('DELETE FROM rentnerproxy.proxy_hosts WHERE id=ANY($1::uuid[])', [
                hosts,
            ])
            await database.close()
            if (sealCreated) {
                await rm('/tmp/monitoring-b2/measurement.json', { force: true })
                await rm('/tmp/monitoring-b2/mode.json')
                await rmdir('/tmp/monitoring-b2')
            }
            if (previousRun === undefined) delete process.env.RENTNERPROXY_RELIABILITY_ISOLATED
            else process.env.RENTNERPROXY_RELIABILITY_ISOLATED = previousRun
            if (previousFixture === undefined) delete process.env.B2_APPLIANCE_FIXTURE
            else process.env.B2_APPLIANCE_FIXTURE = previousFixture
        }
    },
    30000,
)
