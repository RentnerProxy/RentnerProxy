// oxlint-disable no-await-in-loop -- Faults and replay transitions must run in durable order.
import { afterEach, describe, expect, test } from 'bun:test'
import { SQL } from 'bun'
import { createHash } from 'node:crypto'
import { Database } from 'bun:sqlite'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { createMonitoringB2Authority } from '../../../../../scripts/runtime-scale/monitoring-b2-authority.ts'
import { createMonitoringB2Replay } from '../../../../../scripts/runtime-scale/monitoring-b2-replay.ts'
import { createMonitoringB2WebReplay } from '../../../../../scripts/runtime-scale/monitoring-b2-web-replay.ts'
import { assertIsolatedMonitoringDatabase } from '../../../../../scripts/runtime-scale/monitoring.ts'
import { monitoringB2ClaimSchema } from '../../../../../scripts/runtime-scale/monitoring-b2.validation.ts'
import type { MonitoringB2Claim } from '../../../../../scripts/runtime-scale/Types/monitoring-b2.types.ts'
import { monitoringB2PolicyTransaction } from '../../../../../scripts/runtime-scale/monitoring-b2-fixture-storage.ts'
import {
    admitMonitoringB2Probe,
    type MonitoringB2ProbeRequest,
} from '../../../../../scripts/runtime-scale/monitoring-b2-probe-broker.ts'

let isolated = false
try {
    assertIsolatedMonitoringDatabase(
        process.env.RENTNERPROXY_DATABASE_INTEGRATION,
        process.env.DATABASE_URL,
    )
    isolated = process.env.RENTNERPROXY_MONITORING_B2 === '1'
} catch {
    /* Ordinary checks never contact a database. */
}
const integration = isolated ? test : test.skip
let authority: Awaited<ReturnType<typeof createMonitoringB2Authority>> | undefined
let worker: SQL | undefined
let directory: string | undefined
const hosts: string[] = []
const stage = (status: string, code: string) => ({
    status,
    code,
    durationMs: 1,
    addresses: [],
    httpStatus: null,
})
async function originalTestPublication() {
    const [contexts, storage] = await Promise.all([
        import('../../../server/RuntimeDiagnostics/domain-diagnostics-data.ts'),
        import('../../../server/HostMonitoring/host-monitoring.storage.ts'),
    ])
    return {
        contexts: contexts.readHostMonitoringContextsInTransaction,
        prepare: storage.prepareWorkerTransaction,
        proof: {
            originalChunkSha256: 'isolated-original-source',
            fragmentSha256: 'isolated-original-source',
            functions: 5 as const,
            originalSingletonImports: true,
        },
    }
}
afterEach(async () => {
    await worker?.close()
    worker = undefined
    if (authority) {
        for (const id of hosts)
            await authority.database.unsafe('DELETE FROM rentnerproxy.proxy_hosts WHERE id=$1', [
                id,
            ])
        await authority.cleanup()
        authority = undefined
    }
    hosts.length = 0
    if (directory) {
        const owned = resolve(directory)
        if (
            !owned.startsWith(resolve(tmpdir()) + '\\rentnerproxy-b2-') &&
            !owned.startsWith(resolve(tmpdir()) + '/rentnerproxy-b2-')
        )
            throw new Error('B2 temporary ownership mismatch')
        await rm(owned, { recursive: true, force: true })
        directory = undefined
    }
})
async function fixture() {
    authority = await createMonitoringB2Authority()
    const id = Bun.randomUUIDv7()
    hosts.push(id)
    const domain = `b2-${id}.example.test`
    await authority.database.unsafe(
        "INSERT INTO rentnerproxy.proxy_hosts(id,forward_scheme,forward_host,forward_port) VALUES($1,'http','127.0.0.1',9)",
        [id],
    )
    await authority.database.unsafe(
        'INSERT INTO rentnerproxy.host_domains(proxy_host_id,domain) VALUES($1,$2)',
        [id, domain],
    )
    const [policy] = await authority.database.unsafe<{ id: string }[]>(
        "INSERT INTO rentnerproxy.host_check_policies(proxy_host_id,domain,enabled,failure_threshold,next_check_at) VALUES($1,$2,true,1,clock_timestamp()-interval '1 second') RETURNING id",
        [id, domain],
    )
    await authority.publish(true)
    worker = new SQL(authority.workerUrl, { max: 2, connectionTimeout: 2 })
    return { id, domain, policyId: policy!.id }
}
async function claim() {
    const rows = await worker!.unsafe<{ claim: unknown }[]>(
        `SELECT * FROM ${authority!.namespace}.claim(16,$1,$2)`,
        [authority!.storeId, authority!.controllerEpoch],
    )
    return rows.map((row) => monitoringB2ClaimSchema.parse(row.claim))
}
async function begin(claimed: MonitoringB2Claim) {
    const [row] = await worker!.unsafe<{ admitted: number | null }[]>(
        `SELECT ${authority!.namespace}.begin_check($1::uuid,$2::uuid,$3::uuid,$4,$5,$6) admitted`,
        [
            claimed.policyId,
            claimed.policyVersion,
            claimed.leaseToken,
            claimed.storeId,
            claimed.controllerEpoch,
            claimed.generation,
        ],
    )
    return row!.admitted === null ? null : Number(row!.admitted)
}

describe('B2 actual-table authority and Bun state-machine replay', () => {
    integration(
        'scoped broker admission consumes only one exact live claim and rejects committed edits',
        async () => {
            const { id } = await fixture()
            const [reserved] = await claim()
            expect(reserved).toBeDefined()
            const selector: MonitoringB2ProbeRequest = {
                hostType: 'proxy',
                hostId: reserved!.hostId,
                domain: reserved!.domain,
                expectedRevision: reserved!.before.revision,
                upstreamOnly: true,
                timeoutMs: reserved!.timeoutMs,
            }
            const admit = (input = selector, attempt = reserved!.leaseToken) =>
                admitMonitoringB2Probe(authority!.database, authority!.namespace, input, attempt)
            expect(await admit()).toBeNull()
            expect(await begin(reserved!)).not.toBeNull()
            for (const changed of [
                { hostId: Bun.randomUUIDv7() },
                { domain: 'unconfigured.fixture.test' },
                { expectedRevision: 'sha256:' + '0'.repeat(64) },
                { timeoutMs: selector.timeoutMs === 1000 ? 2000 : 1000 },
            ])
                expect(await admit({ ...selector, ...changed })).toBeNull()
            expect(await admit(selector, Bun.randomUUIDv7())).toBeNull()
            expect(await admit()).toBe(reserved!.leaseExpiresAtMs)
            expect(await admit()).toBeNull()
            // The owner resets only this test marker to isolate the committed-edit fence.
            await authority!.database.unsafe(
                `UPDATE ${authority!.namespace}.admissions SET probe_started_ms=NULL WHERE lease_token=$1`,
                [reserved!.leaseToken],
            )
            await authority!.database.unsafe(
                'UPDATE rentnerproxy.proxy_hosts SET forward_port=10 WHERE id=$1',
                [id],
            )
            expect(await admit()).toBeNull()
            await authority!.publish()
            expect(await admit()).toBeNull()
        },
        15000,
    )
    integration(
        'direct fixture policy changes acquire the original monitoring gate before locking policy rows',
        async () => {
            const { policyId } = await fixture()
            const [{ getAuthDatabase }, { sql }] = await Promise.all([
                import('../../../server/Auth/Core/database.server.ts'),
                import('drizzle-orm'),
            ])
            const blocker = new SQL(process.env.DATABASE_URL!, { max: 1, connectionTimeout: 2 }),
                inspector = new SQL(process.env.DATABASE_URL!, { max: 1, connectionTimeout: 2 })
            const held = Promise.withResolvers<void>(),
                release = Promise.withResolvers<void>()
            const blocking = blocker.begin(async (transaction) => {
                await transaction.unsafe('SELECT pg_advisory_xact_lock(7421916)')
                held.resolve()
                await release.promise
            })
            let mutation: Promise<boolean> | undefined
            try {
                await held.promise
                mutation = monitoringB2PolicyTransaction(getAuthDatabase(), async (transaction) => {
                    await transaction.execute(
                        sql`UPDATE rentnerproxy.host_check_policies SET enabled=false WHERE id=${policyId}::uuid`,
                    )
                }).then(
                    () => true,
                    () => false,
                )
                const deadline = Date.now() + 1000
                let waiting = false
                while (Date.now() < deadline) {
                    const [row] = await inspector.unsafe<{ count: number }[]>(
                        "SELECT count(*)::integer count FROM pg_locks WHERE locktype='advisory' AND objid=7421916 AND NOT granted",
                    )
                    if (row!.count > 0) {
                        waiting = true
                        break
                    }
                    await Bun.sleep(5)
                }
                expect(waiting).toBe(true)
                // An update-before-prepare regression holds this row and makes NOWAIT fail.
                const [policy] = await inspector.unsafe<{ enabled: boolean }[]>(
                    'SELECT enabled FROM rentnerproxy.host_check_policies WHERE id=$1 FOR UPDATE NOWAIT',
                    [policyId],
                )
                expect(policy!.enabled).toBe(true)
            } finally {
                release.resolve()
                await blocking
                expect(await mutation).toBe(true)
                await Promise.all([blocker.close(), inspector.close()])
            }
            const [policy] = await authority!.database.unsafe<{ enabled: boolean }[]>(
                'SELECT enabled FROM rentnerproxy.host_check_policies WHERE id=$1',
                [policyId],
            )
            expect(policy!.enabled).toBe(false)
        },
        15000,
    )
    integration(
        'NULL arguments and a missing store identity cannot mutate leases or execution receipts',
        async () => {
            const { policyId } = await fixture()
            await authority!.database.unsafe(
                `DELETE FROM ${authority!.namespace}.identity WHERE id=1`,
            )
            expect(await claim()).toHaveLength(0)
            expect(
                await worker!.unsafe(`SELECT * FROM ${authority!.namespace}.claim(16,NULL,NULL)`),
            ).toHaveLength(0)
            const [beginNull] = await worker!.unsafe<{ value: null }[]>(
                `SELECT ${authority!.namespace}.begin_check(NULL,NULL,NULL,NULL,NULL,NULL) value`,
            )
            expect(beginNull!.value).toBeNull()
            const [completeNull] = await worker!.unsafe<{ value: boolean }[]>(
                `SELECT ${authority!.namespace}.record_raw_complete(NULL,NULL,NULL,NULL,NULL,NULL) value`,
            )
            expect(completeNull!.value).toBe(false)
            await expect(authority!.resetIdentity()).rejects.toThrow('B2 reset identity missing')
            const [state] = await authority!.database.unsafe<
                { mutations: number; receipts: number }[]
            >(
                `SELECT (SELECT count(*)::integer FROM rentnerproxy.host_check_policies WHERE id=$1 AND (lease_token IS NOT NULL OR lease_expires_at IS NOT NULL)) mutations,(SELECT count(*)::integer FROM ${authority!.namespace}.raw_completed) receipts`,
                [policyId],
            )
            expect(state).toEqual({ mutations: 0, receipts: 0 })
        },
        15000,
    )
    integration(
        'batch replay uses the original application transaction and an exact observation-ID receipt across savepoints',
        async () => {
            const { policyId } = await fixture()
            const [reserved] = await claim()
            directory = await mkdtemp(join(tmpdir(), 'rentnerproxy-b2-'))
            const path = join(directory, 'outbox.sqlite'),
                store = new Database(path, { strict: true })
            store.exec(
                'CREATE TABLE b2_identity(id INTEGER PRIMARY KEY,store_id TEXT,epoch TEXT,acked_sequence INTEGER NOT NULL DEFAULT 0);CREATE TABLE b2_outbox(sequence INTEGER PRIMARY KEY AUTOINCREMENT,attempt_id TEXT,store_id TEXT,claim TEXT,started_ms INTEGER,admitted_ms INTEGER,durable_ms INTEGER,completed_ms INTEGER,result TEXT);CREATE TABLE b2_status(id INTEGER PRIMARY KEY,pressure INTEGER DEFAULT 0,interrupted INTEGER DEFAULT 0,gaps INTEGER DEFAULT 0,resync_required INTEGER DEFAULT 0);INSERT INTO b2_status(id) VALUES(1)',
            )
            store
                .query('INSERT INTO b2_identity(id,store_id,epoch) VALUES(1,?,?)')
                .run(authority!.storeId, authority!.controllerEpoch)
            const now = (await begin(reserved!))!,
                payload = JSON.stringify(reserved),
                result = JSON.stringify({ error: 'runtime_unavailable' })
            const put = (nativeResult = result) =>
                store
                    .query('INSERT INTO b2_outbox VALUES(1,?,?,?,?,?,?,?,?)')
                    .run(
                        reserved!.leaseToken,
                        reserved!.storeId,
                        payload,
                        now,
                        now,
                        now,
                        now,
                        nativeResult,
                    )
            put()
            // An eligible first record followed by a terminal rejection must ACK in durable sequence order.
            const missing = {
                ...reserved!,
                policyId: Bun.randomUUIDv7(),
                leaseToken: Bun.randomUUIDv7(),
            }
            store
                .query('INSERT INTO b2_outbox VALUES(2,?,?,?,?,?,?,?,?)')
                .run(
                    missing.leaseToken,
                    missing.storeId,
                    JSON.stringify(missing),
                    now,
                    now,
                    now,
                    now,
                    result,
                )
            const [{ getAuthDatabase }, schema, repository, state, diagnostics] = await Promise.all(
                [
                    import('../../../server/Auth/Core/database.server.ts'),
                    import('../../../db/schema.ts'),
                    import('../../../server/HostMonitoring/host-monitoring.repository.ts'),
                    import('../../../lib/HostMonitoring/state.ts'),
                    import('../../../lib/RuntimeDiagnostics/domainDiagnostics.ts'),
                ],
            )
            const database = getAuthDatabase()
            const replay = await createMonitoringB2WebReplay(
                {
                    getDatabase: getAuthDatabase,
                    background: () => ({ transaction: database.transaction.bind(database) }),
                    policies: schema.hostCheckPolicies,
                    complete: repository.completeHostMonitoringClaims,
                    contexts: repository.readHostMonitoringClaimContexts,
                    classify: state.classifyHostMonitoringOutcome,
                    unknown: state.unknownHostMonitoringOutcome,
                    resultSchema: diagnostics.domainDiagnosticsResultSchema,
                    publication: originalTestPublication,
                },
                {
                    namespace: authority!.namespace,
                    storeId: authority!.storeId,
                    controllerEpoch: authority!.controllerEpoch,
                    path,
                },
                true,
            )
            try {
                await replay.pass()
                expect(replay.stats()).toMatchObject({
                    accepted: 1,
                    duplicates: 0,
                    rejected: 1,
                    pending: 0,
                })
                expect(
                    store
                        .query<{ acked_sequence: number }, []>(
                            'SELECT acked_sequence FROM b2_identity',
                        )
                        .get()!.acked_sequence,
                ).toBe(2)
                const [proof] = await authority!.database.unsafe<
                    { applied: boolean; count: number }[]
                >(
                    `SELECT EXISTS(SELECT 1 FROM rentnerproxy.host_check_observations o WHERE o.id=r.observation_id AND o.policy_id=r.policy_id AND o.policy_version=r.policy_version) applied,(SELECT count(*)::int FROM rentnerproxy.host_check_observations WHERE policy_id=$1) count FROM ${authority!.namespace}.receipts r WHERE r.policy_id=$1`,
                    [policyId],
                )
                expect(proof).toEqual({ applied: true, count: 1 })
                // Simulate local ACK committed but PostgreSQL pending cleanup rolled back after Web death.
                await authority!.database.unsafe(
                    `INSERT INTO ${authority!.namespace}.replay_pending SELECT attempt_id,store_id,epoch,generation,policy_id,policy_version,digest,due_ms,started_ms,admitted_ms,durable_ms,completed_ms FROM ${authority!.namespace}.receipts WHERE policy_id=$1`,
                    [policyId],
                )
                expect(await replay.repair()).toBe(1)
                expect(replay.stats().repairedPending).toBe(1)
                // The normal adapter pass republishes canonical contexts after a real configuration commit.
                await authority!.database.unsafe(
                    'UPDATE rentnerproxy.proxy_hosts SET forward_port=10 WHERE id=$1',
                    [reserved!.hostId],
                )
                expect(await claim()).toHaveLength(0)
                await replay.repair()
                expect(replay.stats().publications).toBe(1)
                await authority!.database.unsafe(
                    "UPDATE rentnerproxy.host_check_policies SET next_check_at=clock_timestamp()-interval '1 second' WHERE id=$1",
                    [policyId],
                )
                const [fresh] = await claim()
                expect(fresh).toBeDefined()
                expect(fresh!.before.proof).not.toBe(reserved!.before.proof)
                const [admission] = await worker!.unsafe<{ admitted: string | null }[]>(
                    `SELECT ${authority!.namespace}.begin_check($1::uuid,$2::uuid,$3::uuid,$4,$5,$6)::text admitted`,
                    [
                        fresh!.policyId,
                        fresh!.policyVersion,
                        fresh!.leaseToken,
                        fresh!.storeId,
                        fresh!.controllerEpoch,
                        fresh!.generation,
                    ],
                )
                expect(admission!.admitted).not.toBeNull()
                const admittedMs = Number(admission!.admitted)
                // Preserve a real PG reservation/admission, then corrupt before+generation+native result together.
                store
                    .query('INSERT INTO b2_outbox VALUES(3,?,?,?,?,?,?,?,?)')
                    .run(
                        fresh!.leaseToken,
                        fresh!.storeId,
                        JSON.stringify(fresh),
                        admittedMs,
                        admittedMs,
                        admittedMs,
                        admittedMs,
                        result,
                    )
                await authority!.database.unsafe(
                    'UPDATE rentnerproxy.proxy_hosts SET forward_port=11 WHERE id=$1',
                    [fresh!.hostId],
                )
                await replay.repair()
                const [publication] = await authority!.database.unsafe<
                    { before: MonitoringB2Claim['before']; generation: number }[]
                >(
                    `SELECT context.before,identity.generation::integer FROM ${authority!.namespace}.contexts context,${authority!.namespace}.identity identity WHERE context.policy_id=$1 AND identity.id=1`,
                    [policyId],
                )
                const forged = {
                    ...fresh!,
                    generation: publication!.generation,
                    before: publication!.before,
                }
                const forgedResult = {
                    hostType: 'proxy',
                    hostId: forged.hostId,
                    domain: forged.domain,
                    expectedRevision: forged.before.revision,
                    activeRevision: forged.before.revision,
                    checkedAt: new Date().toISOString(),
                    observationPoint: 'appliance',
                    publicReachability: 'not_checked',
                    stages: {
                        configuration: stage('passed', 'configuration_active'),
                        dns: stage('unknown', 'skipped'),
                        upstream: stage('failed', 'upstream_timeout'),
                        tls: stage('not_applicable', 'http_upstream'),
                    },
                }
                store
                    .query('UPDATE b2_outbox SET claim=?,result=? WHERE sequence=3')
                    .run(JSON.stringify(forged), JSON.stringify(forgedResult))
                await replay.pass()
                const [tamper] = await authority!.database.unsafe<
                    { reason: string; observation_id: string | null }[]
                >(
                    `SELECT reason,observation_id FROM ${authority!.namespace}.receipts WHERE attempt_id=$1`,
                    [fresh!.leaseToken],
                )
                expect(tamper).toEqual({ reason: 'original_claim_mismatch', observation_id: null })
                const [original] = await authority!.database.unsafe<
                    { original_claim: unknown; admitted_ms: string }[]
                >(
                    `SELECT original_claim,admitted_ms::text FROM ${authority!.namespace}.admissions WHERE lease_token=$1`,
                    [fresh!.leaseToken],
                )
                expect(monitoringB2ClaimSchema.parse(original!.original_claim)).toEqual(fresh!)
                expect(Number(original!.admitted_ms)).toBe(admittedMs)
                await authority!.database.unsafe(
                    "UPDATE rentnerproxy.host_check_policies SET lease_token=NULL,lease_expires_at=NULL,next_check_at=clock_timestamp()+interval '60 seconds' WHERE id=$1",
                    [policyId],
                )
                await authority!.database.unsafe(
                    "UPDATE rentnerproxy.host_check_policies SET next_check_at=clock_timestamp()-interval '1 second' WHERE id=$1",
                    [policyId],
                )
                const [unchanged] = await claim(),
                    unchangedAdmit = (await begin(unchanged!))!
                store
                    .query('INSERT INTO b2_outbox VALUES(4,?,?,?,?,?,?,?,?)')
                    .run(
                        unchanged!.leaseToken,
                        unchanged!.storeId,
                        JSON.stringify(unchanged),
                        unchangedAdmit,
                        unchangedAdmit,
                        unchangedAdmit,
                        unchangedAdmit,
                        result,
                    )
                await authority!.database.unsafe(
                    'UPDATE rentnerproxy.proxy_hosts SET forward_port=12 WHERE id=$1',
                    [unchanged!.hostId],
                )
                await replay.repair()
                await replay.pass()
                const [stale] = await authority!.database.unsafe<
                    { disposition: string; observation_id: string | null }[]
                >(
                    `SELECT disposition,observation_id FROM ${authority!.namespace}.receipts WHERE attempt_id=$1`,
                    [unchanged!.leaseToken],
                )
                expect(stale).toEqual({ disposition: 'rejected', observation_id: null })
                const [safe] = await authority!.database.unsafe<
                    { observations: number; incidents: number }[]
                >(
                    'SELECT (SELECT count(*)::integer FROM rentnerproxy.host_check_observations WHERE policy_id=$1) observations,(SELECT count(*)::integer FROM rentnerproxy.host_check_incidents WHERE policy_id=$1) incidents',
                    [policyId],
                )
                expect(safe).toEqual({ observations: 1, incidents: 0 })
                await authority!.database.unsafe(
                    `UPDATE ${authority!.namespace}.identity SET paused=true,resync_required=true,generation=generation+1 WHERE id=1`,
                )
                await replay.repair()
                expect(replay.stats().publications).toBe(3)
                const [paused] = await authority!.database.unsafe<
                    { paused: boolean; resync_required: boolean }[]
                >(`SELECT paused,resync_required FROM ${authority!.namespace}.identity WHERE id=1`)
                expect(paused).toEqual({ paused: true, resync_required: true })
                expect(await claim()).toHaveLength(0)
                await authority!.publish(true)
                put()
                await replay.pass()
                expect(replay.stats()).toMatchObject({
                    accepted: 1,
                    duplicates: 1,
                    rejected: 3,
                    pending: 0,
                })
                put(JSON.stringify({ error: 'interrupted' }))
                await expect(replay.pass()).rejects.toThrow('B2 replay payload conflict')
                expect(replay.stats().pending).toBe(1)
                await authority!.resetIdentity()
                await replay.pass()
                expect(replay.stats()).toMatchObject({ accepted: 1, duplicates: 1, pending: 1 })
                // Removing the current unacknowledged sequence is data loss, never a successful replay.
                store.exec(
                    'INSERT INTO b2_outbox(sequence,attempt_id,store_id,claim,started_ms,completed_ms,result) SELECT 5,attempt_id,store_id,claim,started_ms,completed_ms,result FROM b2_outbox WHERE sequence=1; DELETE FROM b2_outbox WHERE sequence=5',
                )
                await replay.pass()
                expect(
                    store
                        .query<{ gaps: number; resync_required: number }, []>(
                            'SELECT gaps,resync_required FROM b2_status WHERE id=1',
                        )
                        .get(),
                ).toEqual({ gaps: 1, resync_required: 1 })
                expect(replay.stats()).toMatchObject({ accepted: 1, duplicates: 1, pending: 1 })
            } finally {
                replay.close()
                store.close()
            }
        },
        15000,
    )
    integration(
        'a raw gap first discovered at final ACK durably latches without advancing the cursor',
        async () => {
            await fixture()
            const [reserved] = await claim()
            directory = await mkdtemp(join(tmpdir(), 'rentnerproxy-b2-'))
            const path = join(directory, 'ack-gap.sqlite'),
                store = new Database(path, { strict: true }),
                now = (await begin(reserved!))!
            store.exec(
                'CREATE TABLE b2_identity(id INTEGER PRIMARY KEY,store_id TEXT,epoch TEXT,acked_sequence INTEGER NOT NULL DEFAULT 0);CREATE TABLE b2_outbox(sequence INTEGER PRIMARY KEY AUTOINCREMENT,attempt_id TEXT,store_id TEXT,claim TEXT,started_ms INTEGER,admitted_ms INTEGER,durable_ms INTEGER,completed_ms INTEGER,result TEXT);CREATE TABLE b2_status(id INTEGER PRIMARY KEY,pressure INTEGER DEFAULT 0,interrupted INTEGER DEFAULT 0,gaps INTEGER DEFAULT 0,resync_required INTEGER DEFAULT 0);INSERT INTO b2_status(id) VALUES(1)',
            )
            store
                .query('INSERT INTO b2_identity(id,store_id,epoch) VALUES(1,?,?)')
                .run(authority!.storeId, authority!.controllerEpoch)
            store
                .query('INSERT INTO b2_outbox VALUES(1,?,?,?,?,?,?,?,?)')
                .run(
                    reserved!.leaseToken,
                    reserved!.storeId,
                    JSON.stringify(reserved),
                    now,
                    now,
                    now,
                    now,
                    JSON.stringify({ error: 'runtime_unavailable' }),
                )
            const [{ getAuthDatabase }, schema, repository, state, diagnostics] = await Promise.all(
                [
                    import('../../../server/Auth/Core/database.server.ts'),
                    import('../../../db/schema.ts'),
                    import('../../../server/HostMonitoring/host-monitoring.repository.ts'),
                    import('../../../lib/HostMonitoring/state.ts'),
                    import('../../../lib/RuntimeDiagnostics/domainDiagnostics.ts'),
                ],
            )
            const database = getAuthDatabase()
            const replay = await createMonitoringB2WebReplay(
                {
                    getDatabase: getAuthDatabase,
                    background: () => ({ transaction: database.transaction.bind(database) }),
                    policies: schema.hostCheckPolicies,
                    complete: async (items) => {
                        const results = await repository.completeHostMonitoringClaims(items)
                        store.exec('DELETE FROM b2_outbox WHERE sequence=1')
                        return results
                    },
                    contexts: repository.readHostMonitoringClaimContexts,
                    classify: state.classifyHostMonitoringOutcome,
                    unknown: state.unknownHostMonitoringOutcome,
                    resultSchema: diagnostics.domainDiagnosticsResultSchema,
                    publication: originalTestPublication,
                },
                {
                    namespace: authority!.namespace,
                    storeId: authority!.storeId,
                    controllerEpoch: authority!.controllerEpoch,
                    path,
                },
                true,
            )
            try {
                await replay.pass()
                expect(
                    store
                        .query<{ gaps: number; resync_required: number }, []>(
                            'SELECT gaps,resync_required FROM b2_status WHERE id=1',
                        )
                        .get(),
                ).toEqual({ gaps: 1, resync_required: 1 })
                expect(
                    store
                        .query<{ acked_sequence: number }, []>(
                            'SELECT acked_sequence FROM b2_identity WHERE id=1',
                        )
                        .get()!.acked_sequence,
                ).toBe(0)
                await replay.pass()
                expect(replay.stats().accepted).toBe(1)
            } finally {
                replay.close()
                store.close()
            }
            const reopened = new Database(path, { readonly: true })
            try {
                expect(
                    reopened
                        .query<{ resync_required: number }, []>(
                            'SELECT resync_required FROM b2_status WHERE id=1',
                        )
                        .get()!.resync_required,
                ).toBe(1)
            } finally {
                reopened.close()
            }
        },
        15000,
    )
    integration(
        'immutable original admissions are bounded, fail closed at pressure, and retain the first trusted admission time',
        async () => {
            const { policyId } = await fixture()
            await authority!.database.unsafe(
                `INSERT INTO ${authority!.namespace}.admissions(lease_token,original_claim) SELECT gen_random_uuid(),'{"syntheticPressure":true}'::jsonb FROM generate_series(1,4096)`,
            )
            expect(await claim()).toHaveLength(0)
            const [full] = await authority!.database.unsafe<{ records: number; leases: number }[]>(
                `SELECT (SELECT count(*)::integer FROM ${authority!.namespace}.admissions) records,(SELECT count(*)::integer FROM rentnerproxy.host_check_policies WHERE id=$1 AND lease_token IS NOT NULL) leases`,
                [policyId],
            )
            expect(full).toEqual({ records: 4096, leases: 0 })
            // Controlled fixture age, never a two-minute soak or deletion of an actual raw outcome.
            await authority!.database.unsafe(
                `UPDATE ${authority!.namespace}.admissions SET created_at=clock_timestamp()-interval '120.001 seconds'`,
            )
            const [reserved] = await claim()
            expect(reserved).toBeDefined()
            const first = (await begin(reserved!))!
            expect(Number.isSafeInteger(first)).toBe(true)
            await Bun.sleep(2)
            expect(await begin(reserved!)).toBe(first)
            const [stored] = await authority!.database.unsafe<
                { original_claim: unknown; admitted_ms: string; records: number }[]
            >(
                `SELECT original_claim,admitted_ms::text,(SELECT count(*)::integer FROM ${authority!.namespace}.admissions) records FROM ${authority!.namespace}.admissions WHERE lease_token=$1`,
                [reserved!.leaseToken],
            )
            expect(monitoringB2ClaimSchema.parse(stored!.original_claim)).toEqual(reserved!)
            expect(Number(stored!.admitted_ms)).toBe(first)
            expect(stored!.records).toBe(1)
            for (const query of [
                `SELECT * FROM ${authority!.namespace}.admissions`,
                `UPDATE ${authority!.namespace}.admissions SET original_claim='{}'::jsonb`,
            ])
                await expect(worker!.unsafe(query)).rejects.toThrow()
            await authority!.database.unsafe(
                "UPDATE rentnerproxy.host_check_policies SET lease_expires_at=clock_timestamp()-interval '1 millisecond' WHERE id=$1",
                [policyId],
            )
            expect(await begin(reserved!)).toBeNull()
            const [retained] = await authority!.database.unsafe<{ admitted_ms: string }[]>(
                `SELECT admitted_ms::text FROM ${authority!.namespace}.admissions WHERE lease_token=$1`,
                [reserved!.leaseToken],
            )
            expect(Number(retained!.admitted_ms)).toBe(first)
            await authority!.resetIdentity()
            expect(await begin(reserved!)).toBeNull()
            expect(await claim()).toHaveLength(0)
        },
        15000,
    )
    integration(
        'raw completion releases only execution capacity while preserving every final application lease fence',
        async () => {
            await fixture()
            const extra = Array.from({ length: 128 }, (_, index) => {
                const id = Bun.randomUUIDv7()
                hosts.push(id)
                return { id, domain: `raw-${index}-${id}.example.test` }
            })
            const payload = JSON.stringify(extra)
            await authority!.database.unsafe(
                `INSERT INTO rentnerproxy.proxy_hosts(id,forward_scheme,forward_host,forward_port) SELECT id::uuid,'http','127.0.0.1',9 FROM jsonb_to_recordset($1::text::jsonb) entry(id text,domain text)`,
                [payload],
            )
            await authority!.database.unsafe(
                `INSERT INTO rentnerproxy.host_domains(proxy_host_id,domain) SELECT id::uuid,domain FROM jsonb_to_recordset($1::text::jsonb) entry(id text,domain text)`,
                [payload],
            )
            await authority!.database.unsafe(
                `INSERT INTO rentnerproxy.host_check_policies(proxy_host_id,domain,enabled,next_check_at) SELECT id::uuid,domain,true,clock_timestamp()-interval '1 second' FROM jsonb_to_recordset($1::text::jsonb) entry(id text,domain text)`,
                [payload],
            )
            await authority!.publish(true)
            const first: MonitoringB2Claim[] = []
            for (let i = 0; i < 4; i++) first.push(...(await claim()))
            expect(first).toHaveLength(64)
            expect(await claim()).toHaveLength(0)
            for (const reserved of first) {
                expect(await begin(reserved)).not.toBeNull()
                const [recorded] = await worker!.unsafe<{ recorded: boolean }[]>(
                    `SELECT ${authority!.namespace}.record_raw_complete($1::uuid,$2::uuid,$3::uuid,$4,$5,$6) recorded`,
                    [
                        reserved.policyId,
                        reserved.policyVersion,
                        reserved.leaseToken,
                        reserved.storeId,
                        reserved.controllerEpoch,
                        reserved.generation,
                    ],
                )
                expect(recorded!.recorded).toBe(true)
                expect(await begin(reserved)).toBeNull()
            }
            const second: MonitoringB2Claim[] = []
            for (let i = 0; i < 4; i++) second.push(...(await claim()))
            expect(second).toHaveLength(64)
            expect(await claim()).toHaveLength(0)
            expect(new Set([...first, ...second].map((item) => item.policyId)).size).toBe(128)
            const [counts] = await authority!.database.unsafe<
                { leases: number; raw: number; observations: number }[]
            >(
                `SELECT (SELECT count(*)::int FROM rentnerproxy.host_check_policies WHERE lease_expires_at>clock_timestamp()) leases,(SELECT count(*)::int FROM ${authority!.namespace}.raw_completed) raw,(SELECT count(*)::int FROM rentnerproxy.host_check_observations) observations`,
            )
            expect(counts).toEqual({ leases: 128, raw: 64, observations: 0 })
        },
        15000,
    )
    integration(
        'least-privilege claims deny committed edits, deletes and restore without cached admission',
        async () => {
            const { id, policyId } = await fixture()
            for (const query of [
                'SELECT * FROM rentnerproxy.users',
                'SELECT * FROM rentnerproxy.host_check_policies',
                `SELECT * FROM ${authority!.namespace}.contexts`,
                `UPDATE rentnerproxy.host_check_policies SET enabled=false`,
                `INSERT INTO ${authority!.namespace}.receipts VALUES (NULL)`,
            ])
                await expect(worker!.unsafe(query)).rejects.toThrow()
            await expect(
                worker!.unsafe(`SELECT * FROM ${authority!.namespace}.claim(17,$1,$2)`, [
                    authority!.storeId,
                    authority!.controllerEpoch,
                ]),
            ).rejects.toThrow()
            expect(
                await worker!.unsafe(`SELECT * FROM ${authority!.namespace}.claim(16,NULL,NULL)`),
            ).toHaveLength(0)
            await expect(
                worker!.unsafe(`SELECT * FROM ${authority!.namespace}.claim(NULL,NULL,NULL)`),
            ).rejects.toThrow()
            const [reserved] = await claim()
            expect(reserved).toBeDefined()
            expect(await begin(reserved!)).not.toBeNull()
            expect(await claim()).toHaveLength(0)
            await authority!.database.unsafe(
                'UPDATE rentnerproxy.host_check_policies SET enabled=false,lease_token=NULL,lease_expires_at=NULL WHERE id=$1',
                [policyId],
            )
            expect(await begin(reserved!)).toBeNull()
            expect(await claim()).toHaveLength(0)
            await authority!.database.unsafe(
                'UPDATE rentnerproxy.host_check_policies SET enabled=true WHERE id=$1',
                [policyId],
            )
            expect(await claim()).toHaveLength(0)
            await authority!.publish()
            expect(await claim()).toHaveLength(1)
            await authority!.database.unsafe(
                'UPDATE rentnerproxy.host_check_policies SET lease_token=NULL,lease_expires_at=NULL WHERE id=$1',
                [policyId],
            )
            await authority!.database.unsafe(
                'UPDATE rentnerproxy.proxy_hosts SET forward_port=10 WHERE id=$1',
                [id],
            )
            expect(await claim()).toHaveLength(0)
            await authority!.publish()
            expect(await claim()).toHaveLength(1)
            await authority!.resetIdentity()
            expect(await claim()).toHaveLength(0)
            await authority!.publish()
            expect(await claim()).toHaveLength(0)
            await authority!.database.unsafe(
                'DELETE FROM rentnerproxy.host_check_policies WHERE id=$1',
                [policyId],
            )
            expect(await claim()).toHaveLength(0)
        },
        15000,
    )

    integration(
        'real Rust PostgreSQL handshake stalls fail closed within a bounded client deadline',
        async () => {
            await fixture()
            const binary = process.env.RENTNERPROXY_MONITORING_B2_BINARY
            if (!binary) throw new Error('B2 integration requires the built isolated Rust binary')
            const sockets = new Set<{ end: () => void }>()
            const listener = Bun.listen({
                hostname: '127.0.0.1',
                port: 0,
                socket: {
                    open(socket) {
                        sockets.add(socket)
                    },
                    data() {
                        /* A reachable server deliberately never answers the PostgreSQL handshake. */
                    },
                    close(socket) {
                        sockets.delete(socket)
                    },
                },
            })
            const databaseUrl = new URL(authority!.workerUrl)
            databaseUrl.port = String(listener.port)
            const began = performance.now()
            const child = Bun.spawn([binary], {
                env: {
                    RENTNERPROXY_MONITORING_B2: '1',
                    B2_DATABASE_URL: databaseUrl.toString(),
                    B2_NAMESPACE: authority!.namespace,
                    B2_CONTROLLER_URL: 'http://127.0.0.1:55441',
                    B2_CONTROLLER_TOKEN: 'synthetic-monitoring-scale-token-392',
                    B2_OUTBOX: join(tmpdir(), 'unused-b2-stalled.sqlite'),
                    B2_STORE_ID: authority!.storeId,
                    B2_CONTROLLER_EPOCH: authority!.controllerEpoch,
                    B2_DURATION_SECONDS: '1',
                },
                stdout: 'pipe',
                stderr: 'pipe',
            })
            try {
                const exit = await Promise.race([child.exited, Bun.sleep(6000).then(() => null)])
                expect(exit).toBe(1)
                expect(performance.now() - began).toBeLessThan(5500)
                expect(await new Response(child.stdout).text()).toBe('')
                expect(await new Response(child.stderr).text()).toContain(
                    'B2 experiment failed closed',
                )
                expect(await claim()).toHaveLength(1)
            } finally {
                if (child.exitCode === null) child.kill('SIGKILL')
                await child.exited
                for (const socket of sockets) socket.end()
                listener.stop(true)
            }
        },
        12000,
    )
    integration(
        'real Rust never starts HTTP after a fresh admission response outlives its reservation',
        async () => {
            await fixture()
            const binary = process.env.RENTNERPROXY_MONITORING_B2_BINARY
            if (!binary) throw new Error('B2 integration requires the built isolated Rust binary')
            directory = await mkdtemp(join(tmpdir(), 'rentnerproxy-b2-'))
            const namespace = authority!.namespace,
                role = new URL(authority!.workerUrl).username
            await authority!.database
                .unsafe(`ALTER FUNCTION ${namespace}.claim(integer,text,text) RENAME TO reserved_claim;
                CREATE FUNCTION ${namespace}.claim(requested integer,expected_store text,expected_epoch text) RETURNS TABLE(claim jsonb) LANGUAGE sql AS $$SELECT jsonb_set(claim,'{leaseExpiresAtMs}',to_jsonb(floor(extract(epoch FROM clock_timestamp())*1000+1000)::bigint)) FROM ${namespace}.reserved_claim(requested,expected_store,expected_epoch)$$;
                ALTER FUNCTION ${namespace}.begin_check(uuid,uuid,uuid,text,text,bigint) RENAME TO original_begin_check;
                CREATE FUNCTION ${namespace}.begin_check(p uuid,v uuid,t uuid,s text,e text,g bigint) RETURNS bigint LANGUAGE plpgsql AS $$DECLARE admitted bigint; BEGIN admitted:=${namespace}.original_begin_check(p,v,t,s,e,g); PERFORM pg_sleep(1.2); RETURN admitted; END$$;
                GRANT EXECUTE ON FUNCTION ${namespace}.claim(integer,text,text),${namespace}.begin_check(uuid,uuid,uuid,text,text,bigint) TO ${role}`)
            let requests = 0
            const controller = Bun.serve({
                hostname: '127.0.0.1',
                port: 55441,
                fetch() {
                    requests++
                    return new Response(null, { status: 500 })
                },
            })
            const child = Bun.spawn([binary], {
                env: {
                    RENTNERPROXY_MONITORING_B2: '1',
                    B2_DATABASE_URL: authority!.workerUrl,
                    B2_NAMESPACE: namespace,
                    B2_CONTROLLER_URL: 'http://127.0.0.1:55441',
                    B2_CONTROLLER_TOKEN: 'synthetic-monitoring-scale-token-392',
                    B2_OUTBOX: join(directory, 'late.sqlite'),
                    B2_STORE_ID: authority!.storeId,
                    B2_CONTROLLER_EPOCH: authority!.controllerEpoch,
                    B2_DURATION_SECONDS: '1',
                },
                stdout: 'pipe',
                stderr: 'pipe',
            })
            try {
                expect(await child.exited).toBe(1)
                expect(requests).toBe(0)
                const store = new Database(join(directory, 'late.sqlite'), { readonly: true })
                try {
                    expect(
                        store
                            .query<{ count: number }, []>(
                                'SELECT count(*) count FROM b2_outbox WHERE admitted_ms IS NOT NULL AND result IS NULL',
                            )
                            .get()!.count,
                    ).toBe(1)
                } finally {
                    store.close()
                }
            } finally {
                if (child.exitCode === null) child.kill('SIGKILL')
                await child.exited
                await new Response(child.stderr).text()
                await controller.stop(true)
            }
        },
        10000,
    )
    integration(
        'real Rust scheduling durably starts and replays through the existing Bun incident owner exactly once',
        async () => {
            const { policyId } = await fixture()
            directory = await mkdtemp(join(tmpdir(), 'rentnerproxy-b2-'))
            const path = join(directory, 'outbox.sqlite')
            const binary = process.env.RENTNERPROXY_MONITORING_B2_BINARY
            if (!binary) throw new Error('B2 integration requires the built isolated Rust binary')
            const processResult = Bun.spawn([binary], {
                env: {
                    ...process.env,
                    RENTNERPROXY_MONITORING_B2: '1',
                    B2_DATABASE_URL: authority!.workerUrl,
                    B2_NAMESPACE: authority!.namespace,
                    B2_CONTROLLER_URL: 'http://127.0.0.1:55441',
                    B2_CONTROLLER_TOKEN: 'synthetic-monitoring-scale-token-392',
                    B2_OUTBOX: path,
                    B2_STORE_ID: authority!.storeId,
                    B2_CONTROLLER_EPOCH: authority!.controllerEpoch,
                    B2_DURATION_SECONDS: '1',
                },
                stdout: 'pipe',
                stderr: 'pipe',
            })
            const [exit, output, diagnostic] = await Promise.all([
                processResult.exited,
                new Response(processResult.stdout).text(),
                new Response(processResult.stderr).text(),
            ])
            expect(diagnostic).toBe('')
            expect(exit).toBe(0)
            expect(JSON.parse(output)).toMatchObject({
                runtime: 'rust',
                reserved: 1,
                durableResults: 1,
                admitted: 1,
                databaseConnections: 1,
            })
            const store = new Database(path, { strict: true })
            const raw = store
                .query<
                    {
                        sequence: number
                        attempt_id: string
                        store_id: string
                        claim: string
                        started_ms: number
                        admitted_ms: number
                        durable_ms: number
                        completed_ms: number
                        result: string
                    },
                    []
                >('SELECT * FROM b2_outbox')
                .get()!
            expect(raw.started_ms).toBeLessThanOrEqual(raw.completed_ms)
            const claimed = monitoringB2ClaimSchema.parse(JSON.parse(raw.claim))
            // Protocol fixture only: native HEAD semantics are covered by the separate real-controller run.
            const result = {
                hostType: 'proxy',
                hostId: claimed.hostId,
                domain: claimed.domain,
                expectedRevision: claimed.before.revision,
                activeRevision: claimed.before.revision,
                checkedAt: new Date().toISOString(),
                observationPoint: 'appliance',
                publicReachability: 'not_checked',
                stages: {
                    configuration: stage('passed', 'configuration_active'),
                    dns: stage('unknown', 'skipped'),
                    upstream: stage('failed', 'upstream_timeout'),
                    tls: stage('not_applicable', 'http_upstream'),
                },
            }
            raw.result = JSON.stringify(result)
            store
                .query('UPDATE b2_outbox SET result=? WHERE attempt_id=?')
                .run(raw.result, raw.attempt_id)
            const replay = await createMonitoringB2Replay(
                path,
                authority!.namespace,
                authority!.storeId,
                authority!.controllerEpoch,
            )
            try {
                expect(await replay.replayBatch()).toMatchObject({
                    accepted: 1,
                    duplicates: 0,
                    rejected: 0,
                    paused: false,
                })
                expect(
                    store
                        .query<{ acked_sequence: number }, []>(
                            'SELECT acked_sequence FROM b2_identity',
                        )
                        .get()!.acked_sequence,
                ).toBe(raw.sequence)
                // Recreate the exact durable record to model commit-before-ACK followed by process death.
                store
                    .query(
                        'INSERT INTO b2_outbox(sequence,attempt_id,store_id,claim,started_ms,admitted_ms,durable_ms,completed_ms,result) VALUES(?,?,?,?,?,?,?,?,?)',
                    )
                    .run(
                        raw.sequence,
                        raw.attempt_id,
                        raw.store_id,
                        raw.claim,
                        raw.started_ms,
                        raw.admitted_ms,
                        raw.durable_ms,
                        raw.completed_ms,
                        raw.result,
                    )
                expect(await replay.replayBatch()).toMatchObject({
                    accepted: 0,
                    duplicates: 1,
                    rejected: 0,
                })
                const [counts] = await authority!.database.unsafe<
                    { observations: number; incidents: number; receipts: number }[]
                >(
                    `SELECT (SELECT count(*)::int FROM rentnerproxy.host_check_observations WHERE policy_id=$1) observations,(SELECT count(*)::int FROM rentnerproxy.host_check_incidents WHERE policy_id=$1) incidents,(SELECT count(*)::int FROM ${authority!.namespace}.receipts) receipts`,
                    [policyId],
                )
                expect(counts).toEqual({ observations: 1, incidents: 1, receipts: 1 })
                const [proof] = await authority!.database.unsafe<{ applied: boolean }[]>(
                    `SELECT EXISTS(SELECT 1 FROM rentnerproxy.host_check_observations o WHERE o.policy_id=r.policy_id AND o.xmin::text=r.transaction_id) applied FROM ${authority!.namespace}.receipts r`,
                )
                expect(proof!.applied).toBe(true)
                // A candidate receipt without an observation is a durable rejection, never an applied duplicate.
                const rejectedToken = Bun.randomUUIDv7()
                const rejectedClaim = JSON.stringify({ ...claimed, leaseToken: rejectedToken })
                const rejectedDigest = createHash('sha256')
                    .update(rejectedClaim)
                    .update('\0')
                    .update(raw.result)
                    .update(
                        JSON.stringify([
                            raw.started_ms,
                            raw.admitted_ms,
                            raw.durable_ms,
                            raw.completed_ms,
                        ]),
                    )
                    .digest('hex')
                await authority!.database.unsafe(
                    `INSERT INTO ${authority!.namespace}.receipts(attempt_id,store_id,epoch,generation,policy_id,policy_version,digest,transaction_id) VALUES($1,$2,$3,$4,$5,$6,$7,'0')`,
                    [
                        rejectedToken,
                        claimed.storeId,
                        claimed.controllerEpoch,
                        claimed.generation,
                        claimed.policyId,
                        claimed.policyVersion,
                        rejectedDigest,
                    ],
                )
                store
                    .query(
                        'INSERT INTO b2_outbox(attempt_id,store_id,claim,started_ms,admitted_ms,durable_ms,completed_ms,result) VALUES(?,?,?,?,?,?,?,?)',
                    )
                    .run(
                        rejectedToken,
                        raw.store_id,
                        rejectedClaim,
                        raw.started_ms,
                        raw.admitted_ms,
                        raw.durable_ms,
                        raw.completed_ms,
                        raw.result,
                    )
                expect(await replay.replayBatch()).toMatchObject({
                    accepted: 0,
                    duplicates: 0,
                    rejected: 1,
                })
                const [rejection] = await authority!.database.unsafe<
                    { disposition: string; reason: string }[]
                >(
                    `SELECT disposition,reason FROM ${authority!.namespace}.receipts WHERE attempt_id=$1`,
                    [rejectedToken],
                )
                expect(rejection).toEqual({
                    disposition: 'rejected',
                    reason: 'completion_rejected',
                })

                // Cross-policy selector splicing never enters the observation or incident state machine.
                const splicedToken = Bun.randomUUIDv7()
                const splicedClaim = JSON.stringify({
                    ...claimed,
                    leaseToken: splicedToken,
                    hostId: Bun.randomUUIDv7(),
                })
                await authority!.database.unsafe(
                    "UPDATE rentnerproxy.host_check_policies SET lease_token=$1,lease_expires_at=clock_timestamp()+interval '45 seconds' WHERE id=$2",
                    [splicedToken, policyId],
                )
                store
                    .query(
                        'INSERT INTO b2_outbox(attempt_id,store_id,claim,started_ms,admitted_ms,durable_ms,completed_ms,result) VALUES(?,?,?,?,?,?,?,?)',
                    )
                    .run(
                        splicedToken,
                        raw.store_id,
                        splicedClaim,
                        raw.started_ms,
                        raw.admitted_ms,
                        raw.durable_ms,
                        raw.completed_ms,
                        raw.result,
                    )
                expect(await replay.replayBatch()).toMatchObject({
                    accepted: 0,
                    duplicates: 0,
                    rejected: 1,
                })
                const [spliceReceipt] = await authority!.database.unsafe<
                    { disposition: string; reason: string }[]
                >(
                    `SELECT disposition,reason FROM ${authority!.namespace}.receipts WHERE attempt_id=$1`,
                    [splicedToken],
                )
                expect(spliceReceipt!.disposition).toBe('rejected')
                expect(spliceReceipt!.reason).toBe('original_claim_unavailable')

                store
                    .query(
                        'INSERT INTO b2_outbox(attempt_id,store_id,claim,started_ms,admitted_ms,durable_ms,completed_ms,result) VALUES(?,?,?,?,?,?,?,?)',
                    )
                    .run(
                        raw.attempt_id,
                        raw.store_id,
                        raw.claim,
                        raw.started_ms,
                        raw.admitted_ms,
                        raw.durable_ms,
                        raw.completed_ms,
                        JSON.stringify({ ...result, checkedAt: '2020-01-01T00:00:00.000Z' }),
                    )
                await expect(replay.replayBatch()).rejects.toThrow('B2 conflicting replay payload')
                store
                    .query('UPDATE b2_outbox SET result=? WHERE attempt_id=?')
                    .run(raw.result, raw.attempt_id)
                store
                    .query('UPDATE b2_outbox SET durable_ms=durable_ms+1 WHERE attempt_id=?')
                    .run(raw.attempt_id)
                await expect(replay.replayBatch()).rejects.toThrow('B2 conflicting replay payload')
                store
                    .query('UPDATE b2_outbox SET durable_ms=? WHERE attempt_id=?')
                    .run(raw.durable_ms, raw.attempt_id)
                await authority!.resetIdentity()
                expect(await replay.replayBatch()).toMatchObject({ accepted: 0, paused: true })
                expect(replay.stats().pending).toBe(1)
            } finally {
                replay.close()
                store.close()
            }
        },
        15000,
    )
})
