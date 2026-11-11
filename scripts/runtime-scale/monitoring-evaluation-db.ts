// oxlint-disable no-await-in-loop -- Paired measurements and state transitions must remain ordered.
import { SQL } from 'bun'
import { randomUUID } from 'node:crypto'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { isAbsolute, join, relative, resolve } from 'node:path'
import { assertIsolatedMonitoringDatabase, monitoringDistribution } from './monitoring.ts'
import {
    runMonitoringEvaluationProbes,
    cleanupMonitoringEvaluation,
} from './monitoring-evaluation-lifecycle.ts'
import { createMonitoringEvaluationOutbox } from './monitoring-evaluation-outbox.ts'
import {
    monitoringEvaluationContract,
    monitoringRevocationCounterexample,
} from './monitoring-evaluation.ts'
import type { MonitoringEvaluationResult } from './Types/monitoring-evaluation.types.ts'

/**
 * Disposable SQL/transport experiment. This tests a possible restricted authority
 * surface; it is not a Rust scheduler, a production DB role or an appliance benchmark.
 */
export async function runMonitoringAuthoritySlice(policies: 100 | 1000 = 100) {
    assertIsolatedMonitoringDatabase(
        process.env.RENTNERPROXY_DATABASE_INTEGRATION,
        process.env.DATABASE_URL,
    )
    if (policies !== 100 && policies !== 1000) throw new Error('Unsupported authority slice size')
    console.log(
        JSON.stringify({
            predeclaredContract: monitoringEvaluationContract,
            slice: {
                policies,
                pairs: 3,
                slots: 64,
                wallBudgetMs: 30000,
                latencyMs: [2, 5, 10, 20],
                scope: 'Synthetic SQL/view/loopback-HEAD/storage protocol only; no Rust, SSR, Caddy or full application authorization graph; cannot establish migration eligibility',
            },
        }),
    )
    const suffix = randomUUID().replaceAll('-', '')
    const namespace = 'monitor_eval_' + suffix
    const role = 'monitor_eval_role_' + suffix
    const password = 'synthetic-evaluation-only-' + suffix
    const sqlOptions = {
        max: 2,
        connectionTimeout: 3,
        connection: {
            statement_timeout: 2000,
            lock_timeout: 2000,
            idle_in_transaction_session_timeout: 2000,
        },
    }
    const database = new SQL(process.env.DATABASE_URL!, sqlOptions)
    const workerUrl = new URL(process.env.DATABASE_URL!)
    workerUrl.username = role
    workerUrl.password = password
    const worker = new SQL(workerUrl.toString(), sqlOptions)
    const directory = await mkdtemp(join(tmpdir(), 'rentnerproxy-monitoring-evaluation-'))
    const cleanupRelative = relative(resolve(tmpdir()), resolve(directory))
    const cleanupIsOwned =
        !isAbsolute(cleanupRelative) &&
        !cleanupRelative.startsWith('..') &&
        cleanupRelative.startsWith('rentnerproxy-monitoring-evaluation-')
    const store = createMonitoringEvaluationOutbox(join(directory, 'outbox.sqlite'), 'store-1')
    const samples: unknown[] = []
    let executionFailed = false
    let executionFailure: unknown
    let roleCreated = false
    let schemaCreated = false
    let networkActive = 0
    let maxNetworkActive = 0
    const deadline = performance.now() + 30000
    let replayExample: MonitoringEvaluationResult | undefined
    const upstream = Bun.serve({
        hostname: '127.0.0.1',
        port: 0,
        async fetch(request) {
            if (
                request.headers.get('authorization') !==
                'Bearer synthetic-monitoring-evaluation-only'
            )
                return new Response(null, { status: 401 })
            const id = Number(new URL(request.url).pathname.slice(1))
            if (request.method !== 'HEAD' || !Number.isInteger(id) || id < 1 || id > policies)
                return new Response(null, { status: 404 })
            networkActive++
            maxNetworkActive = Math.max(maxNetworkActive, networkActive)
            try {
                await Bun.sleep([2, 5, 10, 20][id % 4]!)
                return new Response(null, { status: 200 })
            } finally {
                networkActive--
            }
        },
    })
    try {
        await database.unsafe(`CREATE SCHEMA ${namespace}`)
        schemaCreated = true
        await database.unsafe(`
            CREATE TABLE ${namespace}.policies (id INTEGER PRIMARY KEY, version INTEGER NOT NULL DEFAULT 1, grant_version INTEGER NOT NULL DEFAULT 1, enabled BOOLEAN NOT NULL DEFAULT TRUE, grant_enabled BOOLEAN NOT NULL DEFAULT TRUE, epoch TEXT NOT NULL DEFAULT 'epoch-1');
            CREATE TABLE ${namespace}.receipts (attempt_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE ${namespace}.application_secrets (value TEXT NOT NULL);
            INSERT INTO ${namespace}.application_secrets VALUES ('synthetic-secret-never-returned');
            INSERT INTO ${namespace}.policies (id) SELECT generate_series(1, ${policies});
            CREATE VIEW ${namespace}.effective_policies AS SELECT id, version, grant_version, epoch FROM ${namespace}.policies WHERE enabled AND grant_enabled;
            REVOKE ALL ON SCHEMA ${namespace} FROM PUBLIC;
            CREATE ROLE ${role} LOGIN PASSWORD '${password}' CONNECTION LIMIT 2 NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
        `)
        roleCreated = true
        await database.unsafe(
            `GRANT USAGE ON SCHEMA ${namespace} TO ${role}; GRANT SELECT ON ${namespace}.effective_policies TO ${role}`,
        )
        const denied: string[] = []
        for (const [name, statement] of [
            ['secret-read', `SELECT * FROM ${namespace}.application_secrets`],
            ['raw-policy-read', `SELECT * FROM ${namespace}.policies`],
            ['policy-write', `UPDATE ${namespace}.policies SET enabled = FALSE`],
            ['receipt-write', `INSERT INTO ${namespace}.receipts VALUES ('forged', 'forged')`],
        ]) {
            try {
                await worker.unsafe(statement!)
            } catch {
                denied.push(name!)
            }
        }
        if (denied.length !== 4) throw new Error('Restricted authority permission check failed')
        store.publish('store-1', 1, 'resync', true)
        for (let pair = 0; pair < 3; pair++) {
            // Alternate order so filesystem/database warmup does not always favor one side.
            for (const mode of pair % 2
                ? ['restricted-view-outbox', 'application-view']
                : ['application-view', 'restricted-view-outbox']) {
                const connection = mode === 'application-view' ? database : worker
                const cpu = process.cpuUsage()
                const started = performance.now()
                const latency: number[] = []
                const results: MonitoringEvaluationResult[] = []
                await runMonitoringEvaluationProbes(policies, async (id) => {
                    if (performance.now() >= deadline)
                        throw new Error('Authority slice budget exhausted')
                    const start = performance.now()
                    const rows = await connection.unsafe<
                        {
                            id: number
                            version: number
                            grant_version: number
                            epoch: string
                        }[]
                    >(`SELECT * FROM ${namespace}.effective_policies WHERE id = $1`, [id])
                    const policy = rows[0]
                    if (!policy) throw new Error('Unexpected missing synthetic authority')
                    const response = await fetch(`http://127.0.0.1:${upstream.port}/${id}`, {
                        method: 'HEAD',
                        redirect: 'error',
                        headers: {
                            authorization: 'Bearer synthetic-monitoring-evaluation-only',
                        },
                        signal: AbortSignal.timeout(1000),
                    })
                    if (response.status !== 200) throw new Error('Loopback probe failed')
                    const now = Date.now()
                    const result: MonitoringEvaluationResult = {
                        storeId: 'store-1',
                        controllerEpoch: policy.epoch,
                        policyId: String(id),
                        policyVersion: policy.version,
                        grantVersion: policy.grant_version,
                        revision: 'revision-1',
                        hostProof: 'proof-1',
                        leaseToken: `${pair}-${mode}-${id}`,
                        leaseExpiresAtMs: now + monitoringEvaluationContract.leaseMs,
                        enabled: true,
                        paused: false,
                        attemptId: `${pair}-${mode}-${id}`,
                        checkedAtMs: now,
                        outcome: 'healthy',
                    }
                    if (
                        mode === 'restricted-view-outbox' &&
                        store.append(result, now) !== 'recorded'
                    )
                        throw new Error('Outbox did not durably accept synthetic result')
                    results.push(result)
                    replayExample ??= result
                    latency.push(performance.now() - start)
                })
                const networkAndStoreMs = performance.now() - started
                const replayStart = performance.now()
                for (const result of results) {
                    if (performance.now() >= deadline)
                        throw new Error('Authority slice budget exhausted')
                    // The application alone writes receipts. Unique attempts model commit-before-ACK.
                    await database.unsafe(
                        `INSERT INTO ${namespace}.receipts VALUES ($1, $2) ON CONFLICT (attempt_id) DO UPDATE SET payload = excluded.payload WHERE ${namespace}.receipts.payload = excluded.payload RETURNING attempt_id`,
                        [result.attemptId, JSON.stringify(result)],
                    )
                }
                if (mode === 'restricted-view-outbox') {
                    while (store.read().length) {
                        const batch = store.read()
                        store.acknowledge(
                            batch.map((row) => ({
                                sequence: row.sequence,
                                attemptId: row.attempt_id,
                                storeId: 'store-1',
                            })),
                        )
                    }
                }
                const used = process.cpuUsage(cpu)
                samples.push({
                    pair,
                    mode,
                    policies,
                    actualHeads: results.length,
                    networkAndStoreMs,
                    applicationReceiptAndAckMs: performance.now() - replayStart,
                    totalMs: performance.now() - started,
                    processCpuMs: (used.user + used.system) / 1000,
                    processRssMiB: process.memoryUsage.rss() / 1024 / 1024,
                    latency: monitoringDistribution(latency),
                    queryCount: policies * 2,
                    extraOutboxRecords: mode === 'restricted-view-outbox' ? policies : 0,
                })
            }
        }
        const cached = await worker.unsafe(
            `SELECT * FROM ${namespace}.effective_policies WHERE id = 1`,
        )
        await database.unsafe(
            `UPDATE ${namespace}.policies SET grant_enabled = FALSE, grant_version = grant_version + 1 WHERE id = 1`,
        )
        const fresh = await worker.unsafe(
            `SELECT * FROM ${namespace}.effective_policies WHERE id = 1`,
        )
        await database.unsafe(
            `UPDATE ${namespace}.policies SET epoch = 'epoch-2', version = version + 1 WHERE id = 2`,
        )
        const restarted = await worker.unsafe(
            `SELECT * FROM ${namespace}.effective_policies WHERE id = 2`,
        )
        const replay = await database.unsafe(
            `INSERT INTO ${namespace}.receipts VALUES ($1, $2) ON CONFLICT (attempt_id) DO UPDATE SET payload = excluded.payload WHERE ${namespace}.receipts.payload = excluded.payload RETURNING attempt_id`,
            [replayExample!.attemptId, JSON.stringify(replayExample)],
        )
        const conflict = await database.unsafe(
            `INSERT INTO ${namespace}.receipts VALUES ($1, $2) ON CONFLICT (attempt_id) DO UPDATE SET payload = excluded.payload WHERE ${namespace}.receipts.payload = excluded.payload RETURNING attempt_id`,
            [replayExample!.attemptId, JSON.stringify({ ...replayExample, outcome: 'unhealthy' })],
        )
        const counts = await database.unsafe(
            `SELECT count(*)::integer AS count FROM ${namespace}.receipts`,
        )
        const result = {
            kind: 'bounded-authority-storage-slice',
            samples,
            denied,
            maxNetworkActive,
            revocation: {
                cachedStillHasPolicy: cached.length === 1,
                freshDeniesPolicy: fresh.length === 0,
                ...monitoringRevocationCounterexample(),
            },
            epochChanged: restarted[0]?.epoch === 'epoch-2',
            duplicateReplayIdempotent: replay.length === 1 && counts[0]?.count === policies * 6,
            conflictingReplayRejected: conflict.length === 0,
            remainingOutboxRecords: store.status().records,
            recommendation: 'keep-hybrid',
            migrationProof: false,
            limitations: [
                'Same Bun SQL/network driver on both paths; these are protocol/storage costs, not a Rust-versus-Bun resource comparison',
                'Synthetic view omits the production grant/context graph, DB claim fencing, configuration lock and incident transactions',
                'No process fault, real controller, Caddy/SSR load or 1000/60-second cadence; #401 migration gates remain unproven',
                'A restricted DB role needs separately designed claim/commit functions, independent credentials, rotation and destructive-restore fencing',
            ],
        }
        if (
            !result.revocation.freshDeniesPolicy ||
            !result.epochChanged ||
            !result.duplicateReplayIdempotent ||
            !result.conflictingReplayRejected ||
            maxNetworkActive > 64
        )
            throw new Error('Authority slice safety assertion failed')
        console.log(JSON.stringify(result))
        return result
    } catch (error) {
        executionFailed = true
        executionFailure = error
        throw error
    } finally {
        await cleanupMonitoringEvaluation(
            [
                () => upstream.stop(true),
                () => store.close(),
                () => worker.close(),
                () =>
                    schemaCreated ? database.unsafe(`DROP SCHEMA ${namespace} CASCADE`) : undefined,
                () => (roleCreated ? database.unsafe(`DROP ROLE ${role}`) : undefined),
                () => database.close(),
                () =>
                    cleanupIsOwned ? rm(directory, { recursive: true, force: true }) : undefined,
            ],
            executionFailed ? [executionFailure] : [],
        )
    }
}

if (import.meta.main) {
    const args = process.argv.slice(2)
    if (args.length !== 2 || args[0] !== '--policies' || !['100', '1000'].includes(args[1]!)) {
        console.error('Explicit isolated opt-in required: --policies 100|1000')
        process.exitCode = 1
    } else {
        try {
            await runMonitoringAuthoritySlice(Number(args[1]) as 100 | 1000)
        } catch {
            console.error(
                'Monitoring authority slice failed; no driver errors or credentials logged',
            )
            process.exitCode = 1
        }
    }
}
