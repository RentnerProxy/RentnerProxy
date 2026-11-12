// oxlint-disable no-await-in-loop -- Ordered receipts and local ACKs cannot race the original completion transaction.
import { lstat, readFile, writeFile } from 'node:fs/promises'
import { Database } from 'bun:sqlite'
import { createHash } from 'node:crypto'
import { inArray, sql } from 'drizzle-orm'
import type { SQL } from 'drizzle-orm'
import { z } from 'zod'
import { loadMonitoringB2Application } from './monitoring-b2-bundled.ts'
import {
    monitoringB2ClaimSchema,
    monitoringB2AdmissionRejection,
    monitoringB2OriginalAdmissionSchema,
} from './monitoring-b2.validation.ts'
import type { MonitoringB2RawRecord, MonitoringB2Runtime } from './Types/monitoring-b2.types.ts'
import { monitoringB2WebKey } from './monitoring-b2-capabilities.ts'

/** Entry point for the small fixture adapter attached to the original web process. */
export async function attachMonitoringB2Replay(workerPath: string, signal?: AbortSignal) {
    const application = await loadMonitoringB2Application(workerPath)
    const metadataFile = '/tmp/monitoring-b2/runtime.json'
    const stat = await lstat(metadataFile)
    if (!stat.isFile() || stat.uid !== 0 || (stat.mode & 0o022) !== 0 || stat.size > 4096)
        throw new Error('B2 replay runtime seal invalid')
    const metadata = z
        .strictObject({
            namespace: z.string().regex(/^monitor_b2_[a-f0-9]{32}$/u),
            storeId: z.uuid(),
            controllerEpoch: z.uuid(),
            path: z.string().regex(/^\/tmp\/monitoring-b2\/state\/[a-z0-9-]+\.sqlite$/u),
        })
        .parse(JSON.parse(await readFile(metadataFile, 'utf8')))
    if (signal?.aborted) return
    return createMonitoringB2WebReplay(application, metadata)
}

/** Shared adapter logic can be exercised against isolated original application transactions without launching a web server. */
export async function createMonitoringB2WebReplay(
    application: Awaited<ReturnType<typeof loadMonitoringB2Application>>,
    metadata: MonitoringB2Runtime,
    manual = false,
) {
    if (
        process.env.RENTNERPROXY_MONITORING_B2 !== '1' ||
        !/^monitor_b2_[a-f0-9]{32}$/u.test(metadata.namespace)
    )
        throw new Error('B2 replay fixture opt-in required')
    const webKey = manual ? undefined : await monitoringB2WebKey()
    const ns = sql.identifier(metadata.namespace),
        resource = application.background('monitoring'),
        outbox = new Database(metadata.path, { strict: true })
    outbox.exec('PRAGMA synchronous=FULL; PRAGMA busy_timeout=2000;')
    const execute = <T extends Record<string, unknown>>(statement: SQL) =>
        resource.transaction(async (transaction) => await transaction.execute<T>(statement))
    const counters = {
        accepted: 0,
        duplicates: 0,
        rejected: 0,
        errors: 0,
        repairedPending: 0,
        publications: 0,
    }
    let publicationProof: Awaited<ReturnType<typeof application.publication>>['proof'] | undefined
    let publicationStage = 'idle',
        publicationDiagnostic = 'none'
    let lastRepairAt = 0
    let running: Promise<void> | null = null,
        stopped = false,
        paused = false
    const identity = sql`store_id=${metadata.storeId} AND epoch=${metadata.controllerEpoch} AND NOT paused AND NOT resync_required AND generation=published_generation`
    const appliedProof = sql`EXISTS(SELECT 1 FROM rentnerproxy.host_check_observations observation WHERE observation.id=receipt.observation_id AND observation.policy_id=receipt.policy_id AND observation.policy_version=receipt.policy_version)`
    /** Preserve the raw sequence prefix; discovered loss durably latches resync instead of being skipped. */
    function contiguous() {
        const cursor = outbox
            .query<{ acked: number; allocated: number; count: number; resync: number }, []>(
                "SELECT identity.acked_sequence acked,coalesce((SELECT seq FROM sqlite_sequence WHERE name='b2_outbox'),0) allocated,(SELECT count(*) FROM b2_outbox WHERE sequence>identity.acked_sequence) count,status.resync_required resync FROM b2_identity identity,b2_status status WHERE identity.id=1 AND status.id=1",
            )
            .get()
        if (!cursor || cursor.resync) return false
        if (cursor.allocated < cursor.acked || cursor.allocated - cursor.acked !== cursor.count) {
            outbox.exec('UPDATE b2_status SET gaps=gaps+1,resync_required=1,pressure=1 WHERE id=1')
            return false
        }
        return true
    }
    /** A crash after local ACK may leave only terminal metadata; exact durable receipts make its repair safe. */
    async function repair() {
        const result = await resource.transaction(async (transaction) => {
            const [gate] = await transaction.execute<{ valid: boolean; refresh: boolean }>(
                sql`SELECT ${identity} valid,store_id=${metadata.storeId} AND epoch=${metadata.controllerEpoch} AND NOT paused AND (resync_required OR generation<>published_generation) refresh FROM ${ns}.identity WHERE id=1 FOR SHARE`,
            )
            if (!gate?.valid) return { removed: 0, refresh: gate?.refresh === true }
            const rows = await transaction.execute<{ attempt_id: string }>(
                sql`DELETE FROM ${ns}.replay_pending pending USING ${ns}.receipts receipt WHERE pending.attempt_id=receipt.attempt_id AND pending.store_id=receipt.store_id AND pending.epoch=receipt.epoch AND pending.generation=receipt.generation AND pending.policy_id=receipt.policy_id AND pending.policy_version=receipt.policy_version AND pending.digest=receipt.digest AND pending.store_id=${metadata.storeId} AND pending.epoch=${metadata.controllerEpoch} AND (receipt.disposition='rejected' OR ${appliedProof}) RETURNING pending.attempt_id`,
            )
            return { removed: rows.length, refresh: false }
        })
        counters.repairedPending += result.removed
        lastRepairAt = Date.now()
        if (result.refresh) await republish()
        return result.removed
    }
    /** Republish canonical current contexts under original locks, without reactivating a paused/reset owner. */
    async function republish() {
        publicationStage = 'load'
        try {
            const publication = await application.publication()
            publicationStage = 'transaction'
            const published = await resource.transaction(async (transaction) => {
                await publication.prepare(transaction)
                const [gate] = await transaction.execute<{ refresh: boolean }>(
                    sql`SELECT store_id=${metadata.storeId} AND epoch=${metadata.controllerEpoch} AND NOT paused AND (resync_required OR generation<>published_generation) refresh FROM ${ns}.identity WHERE id=1 FOR UPDATE`,
                )
                if (!gate?.refresh) return false
                const policies = await transaction.select().from(application.policies)
                const contexts = await publication.contexts(
                    transaction,
                    policies.map((policy) => ({
                        hostType: 'proxy' as const,
                        hostId: policy.proxyHostId,
                        domain: policy.domain,
                    })),
                )
                const rows = policies.flatMap((policy) => {
                    const before = contexts.get(policy.proxyHostId)
                    return policy.enabled && before?.enabled
                        ? [{ policy_id: policy.id, policy_version: policy.version, before }]
                        : []
                })
                await transaction.execute(sql`DELETE FROM ${ns}.contexts`)
                if (rows.length)
                    await transaction.execute(
                        sql`INSERT INTO ${ns}.contexts(policy_id,policy_version,before) SELECT policy_id,policy_version,before FROM jsonb_to_recordset(${JSON.stringify(rows)}::text::jsonb) AS record(policy_id uuid,policy_version uuid,before jsonb)`,
                    )
                await transaction.execute(
                    sql`UPDATE ${ns}.identity SET published_generation=generation,resync_required=false WHERE id=1`,
                )
                return true
            })
            if (published) {
                counters.publications++
                publicationProof = publication.proof
            }
            publicationStage = 'complete'
            publicationDiagnostic = 'none'
            return published
        } catch (error) {
            publicationStage = 'failed-' + publicationStage
            const value =
                error && typeof error === 'object'
                    ? (error as { code?: unknown; errno?: unknown; name?: unknown })
                    : {}
            publicationDiagnostic =
                ([value.code, value.errno].find(
                    (code) => typeof code === 'string' && /^[A-Z0-9]{5}$/u.test(code),
                ) as string | undefined) ??
                (['ReferenceError', 'SyntaxError', 'ResolveMessage'].includes(String(value.name))
                    ? String(value.name)
                    : 'canonical-publication-failed')
            throw error
        }
    }
    /** Persist a terminal rejection only for the exact digest; an applied observation is never overwritten. */
    async function reject(row: MonitoringB2RawRecord, digest: string, reason: string) {
        const claim = monitoringB2ClaimSchema.parse(JSON.parse(row.claim))
        const stored = await execute<{
            attempt_id: string
        }>(sql`INSERT INTO ${ns}.receipts(attempt_id,store_id,epoch,generation,policy_id,policy_version,digest,transaction_id,disposition,reason,due_ms,started_ms,admitted_ms,durable_ms,completed_ms)
            VALUES(${claim.leaseToken}::uuid,${claim.storeId},${claim.controllerEpoch},${claim.generation},${claim.policyId}::uuid,${claim.policyVersion}::uuid,${digest},mod(pg_current_xact_id()::text::numeric,4294967296)::text,'rejected',${reason},${claim.dueAtMs},${row.started_ms},${row.admitted_ms},${row.durable_ms},${row.completed_ms})
            ON CONFLICT(attempt_id) DO UPDATE SET disposition='rejected',reason=excluded.reason WHERE ${ns}.receipts.digest=excluded.digest AND ${ns}.receipts.observation_id IS NULL RETURNING attempt_id`)
        if (stored.length !== 1) throw new Error('B2 terminal rejection receipt conflict')
    }
    /** Fence one bounded raw batch through the original Bun completion owner, then ACK terminal receipts. */
    async function pass() {
        if (!contiguous()) return
        if (Date.now() - lastRepairAt >= 1000) await repair()
        const rows = outbox
            .query<MonitoringB2RawRecord, []>('SELECT * FROM b2_outbox ORDER BY sequence LIMIT 16')
            .all()
        if (!rows.length) return
        const [current] = await execute<{ valid: boolean }>(
            sql`SELECT ${identity} valid FROM ${ns}.identity WHERE id=1`,
        )
        if (!current?.valid) return
        const settled: { row: MonitoringB2RawRecord; digest: string }[] = [],
            eligible: {
                row: MonitoringB2RawRecord
                digest: string
                claim: z.infer<typeof monitoringB2ClaimSchema>
                policy: typeof application.policies.$inferSelect
                outcome: ReturnType<typeof application.classify>
            }[] = []
        const ids = rows.map((row) => monitoringB2ClaimSchema.parse(JSON.parse(row.claim)).policyId)
        const policies = ids.length
            ? await resource.transaction(
                  async (transaction) =>
                      await transaction
                          .select()
                          .from(application.policies)
                          .where(inArray(application.policies.id, ids)),
              )
            : []
        const byId = new Map(policies.map((policy) => [policy.id, policy]))
        const originalRows = await execute<{
            attempt: string
            original_claim: unknown
            admitted_ms: string | null
        }>(
            sql`SELECT lease_token::text attempt,original_claim,admitted_ms::text FROM ${ns}.admissions WHERE lease_token IN (${sql.join(
                rows.map((row) => sql`${row.attempt_id}::uuid`),
                sql`,`,
            )})`,
        )
        const originalSchema = monitoringB2OriginalAdmissionSchema.extend({ attempt: z.uuid() })
        const originals = new Map(
            originalRows.map((row) => {
                const original = originalSchema.parse(row)
                return [original.attempt, original] as const
            }),
        )
        for (const row of rows) {
            if (row.result === null || row.completed_ms === null) break
            const claim = monitoringB2ClaimSchema.parse(JSON.parse(row.claim))
            if (
                row.attempt_id !== claim.leaseToken ||
                row.store_id !== claim.storeId ||
                claim.storeId !== metadata.storeId ||
                claim.controllerEpoch !== metadata.controllerEpoch ||
                Buffer.byteLength(row.claim) + Buffer.byteLength(row.result) > 4096
            )
                throw new Error('B2 replay raw identity invalid')
            const digest = createHash('sha256')
                .update(row.claim)
                .update('\0')
                .update(row.result)
                .update(
                    JSON.stringify([
                        row.started_ms,
                        row.admitted_ms,
                        row.durable_ms,
                        row.completed_ms,
                    ]),
                )
                .digest('hex')
            const [receipt] = await execute<{
                digest: string
                applied: boolean
                disposition: string
            }>(
                sql`SELECT receipt.digest,receipt.disposition,${appliedProof} applied FROM ${ns}.receipts receipt WHERE attempt_id=${row.attempt_id}::uuid`,
            )
            if (receipt) {
                if (receipt.digest !== digest) throw new Error('B2 replay payload conflict')
                if (receipt.applied) counters.duplicates++
                else {
                    await reject(row, digest, 'completion_rejected')
                    counters.rejected++
                }
                settled.push({ row, digest })
                continue
            }
            const policy = byId.get(claim.policyId),
                payload: unknown = JSON.parse(row.result),
                parsed = application.resultSchema.safeParse(payload)
            const error =
                typeof payload === 'object' &&
                payload !== null &&
                Object.keys(payload).length === 1 &&
                'error' in payload
                    ? String(payload.error)
                    : null
            let reason =
                monitoringB2AdmissionRejection(claim, originals.get(row.attempt_id), row) ??
                (!policy
                    ? 'policy_missing'
                    : policy.version !== claim.policyVersion
                      ? 'policy_changed'
                      : policy.leaseToken !== claim.leaseToken
                        ? 'lease_lost'
                        : policy.proxyHostId !== claim.hostId ||
                            policy.domain !== claim.domain ||
                            policy.timeoutMs !== claim.timeoutMs
                          ? 'policy_selector_mismatch'
                          : Date.now() - row.started_ms >= 120000
                            ? 'retention_expired'
                            : null)
            if (
                !reason &&
                parsed.success &&
                (parsed.data.hostType !== 'proxy' ||
                    parsed.data.hostId !== claim.hostId ||
                    parsed.data.domain !== claim.domain ||
                    parsed.data.expectedRevision !== claim.before.revision)
            )
                reason = 'result_selector_mismatch'
            if (
                !reason &&
                !parsed.success &&
                !['runtime_unavailable', 'interrupted', 'authority_withdrawn'].includes(error ?? '')
            )
                reason = 'invalid_native_result'
            if (!reason && error === 'authority_withdrawn') reason = 'authority_withdrawn'
            if (reason || !policy) {
                await reject(row, digest, reason ?? 'policy_missing')
                counters.rejected++
                settled.push({ row, digest })
                continue
            }
            const outcome = parsed.success
                ? application.classify(parsed.data, policy.expectedStatus)
                : application.unknown('runtime_unavailable')
            eligible.push({ row, digest, claim, policy, outcome })
        }
        // The original completion owns the final canonical read under its transaction locks.
        // The immutable PG claim above binds before/gen and first admission without another full snapshot.
        const admitted = eligible
        if (admitted.length) {
            const pending = admitted.map((item) => ({
                attempt_id: item.claim.leaseToken,
                store_id: item.claim.storeId,
                epoch: item.claim.controllerEpoch,
                generation: item.claim.generation,
                policy_id: item.claim.policyId,
                policy_version: item.claim.policyVersion,
                digest: item.digest,
                due_ms: item.claim.dueAtMs,
                started_ms: item.row.started_ms,
                admitted_ms: item.row.admitted_ms,
                durable_ms: item.row.durable_ms,
                completed_ms: item.row.completed_ms,
            }))
            await resource.transaction(async (transaction) => {
                const [gate] = await transaction.execute<{ valid: boolean }>(
                    sql`SELECT ${identity} valid FROM ${ns}.identity WHERE id=1 FOR SHARE`,
                )
                if (!gate?.valid) throw new Error('B2 replay identity changed before registration')
                const [size] = await transaction.execute<{ count: number }>(
                    sql`SELECT count(*)::integer count FROM ${ns}.replay_pending`,
                )
                if ((size?.count ?? 4096) + admitted.length > 4096)
                    throw new Error('B2 replay registration pressure')
                const registered = await transaction.execute<{ attempt_id: string }>(
                    sql`INSERT INTO ${ns}.replay_pending SELECT * FROM jsonb_to_recordset(${JSON.stringify(pending)}::text::jsonb) AS entry(attempt_id uuid,store_id text,epoch text,generation bigint,policy_id uuid,policy_version uuid,digest text,due_ms bigint,started_ms bigint,admitted_ms bigint,durable_ms bigint,completed_ms bigint) ON CONFLICT(attempt_id) DO UPDATE SET digest=excluded.digest WHERE ${ns}.replay_pending.digest=excluded.digest AND ${ns}.replay_pending.store_id=excluded.store_id AND ${ns}.replay_pending.epoch=excluded.epoch AND ${ns}.replay_pending.generation=excluded.generation RETURNING attempt_id`,
                )
                if (registered.length !== admitted.length)
                    throw new Error('B2 pending replay payload conflict')
            })
            const results = await application.complete(
                admitted.map((item) => ({
                    claim: { policy: item.policy, leaseToken: item.claim.leaseToken },
                    outcome: item.outcome,
                    before: item.claim.before,
                    signal: new AbortController().signal,
                })),
            )
            for (let i = 0; i < admitted.length; i++) {
                const item = admitted[i]!,
                    result = results[i]!
                if (result.status === 'fulfilled' && result.value) counters.accepted++
                else {
                    await reject(
                        item.row,
                        item.digest,
                        item.policy.leaseExpiresAt &&
                            item.policy.leaseExpiresAt.getTime() <= Date.now()
                            ? 'lease_expired'
                            : 'authority_or_context_changed',
                    )
                    counters.rejected++
                }
                settled.push(item)
            }
        }
        // Immediate terminal rejections and later original completions must ACK the same raw prefix.
        settled.sort((left, right) => left.row.sequence - right.row.sequence)
        if (settled.length)
            await resource.transaction(async (transaction) => {
                const [gate] = await transaction.execute<{ valid: boolean }>(
                    sql`SELECT ${identity} valid FROM ${ns}.identity WHERE id=1 FOR SHARE`,
                )
                if (!gate?.valid) return
                const terminal = await transaction.execute<{ attempt: string; digest: string }>(
                    sql`SELECT receipt.attempt_id::text attempt,receipt.digest FROM ${ns}.receipts receipt WHERE attempt_id IN (${sql.join(
                        settled.map((item) => sql`${item.row.attempt_id}::uuid`),
                        sql`,`,
                    )}) AND (receipt.disposition='rejected' OR ${appliedProof})`,
                )
                const receipts = new Map(terminal.map((item) => [item.attempt, item.digest]))
                if (settled.some((item) => receipts.get(item.row.attempt_id) !== item.digest))
                    throw new Error('B2 replay ACK lacks terminal receipt')
                const acknowledged = outbox
                    .transaction(() => {
                        const local = outbox
                            .query<{ store_id: string; epoch: string }, []>(
                                'SELECT store_id,epoch FROM b2_identity WHERE id=1',
                            )
                            .get()
                        if (
                            local?.store_id !== metadata.storeId ||
                            local.epoch !== metadata.controllerEpoch
                        )
                            throw new Error('B2 local restore changed before ACK')
                        // Commit a newly discovered gap latch without acknowledging any raw record.
                        if (!contiguous()) return false
                        let cursor = outbox
                            .query<{ acked_sequence: number }, []>(
                                'SELECT acked_sequence FROM b2_identity WHERE id=1',
                            )
                            .get()!.acked_sequence
                        for (const item of settled) {
                            if (item.row.sequence > cursor + 1)
                                throw new Error('B2 terminal ACK must preserve the raw prefix')
                            outbox
                                .query(
                                    'DELETE FROM b2_outbox WHERE sequence=? AND attempt_id=? AND store_id=?',
                                )
                                .run(item.row.sequence, item.row.attempt_id, metadata.storeId)
                            cursor = Math.max(cursor, item.row.sequence)
                        }
                        outbox
                            .query('UPDATE b2_identity SET acked_sequence=? WHERE id=1')
                            .run(cursor)
                        return true
                    })
                    .immediate()
                if (!acknowledged) return
                await transaction.execute(
                    sql`DELETE FROM ${ns}.replay_pending WHERE attempt_id IN (${sql.join(
                        settled.map((item) => sql`${item.row.attempt_id}::uuid`),
                        sql`,`,
                    )})`,
                )
            })
    }
    let lastPublished = ''
    /** Publish changed replay counters and proof diagnostics; these counters are not new execution authority. */
    async function publish() {
        const next = JSON.stringify({
            ...counters,
            pending: outbox
                .query<{ count: number }, []>('SELECT count(*) count FROM b2_outbox')
                .get()!.count,
            originalPool: true,
            originalBatchCompletion: true,
            publicationProof,
            publicationStage,
            publicationDiagnostic,
        })
        if (next === lastPublished) return
        await writeFile('/tmp/monitoring-b2/state/replay-proof.json', next, { mode: 0o644 })
        lastPublished = next
    }
    const operations = {
        pass,
        repair,
        republish,
        drain: async () => {
            if (manual) throw new Error('B2 manual adapter does not own maintenance')
            await stopLocal()
        },
        stats: () => ({
            ...counters,
            publicationProof,
            publicationStage,
            publicationDiagnostic,
            pending: outbox
                .query<{ count: number }, []>('SELECT count(*) count FROM b2_outbox')
                .get()!.count,
        }),
        close: () => outbox.close(),
    }
    if (manual) return operations
    const control = Bun.serve({
        hostname: '127.0.0.1',
        port: 9022,
        async fetch(request) {
            if (request.headers.get('authorization') !== 'Bearer ' + webKey)
                return new Response(null, { status: 403 })
            if (request.method === 'GET' && new URL(request.url).pathname === '/status')
                return Response.json({ attached: true, paused, stopped })
            if (request.method === 'POST' && new URL(request.url).pathname === '/drain') {
                await stopLocal(true)
                return Response.json({ drained: true })
            }
            if (request.method === 'POST' && new URL(request.url).pathname === '/pause') {
                paused = true
                await running
                return Response.json({ paused: true })
            }
            if (request.method === 'POST' && new URL(request.url).pathname === '/resume') {
                if (stopped) return new Response(null, { status: 409 })
                paused = false
                return Response.json({ paused: false })
            }
            return new Response(null, { status: 404 })
        },
    })
    const timer = setInterval(() => {
        if (stopped || paused || running) return
        running = pass()
            .catch(() => {
                counters.errors++
            })
            .then(publish)
            .finally(() => {
                running = null
            })
    }, 20)
    let controlDrain: Promise<void> | undefined
    let localDrain: Promise<void> | null = null,
        maintenanceDrain: Promise<void> | null = null
    /** Stop timers, await the owned pass, close SQLite and write the identity-bound replay drain receipt. */
    function stopLocal(keepControl = false) {
        stopped = true
        clearInterval(timer)
        if (!keepControl) controlDrain ??= control.stop(true)
        localDrain ??= (async () => {
            await running
            await publish()
            outbox.close()
            const stat = await readFile('/proc/self/stat', 'utf8')
            const startTicks = stat
                .slice(stat.lastIndexOf(')') + 2)
                .trim()
                .split(/\s+/u)[19]
            if (!startTicks || !/^\d+$/u.test(startTicks))
                throw new Error('B2 replay drain identity unavailable')
            await writeFile(
                '/tmp/monitoring-b2/state/replay-drained.json',
                JSON.stringify({
                    drained: true,
                    pid: process.pid,
                    startTicks,
                    storeId: metadata.storeId,
                    controllerEpoch: metadata.controllerEpoch,
                }),
                { mode: 0o644 },
            )
        })()
        return Promise.all([localDrain, controlDrain]).then(() => undefined)
    }
    process.once('rentnerproxy:shutdown', (pending: Promise<void>[]) => {
        pending.push(stopLocal())
    })
    process.once('rentnerproxy:maintenance', (pending: Promise<void>[]) => {
        maintenanceDrain ??= (async () => {
            await stopLocal()
            const response = await fetch('http://127.0.0.1:9020/quiesce', {
                method: 'POST',
                headers: {
                    authorization: 'Bearer ' + webKey,
                    'content-type': 'application/json',
                },
                body: '{}',
                signal: AbortSignal.timeout(15000),
            })
            if (!response.ok) throw new Error('B2 native maintenance quiescence failed')
            z.object({
                quiesced: z.literal(true),
                nativeStopped: z.literal(true),
                activeNativeHeads: z.literal(0),
            }).parse(await response.json())
        })()
        pending.push(maintenanceDrain)
    })
    await publish()
    return operations
}
