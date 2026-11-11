// oxlint-disable no-await-in-loop -- Result receipts and ACKs follow durable transaction order.
import { Database } from 'bun:sqlite'
import { createHash } from 'node:crypto'
import {
    monitoringB2ClaimSchema,
    monitoringB2AdmissionRejection,
    monitoringB2OriginalAdmissionSchema,
} from './monitoring-b2.validation.ts'
import { monitoringB2DatabaseUrl } from './monitoring-b2-isolation.ts'
import type { MonitoringB2Claim, MonitoringB2RawRecord } from './Types/monitoring-b2.types.ts'

/** Bun alone interprets raw results and calls the existing authoritative incident state machine. */
export async function createMonitoringB2Replay(
    path: string,
    namespace: string,
    storeId: string,
    controllerEpoch: string,
) {
    await monitoringB2DatabaseUrl()
    if (
        process.env.RENTNERPROXY_MONITORING_B2 !== '1' ||
        !/^monitor_b2_[a-f0-9]{32}$/u.test(namespace)
    )
        throw new Error('B2 replay fixture identity required')
    const [
        { getAuthDatabase },
        { hostCheckPolicies },
        { eq, sql },
        { completeHostMonitoringClaim },
        { readHostMonitoringContextInTransaction },
        { domainDiagnosticsResultSchema },
        { classifyHostMonitoringOutcome, unknownHostMonitoringOutcome },
    ] = await Promise.all([
        import('../../web/src/server/Auth/Core/database.server.ts'),
        import('../../web/src/db/schema.ts'),
        import('drizzle-orm'),
        import('../../web/src/server/HostMonitoring/host-monitoring.repository.ts'),
        import('../../web/src/server/RuntimeDiagnostics/domain-diagnostics-data.ts'),
        import('../../web/src/lib/RuntimeDiagnostics/domainDiagnostics.ts'),
        import('../../web/src/lib/HostMonitoring/state.ts'),
    ])
    const db = getAuthDatabase()
    const outbox = new Database(path, { strict: true })
    outbox.exec('PRAGMA busy_timeout=2000; PRAGMA synchronous=FULL;')
    const batch = () =>
        outbox
            .query<MonitoringB2RawRecord, []>('SELECT * FROM b2_outbox ORDER BY sequence LIMIT 16')
            .all()
    let accepted = 0,
        duplicates = 0,
        rejected = 0
    const currentIdentity = sql`store_id=${storeId} AND epoch=${controllerEpoch} AND NOT paused AND NOT resync_required AND generation=published_generation`
    /** Latch a missing raw sequence as incomplete evidence; never advance ACK across the gap. */
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
    /** Require both PostgreSQL authority and the local store to retain this replay owner's identity. */
    async function identityMatches() {
        const rows = await db.execute<{ valid: boolean }>(
            sql`SELECT ${currentIdentity} AS valid FROM ${sql.identifier(namespace)}.identity WHERE id=1`,
        )
        const local = outbox
            .query<{ store_id: string; epoch: string }, []>(
                'SELECT store_id,epoch FROM b2_identity WHERE id=1',
            )
            .get()
        return (
            rows[0]?.valid === true &&
            local?.store_id === storeId &&
            local.epoch === controllerEpoch
        )
    }
    /** Terminal rejection has a durable reason and cannot be mistaken for an applied observation. */
    async function reject(
        claim: MonitoringB2Claim,
        digest: string,
        reason: string,
        row: MonitoringB2RawRecord,
    ) {
        const rows = await db.execute<{
            attempt_id: string
        }>(sql`INSERT INTO ${sql.identifier(namespace)}.receipts(attempt_id,store_id,epoch,generation,policy_id,policy_version,digest,transaction_id,disposition,reason,due_ms,started_ms,admitted_ms,durable_ms,completed_ms)
            VALUES(${claim.leaseToken}::uuid,${storeId},${controllerEpoch},${claim.generation},${claim.policyId}::uuid,${claim.policyVersion}::uuid,${digest},mod(pg_current_xact_id()::text::numeric,4294967296)::text,'rejected',${reason},${claim.dueAtMs},${row.started_ms},${row.admitted_ms},${row.durable_ms},${row.completed_ms})
            ON CONFLICT(attempt_id) DO UPDATE SET disposition='rejected',reason=excluded.reason
            WHERE ${sql.identifier(namespace)}.receipts.digest=excluded.digest AND NOT EXISTS(SELECT 1 FROM rentnerproxy.host_check_observations o WHERE o.policy_id=${sql.identifier(namespace)}.receipts.policy_id AND o.policy_version=${sql.identifier(namespace)}.receipts.policy_version AND o.xmin::text=${sql.identifier(namespace)}.receipts.transaction_id)
            RETURNING attempt_id`)
        if (rows.length !== 1) throw new Error('B2 rejection receipt conflict')
    }
    /** The shared PG identity lock orders reset against ACK; local identity is checked inside its SQLite transaction. */
    async function acknowledge(row: MonitoringB2RawRecord, digest: string) {
        return db.transaction(async (transaction) => {
            const identity = await transaction.execute<{ valid: boolean }>(
                sql`SELECT ${currentIdentity} AS valid FROM ${sql.identifier(namespace)}.identity WHERE id=1 FOR SHARE`,
            )
            if (identity[0]?.valid !== true) return false
            const terminal = await transaction.execute<{ valid: boolean }>(
                sql`SELECT (disposition='rejected' OR EXISTS(SELECT 1 FROM rentnerproxy.host_check_observations o WHERE o.policy_id=r.policy_id AND o.policy_version=r.policy_version AND o.xmin::text=r.transaction_id)) AS valid FROM ${sql.identifier(namespace)}.receipts r WHERE attempt_id=${row.attempt_id}::uuid AND digest=${digest} AND store_id=${storeId} AND epoch=${controllerEpoch}`,
            )
            if (terminal[0]?.valid !== true)
                throw new Error('B2 ACK lacks a terminal durable receipt')
            return outbox
                .transaction(() => {
                    const local = outbox
                        .query<{ store_id: string; epoch: string }, []>(
                            'SELECT store_id,epoch FROM b2_identity WHERE id=1',
                        )
                        .get()
                    if (local?.store_id !== storeId || local.epoch !== controllerEpoch) return false
                    if (!contiguous()) return false
                    const cursor = outbox
                        .query<{ acked_sequence: number }, []>(
                            'SELECT acked_sequence FROM b2_identity WHERE id=1',
                        )
                        .get()!.acked_sequence
                    if (row.sequence > cursor + 1)
                        throw new Error('B2 terminal ACK must preserve the raw prefix')
                    outbox
                        .query(
                            'DELETE FROM b2_outbox WHERE sequence=? AND attempt_id=? AND store_id=?',
                        )
                        .run(row.sequence, row.attempt_id, storeId)
                    outbox
                        .query('UPDATE b2_identity SET acked_sequence=? WHERE id=1')
                        .run(Math.max(cursor, row.sequence))
                    return true
                })
                .immediate()
        })
    }
    return {
        /** At most 16 records pass exact payload, terminal receipt, lease, canonical context and restore fences per pass. */
        async replayBatch() {
            if (!contiguous() || !(await identityMatches()))
                return { accepted: 0, duplicates: 0, rejected: 0, paused: true }
            const counts = { accepted: 0, duplicates: 0, rejected: 0, paused: false }
            for (const row of batch()) {
                if (row.result === null || row.completed_ms === null) break
                if (Buffer.byteLength(row.claim) + Buffer.byteLength(row.result) > 4096)
                    throw new Error('B2 replay record budget exceeded')
                const claim = monitoringB2ClaimSchema.parse(JSON.parse(row.claim))
                if (
                    claim.leaseToken !== row.attempt_id ||
                    claim.storeId !== row.store_id ||
                    claim.storeId !== storeId ||
                    claim.controllerEpoch !== controllerEpoch
                )
                    throw new Error('B2 replay identity conflict')
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
                const previous = await db.execute<{
                    digest: string
                    applied: boolean
                    disposition: string
                }>(
                    sql`SELECT r.digest,r.disposition,EXISTS(SELECT 1 FROM rentnerproxy.host_check_observations o WHERE o.policy_id=r.policy_id AND o.policy_version=r.policy_version AND o.xmin::text=r.transaction_id) AS applied FROM ${sql.identifier(namespace)}.receipts r WHERE attempt_id=${row.attempt_id}::uuid`,
                )
                if (previous[0]) {
                    if (previous[0].digest !== digest)
                        throw new Error('B2 conflicting replay payload')
                    if (previous[0].applied) counts.duplicates++
                    else {
                        if (previous[0].disposition !== 'rejected')
                            await reject(claim, digest, 'completion_rejected', row)
                        counts.rejected++
                    }
                } else {
                    const [original] = await db.execute<{
                        original_claim: unknown
                        admitted_ms: string | null
                    }>(
                        sql`SELECT original_claim,admitted_ms::text FROM ${sql.identifier(namespace)}.admissions WHERE lease_token=${row.attempt_id}::uuid`,
                    )
                    const [policy] = await db
                        .select()
                        .from(hostCheckPolicies)
                        .where(eq(hostCheckPolicies.id, claim.policyId))
                        .limit(1)
                    let reason: string | null =
                        monitoringB2AdmissionRejection(
                            claim,
                            original
                                ? monitoringB2OriginalAdmissionSchema.parse(original)
                                : undefined,
                            row,
                        ) ??
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
                    let payload: unknown
                    try {
                        payload = JSON.parse(row.result)
                    } catch {
                        reason = 'invalid_native_result'
                    }
                    const parsed = domainDiagnosticsResultSchema.safeParse(payload)
                    const transportCode =
                        typeof payload === 'object' &&
                        payload !== null &&
                        Object.keys(payload).length === 1 &&
                        'error' in payload
                            ? String(payload.error)
                            : null
                    const transportError =
                        transportCode !== null &&
                        ['runtime_unavailable', 'interrupted', 'authority_withdrawn'].includes(
                            transportCode,
                        )
                    if (!reason && !parsed.success && !transportError)
                        reason = 'invalid_native_result'
                    if (
                        !reason &&
                        parsed.success &&
                        (parsed.data.hostType !== 'proxy' ||
                            parsed.data.hostId !== claim.hostId ||
                            parsed.data.domain !== claim.domain ||
                            parsed.data.expectedRevision !== claim.before.revision)
                    )
                        reason = 'result_selector_mismatch'
                    if (!reason && transportCode === 'authority_withdrawn')
                        reason = 'authority_withdrawn'
                    const outcome =
                        parsed.success && policy
                            ? classifyHostMonitoringOutcome(parsed.data, policy.expectedStatus)
                            : unknownHostMonitoringOutcome('runtime_unavailable')
                    let applied = false
                    if (!reason && policy) {
                        applied = await completeHostMonitoringClaim(
                            { policy, leaseToken: claim.leaseToken },
                            outcome,
                            async (transaction, currentPolicy) => {
                                if (
                                    currentPolicy.proxyHostId !== claim.hostId ||
                                    currentPolicy.domain !== claim.domain ||
                                    currentPolicy.timeoutMs !== claim.timeoutMs
                                )
                                    return false
                                const identity = await transaction.execute<{ valid: boolean }>(
                                    sql`SELECT ${currentIdentity} AND generation=${claim.generation} AS valid FROM ${sql.identifier(namespace)}.identity WHERE id=1 FOR SHARE`,
                                )
                                if (identity[0]?.valid !== true) return false
                                const current = await readHostMonitoringContextInTransaction(
                                    transaction,
                                    {
                                        hostType: 'proxy',
                                        hostId: currentPolicy.proxyHostId,
                                        domain: currentPolicy.domain,
                                    },
                                )
                                if (
                                    !current.enabled ||
                                    current.proof !== claim.before.proof ||
                                    current.revision !== claim.before.revision ||
                                    current.availability.proof !==
                                        claim.before.availability.proof ||
                                    current.availability.revision !==
                                        claim.before.availability.revision
                                )
                                    return false
                                // Receipt and observation use the original completion TX. A post-callback expiry
                                // can leave only a candidate receipt; that becomes an explicit rejection.
                                await transaction.execute(
                                    sql`INSERT INTO ${sql.identifier(namespace)}.receipts(attempt_id,store_id,epoch,generation,policy_id,policy_version,digest,transaction_id,due_ms,started_ms,admitted_ms,durable_ms,completed_ms) VALUES(${row.attempt_id}::uuid,${storeId},${controllerEpoch},${claim.generation},${claim.policyId}::uuid,${claim.policyVersion}::uuid,${digest},mod(pg_current_xact_id()::text::numeric,4294967296)::text,${claim.dueAtMs},${row.started_ms},${row.admitted_ms},${row.durable_ms},${row.completed_ms})`,
                                )
                                return true
                            },
                            claim.before.availability,
                        )
                    }
                    if (applied) counts.accepted++
                    else {
                        await reject(
                            claim,
                            digest,
                            reason ??
                                (policy?.leaseExpiresAt &&
                                policy.leaseExpiresAt.getTime() <= Date.now()
                                    ? 'lease_expired'
                                    : 'authority_or_context_changed'),
                            row,
                        )
                        counts.rejected++
                    }
                }
                if (!(await acknowledge(row, digest))) {
                    counts.paused = true
                    break
                }
            }
            accepted += counts.accepted
            duplicates += counts.duplicates
            rejected += counts.rejected
            return counts
        },
        stats: () => ({
            accepted,
            duplicates,
            rejected,
            pending: outbox
                .query<{ count: number }, []>('SELECT count(*) AS count FROM b2_outbox')
                .get()!.count,
        }),
        close: () => outbox.close(),
    }
}
