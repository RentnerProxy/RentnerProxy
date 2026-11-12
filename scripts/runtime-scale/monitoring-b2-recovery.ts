// oxlint-disable no-await-in-loop -- Fault installation, durable replay and its observations must execute in order.
import { SQL } from 'bun'
import { Database } from 'bun:sqlite'
import { randomUUID } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { monitoringB2ClaimSchema } from './monitoring-b2.validation.ts'
import { monitoringB2WebKey } from './monitoring-b2-capabilities.ts'
import type {
    MonitoringB2Claim,
    MonitoringB2RawRecord,
    MonitoringB2RecoveryContext,
} from './Types/monitoring-b2.types.ts'
import type {
    MonitoringB2RecoveryResult,
    MonitoringB2RecoveryScenario,
    MonitoringB2ReplayEvidence,
} from './Types/monitoring-b2-recovery.types.ts'

type MonitoringB2RecoveryFixture = MonitoringB2RecoveryContext

/** Require PostgreSQL's exact permission SQLSTATE, including Bun's errno/cause wrappers; unrelated failures remain insufficient proof. */
export function monitoringB2PermissionDenied(error: unknown): boolean {
    let current = error
    const seen = new Set<unknown>()
    for (let depth = 0; depth < 4; depth++) {
        if (typeof current !== 'object' || current === null || seen.has(current)) return false
        seen.add(current)
        if (
            ('code' in current && current.code === '42501') ||
            ('errno' in current && current.errno === '42501')
        )
            return true
        current = 'cause' in current ? current.cause : null
    }
    return false
}

/** Read the existing web replay counters; a missing/malformed proof is not zero-loss recovery. */
async function replayCounters() {
    const value: unknown = JSON.parse(
        await readFile('/tmp/monitoring-b2/state/replay-proof.json', 'utf8'),
    )
    if (
        typeof value !== 'object' ||
        value === null ||
        !('errors' in value) ||
        !('duplicates' in value) ||
        typeof value.errors !== 'number' ||
        typeof value.duplicates !== 'number'
    )
        throw new Error('replay-counter-proof-unavailable')
    return { errors: value.errors, duplicates: value.duplicates }
}

/** Require exact raw, network and applied-observation counts plus known lease timing within the replay budget. */
export function assessMonitoringB2Replay(evidence: MonitoringB2ReplayEvidence) {
    const failures: string[] = []
    for (const [key, value] of Object.entries(evidence))
        if (typeof value !== 'number' || !Number.isFinite(value) || value < 0)
            failures.push(key + ':invalid-measurement')
    for (const key of [
        'rawDurableRecords',
        'distinctRawPolicies',
        'nativeHeads',
        'distinctNativeHosts',
        'replayedResults',
        'observations',
    ] as const)
        if (evidence[key] !== 1000) failures.push(key + ':expected-1000')
    if (evidence.replayMs > 30000) failures.push('replay-exceeded-30-seconds')
    if (evidence.pending !== 0) failures.push('durable-backlog-not-drained')
    if (evidence.rejectedApplied !== 0) failures.push('rejected-receipt-counted-applied')
    if (evidence.staleCommits !== 0) failures.push('expired-lease-commit-not-excluded')
    return failures
}

/** Control only the authenticated fixture replay owner and require its acknowledged pause state. */
async function replayControl(action: 'pause' | 'resume') {
    const response = await fetch('http://127.0.0.1:9022/' + action, {
        method: 'POST',
        headers: { authorization: 'Bearer ' + (await monitoringB2WebKey()) },
        signal: AbortSignal.timeout(5000),
    })
    if (!response.ok) throw new Error('replay-control-unavailable')
    const body: unknown = await response.json()
    if (
        typeof body !== 'object' ||
        body === null ||
        !('paused' in body) ||
        body.paused !== (action === 'pause')
    )
        throw new Error('replay-control-not-confirmed')
}

/** Poll an observable fixture condition to its short deadline; never replace missing proof with a sleep. */
async function until(predicate: () => boolean | Promise<boolean>, budgetMs: number) {
    const deadline = performance.now() + budgetMs
    do {
        if (await predicate()) return
        await Bun.sleep(25)
    } while (performance.now() < deadline)
    throw new Error('bounded-observation-timeout')
}

function open(path: string) {
    const store = new Database(path, { strict: true })
    store.exec('PRAGMA synchronous=FULL; PRAGMA busy_timeout=2000;')
    return store
}

function rawRows(store: Database) {
    return store
        .query<MonitoringB2RawRecord, []>('SELECT * FROM b2_outbox ORDER BY sequence LIMIT 4096')
        .all()
}

/** Restore explicit fixture raw records transactionally for controlled replay/pressure fault cases only. */
function putRows(store: Database, rows: readonly MonitoringB2RawRecord[]) {
    const insert = store.query(
        'INSERT INTO b2_outbox(attempt_id,store_id,claim,started_ms,admitted_ms,durable_ms,completed_ms,result) VALUES(?,?,?,?,?,?,?,?)',
    )
    store.transaction(() => {
        for (const row of rows)
            insert.run(
                row.attempt_id,
                row.store_id,
                row.claim,
                row.started_ms,
                row.admitted_ms,
                row.durable_ms,
                row.completed_ms,
                row.result,
            )
    })()
}

/** Count terminal receipts against actual observation identities, keeping rejected receipts separate. */
async function receiptCounts(context: MonitoringB2RecoveryFixture, namespace: string) {
    const [counts] = await context.database.unsafe<
        {
            replayedResults: number
            observations: number
            rejected: number
            rejectedApplied: number
            incidents: number
            pending: number
        }[]
    >(`SELECT
        count(*) FILTER(WHERE r.disposition='candidate' AND o.id IS NOT NULL)::integer AS "replayedResults",
        count(DISTINCT o.id)::integer observations,
        count(*) FILTER(WHERE r.disposition='rejected')::integer rejected,
        count(*) FILTER(WHERE r.disposition='rejected' AND o.id IS NOT NULL)::integer AS "rejectedApplied",
        (SELECT count(*)::integer FROM rentnerproxy.host_check_incidents WHERE policy_id IN (SELECT policy_id FROM ${namespace}.contexts)) incidents,
        (SELECT count(*)::integer FROM ${namespace}.replay_pending) pending
        FROM ${namespace}.receipts r LEFT JOIN rentnerproxy.host_check_observations o
        ON o.id=r.observation_id AND o.policy_id=r.policy_id AND o.policy_version=r.policy_version`)
    if (!counts) throw new Error('durable-receipt-observation-unavailable')
    return counts
}

/** Collect bounded completed raw records with replay paused; separate checks establish actual network counts. */
async function collectNativeRaw(
    context: MonitoringB2RecoveryFixture,
    store: Database,
    count: number,
) {
    context.fastProbes(true)
    await context.start()
    await until(
        () =>
            store
                .query<{ count: number }, []>(
                    'SELECT count(*) count FROM b2_outbox WHERE admitted_ms IS NOT NULL AND durable_ms IS NOT NULL AND completed_ms IS NOT NULL AND result IS NOT NULL',
                )
                .get()!.count >= count,
        30000,
    )
    await context.stopNative()
    return rawRows(store)
}

/** Exercise bounded recovery, identical replay and conflicting payloads through the existing Bun sink. */
async function proveReplay(
    context: MonitoringB2RecoveryFixture,
    namespace: string,
    store: Database,
) {
    const rows = await collectNativeRaw(context, store, 1000)
    const claims = rows.map((row) => monitoringB2ClaimSchema.parse(JSON.parse(row.claim)))
    const before = await receiptCounts(context, namespace)
    if (before.observations !== 0) throw new Error('replay-was-not-paused')
    const began = performance.now()
    await replayControl('resume')
    // No wake is delivered: the durable raw store and the original Bun polling owner recover it.
    await until(() => rawRows(store).length === 0, 30000)
    const replayMs = performance.now() - began
    await replayControl('pause')
    const after = await receiptCounts(context, namespace)
    const replayVisibleBeforeEarliestLeaseDeadline =
        Date.now() < Math.min(...claims.map((claim) => claim.leaseExpiresAtMs))
    const heads = context.heads()
    const evidence: MonitoringB2ReplayEvidence = {
        rawDurableRecords: rows.filter(
            (row) =>
                row.admitted_ms !== null && row.durable_ms !== null && row.completed_ms !== null,
        ).length,
        distinctRawPolicies: new Set(claims.map((claim) => claim.policyId)).size,
        nativeHeads: heads.length,
        distinctNativeHosts: new Set(heads.map((head) => head.hostId)).size,
        replayedResults: after.replayedResults,
        observations: after.observations,
        replayMs,
        pending: after.pending,
        rejectedApplied: after.rejectedApplied,
        // First durable visibility before every lease deadline is a conservative commit upper bound.
        // After that bound, commit timing is unknown; never fabricate a zero violation count.
        staleCommits: replayVisibleBeforeEarliestLeaseDeadline ? 0 : null,
    }
    const failures = assessMonitoringB2Replay(evidence)
    const duplicate = rows.slice(0, 16)
    const previousCounters = await replayCounters()
    putRows(store, duplicate)
    await replayControl('resume')
    // Repeated resume notifications cannot create an additional application transaction outcome.
    await replayControl('resume')
    await until(() => rawRows(store).length === 0, 5000)
    await replayControl('pause')
    const repeated = await receiptCounts(context, namespace)
    const repeatedCounters = await replayCounters()
    const duplicateObservations = repeated.observations - after.observations
    const duplicateIncidents = repeated.incidents - after.incidents
    const duplicateReplayPassed =
        duplicateObservations === 0 &&
        duplicateIncidents === 0 &&
        repeated.pending === 0 &&
        repeatedCounters.duplicates - previousCounters.duplicates === duplicate.length
    if (!duplicateReplayPassed) failures.push('duplicate-durable-record-created-another-outcome')
    // A conflicting raw digest must remain unacknowledged. The original exact record then repairs it.
    const first = duplicate[0]
    if (!first) throw new Error('duplicate-proof-record-unavailable')
    putRows(store, [{ ...first, result: JSON.stringify({ error: 'interrupted' }) }])
    await replayControl('resume')
    await until(
        async () =>
            await replayCounters()
                .then((counters) => counters.errors > repeatedCounters.errors)
                .catch(() => false),
        5000,
    )
    await replayControl('pause')
    const conflict = await receiptCounts(context, namespace)
    const conflictingReplayRejected =
        rawRows(store).length === 1 && conflict.observations === repeated.observations
    if (!conflictingReplayRejected) failures.push('conflicting-digest-not-fenced')
    // Repair this controlled corruption in place; deleting its sequence would manufacture a gap.
    store
        .query('UPDATE b2_outbox SET result=? WHERE attempt_id=?')
        .run(first.result, first.attempt_id)
    await replayControl('resume')
    await until(() => rawRows(store).length === 0, 5000)
    return {
        evidence: {
            ...evidence,
            records: rows.length,
            noWakeReplayPassed: after.observations === 1000,
            replayVisibleBeforeEarliestLeaseDeadline,
            duplicateRecords: duplicate.length,
            duplicateReceipts: repeatedCounters.duplicates - previousCounters.duplicates,
            duplicateObservations,
            duplicateIncidents,
            duplicateReplayPassed,
            conflictingReplayRejected,
        },
        failures,
    }
}

/** Retain raw evidence past the original lease and require rejection without a new observation. */
async function proveLeaseExpiry(
    context: MonitoringB2RecoveryFixture,
    namespace: string,
    store: Database,
) {
    const rows = await collectNativeRaw(context, store, 16)
    const tokens = rows.map((row) => row.attempt_id)
    const [before] = await context.database.unsafe<{ observations: number }[]>(
        'SELECT count(*)::integer observations FROM rentnerproxy.host_check_observations WHERE policy_id=ANY($1::uuid[])',
        [context.state()!.hosts.map((host) => host.policyId)],
    )
    await context.database.unsafe(
        "UPDATE rentnerproxy.host_check_policies SET lease_expires_at=clock_timestamp()-interval '1 millisecond' WHERE lease_token=ANY($1::uuid[])",
        [tokens],
    )
    const retainedWithin120Seconds = rows.every((row) => Date.now() - row.started_ms < 120000)
    await replayControl('resume')
    await until(() => rawRows(store).length === 0, 10000)
    const counts = await receiptCounts(context, namespace)
    const failures: string[] = []
    if (!retainedWithin120Seconds) failures.push('raw-record-outside-retention-test-boundary')
    if (counts.rejected !== rows.length) failures.push('expired-final-lease-not-rejected')
    if (counts.observations !== before?.observations) failures.push('expired-final-lease-applied')
    if (counts.rejectedApplied !== 0) failures.push('rejected-final-lease-counted-applied')
    return {
        evidence: {
            rawDurableRecords: rows.length,
            nativeHeads: context.heads().length,
            retainedWithin120Seconds,
            rejected: counts.rejected,
            observations: counts.observations,
            rejectedApplied: counts.rejectedApplied,
            staleCommits: counts.observations - (before?.observations ?? 0),
            finalLeaseSeconds: 45,
            rawRetentionSeconds: 120,
        },
        failures,
    }
}

/** Test real SQL/application grant, configuration, generation and reset fences without starting native HEADs. */
async function proveFences(context: MonitoringB2RecoveryFixture, namespace: string) {
    const authority = context.authority()!
    await context.stopNative()
    const applicationRole = randomUUID(),
        applicationPermission = randomUUID()
    const worker = new SQL(authority.workerUrl, { max: 1, connectionTimeout: 2 })
    const evidence: Record<string, number | boolean | string | null> = {}
    const claim = async () => {
        const rows = await worker.unsafe<{ claim: unknown }[]>(
            `SELECT * FROM ${namespace}.claim(1,$1,$2)`,
            [authority.storeId, authority.controllerEpoch],
        )
        return rows.map((row) => monitoringB2ClaimSchema.parse(row.claim))
    }
    const begin = async (claimed: MonitoringB2Claim) => {
        const [row] = await worker.unsafe<{ admitted: number | null }[]>(
            `SELECT ${namespace}.begin_check($1::uuid,$2::uuid,$3::uuid,$4,$5,$6) admitted`,
            [
                claimed.policyId,
                claimed.policyVersion,
                claimed.leaseToken,
                claimed.storeId,
                claimed.controllerEpoch,
                claimed.generation,
            ],
        )
        return row?.admitted ?? null
    }
    try {
        await context.database.unsafe(
            'INSERT INTO rentnerproxy.roles(id,key,name) VALUES($1,$2,$2)',
            [applicationRole, 'b2-fixture-role-' + applicationRole],
        )
        await context.database.unsafe(
            'INSERT INTO rentnerproxy.permissions(id,key,name) VALUES($1,$2,$2)',
            [applicationPermission, 'b2-fixture-permission-' + applicationPermission],
        )
        await context.start()
        await until(() => Date.now() >= context.state()!.originMs, 2500)
        const [reserved] = await claim()
        if (!reserved) throw new Error('fresh-fence-claim-unavailable')
        evidence.freshAuthorityAdmitted = (await begin(reserved)) !== null
        evidence.foreignStoreRejected =
            (await begin({ ...reserved, storeId: randomUUID() })) === null
        evidence.foreignEpochRejected =
            (await begin({ ...reserved, controllerEpoch: randomUUID() })) === null
        evidence.nullIdentityClaimRejected =
            (await worker.unsafe(`SELECT * FROM ${namespace}.claim(1,NULL,NULL)`)).length === 0
        let unauthorizedWriteRejected = false
        try {
            await worker.unsafe(
                'UPDATE rentnerproxy.host_check_policies SET enabled=false WHERE id=$1',
                [reserved.policyId],
            )
        } catch (error) {
            unauthorizedWriteRejected = monitoringB2PermissionDenied(error)
        }
        evidence.unauthorizedWriteRejected = unauthorizedWriteRejected
        const role = new URL(authority.workerUrl).username
        if (!/^monitor_b2_role_[a-f0-9]{32}$/u.test(role))
            throw new Error('worker-role-identity-invalid')
        await context.database.unsafe(
            `REVOKE EXECUTE ON FUNCTION ${namespace}.begin_check(uuid,uuid,uuid,text,text,bigint) FROM ${role}`,
        )
        let denied = false
        try {
            await begin(reserved)
        } catch (error) {
            denied = monitoringB2PermissionDenied(error)
        } finally {
            await context.database.unsafe(
                `GRANT EXECUTE ON FUNCTION ${namespace}.begin_check(uuid,uuid,uuid,text,text,bigint) TO ${role}`,
            )
        }
        evidence.withdrawnSqlGrantRejected = denied
        evidence.restoredSqlGrantAdmitted = (await begin(reserved)) !== null
        // This unassigned disposable role exercises an actual application grant commit without
        // changing the authenticated fixture user's permissions.
        await context.database.unsafe(
            'INSERT INTO rentnerproxy.role_permissions(role_id,permission_id) VALUES($1,$2)',
            [applicationRole, applicationPermission],
        )
        evidence.committedApplicationGrantRejected = (await begin(reserved)) === null
        evidence.unpublishedApplicationGrantClaimRejected = (await claim()).length === 0
        await authority.publish(true)
        const [afterGrant] = await claim()
        if (!afterGrant || (await begin(afterGrant)) === null)
            throw new Error('fresh-post-grant-authority-unavailable')
        evidence.freshApplicationGrantAdmitted = true
        await context.database.unsafe(
            'UPDATE rentnerproxy.proxy_hosts SET forward_port=forward_port+1 WHERE id=$1',
            [afterGrant.hostId],
        )
        evidence.committedConfigRejected = (await begin(afterGrant)) === null
        evidence.unpublishedConfigClaimRejected = (await claim()).length === 0
        await authority.publish(true)
        evidence.oldGenerationRejectedAfterPublish = (await begin(afterGrant)) === null
        const [fresh] = await claim()
        evidence.freshGenerationAdmitted = fresh !== undefined && (await begin(fresh)) !== null
        await authority.resetIdentity()
        evidence.oldStoreEpochRejected = fresh !== undefined && (await begin(fresh)) === null
        evidence.resetRequiresPublication = (await claim()).length === 0
        await authority.publish()
        evidence.oldStoreEpochRejectedAfterPublish = (await claim()).length === 0
        const [counts] = await context.database.unsafe<{ observations: number; raw: number }[]>(
            `SELECT (SELECT count(*)::integer FROM rentnerproxy.host_check_observations WHERE policy_id=ANY($1::uuid[])) observations,(SELECT count(*)::integer FROM ${namespace}.raw_completed) raw`,
            [context.state()!.hosts.map((host) => host.policyId)],
        )
        evidence.unauthorizedWrites = counts!.observations + counts!.raw
        evidence.nativeHeads = context.heads().length
        const failures = Object.entries(evidence)
            .filter(([, value]) => value === false)
            .map(([key]) => key + ':not-proven')
        if (evidence.unauthorizedWrites !== 0 || evidence.nativeHeads !== 0)
            failures.push('admission-only-fence-test-produced-side-effects')
        return { evidence, failures }
    } finally {
        await worker.close()
        await context.database.unsafe('DELETE FROM rentnerproxy.roles WHERE id=$1', [
            applicationRole,
        ])
        await context.database.unsafe('DELETE FROM rentnerproxy.permissions WHERE id=$1', [
            applicationPermission,
        ])
    }
}

function pressurePayload(field: string) {
    const base = JSON.stringify({ syntheticPressure: true, [field]: '' })
    return JSON.stringify({ syntheticPressure: true, [field]: 'x'.repeat(2048 - base.length) })
}

/** Fill only synthetic raw occupancy and verify visible admission pressure without counting it as executions. */
async function provePressure(context: MonitoringB2RecoveryFixture, store: Database) {
    await context.stopNative()
    context.fastProbes(true)
    const now = Date.now()
    const pressureRows = Array.from({ length: 4096 }, (_, index) => ({
        sequence: index + 1,
        attempt_id: randomUUID(),
        store_id: context.authority()!.storeId,
        claim: pressurePayload('controlledClaimPadding'),
        result: pressurePayload('controlledResultPadding'),
        started_ms: now,
        admitted_ms: null,
        durable_ms: null,
        completed_ms: now,
    }))
    // These are controlled occupancy records, never claims, admissions or network starts.
    putRows(store, pressureRows)
    const occupancy = () =>
        store
            .query<{ records: number; bytes: number }, []>(
                'SELECT count(*) records,coalesce(sum(length(cast(claim AS blob))+coalesce(length(cast(result AS blob)),0)),0) bytes FROM b2_outbox',
            )
            .get()!
    const capacity = occupancy()
    await context.restartNative()
    await context.start()
    const pressure = () =>
        store.query<{ pressure: number }, []>('SELECT pressure FROM b2_status WHERE id=1').get()
            ?.pressure === 1
    await until(pressure, 5000)
    await until(() => Date.now() >= context.state()!.originMs + 750, 2500)
    const capacityHeads = context.heads().length
    await context.stopNative()
    discardSyntheticPressure(store)
    putRows(store, [{ ...pressureRows[0]!, started_ms: Date.now() - 120000 }])
    // Clear the previous status so a reopened native owner must observe the independent age fence.
    store.exec('UPDATE b2_status SET pressure=0 WHERE id=1')
    await context.restartNative()
    await until(pressure, 5000)
    await Bun.sleep(750)
    const retentionHeads = context.heads().length
    const retainedRecords = occupancy().records
    await context.stopNative()
    discardSyntheticPressure(store)
    store.exec('UPDATE b2_status SET pressure=0 WHERE id=1')
    await context.restartNative()
    await until(() => context.heads().length > retentionHeads, 5000)
    await context.stopNative()
    const actualRows = rawRows(store)
    const actualAdmitted = actualRows.filter(
        (row) => row.admitted_ms !== null && row.durable_ms !== null,
    ).length
    const recoveryHeads = context.heads().length - retentionHeads
    const failures: string[] = []
    if (capacity.records !== 4096 || capacity.bytes !== 16 * 1024 * 1024)
        failures.push('controlled-pressure-boundary-not-populated')
    if (capacityHeads !== 0 || retentionHeads !== 0)
        failures.push('native-head-started-under-durable-pressure')
    if (retainedRecords !== 1) failures.push('aged-record-discarded-instead-of-backpressure')
    if (recoveryHeads === 0 || actualAdmitted === 0)
        failures.push('native-admission-did-not-recover-after-pressure')
    return {
        evidence: {
            syntheticPressureRecords: capacity.records,
            syntheticPayloadBytes: capacity.bytes,
            maxRecords: 4096,
            maxPayloadBytes: 16 * 1024 * 1024,
            retentionMs: 120000,
            capacityPressureConfirmed: true,
            retentionPressureConfirmed: true,
            nativeReopens: 3,
            capacityHeads,
            retentionHeads,
            retainedRecords,
            recoveryHeads,
            actualAdmitted,
            nativeStopped: true,
        },
        failures,
    }
}

/** Remove only the explicitly marked pressure payloads before resuming the next owned fixture case. */
function discardSyntheticPressure(store: Database) {
    store.transaction(() => {
        const rows = rawRows(store)
        if (
            !rows.length ||
            rows.some(
                (row) =>
                    row.admitted_ms !== null || !row.claim.includes('"syntheticPressure":true'),
            )
        )
            throw new Error('refusing-to-discard-actual-native-records')
        store.exec(
            "UPDATE b2_identity SET discarded=discarded+(SELECT count(*) FROM b2_outbox),acked_sequence=(SELECT seq FROM sqlite_sequence WHERE name='b2_outbox') WHERE id=1; DELETE FROM b2_outbox;",
        )
    })()
}

/** Remove one actual fixture raw sequence and require persistent resync without partial observation replay. */
async function proveGap(context: MonitoringB2RecoveryFixture, namespace: string, store: Database) {
    const rows = await collectNativeRaw(context, store, 16)
    const first = rows[0]
    if (!first) throw new Error('actual-raw-gap-record-unavailable')
    store.query('DELETE FROM b2_outbox WHERE attempt_id=?').run(first.attempt_id)
    await replayControl('resume')
    const status = () =>
        store
            .query<{ gaps: number; resync_required: number }, []>(
                'SELECT gaps,resync_required FROM b2_status WHERE id=1',
            )
            .get()!
    await until(() => status().gaps > 0 && status().resync_required === 1, 5000)
    const headsBeforeRestart = context.heads().length
    await context.restartNative()
    await Bun.sleep(750)
    await context.stopNative()
    const counts = await receiptCounts(context, namespace)
    const after = status()
    const headsAfterRestart = context.heads().length
    const failures: string[] = []
    if (after.resync_required !== 1 || after.gaps < 1) failures.push('raw-gap-not-latched')
    if (headsAfterRestart !== headsBeforeRestart)
        failures.push('native-admission-resumed-after-gap')
    if (counts.observations !== 0 || counts.replayedResults !== 0)
        failures.push('gap-allowed-partial-application-replay')
    return {
        evidence: {
            actualRawRecordsBeforeLoss: rows.length,
            intentionallyLostActualRecords: 1,
            gaps: after.gaps,
            resyncRequired: after.resync_required === 1,
            nativeRestarted: true,
            headsBeforeRestart,
            headsAfterRestart,
            observations: counts.observations,
            replayedResults: counts.replayedResults,
            automaticResumeRejected:
                after.resync_required === 1 && headsAfterRestart === headsBeforeRestart,
        },
        failures,
    }
}

/** Runs only in the root-sealed disposable appliance; unit orchestration is never application evidence. */
export async function runMonitoringB2Recovery(
    context: MonitoringB2RecoveryFixture,
    scenario: MonitoringB2RecoveryScenario,
): Promise<MonitoringB2RecoveryResult> {
    const result: MonitoringB2RecoveryResult = {
        scenario,
        passed: false,
        evidence: {},
        failures: [],
    }
    const state = context.state(),
        authority = context.authority()
    if (
        process.platform !== 'linux' ||
        process.getuid?.() !== 0 ||
        process.env.RENTNERPROXY_MONITORING_B2 !== '1' ||
        !state ||
        state.design !== 'B2' ||
        state.originMs !== 0 ||
        state.hosts.length !== 1000 ||
        !authority ||
        !/^monitor_b2_[a-f0-9]{32}$/u.test(authority.namespace) ||
        context.outboxPath() !== '/tmp/monitoring-b2/state/' + state.windowId + '.sqlite'
    ) {
        result.failures.push('owned-prepared-B2-appliance-required')
        return result
    }
    let store: Database | undefined
    try {
        await replayControl('pause')
        store = open(context.outboxPath())
        const proof =
            scenario === 'gap'
                ? await proveGap(context, authority.namespace, store)
                : scenario === 'pressure'
                  ? await provePressure(context, store)
                  : scenario === 'fences'
                    ? await proveFences(context, authority.namespace)
                    : scenario === 'lease-expiry'
                      ? await proveLeaseExpiry(context, authority.namespace, store)
                      : await proveReplay(context, authority.namespace, store)
        Object.assign(result.evidence, proof.evidence)
        result.failures.push(...proof.failures)
        result.passed = result.failures.length === 0
    } catch (error) {
        result.evidence.diagnostic =
            error instanceof Error && /^[a-z0-9-]{1,80}$/u.test(error.message)
                ? error.message
                : 'fixture-operation-failed'
        result.failures.push('real-fixture-operation-failed-or-timed-out')
    } finally {
        store?.close()
        context.fastProbes(false)
        try {
            await context.stopNative()
            result.evidence.nativeStopped = true
        } catch {
            result.failures.push('native-stop-not-confirmed')
            result.passed = false
        }
        try {
            await replayControl('resume')
        } catch {
            result.failures.push('replay-resume-not-confirmed')
            result.passed = false
        }
    }
    return result
}
