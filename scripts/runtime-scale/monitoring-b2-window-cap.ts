import { SQL } from 'bun'
import { createHash, randomBytes } from 'node:crypto'
import { lstat, readFile, writeFile, rename, rm } from 'node:fs/promises'
import { z } from 'zod'
import { monitoringB2DatabaseUrl } from './monitoring-b2-isolation.ts'
import type {
    MonitoringB2WindowCapScope,
    MonitoringB2WindowCapProof,
    MonitoringB2WindowBackendExitDependencies,
} from './Types/monitoring-b2-window-cap.types.ts'

const selectors = z
    .array(z.uuid())
    .length(1000)
    .refine((ids) => new Set(ids).size === 1000)

/** Allow PostgreSQL to finish socket termination, but reject a backend retained for two seconds. */
export async function waitForMonitoringB2WindowBackendExit(
    dependencies: MonitoringB2WindowBackendExitDependencies,
) {
    const deadline = dependencies.monotonic() + 2000,
        controller = new AbortController(),
        failure = new Error('B2 readiness observer backend retained')
    let timer: ReturnType<typeof setTimeout> | undefined
    const expired = new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
            reject(failure)
            controller.abort(failure)
        }, 2000)
    })
    try {
        // oxlint-disable no-await-in-loop -- Backend exit is asynchronous; both observations and pauses share the same deadline.
        while (await Promise.race([dependencies.present(controller.signal), expired])) {
            if (dependencies.monotonic() >= deadline) throw failure
            await Promise.race([dependencies.sleep(10), expired])
        }
        // oxlint-enable no-await-in-loop
    } finally {
        clearTimeout(timer)
    }
}

/** Cancel only this owned PostgreSQL statement when the exit deadline expires, then detach its listener on settlement. */
export async function observeMonitoringB2WindowBackendQuery(
    query: SQL.Query<unknown[]>,
    signal: AbortSignal,
) {
    const cancel = () => {
        query.cancel()
    }
    signal.addEventListener('abort', cancel, { once: true })
    try {
        if (signal.aborted) cancel()
        const rows = await query
        signal.throwIfAborted()
        return rows.length !== 0
    } finally {
        signal.removeEventListener('abort', cancel)
    }
}

/** Close the private observer and prove its PostgreSQL backend exits before measurement. */
async function closeWindowObserver(observer: SQL, database: SQL, observerPid: number | undefined) {
    await observer.close()
    if (observerPid !== undefined) {
        await waitForMonitoringB2WindowBackendExit({
            present: (signal) =>
                observeMonitoringB2WindowBackendQuery(
                    database.unsafe('SELECT pid FROM pg_stat_activity WHERE pid=$1', [observerPid]),
                    signal,
                ),
            monotonic: () => performance.now(),
            sleep: Bun.sleep,
        })
    }
}

/** Observe readiness as the restricted native role, then prove the observer backend has closed. */
export async function qualifyMonitoringB2WindowSession(
    database: SQL,
    workerUrl: string,
    expectedQuery: string,
    ownerReady: () => Promise<boolean>,
) {
    const observer = new SQL(workerUrl, { max: 1, connectionTimeout: 3 })
    let observerPid: number | undefined
    try {
        const [self] = await observer.unsafe<{ pid: number }[]>('SELECT pg_backend_pid() pid')
        if (!self) throw new Error('B2 readiness observer identity absent')
        observerPid = self.pid
        const until = Date.now() + 10000
        // oxlint-disable no-await-in-loop -- Readiness samples must settle before the next bounded observation.
        while (Date.now() < until) {
            const rows = await observer.unsafe<{ pid: number; state: string; query: string }[]>(
                'SELECT pid,state,query FROM pg_stat_activity WHERE usename=current_user AND datname=current_database() AND pid<>pg_backend_pid()',
            )
            if (
                rows.length === 1 &&
                rows[0]!.state === 'idle' &&
                rows[0]!.query === expectedQuery &&
                (await ownerReady())
            )
                return
            await Bun.sleep(25)
        }
        // oxlint-enable no-await-in-loop
        throw new Error('B2 fresh measurement owner readiness unproven')
    } finally {
        await closeWindowObserver(observer, database, observerPid)
    }
}

/** Seal the exact unordered policy set, independently of SQL parameter serialization. */
export function monitoringB2WindowPolicyHash(ids: readonly string[]) {
    return createHash('sha256').update(selectors.parse(ids).toSorted().join('\n')).digest('hex')
}

/** Validate the unchanged readable boot-mode seal before private measurement setup. */
async function assertMode(scope: MonitoringB2WindowCapScope) {
    if (
        !/^[a-f0-9]{12}$/u.test(scope.runId) ||
        !/^[a-z0-9-]{1,40}$/u.test(scope.windowId) ||
        process.env.RENTNERPROXY_RELIABILITY_ISOLATED !== scope.runId ||
        process.env.B2_APPLIANCE_FIXTURE !== scope.runId
    )
        throw new Error('B2 finite workload run seal mismatch')
    await monitoringB2DatabaseUrl()
    const directory = await lstat('/tmp/monitoring-b2'),
        metadata = await lstat('/tmp/monitoring-b2/mode.json')
    if (
        !directory.isDirectory() ||
        directory.uid !== 0 ||
        (directory.mode & 0o022) !== 0 ||
        !metadata.isFile() ||
        metadata.uid !== 0 ||
        metadata.gid !== 0 ||
        metadata.nlink !== 1 ||
        (metadata.mode & 0o022) !== 0 ||
        metadata.size > 4096
    )
        throw new Error('B2 finite workload boot seal invalid')
    const value = JSON.parse(await readFile('/tmp/monitoring-b2/mode.json', 'utf8'))
    if (value.runId !== scope.runId || value.design !== scope.design)
        throw new Error('B2 finite workload owner mismatch')
}

/** Atomically create the separate root-only opt-in without changing web-readable mode.json. */
export async function writeMonitoringB2WindowSeal(scope: MonitoringB2WindowCapScope) {
    if (scope.enabled !== true) throw new Error('B2 finite workload opt-in required')
    await assertMode(scope)
    const path = '/tmp/monitoring-b2/measurement.json',
        temporary = path + '.' + randomBytes(6).toString('hex') + '.next'
    const value = {
        runId: scope.runId,
        design: scope.design,
        windowId: scope.windowId,
        finiteWindow: true,
        policySetSha256: monitoringB2WindowPolicyHash(scope.policyIds),
    }
    try {
        await writeFile(temporary, JSON.stringify(value), { mode: 0o600, flag: 'wx' })
        await rename(temporary, path)
    } finally {
        await rm(temporary, { force: true })
    }
}

/** Require the independent root-only measurement flag and exact sealed policy selectors. */
async function assertSeal(scope: MonitoringB2WindowCapScope) {
    await assertMode(scope)
    const path = '/tmp/monitoring-b2/measurement.json',
        metadata = await lstat(path)
    if (
        !metadata.isFile() ||
        metadata.uid !== 0 ||
        metadata.gid !== 0 ||
        metadata.nlink !== 1 ||
        (metadata.mode & 0o777) !== 0o600 ||
        metadata.size > 4096
    )
        throw new Error('B2 finite workload file seal invalid')
    const value = JSON.parse(await readFile(path, 'utf8'))
    if (
        value.runId !== scope.runId ||
        value.design !== scope.design ||
        value.finiteWindow !== true ||
        value.windowId !== scope.windowId ||
        value.policySetSha256 !== monitoringB2WindowPolicyHash(scope.policyIds)
    )
        throw new Error('B2 finite workload selectors mismatch')
}

/** Install a disposable, scoped next-due boundary only after explicit root-sealed opt-in. */
export async function createMonitoringB2WindowCap(
    database: SQL,
    scope: MonitoringB2WindowCapScope,
) {
    if (scope.enabled !== true) return undefined
    await assertSeal(scope)
    const ids = selectors.parse(scope.policyIds),
        namespace = 'monitor_b2_window_' + scope.runId
    const policySetSha256 = monitoringB2WindowPolicyHash(ids)
    await database.begin(async (transaction) => {
        const [owned] = await transaction.unsafe<{ count: number }[]>(
            'SELECT count(*)::integer count FROM rentnerproxy.host_check_policies WHERE id=ANY($1::uuid[]) AND NOT enabled AND lease_token IS NULL AND next_check_at>clock_timestamp()',
            [ids],
        )
        if (owned?.count !== 1000)
            throw new Error('B2 finite workload requires exact idle policies')
        await transaction.unsafe(`CREATE SCHEMA ${namespace}; REVOKE ALL ON SCHEMA ${namespace} FROM PUBLIC;
CREATE TABLE ${namespace}.scope(policy_id uuid PRIMARY KEY);
CREATE TABLE ${namespace}.horizon(id integer PRIMARY KEY CHECK(id=1),run_id text NOT NULL,window_id text NOT NULL,origin_ms bigint,end_ms bigint,deferred_ms bigint);
REVOKE ALL ON ALL TABLES IN SCHEMA ${namespace} FROM PUBLIC;`)
        await transaction.unsafe(`INSERT INTO ${namespace}.scope SELECT unnest($1::uuid[])`, [ids])
        await transaction.unsafe(
            `INSERT INTO ${namespace}.horizon(id,run_id,window_id) VALUES(1,$1,$2)`,
            [scope.runId, scope.windowId],
        )
        await transaction.unsafe(`CREATE FUNCTION ${namespace}.limit_due() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,${namespace},pg_temp AS $$
DECLARE boundary bigint; deferred bigint;
BEGIN
IF EXISTS(SELECT 1 FROM ${namespace}.scope WHERE policy_id=NEW.id) THEN
 SELECT end_ms,deferred_ms INTO boundary,deferred FROM ${namespace}.horizon WHERE id=1 AND origin_ms IS NOT NULL;
 IF FOUND AND NEW.next_check_at>=to_timestamp(boundary::double precision/1000) THEN
  NEW.next_check_at:=greatest(NEW.next_check_at,to_timestamp(deferred::double precision/1000));
 END IF;
END IF;
RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION ${namespace}.limit_due() FROM PUBLIC;
CREATE TRIGGER ${namespace} BEFORE UPDATE OF next_check_at ON rentnerproxy.host_check_policies FOR EACH ROW EXECUTE FUNCTION ${namespace}.limit_due();`)
    })
    let dropped = false
    /** Query the installed trigger, exact UUID set and real horizon before claiming a finite workload. */
    async function proof(): Promise<MonitoringB2WindowCapProof> {
        if (dropped) throw new Error('B2 finite workload already removed')
        await assertSeal(scope)
        const [row] = await database.unsafe<
            {
                run_id: string
                window_id: string
                origin_ms: string | null
                end_ms: string | null
                deferred_ms: string | null
                policies: number
                matched: number
                foreign_ids: number
                installed: boolean
            }[]
        >(
            `SELECT h.*,
 (SELECT count(*)::integer FROM ${namespace}.scope) policies,
 (SELECT count(*)::integer FROM ${namespace}.scope s JOIN rentnerproxy.host_check_policies p ON p.id=s.policy_id) matched,
 (SELECT count(*)::integer FROM ${namespace}.scope WHERE NOT(policy_id=ANY($1::uuid[]))) foreign_ids,
 EXISTS(SELECT 1 FROM pg_trigger WHERE tgname=$2 AND tgrelid='rentnerproxy.host_check_policies'::regclass AND tgfoid='${namespace}.limit_due()'::regprocedure AND tgenabled='O' AND NOT tgisinternal) installed
 FROM ${namespace}.horizon h WHERE id=1`,
            [ids, namespace],
        )
        if (
            !row ||
            row.run_id !== scope.runId ||
            row.window_id !== scope.windowId ||
            row.policies !== 1000 ||
            row.matched !== 1000 ||
            row.foreign_ids !== 0 ||
            !row.installed
        )
            throw new Error('B2 finite workload installed scope unproven')
        return {
            runId: scope.runId,
            windowId: scope.windowId,
            policySetSha256,
            policies: row.policies,
            installed: row.installed,
            originMs: row.origin_ms === null ? null : Number(row.origin_ms),
            endMs: row.end_ms === null ? null : Number(row.end_ms),
            deferredMs: row.deferred_ms === null ? null : Number(row.deferred_ms),
        }
    }
    return {
        proof,
        /** Arm once before policy enablement, retaining the original first and second due times. */
        arm: async (originMs: number) => {
            if (!Number.isSafeInteger(originMs) || originMs <= Date.now())
                throw new Error('B2 finite workload origin is not future')
            await proof()
            const changed = await database.unsafe(
                `UPDATE ${namespace}.horizon SET origin_ms=$1,end_ms=$2,deferred_ms=$3 WHERE id=1 AND origin_ms IS NULL RETURNING id`,
                [originMs, originMs + 120000, originMs + 180000],
            )
            if (changed.length !== 1) throw new Error('B2 finite workload horizon already armed')
            return proof()
        },
        /** Remove only this verified private trigger/schema before the next fixture is seeded. */
        drop: async () => {
            if (dropped) return
            await proof()
            await database.begin(async (transaction) => {
                await transaction.unsafe(
                    `DROP TRIGGER ${namespace} ON rentnerproxy.host_check_policies; DROP SCHEMA ${namespace} CASCADE`,
                )
            })
            dropped = true
        },
    }
}
