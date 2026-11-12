// oxlint-disable no-await-in-loop -- Each fault must settle and restore its owned process identities before the next observation.
import { z } from 'zod'
import { monitoringEvaluationContract as contract } from './monitoring-evaluation.ts'
import type { ReliabilityContext } from '../runtime-reliability/Types/harness.types.ts'
import type {
    MonitoringApplianceFaultSession,
    MonitoringApplianceFaultRecord,
} from './Types/monitoring-whole-appliance.types.ts'

export const monitoringRecoveryScenarios = [
    'replay',
    'fences',
    'lease-expiry',
    'pressure',
    'gap',
    'automatic-publication',
] as const
export const monitoringApplianceFaultCases = [
    ...monitoringRecoveryScenarios,
    'web-term',
    'web-kill',
    'postgres-outage',
    'controller-restart',
    'fresh-volume-restore',
    'bun-stop',
] as const
const count = z.number().int().nonnegative()
const scalar = z.union([z.string(), z.number().finite(), z.boolean(), z.null()])
const resultSchema = z.strictObject({
    scenario: z.enum(monitoringRecoveryScenarios),
    passed: z.boolean(),
    evidence: z.record(z.string(), scalar),
    failures: z.array(z.string()),
})
const fenceKeys = [
    'freshAuthorityAdmitted',
    'foreignStoreRejected',
    'foreignEpochRejected',
    'nullIdentityClaimRejected',
    'unauthorizedWriteRejected',
    'withdrawnSqlGrantRejected',
    'restoredSqlGrantAdmitted',
    'committedConfigRejected',
    'unpublishedConfigClaimRejected',
    'oldGenerationRejectedAfterPublish',
    'freshGenerationAdmitted',
    'oldStoreEpochRejected',
    'resetRequiresPublication',
    'oldStoreEpochRejectedAfterPublish',
    'committedApplicationGrantRejected',
    'unpublishedApplicationGrantClaimRejected',
    'freshApplicationGrantAdmitted',
    'managementCapabilityRejected',
    'appSecretReadRejected',
    'controllerSecretReadRejected',
    'rootControlRejected',
    'webControlRejected',
    'probeTokenRejectedByController',
    'nativeGroupsRestricted',
] as const

/** A successful endpoint or boolean cannot fill missing numeric or authority evidence. */
export function assessMonitoringApplianceRecovery(
    value: unknown,
    scenario: (typeof monitoringRecoveryScenarios)[number],
) {
    let result: z.infer<typeof resultSchema>
    try {
        result = resultSchema.parse(value)
    } catch {
        return {
            scenario,
            state: 'incomplete' as const,
            failures: ['invalid-recovery-result'],
            evidence: null,
        }
    }
    if (result.scenario !== scenario)
        return {
            scenario,
            state: 'incomplete' as const,
            failures: ['recovery-scenario-mismatch'],
            evidence: result.evidence,
        }
    const failures = [...result.failures]
    let missing = false
    const numeric = (key: string) => {
        const parsed = count.safeParse(result.evidence[key])
        if (!parsed.success) {
            missing = true
            failures.push('unproven-' + key)
            return null
        }
        return parsed.data
    }
    const truth = (key: string) => {
        const actual = result.evidence[key]
        if (typeof actual !== 'boolean') {
            missing = true
            failures.push('unproven-' + key)
        } else if (!actual) failures.push(key + '-failed')
    }
    const exact = (key: string, expected: number) => {
        const actual = numeric(key)
        if (actual !== null && actual !== expected) failures.push(key + '-gate')
    }
    const minimum = (key: string, expected: number) => {
        const actual = numeric(key)
        if (actual !== null && actual < expected) failures.push(key + '-gate')
    }
    truth('nativeStopped')
    if (scenario === 'replay') {
        for (const key of [
            'records',
            'rawDurableRecords',
            'distinctRawPolicies',
            'nativeHeads',
            'distinctNativeHosts',
            'replayedResults',
            'observations',
        ])
            exact(key, contract.replayRecords)
        for (const key of [
            'pending',
            'rejectedApplied',
            'staleCommits',
            'duplicateObservations',
            'duplicateIncidents',
        ])
            exact(key, 0)
        minimum('duplicateRecords', 1)
        minimum('duplicateReceipts', 1)
        truth('noWakeReplayPassed')
        truth('duplicateReplayPassed')
        truth('conflictingReplayRejected')
        const elapsed = z.number().finite().nonnegative().safeParse(result.evidence.replayMs)
        if (!elapsed.success) {
            missing = true
            failures.push('unproven-replayMs')
        } else if (elapsed.data > contract.replayMs) failures.push('replay-duration-gate')
    } else if (scenario === 'fences') {
        for (const key of fenceKeys) truth(key)
        exact('unauthorizedWrites', 0)
        exact('nativeHeads', 0)
        exact('nativeUID', 20004)
        exact('nativeGID', 20004)
        exact('brokerManagementDenials', 5)
        exact('directManagementDenials', 5)
        exact('rootControlStatus', 403)
        exact('webControlStatus', 403)
        for (const key of [
            'nativeGroups',
            'appSecretErrno',
            'controllerSecretErrno',
            'rootCapErrno',
            'webCapErrno',
        ]) {
            const actual = result.evidence[key]
            if (typeof actual !== 'string') {
                missing = true
                failures.push('unproven-' + key)
            } else if (
                !(key === 'nativeGroups'
                    ? actual === '20004'
                    : ['EACCES', 'EPERM'].includes(actual))
            )
                failures.push(key + '-gate')
        }
    } else if (scenario === 'lease-expiry') {
        minimum('rawDurableRecords', 16)
        minimum('nativeHeads', 16)
        const rejected = numeric('rejected'),
            raw = numeric('rawDurableRecords')
        if (rejected !== null && raw !== null && rejected !== raw)
            failures.push('expired-raw-rejection-gate')
        for (const key of ['observations', 'rejectedApplied', 'staleCommits']) exact(key, 0)
        exact('finalLeaseSeconds', contract.leaseMs / 1000)
        exact('rawRetentionSeconds', contract.retentionMs / 1000)
        truth('retainedWithin120Seconds')
    } else if (scenario === 'pressure') {
        for (const key of ['syntheticPressureRecords', 'maxRecords'])
            exact(key, contract.maxOutboxRecords)
        for (const key of ['syntheticPayloadBytes', 'maxPayloadBytes'])
            exact(key, contract.maxOutboxBytes)
        exact('retentionMs', contract.retentionMs)
        exact('nativeReopens', 3)
        exact('capacityHeads', 0)
        exact('retentionHeads', 0)
        exact('retainedRecords', 1)
        minimum('recoveryHeads', 1)
        minimum('actualAdmitted', 1)
        truth('capacityPressureConfirmed')
        truth('retentionPressureConfirmed')
    } else if (scenario === 'automatic-publication') {
        for (const key of [
            'configurationWithdrawn',
            'actualFreshHead',
            'originalSingletonImports',
            'manualPausePreserved',
        ])
            truth(key)
        exact('withdrawnClaimRows', 0)
        minimum('automaticPublications', 2)
        for (const key of ['originalChunkSha256', 'fragmentSha256'])
            if (!/^[a-f0-9]{64}$/u.test(String(result.evidence[key] ?? ''))) {
                missing = true
                failures.push('unproven-' + key)
            }
    } else {
        minimum('actualRawRecordsBeforeLoss', 16)
        exact('intentionallyLostActualRecords', 1)
        minimum('gaps', 1)
        const before = numeric('headsBeforeRestart'),
            after = numeric('headsAfterRestart')
        if (before !== null && after !== null && before !== after)
            failures.push('gap-resumed-network-work')
        exact('observations', 0)
        exact('replayedResults', 0)
        truth('resyncRequired')
        truth('nativeRestarted')
        truth('automaticResumeRejected')
    }
    if (!result.passed) failures.push('fixture-recovery-failed')
    return {
        scenario,
        state: missing
            ? ('incomplete' as const)
            : failures.length
              ? ('failed' as const)
              : ('passed' as const),
        failures: [...new Set(failures)],
        evidence: result.evidence,
    }
}

/** Count the actual native executables, including an orphan that rootcontrol no longer tracks. */
export async function monitoringNativeProcessCensus() {
    const { readFile, readdir } = await import('node:fs/promises')
    const rows: { pid: number; startTicks: string; state: string }[] = []
    for (const entry of await readdir('/proc')) {
        if (!/^\d+$/u.test(entry)) continue
        try {
            const directory = '/proc/' + entry
            const args = (await readFile(directory + '/cmdline', 'utf8')).split('\0')
            if (args[0]?.split('/').at(-1) !== 'monitoring-b2') continue
            const stat = await readFile(directory + '/stat', 'utf8')
            const fields = stat
                .slice(stat.lastIndexOf(')') + 2)
                .trim()
                .split(/\s+/u)
            if (!/^\d+$/u.test(fields[19] ?? '')) throw new Error('B2 native identity unavailable')
            rows.push({ pid: Number(entry), startTicks: fields[19]!, state: fields[0]! })
        } catch (error) {
            if (
                error &&
                typeof error === 'object' &&
                'code' in error &&
                ['ENOENT', 'ESRCH'].includes(String(error.code))
            )
                continue
            throw error
        }
    }
    return rows
}

/** Read the actual isolated native process census; malformed process proof fails the fault case. */
export async function readMonitoringNativeProcesses(context: ReliabilityContext) {
    return z
        .array(
            z.strictObject({
                pid: count.positive(),
                startTicks: z.string().regex(/^\d+$/u),
                state: z.string().length(1),
            }),
        )
        .parse(
            JSON.parse(
                await context.docker(
                    [
                        'exec',
                        context.container,
                        'bun',
                        '--no-env-file',
                        '-e',
                        `process.stdout.write(JSON.stringify(await (${monitoringNativeProcessCensus.toString()})()));`,
                    ],
                    { timeoutMs: 5000 },
                ),
            ),
        )
}

/** Execute a brief actual PostgreSQL STOP under an independent resumption guard. */
async function pauseMonitoringPostgres() {
    const { readdir, readFile } = await import('node:fs/promises')
    const targets: { pid: number; ticks: string; uid: string }[] = []
    for (const pid of await readdir('/proc')) {
        if (!/^\d+$/u.test(pid)) continue
        try {
            if ((await readFile('/proc/' + pid + '/comm', 'utf8')).trim() !== 'postgres') continue
            const stat = await readFile('/proc/' + pid + '/stat', 'utf8'),
                status = await readFile('/proc/' + pid + '/status', 'utf8')
            const ticks = stat
                    .slice(stat.lastIndexOf(')') + 2)
                    .trim()
                    .split(/\s+/u)[19],
                uid = /^Uid:[\t ]+(\d+)/mu.exec(status)?.[1]
            if (!ticks || !uid) throw new Error('B2 PostgreSQL identity incomplete')
            targets.push({ pid: Number(pid), ticks, uid })
        } catch (error) {
            if (
                error &&
                typeof error === 'object' &&
                'code' in error &&
                ['ENOENT', 'ESRCH'].includes(String(error.code))
            )
                continue
            throw error
        }
    }
    if (!targets.length) throw new Error('B2 PostgreSQL fault had no target')
    /** Signal only retained PostgreSQL PID/start identities and confirm their bounded observed stop/resume. */
    async function signal(rows: typeof targets, kind: 'SIGSTOP' | 'SIGCONT', deadline = 0) {
        const { readFile: readProcFile } = await import('node:fs/promises')
        let signaled = 0
        for (const row of rows) {
            try {
                const directory = '/proc/' + row.pid
                const stat = await readProcFile(directory + '/stat', 'utf8'),
                    status = await readProcFile(directory + '/status', 'utf8')
                if (
                    stat
                        .slice(stat.lastIndexOf(')') + 2)
                        .trim()
                        .split(/\s+/u)[19] !== row.ticks ||
                    /^Uid:[\t ]+(\d+)/mu.exec(status)?.[1] !== row.uid ||
                    (await readProcFile(directory + '/comm', 'utf8')).trim() !== 'postgres'
                )
                    continue
                if (kind === 'SIGSTOP' && deadline - Date.now() < 4500)
                    throw new Error('B2 PG guard headroom exhausted')
                process.kill(row.pid, kind)
                signaled++
            } catch (error) {
                if (
                    error &&
                    typeof error === 'object' &&
                    'code' in error &&
                    ['ENOENT', 'ESRCH'].includes(String(error.code))
                )
                    continue
                throw error
            }
        }
        return signaled
    }
    const guard = Bun.spawn(
        [
            process.execPath,
            '--no-env-file',
            '-e',
            `const deadline=Date.now()+7000;process.stdout.write('ready:'+deadline+'\\n');await Bun.sleep(Math.max(0,deadline-Date.now()));await (${signal.toString()})(${JSON.stringify(targets)},'SIGCONT')`,
        ],
        { stdout: 'pipe', stderr: 'ignore' },
    )
    const reader = guard.stdout.getReader()
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
        const acknowledgment = async () => {
            const decoder = new TextDecoder()
            let text = ''
            while (text.length < 32 && !text.includes('\n')) {
                const next = await reader.read()
                if (next.done) break
                text += decoder.decode(next.value, { stream: true })
            }
            return text
        }
        const ready = /^ready:(\d{13})\n$/u.exec(
            await Promise.race([
                acknowledgment(),
                new Promise<never>((_, reject) => {
                    timer = setTimeout(() => reject(new Error('B2 PG guard not ready')), 1000)
                }),
            ]),
        )
        if (!ready) throw new Error('B2 PG guard rejected')
        const deadline = Number(ready[1])
        const paused = await signal(targets, 'SIGSTOP', deadline)
        if (paused !== targets.length) throw new Error('B2 PG pause identity mismatch')
        for (const row of targets) {
            const stat = await readFile('/proc/' + row.pid + '/stat', 'utf8')
            if (
                !['T', 't'].includes(
                    stat
                        .slice(stat.lastIndexOf(')') + 2)
                        .trim()
                        .split(/\s+/u)[0]!,
                )
            )
                throw new Error('B2 PostgreSQL STOP not confirmed')
        }
        if (deadline - Date.now() < 4250)
            throw new Error('B2 PostgreSQL confirmed pause has insufficient guard budget')
        const stoppedAtMs = Date.now()
        await Bun.sleep(4000)
        const resumed = await signal(targets, 'SIGCONT')
        return { paused, resumed, stoppedAtMs, resumedAtMs: Date.now() }
    } finally {
        clearTimeout(timer)
        reader.releaseLock()
        await signal(targets, 'SIGCONT')
        guard.kill('SIGKILL')
        await guard.exited
    }
}

/** Suspend only identified fixture PostgreSQL processes and require bounded restoration after the fault. */
export async function exerciseMonitoringPostgresPause(context: ReliabilityContext) {
    return z
        .strictObject({
            paused: count.positive(),
            resumed: count.positive(),
            stoppedAtMs: count,
            resumedAtMs: count,
        })
        .parse(
            JSON.parse(
                await context.docker(
                    [
                        'exec',
                        context.container,
                        'bun',
                        '--no-env-file',
                        '-e',
                        `process.stdout.write(JSON.stringify(await (${pauseMonitoringPostgres.toString()})()));`,
                    ],
                    { timeoutMs: 10000 },
                ),
            ),
        )
}

const identitySchema = z.looseObject({
    pid: count.positive(),
    startTicks: z.string().regex(/^\d+$/u),
})
const ownerStatusSchema = z.looseObject({
    owner: z
        .looseObject({
            design: z.enum(['A', 'B2']),
            runId: z.string(),
            storeId: z.string(),
            controllerEpoch: z.string(),
            applianceStartTicks: z.string(),
            restoredPaused: z.boolean(),
        })
        .nullable(),
    nativeIdentity: identitySchema.nullable(),
    nativeCount: count,
    replayAttached: z.boolean(),
})

/** Reads a repeatable, owned history snapshot; only its digest and counts leave the isolated appliance. */
async function sampleMonitoringHistory() {
    const { SQL } = await import('bun'),
        { createHash } = await import('node:crypto'),
        { readFile } = await import('node:fs/promises'),
        { z: validator } = await import('zod')
    if (process.platform !== 'linux')
        throw new Error('B2 history requires its isolated Linux appliance')
    const url = (await readFile('/run/rentnerproxy/database-url/value', 'utf8')).trim(),
        parsed = new URL(url)
    if (
        parsed.hostname !== '127.0.0.1' ||
        parsed.pathname !== '/rentnerproxy' ||
        !['postgres:', 'postgresql:'].includes(parsed.protocol)
    )
        throw new Error('B2 history database guard')
    const database = new SQL(url, {
        max: 1,
        connectionTimeout: 3,
        connection: { statement_timeout: 2000, lock_timeout: 2000 },
    })
    try {
        return await database.begin(async (transaction) => {
            await transaction.unsafe('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            const ownerRows = await transaction.unsafe<{ value: unknown }[]>(
                'SELECT value FROM rentnerproxy.system_settings WHERE key=$1',
                ['monitoring_b2_experiment_owner_v1'],
            )
            const owner = validator
                .object({
                    namespace: validator.string().regex(/^monitor_b2_[a-f0-9]{32}$/u),
                    hosts: validator
                        .array(validator.object({ policyId: validator.uuid() }))
                        .length(1000),
                })
                .parse(ownerRows[0]?.value)
            const ids = owner.hosts.map((host) => host.policyId)
            if (new Set(ids).size !== 1000)
                throw new Error('B2 history ownership list is not distinct')
            const observations = await transaction.unsafe(
                'SELECT * FROM rentnerproxy.host_check_observations WHERE policy_id=ANY($1::uuid[]) ORDER BY id',
                [ids],
            )
            const incidents = await transaction.unsafe(
                'SELECT * FROM rentnerproxy.host_check_incidents WHERE policy_id=ANY($1::uuid[]) ORDER BY id',
                [ids],
            )
            const receipts = await transaction.unsafe(
                `SELECT * FROM ${owner.namespace}.receipts WHERE policy_id=ANY($1::uuid[]) ORDER BY attempt_id`,
                [ids],
            )
            return {
                digest: createHash('sha256')
                    .update(JSON.stringify({ observations, incidents, receipts }))
                    .digest('hex'),
                observations: observations.length,
                incidents: incidents.length,
                receipts: receipts.length,
                policies: ids.length,
            }
        })
    } finally {
        await database.close()
    }
}

/** Read durable application history from the owned appliance rather than inferring commits from RPC success. */
export async function readMonitoringApplianceHistory(context: ReliabilityContext) {
    return z
        .strictObject({
            digest: z.string().regex(/^[a-f0-9]{64}$/u),
            observations: count,
            incidents: count,
            receipts: count,
            policies: z.literal(1000),
        })
        .parse(
            JSON.parse(
                await context.docker(
                    [
                        'exec',
                        context.container,
                        'bun',
                        '--no-env-file',
                        '-e',
                        `process.stdout.write(JSON.stringify(await (${sampleMonitoringHistory.toString()})()));`,
                    ],
                    { timeoutMs: 10000 },
                ),
            ),
        )
}
/** Poll parsed fault evidence to a bounded deadline, preserving validation failures as failed proof. */
async function waitForMonitoringFault<T>(
    read: () => Promise<T>,
    accept: (value: T) => boolean,
    timeoutMs = 30000,
): Promise<T> {
    const deadline = performance.now() + timeoutMs
    do {
        const value = await read()
        if (accept(value) && performance.now() <= deadline) return value
        await Bun.sleep(250)
    } while (performance.now() < deadline)
    throw new Error('B2 actual fault progress deadline')
}

/** Every case uses a fresh sealed appliance; domain callbacks and OS faults retain distinct evidence. */
export async function runMonitoringApplianceFaultCase(
    session: MonitoringApplianceFaultSession,
    name: (typeof monitoringApplianceFaultCases)[number],
): Promise<MonitoringApplianceFaultRecord> {
    if (monitoringRecoveryScenarios.some((scenario) => scenario === name)) {
        const scenario = z.enum(monitoringRecoveryScenarios).parse(name)
        z.object({ started: z.literal(true), scenario: z.literal(scenario) }).parse(
            await session.request('/recovery', { scenario }),
        )
        const result = await waitForMonitoringFault(
            async () =>
                z
                    .object({ running: z.boolean(), result: z.unknown() })
                    .parse(await session.request('/recovery')),
            (value) => !value.running && value.result !== null,
            100000,
        )
        const assessed = assessMonitoringApplianceRecovery(result.result, scenario)
        if ((await readMonitoringNativeProcesses(session.context)).length !== 0) {
            assessed.failures.push('recovery-retained-untracked-native-writer')
            assessed.state = 'failed'
        }
        return { name, ...assessed }
    }
    if (name === 'bun-stop') {
        await session.start()
        const result = await session.continuity()
        return {
            name,
            state: result.failures.length ? ('failed' as const) : ('passed' as const),
            evidence: result,
            failures: result.failures,
        }
    }
    await session.start()
    const before = await waitForMonitoringFault(session.report, (value) => value.heads.length >= 16)
    const ownerBefore = ownerStatusSchema.parse(await session.request('/owner-status'))
    if (
        !ownerBefore.owner ||
        ownerBefore.owner.design !== 'B2' ||
        !ownerBefore.nativeIdentity ||
        ownerBefore.nativeCount !== 1
    )
        throw new Error('B2 process fault lacks actual owner identity')
    const failures: string[] = []
    if (name === 'web-term' || name === 'web-kill') {
        await session.recover('web', name === 'web-kill' ? 'SIGKILL' : 'SIGTERM')
        const ownerAfter = ownerStatusSchema.parse(await session.request('/owner-status'))
        if (
            JSON.stringify(ownerBefore.nativeIdentity) !==
                JSON.stringify(ownerAfter.nativeIdentity) ||
            !ownerAfter.replayAttached ||
            ownerBefore.owner.applianceStartTicks !== ownerAfter.owner?.applianceStartTicks
        )
            failures.push('web-recovery-native-owner-or-replay-changed')
        const after = await waitForMonitoringFault(
            session.report,
            (value) =>
                value.heads.length >= before.heads.length + 16 &&
                value.observations.length >= before.observations.length + 16,
        )
        if (
            new Set(
                after.observations.map(
                    (row) =>
                        row.policyId + ':' + Math.floor((row.checkedAtMs - after.originMs) / 60000),
                ),
            ).size !== after.observations.length
        )
            failures.push('duplicate-observation-during-cadence-recovery')
        if (JSON.stringify(before.controllerIdentity) !== JSON.stringify(after.controllerIdentity))
            failures.push('web-fault-changed-controller')
        return {
            name,
            state: failures.length ? ('failed' as const) : ('passed' as const),
            failures,
            evidence: {
                beforeHeads: before.heads.length,
                afterHeads: after.heads.length,
                beforeObservations: before.observations.length,
                afterObservations: after.observations.length,
                ownerBefore,
                ownerAfter,
            },
        }
    }
    if (name === 'postgres-outage') {
        const pause = await exerciseMonitoringPostgresPause(session.context)
        if (pause.resumedAtMs - pause.stoppedAtMs < 4000 || pause.paused !== pause.resumed)
            failures.push('actual-postgres-pause-unproven')
        identitySchema.parse(
            z
                .object({ nativeIdentity: identitySchema })
                .parse(await session.request('/native-restart', {})).nativeIdentity,
        )
        const after = await waitForMonitoringFault(
            session.report,
            (value) =>
                value.heads.length > before.heads.length &&
                value.observations.length > before.observations.length,
        )
        const admissionsDuringPause = after.starts.filter(
            (start) =>
                start.admittedAtMs >= pause.stoppedAtMs && start.admittedAtMs < pause.resumedAtMs,
        ).length
        if (admissionsDuringPause !== 0) failures.push('postgres-outage-admitted-new-work')
        if (JSON.stringify(before.controllerIdentity) !== JSON.stringify(after.controllerIdentity))
            failures.push('postgres-outage-changed-controller')
        return {
            name,
            state: failures.length ? ('failed' as const) : ('passed' as const),
            failures,
            evidence: {
                pause,
                admissionsDuringPause,
                beforeHeads: before.heads.length,
                afterHeads: after.heads.length,
                committedAfterRecovery: after.observations.length - before.observations.length,
            },
        }
    }
    let quiescence: unknown = null
    let historyBefore: Awaited<ReturnType<typeof readMonitoringApplianceHistory>> | null = null
    if (name === 'fresh-volume-restore') {
        await waitForMonitoringFault(
            () => readMonitoringApplianceHistory(session.context),
            (value) => value.observations >= 16 && value.incidents >= 1 && value.receipts >= 16,
            150000,
        )
        quiescence = z
            .object({
                quiesced: z.literal(true),
                nativeStopped: z.literal(true),
                activeNativeHeads: z.literal(0),
            })
            .parse(await session.request('/quiesce', {}))
        if ((await readMonitoringNativeProcesses(session.context)).length !== 0)
            throw new Error('B2 quiescence retained an untracked native writer')
        historyBefore = await readMonitoringApplianceHistory(session.context)
        await session.backupRestore()
    } else await session.recover('controller')
    await session.startControl()
    const ownerRestored = ownerStatusSchema.parse(await session.request('/owner-status'))
    const actualNative = await readMonitoringNativeProcesses(session.context)
    const historyAfter =
        name === 'fresh-volume-restore'
            ? await readMonitoringApplianceHistory(session.context)
            : null
    if (historyBefore && JSON.stringify(historyBefore) !== JSON.stringify(historyAfter))
        failures.push('restored-monitoring-history-or-receipts-changed')
    const proof = z
        .object({
            design: z.literal('B2'),
            originalStartCalls: z.literal(0),
            suppressedStartCalls: z.literal(1),
            restoredPaused: z.literal(true),
        })
        .parse(await session.request('/web-proof'))
    if (
        !ownerRestored.owner?.restoredPaused ||
        ownerRestored.owner.design !== 'B2' ||
        ownerRestored.replayAttached ||
        ownerRestored.nativeIdentity !== null ||
        ownerRestored.nativeCount !== 0 ||
        actualNative.length !== 0 ||
        ownerRestored.owner.storeId === ownerBefore.owner.storeId ||
        ownerRestored.owner.controllerEpoch === ownerBefore.owner.controllerEpoch ||
        ownerRestored.owner.applianceStartTicks === ownerBefore.owner.applianceStartTicks
    )
        failures.push('restored-owner-fence-or-no-autostart-unproven')
    const rebootstrap = z
        .object({
            rebootstrap: z.literal(true),
            restoredPaused: z.literal(true),
            oldWriterClaimRows: z.literal(0),
            removedPolicies: z.literal(1000),
            remainingBaselineHosts: z.literal(2),
            nativeIdentity: z.null(),
        })
        .parse(
            await session.request('/rebootstrap', {
                expectedOldStoreId: ownerBefore.owner.storeId,
                expectedOldEpoch: ownerBefore.owner.controllerEpoch,
            }),
        )
    await session.prepareAfterRebootstrap()
    await session.start()
    const after = await waitForMonitoringFault(
        session.report,
        (value) => value.heads.length >= 16 && value.observations.length >= 16,
    )
    return {
        name,
        state: failures.length ? ('failed' as const) : ('passed' as const),
        failures,
        evidence: {
            quiescence,
            historyBefore,
            historyAfter,
            ownerBefore,
            ownerRestored,
            proof,
            actualNative,
            rebootstrap,
            fixtureReseedAfterHistoryVerification: true,
            freshHeads: after.heads.length,
            freshObservations: after.observations.length,
        },
    }
}
