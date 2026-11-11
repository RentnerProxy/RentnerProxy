// oxlint-disable no-await-in-loop -- Configuration commits, withdrawn authority and observed native resumption are ordered boundaries.
import { SQL } from 'bun'
import { readFile } from 'node:fs/promises'
import type { MonitoringB2RecoveryContext } from './Types/monitoring-b2.types.ts'
import { monitoringB2WebKey } from './monitoring-b2-capabilities.ts'

export interface MonitoringB2PublicationResult {
    scenario: 'automatic-publication'
    passed: boolean
    evidence: Record<string, number | string | boolean | null>
    failures: string[]
}

/** Ask the existing web replay owner to pause/resume and require its explicit state acknowledgement. */
async function replay(action: 'pause' | 'resume') {
    const response = await fetch('http://127.0.0.1:9022/' + action, {
        method: 'POST',
        headers: { authorization: 'Bearer ' + (await monitoringB2WebKey()) },
        signal: AbortSignal.timeout(5000),
    })
    if (
        !response.ok ||
        ((await response.json()) as { paused?: boolean }).paused !== (action === 'pause')
    )
        throw new Error('publication-replay-control-failed')
}
/** Poll fixture observations within the caller's short budget; absence of proof is a failure. */
async function until(predicate: () => Promise<boolean> | boolean, budgetMs = 10000) {
    const deadline = Date.now() + budgetMs
    while (Date.now() < deadline) {
        if (await predicate()) return
        await Bun.sleep(25)
    }
    throw new Error('publication-observation-timeout')
}

/** Actual configured-host mutation, automatic canonical Bun publication and native HTTP resumption. */
export async function runMonitoringB2PublicationCase(
    context: MonitoringB2RecoveryContext,
): Promise<MonitoringB2PublicationResult> {
    const result: MonitoringB2PublicationResult = {
        scenario: 'automatic-publication',
        passed: false,
        evidence: { nativeStopped: false },
        failures: [],
    }
    const state = context.state(),
        authority = context.authority(),
        configureHost = context.configureHost
    if (
        process.platform !== 'linux' ||
        process.getuid?.() !== 0 ||
        state?.design !== 'B2' ||
        state.originMs !== 0 ||
        !authority ||
        !configureHost
    ) {
        result.failures.push('owned-prepared-B2-required')
        return result
    }
    const target = state.hosts[0]!
    const identity = async () => {
        const [row] = await context.database.unsafe<
            {
                generation: string
                published: string
                paused: boolean
                resync: boolean
                contexts: number
            }[]
        >(
            `SELECT generation::text,published_generation::text published,paused,resync_required resync,(SELECT count(*)::integer FROM ${authority.namespace}.contexts) contexts FROM ${authority.namespace}.identity WHERE id=1`,
        )
        if (!row) throw new Error('publication-identity-missing')
        return row
    }
    const observations = async () => {
        const [row] = await context.database.unsafe<{ count: number }[]>(
            'SELECT count(*)::integer count FROM rentnerproxy.host_check_observations WHERE policy_id=$1',
            [target.policyId],
        )
        return row?.count ?? 0
    }
    let worker: SQL | undefined
    let phase = 'initial-probe'
    try {
        context.fastProbes(true)
        await context.start()
        await until(async () => (await observations()) >= 1)
        await replay('pause')
        const before = await identity()
        await configureHost(false)
        phase = 'withdrawn'
        const withdrawn = await identity()
        worker = new SQL(authority.workerUrl, { max: 1, connectionTimeout: 2 })
        const denied = await worker.unsafe(`SELECT * FROM ${authority.namespace}.claim(1,$1,$2)`, [
            authority.storeId,
            authority.controllerEpoch,
        ])
        result.evidence.withdrawnClaimRows = denied.length
        result.evidence.configurationWithdrawn =
            BigInt(withdrawn.generation) > BigInt(before.generation) &&
            withdrawn.resync &&
            withdrawn.published !== withdrawn.generation
        await replay('resume')
        phase = 'disabled-publication'
        await until(async () => {
            const row = await identity()
            return (
                !row.paused &&
                !row.resync &&
                row.published === row.generation &&
                row.contexts === 999
            )
        })
        const disabled = await identity()
        phase = 'reenable'
        await replay('pause')
        const enabled = await configureHost(true)
        await replay('resume')
        phase = 'fresh-probe'
        // The re-due target follows the original overdue queue: 63 batches at 250ms for 1,000 policies.
        // Reserve 4.25s for canonical publication and final replay; capacity/replay gates stay unchanged.
        const freshProbeBudgetMs = Math.ceil(state.hosts.length / 16) * 250 + 4250
        result.evidence.freshProbeBudgetMs = freshProbeBudgetMs
        await until(async () => {
            const row = await identity()
            return (
                !row.paused &&
                !row.resync &&
                row.published === row.generation &&
                row.contexts === 1000 &&
                context
                    .heads()
                    .some(
                        (head) => head.hostId === target.hostId && head.atMs >= enabled.changedAtMs,
                    ) &&
                (await observations()) >= 2
            )
        }, freshProbeBudgetMs)
        const recovered = await identity()
        const proof = JSON.parse(
            await readFile('/tmp/monitoring-b2/state/replay-proof.json', 'utf8'),
        ) as {
            publications?: number
            publicationProof?: {
                originalChunkSha256?: string
                fragmentSha256?: string
                originalSingletonImports?: boolean
            }
        }
        result.evidence.automaticPublications = proof.publications ?? 0
        result.evidence.disabledGeneration = disabled.generation
        result.evidence.recoveredGeneration = recovered.generation
        result.evidence.newConfiguredRevision = enabled.revision
        result.evidence.actualFreshHead = context
            .heads()
            .some((head) => head.hostId === target.hostId && head.atMs >= enabled.changedAtMs)
        result.evidence.originalChunkSha256 = proof.publicationProof?.originalChunkSha256 ?? null
        result.evidence.fragmentSha256 = proof.publicationProof?.fragmentSha256 ?? null
        result.evidence.originalSingletonImports =
            proof.publicationProof?.originalSingletonImports === true
        await context.database.unsafe(
            `UPDATE ${authority.namespace}.identity SET paused=true,resync_required=true,generation=generation+1 WHERE id=1`,
        )
        phase = 'manual-pause'
        const paused = await identity()
        await Bun.sleep(1250)
        const stillPaused = await identity()
        result.evidence.manualPausePreserved =
            stillPaused.paused && stillPaused.resync && stillPaused.published === paused.published
        if (denied.length !== 0 || result.evidence.configurationWithdrawn !== true)
            result.failures.push('configuration-withdrawal-not-proven')
        if (
            (proof.publications ?? 0) < 2 ||
            !result.evidence.actualFreshHead ||
            !result.evidence.originalSingletonImports ||
            !/^[a-f0-9]{64}$/u.test(String(result.evidence.fragmentSha256))
        )
            result.failures.push('automatic-canonical-recovery-not-proven')
        if (!result.evidence.manualPausePreserved)
            result.failures.push('manual-pause-auto-reactivated')
        result.passed = result.failures.length === 0
        phase = 'finished'
    } catch {
        result.failures.push('real-automatic-publication-operation-failed')
        result.evidence.phase = phase
        try {
            const row = await identity()
            result.evidence.generation = row.generation
            result.evidence.publishedGeneration = row.published
            result.evidence.contexts = row.contexts
            result.evidence.paused = row.paused
            result.evidence.resyncRequired = row.resync
            const proof = JSON.parse(
                await readFile('/tmp/monitoring-b2/state/replay-proof.json', 'utf8'),
            ) as {
                publicationStage?: unknown
                publicationDiagnostic?: unknown
                publications?: unknown
                errors?: unknown
            }
            for (const key of ['publicationStage', 'publicationDiagnostic'] as const)
                if (typeof proof[key] === 'string' && /^[a-zA-Z0-9-]{1,80}$/u.test(proof[key]))
                    result.evidence[key] = proof[key]
            for (const key of ['publications', 'errors'] as const)
                if (typeof proof[key] === 'number' && Number.isSafeInteger(proof[key]))
                    result.evidence[key] = proof[key]
        } catch {
            result.evidence.diagnosticUnavailable = true
        }
    } finally {
        await worker?.close()
        context.fastProbes(false)
        try {
            await context.stopNative()
            result.evidence.nativeStopped = true
        } catch {
            result.failures.push('native-stop-not-confirmed')
            result.passed = false
        }
        try {
            await replay('resume')
        } catch {
            result.failures.push('replay-resume-not-confirmed')
            result.passed = false
        }
    }
    return result
}
