// oxlint-disable no-await-in-loop -- Fault injection, recovery probes and lifecycle changes depend on the preceding step.
import assert from 'node:assert/strict'
import { mkdir, writeFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import {
    prepareCertificateFixture,
    exerciseCertificates,
    verifyActiveCertificate,
} from './certificates.ts'
import { exerciseCrowdSec } from './crowdsec.ts'
import {
    analyzeResources,
    buildReliabilityReport,
    createSeededRandom,
    parseReliabilityOptions,
    shouldContinue,
} from './control.ts'
import type { ReliabilityCheck, ReliabilityFailure, ResourceSample } from './Types/control.types.ts'
import { createHarness, ReliabilityError } from './harness.ts'
import type { ReliabilityContext, FixtureResult } from './Types/harness.types.ts'
import type { TrafficDiagnostic } from './Types/fixture-transport.types.ts'
import { runtimeProcessesProbe } from '../smoke/appliance-processes.ts'
import { recoverReliabilityComponent } from './component-recovery.ts'
import type {
    ApplianceProcess,
    ComponentRecoverySnapshot,
    ComponentRecoveryDependencies,
} from './Types/component-recovery.types.ts'
import { postgresPauseProbe } from './postgres-pause.ts'

/** Check the route selected by the persisted desired revision against the fixture's expected upstream. */
async function verifyProxy(context: ReliabilityContext, result: FixtureResult) {
    await context.synced(result)
    const response = await context.http()
    assert.ok(result.expected)
    if (!result.expected.enabled) assert.equal(response.status, 404)
    else {
        assert.equal(response.status, 200)
        assert.equal(
            response.body.backend,
            result.expected.forwardPort === result.primaryPort ? 'a' : 'b',
        )
    }
}
/** Bind bounded Docker observations and exact-identity signals to the shared recovery state machine. */
export function recoveryDependencies(context: ReliabilityContext): ComponentRecoveryDependencies {
    /** Bracket the census with container identity reads; a stop/start race yields no usable snapshot. */
    const snapshot = async (): Promise<ComponentRecoverySnapshot | undefined> => {
        let state: string
        let census: string
        try {
            state = await context.docker(
                [
                    'inspect',
                    '--format',
                    '{{.State.Running}}|{{.State.StartedAt}}',
                    context.container,
                ],
                { timeoutMs: 5_000 },
            )
            if (state.startsWith('false|'))
                return { running: false, startedAt: state.slice(6), processes: [] }
            assert.ok(state.startsWith('true|'), 'appliance inspect returns a valid running state')
            census = await context.docker(
                ['exec', context.container, 'bun', '--no-env-file', '-e', runtimeProcessesProbe()],
                { timeoutMs: 5_000 },
            )
            if (
                state !==
                (await context.docker(
                    [
                        'inspect',
                        '--format',
                        '{{.State.Running}}|{{.State.StartedAt}}',
                        context.container,
                    ],
                    { timeoutMs: 5_000 },
                ))
            )
                return undefined
        } catch (error) {
            if (error instanceof assert.AssertionError) throw error
            return undefined
        }
        return {
            running: true,
            startedAt: state.slice(5),
            processes: JSON.parse(census) as ApplianceProcess[],
        }
    }
    return {
        snapshot,
        /** Verify the exact target again inside the appliance before sending TERM. */
        signal: async (target) => {
            const script = `const stat=await Bun.file('/proc/${target.pid}/stat').text();if(stat.slice(stat.lastIndexOf(')')+2).trim().split(/\\s+/)[19]!==${JSON.stringify(target.startTicks)})throw new Error('component identity changed');process.kill(${target.pid},'SIGTERM');`
            await context.docker(
                ['exec', context.container, 'bun', '--no-env-file', '-e', script],
                { timeoutMs: 5_000 },
            )
        },
        start: async () => {
            await context.docker(['start', context.container])
        },
        /** Probe readiness without spawning a web UID fixture writer during supervisor recovery. */
        ready: async () => {
            try {
                await context.docker(
                    [
                        'exec',
                        context.container,
                        'bun',
                        '--no-env-file',
                        '/opt/rentnerproxy/web/docker/web/healthcheck.ts',
                    ],
                    { timeoutMs: 8_000 },
                )
                return true
            } catch {
                return false
            }
        },
        timeoutError: (label) => new ReliabilityError('timeout', label),
    }
}
/** Preserve durable state only after the component's ownership and readiness barriers have completed. */
async function preserveAcrossRestart(context: ReliabilityContext, component: 'web' | 'controller') {
    const before = await context.fixture('durability-read')
    await recoverReliabilityComponent(component, recoveryDependencies(context))
    const after = await context.fixture('durability-read')
    assert.equal(after.desiredRevision, before.desiredRevision)
    assert.deepEqual(after.durableCounts, before.durableCounts)
    assert.deepEqual(after.importHistory, before.importHistory)
    await context.synced(after)
    context.check('restart', component + ' ownership recovery preserves durable state')
}
/** Inject bounded component faults while retaining durable intent and verifying each recovery boundary. */
async function exerciseFailures(
    context: ReliabilityContext,
    iteration: number,
    primaryPort: number,
    secondaryPort: number,
) {
    const modes =
        iteration === 0
            ? (['caddy', 'web', 'controller', 'postgres', 'admin'] as const)
            : [(['caddy', 'web', 'controller', 'postgres', 'admin'] as const)[iteration % 5]!]
    for (const mode of modes) {
        console.log('Fault recovery: ' + mode)
        if (mode === 'web' || mode === 'controller') {
            await preserveAcrossRestart(context, mode)
        } else if (mode === 'caddy') {
            const before = await context.fixture('snapshot')
            await recoverReliabilityComponent('caddy', recoveryDependencies(context))
            await context.synced(before)
            await context.waitFor(
                async () => (await context.http()).status === 200,
                'Caddy restarts and restores live route',
            )
            context.check('restart', 'Caddy process recovery keeps the desired revision')
        } else if (mode === 'postgres') {
            const beforePause = await context.docker(
                [
                    'inspect',
                    '--format',
                    '{{.State.Running}}|{{.State.StartedAt}}',
                    context.container,
                ],
                { timeoutMs: 5_000 },
            )
            assert.ok(beforePause.startsWith('true|'))
            const paused: unknown = JSON.parse(
                await context.docker(
                    [
                        'exec',
                        context.container,
                        'bun',
                        '--no-env-file',
                        '-e',
                        postgresPauseProbe(context.domain),
                    ],
                    { timeoutMs: 12_000 },
                ),
            )
            assert.ok(
                paused &&
                    typeof paused === 'object' &&
                    'notReady' in paused &&
                    'proxyStatus' in paused &&
                    'resumed' in paused,
            )
            assert.equal(typeof paused.notReady, 'boolean')
            assert.equal(typeof paused.resumed, 'boolean')
            assert.ok(
                paused.proxyStatus === null ||
                    (typeof paused.proxyStatus === 'number' &&
                        Number.isInteger(paused.proxyStatus) &&
                        paused.proxyStatus >= 100 &&
                        paused.proxyStatus <= 599),
            )
            console.log(
                'PostgreSQL pause: notReady=' +
                    paused.notReady +
                    ' proxyStatus=' +
                    paused.proxyStatus +
                    ' resumed=' +
                    paused.resumed,
            )
            assert.equal(paused.notReady, true)
            assert.equal(paused.proxyStatus, 200)
            assert.equal(paused.resumed, true)
            assert.equal(
                await context.docker(
                    [
                        'inspect',
                        '--format',
                        '{{.State.Running}}|{{.State.StartedAt}}',
                        context.container,
                    ],
                    { timeoutMs: 5_000 },
                ),
                beforePause,
                'brief PostgreSQL pause must preserve the running appliance',
            )
            await context.synced(await context.fixture('snapshot'))
            context.check(
                'health',
                'Transient database failure is bounded and active proxy traffic survives',
            )
        } else {
            await context.synced(
                await context.fixture('proxy-update', { secondaryPort, resetTls: true }),
            )
            assert.equal((await context.http()).body.backend, 'b')
            const before = await context.controller('/internal/v1/proxy/status')
            await context.docker([
                'exec',
                context.container,
                'rm',
                '--',
                '/var/lib/rentnerproxy/proxy/caddy-admin.sock',
            ])
            let pending: FixtureResult
            try {
                pending = await context.fixture('proxy-update', { secondaryPort: primaryPort })
                const after = await context.controller('/internal/v1/proxy/status')
                assert.equal(after.body.activeRevision, before.body.activeRevision)
                assert.notEqual(pending.desiredRevision, before.body.activeRevision)
                assert.equal((await context.http()).body.backend, 'b')
            } finally {
                await context.restart()
            }
            await context.synced(pending)
            assert.equal((await context.http()).body.backend, 'a')
            context.check(
                'rollback',
                'Caddy Admin failure retains last active route and recovers persisted intent',
            )
        }
    }
}
/** Exercise both access-policy orderings and verify their published authentication and IP decisions. */
async function exercisePolicies(context: ReliabilityContext, random: () => number) {
    const modes: ('public' | 'authenticated' | 'ip-restricted')[] =
        random() < 0.5
            ? ['authenticated', 'ip-restricted', 'public']
            : ['ip-restricted', 'authenticated', 'public']
    for (const policyMode of modes) {
        await context.synced(await context.fixture('policy-update', { policyMode }))
        const response = await context.http('policy-' + context.domain)
        assert.equal(
            response.status,
            policyMode === 'public' ? 200 : policyMode === 'authenticated' ? 401 : 403,
        )
        if (policyMode === 'authenticated') {
            const authorization =
                'Basic ' +
                Buffer.from('reliability:Reliability-fixture-' + context.runId + '-only').toString(
                    'base64',
                )
            assert.equal(
                (await context.http('policy-' + context.domain, '/', { authorization })).status,
                200,
            )
        }
    }
    context.check('proxy', 'Access policy changes enforce Basic Auth and IP restrictions')
}
/** Run the requested bounded reliability workload and write a sanitized report even when recovery fails. */
async function main() {
    const options = parseReliabilityOptions(process.argv.slice(2))
    const checks: ReliabilityCheck[] = []
    const samples: ResourceSample[] = []
    const harness = await createHarness(options, checks)
    const context = harness.context
    const random = createSeededRandom(options.seed)
    let stage: ReliabilityFailure['stage'] = 'setup'
    let failure: ReliabilityFailure | undefined
    let trafficDiagnostic: TrafficDiagnostic | undefined
    let completedIterations = 0
    let startedAt: number | undefined
    let observedDurationSeconds = 0
    const elapsed = () => (startedAt === undefined ? 0 : (performance.now() - startedAt) / 1000)
    try {
        await harness.setup()
        stage = 'warmup'
        const initial = await context.fixture('prepare')
        await context.synced(initial)
        assert.equal((await context.http()).body.backend, 'a')
        await prepareCertificateFixture(context)
        await context.synced(await context.fixture('proxy-update', { resetTls: true }))
        startedAt = performance.now()
        stage = 'cycle'
        while (shouldContinue(options, completedIterations, elapsed())) {
            const iteration = completedIterations
            console.log('Cycle ' + (iteration + 1) + ' (' + Math.round(elapsed()) + 's)')
            const updated = await context.fixture('proxy-update', { iteration, resetTls: true })
            await verifyProxy(context, { ...updated, primaryPort: harness.primaryPort })
            await context.synced(await context.fixture('proxy-disable'))
            assert.equal((await context.http()).status, 404)
            await context.synced(await context.fixture('proxy-enable'))
            const rapid = await context.fixture('rapid-update', { iteration })
            assert.equal(rapid.mutations, options.concurrency)
            await verifyProxy(context, { ...rapid, primaryPort: harness.primaryPort })
            await context.synced(await context.fixture('create-temporary'))
            assert.equal((await context.http('temporary-' + context.domain)).status, 200)
            await context.synced(await context.fixture('delete-temporary'))
            assert.equal((await context.http('temporary-' + context.domain)).status, 404)
            const redirected = await context.fixture('redirect-update', { iteration })
            await context.synced(redirected)
            const redirect = await context.http('redirect-' + context.domain, '/probe')
            assert.equal(redirect.status, 307)
            assert.ok(redirect.headers.get('location')?.includes('/iteration-' + iteration))
            await context.synced(await context.fixture('redirect-disable'))
            assert.equal((await context.http('redirect-' + context.domain)).status, 404)
            await context.synced(await context.fixture('redirect-enable'))
            await context.synced(await context.fixture('redirect-delete'))
            assert.equal((await context.http('redirect-' + context.domain)).status, 404)
            await context.synced(await context.fixture('prepare'))
            await exercisePolicies(context, random)
            await harness.setUpstreamFailed(true)
            let upstreamStatus: number | null = null
            try {
                upstreamStatus = (await context.http()).status
                assert.equal(upstreamStatus, 503)
            } catch (error) {
                context.recordTrafficFailure(null, upstreamStatus)
                throw error
            } finally {
                await harness.setUpstreamFailed(false)
            }
            assert.equal((await context.http()).status, 200)
            context.check('proxy', 'Host and redirect CRUD plus transient upstream recovery')
            await context.synced(await context.fixture('forward-auth'))
            assert.equal(
                (await context.http('policy-' + context.domain)).body.user,
                'reliability-user',
            )
            await harness.setAuthMode('deny')
            assert.equal((await context.http('policy-' + context.domain)).status, 401)
            await harness.setAuthMode('unavailable')
            assert.notEqual((await context.http('policy-' + context.domain)).status, 200)
            await harness.setAuthMode('allow')
            assert.equal((await context.http('policy-' + context.domain)).status, 200)
            await context.synced(await context.fixture('policy-update', { policyMode: 'public' }))
            context.check('proxy', 'Forward Auth failure denies access and recovers')
            const imported = await context.fixture('npm-import', { iteration })
            assert.equal(imported.historyFound, true)
            assert.ok(imported.identicalSourceRetry)
            assert.equal(imported.identicalSourceRetry.historyFound, true)
            assert.equal(imported.failed, 0)
            assert.equal(imported.imported, iteration < 4 ? 2 : 0)
            assert.equal(imported.skipped, iteration < 4 ? 0 : 2)
            assert.equal(imported.identicalSourceRetry.imported, 0)
            assert.equal(imported.identicalSourceRetry.failed, 0)
            assert.equal(imported.identicalSourceRetry.skipped, 2)
            await context.synced(imported)
            const importedProxy = await context.http(
                'npm-' + (iteration % 4) + '-' + context.domain,
            )
            assert.equal(importedProxy.status, 200)
            assert.equal(importedProxy.body.backend, 'a')
            const importedRedirect = await context.http(
                'npm-redirect-' + (iteration % 4) + '-' + context.domain,
            )
            assert.equal(importedRedirect.status, 302)
            assert.equal(importedRedirect.headers.get('location'), 'http://' + context.domain + '/')
            context.check(
                'revision',
                'NPM importer retry retains history without duplicate domains',
            )
            if (iteration % 3 === 0) await exerciseCrowdSec(context)
            await exerciseFailures(context, iteration, harness.primaryPort, harness.secondaryPort)
            await exerciseCertificates(context, iteration)
            if (iteration % 3 === 0) {
                await verifyActiveCertificate(context)
                if (iteration === 0) {
                    await harness.backupRestore()
                    await verifyActiveCertificate(context)
                }
            }
            await context.synced(await context.fixture('proxy-update', { resetTls: true }))
            const final = await context.fixture('durability-read')
            await context.synced(final)
            assert.equal(final.currentJob?.stage, 'applied')
            assert.ok(final.persistedJobCounts)
            assert.equal(final.persistedJobCounts.nonApplied, 0)
            assert.equal(final.persistedJobCounts.total, Math.floor(iteration / 3) + 1)
            assert.ok(Array.isArray(final.importHistory) && final.importHistory.length === 2)
            assert.ok(final.importHistory.every((run) => run.found === true))
            context.check('revision', 'Final persisted desired revision matches the running proxy')
            // Capture after all recovery probes, with idle traffic and no outstanding fixture commands.
            await Bun.sleep(1000)
            if (options.captureResources) {
                const sample = await harness.resources(elapsed())
                analyzeResources([sample], options.concurrency)
                samples.push(sample)
            }
            completedIterations += 1
            if (shouldContinue(options, completedIterations, elapsed())) await Bun.sleep(1000)
        }
        stage = 'resources'
        observedDurationSeconds = elapsed()
    } catch (error) {
        const category =
            error instanceof ReliabilityError
                ? error.category
                : error instanceof assert.AssertionError
                  ? 'assertion'
                  : 'unexpected'
        failure = { stage, category }
        trafficDiagnostic = await context.captureTrafficDiagnostic(stage, category).catch(() => {
            console.error('Traffic failure evidence: capture unavailable')
            return undefined
        })
        const location =
            error instanceof Error
                ? error.stack?.match(
                      /runtime-reliability[/\\](harness|certificates|crowdsec|run|control|component-recovery)\.ts:(\d+):(\d+)/,
                  )
                : undefined
        console.error(
            'Reliability failed at ' +
                stage +
                ' (' +
                category +
                ')' +
                (error instanceof ReliabilityError ? ': ' + error.message : '') +
                (location ? ' at ' + location[1] + '.ts:' + location[2] + ':' + location[3] : ''),
        )
        observedDurationSeconds = elapsed()
    } finally {
        try {
            await harness.cleanup()
        } catch {
            failure ??= { stage: 'cleanup', category: 'command' }
        }
    }
    const report = buildReliabilityReport({
        targetSha: harness.targetSha,
        imageIdentity: harness.imageIdentity,
        options,
        completedIterations,
        observedDurationSeconds,
        checks,
        samples,
        ...(failure ? { failure } : {}),
    })
    const reportPath = resolve(
        options.reportPath ??
            join(import.meta.dir, '../../tmp/runtime-reliability', options.source + '.json'),
    )
    await mkdir(dirname(reportPath), { recursive: true })
    await writeFile(
        reportPath,
        JSON.stringify(
            { ...report, ...(trafficDiagnostic ? { trafficDiagnostic } : {}) },
            null,
            2,
        ) + '\n',
        { mode: 0o600 },
    )
    console.log(
        'Runtime reliability: ' +
            (report.summary.passed ? 'passed' : 'failed') +
            '; cycles=' +
            completedIterations +
            '; observed=' +
            Math.round(observedDurationSeconds) +
            's; trend=' +
            String(report.resources?.trendEvidence ?? 'disabled'),
    )
    if (!report.summary.passed) process.exitCode = 1
}
if (import.meta.main) await main()
