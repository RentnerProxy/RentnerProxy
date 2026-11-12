// oxlint-disable no-await-in-loop -- Bounded workload observations and fault recovery depend on the preceding durable state.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { chmod, mkdir, writeFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { createHarness, ReliabilityError } from '../runtime-reliability/harness.ts'
import { recoveryDependencies } from '../runtime-reliability/run.ts'
import { recoverReliabilityComponent } from '../runtime-reliability/component-recovery.ts'
import { postgresPauseProbe } from '../runtime-reliability/postgres-pause.ts'
import {
    backgroundDistribution,
    backgroundMonitoringReport,
    backgroundResourceEvidence,
    backgroundRecoveryBaseline,
    backgroundFailureDiagnostic,
    backgroundSlo,
    parseBackgroundOptions,
} from './background.ts'
import { runContentionWindow } from './background-contention-run.ts'
import {
    backgroundPauseDetachedScript,
    backgroundPauseMarker,
    checkBackgroundCancellation,
    withBackgroundPause,
} from './background-pause.ts'
import { exerciseExtendedBackgroundRecovery } from './background-extended.ts'
import { backgroundPolicies } from './background.ts'
import { backgroundReceiptsSchema, backgroundSnapshotSchema } from './background.validation.ts'
import { stageBackgroundFixtureSources } from './background-source.ts'
import type {
    BackgroundOptions,
    BackgroundResourceSample,
    BackgroundSnapshot,
} from './Types/background.types.ts'
import type { ReliabilityContext } from '../runtime-reliability/Types/harness.types.ts'
import type { TrafficDiagnostic } from '../runtime-reliability/Types/fixture-transport.types.ts'

/** Canonicalize appliance and process identities for stable resource-sample comparison; absent snapshots remain unavailable. */
function generationOf(
    value: Awaited<ReturnType<ReturnType<typeof recoveryDependencies>['snapshot']>>,
) {
    return value
        ? {
              startedAt: value.startedAt,
              processes: value.processes
                  .map((item) => ({ pid: item.pid, startTicks: item.startTicks, role: item.role }))
                  .toSorted((a, b) => a.pid - b.pid),
          }
        : undefined
}

/** Stage the explicit fixture sources, bundle against external production packages, and install only into the owned appliance. */
async function installFixture(context: ReliabilityContext) {
    const source = join(context.temp, 'fixture-source')
    await stageBackgroundFixtureSources(source)
    const bundle = join(context.temp, 'reliability-fixture.js')
    await context.command([
        process.execPath,
        '--no-env-file',
        'build',
        join(source, 'scripts/runtime-scale/background-fixture.ts'),
        '--tsconfig-override=' + join(source, 'tsconfig.json'),
        '--target=bun',
        '--packages=external',
        '--outfile=' + bundle,
    ])
    await context.docker([
        'cp',
        bundle,
        context.container + ':/opt/rentnerproxy/web/reliability-fixture.js',
    ])
    const receiver = join(context.temp, 'background-contention-receiver.js')
    await context.command([
        process.execPath,
        '--no-env-file',
        'build',
        join(source, 'scripts/runtime-scale/background-contention-receiver.ts'),
        '--tsconfig-override=' + join(source, 'tsconfig.json'),
        '--target=bun',
        '--packages=external',
        '--outfile=' + receiver,
    ])
    await context.docker([
        'cp',
        receiver,
        context.container + ':/opt/rentnerproxy/web/background-contention-receiver.js',
    ])
    const probe = join(context.temp, 'background-contention-web.js')
    await context.command([
        process.execPath,
        '--no-env-file',
        'build',
        join(source, 'scripts/runtime-scale/background-contention-web.ts'),
        '--tsconfig-override=' + join(source, 'tsconfig.json'),
        '--target=bun',
        '--packages=external',
        '--outfile=' + probe,
    ])
    await context.docker([
        'cp',
        probe,
        context.container + ':/opt/rentnerproxy/web/background-contention-web.js',
    ])
}

/** Pause the single identified Bun owner behind a detached emergency-resume guard and require completed resume before returning. */
async function withPausedWeb<T>(
    context: ReliabilityContext,
    operation: () => Promise<T>,
    signal: AbortSignal,
    proxyDomain = context.domain,
) {
    checkBackgroundCancellation(signal)
    const dependencies = recoveryDependencies(context)
    const before = await dependencies.snapshot()
    const web = before?.processes.filter((item) => item.role === 'web')
    assert.ok(web?.length === 1)
    const target = web[0]!
    const guardPath = backgroundPauseMarker(context.runId)
    const exec = (script: string, detached = false, args: readonly string[] = []) =>
        context.docker([
            'exec',
            ...(detached ? ['--detach'] : []),
            context.container,
            'bun',
            '--no-env-file',
            '-e',
            script,
            ...args,
        ])
    const state = () =>
        exec('try{process.stdout.write(await Bun.file(process.argv[1]).text())}catch{}', false, [
            guardPath,
        ])
    return withBackgroundPause(
        signal,
        async () => {
            await exec(backgroundPauseDetachedScript, true, [
                String(target.pid),
                target.startTicks,
                guardPath,
            ])
            await context.waitFor(
                async () => {
                    checkBackgroundCancellation(signal)
                    return (await state()) === 'armed'
                },
                'web resume guard armed',
                5000,
            )
        },
        async () => {
            assert.equal(
                (await context.http(proxyDomain)).status,
                200,
                'Proxy traffic remains available while the Bun owner is hung',
            )
            checkBackgroundCancellation(signal)
            return operation()
        },
        async () => {
            await exec("await Bun.write(process.argv[1]+'.resume','resume')", false, [guardPath])
            await context.waitFor(
                async () => (await state()) === 'resumed',
                'web resume guard completed',
                5000,
            )
            await context.waitFor(
                dependencies.ready,
                'Bun resumes bounded management service after hang',
                10000,
            )
        },
    )
}

/** Run the bounded seeded appliance scenario with cancellation checks, generation-bound evidence, staged recovery, and harness cleanup. */
export async function runBackgroundScenarios(options: BackgroundOptions) {
    const scenarioStarted = performance.now()
    const harness = await createHarness(
        {
            source: 'current',
            durationSeconds: options.durationSeconds,
            iterations: 1,
            concurrency: 2,
            captureResources: true,
            ...(options.contention ? { backgroundContention: true as const } : {}),
            seed: options.seed,
            ...(options.image ? { image: options.image } : {}),
        },
        [],
    )
    const context = harness.context
    const extra = {
        policyCount: options.policies,
        seed: options.seed,
        duePattern: options.pattern,
        backgroundProfile: options.profile,
    }
    const resources: BackgroundResourceSample[] = []
    const proxyMs: number[] = []
    const managementMs: number[] = []
    const ssrMs: number[] = []
    const recovery: { component: string; elapsedMs: number; passed: boolean }[] = []
    const abort = new AbortController()
    const interrupt = () => abort.abort()
    process.on('SIGINT', interrupt)
    process.on('SIGTERM', interrupt)
    let snapshot: BackgroundSnapshot | undefined
    let contention: Awaited<ReturnType<typeof runContentionWindow>>['report'] | undefined
    let monitoring: ReturnType<typeof backgroundMonitoringReport> | undefined
    let fences: unknown
    let notifications: unknown
    let discovery: unknown
    let extendedRecovery: unknown = {
        status: 'insufficient',
        selected: options.extendedRecovery === true,
        reason: options.extendedRecovery
            ? 'Selected extended recovery has not completed'
            : 'Explicit --extended-recovery true required for certificate interruption, active-work restore and audit replay',
    }
    let failure: string | undefined
    const cleanupFailures: 'contention-stop-failed'[] = []
    let failureContext:
        | (ReturnType<typeof backgroundFailureDiagnostic> & {
              stage: string
              component: 'web' | 'controller' | null
          })
        | undefined
    let stage = 'setup'
    /** Check cancellation before advancing the diagnostic stage used to locate a failed scenario. */
    const enter = (next: string) => {
        checkBackgroundCancellation(abort.signal)
        stage = next
    }
    let recoveryComponent: 'web' | 'controller' | null = null
    let trafficDiagnostic: TrafficDiagnostic | undefined
    let imagePlatform: string | undefined
    let proxyFailures = 0
    let origin = 0
    let elapsedMs = 0
    let receiptsOverflowed = false
    /** Accept resource evidence only when before/after ownership matches, tagging warmup and recovered samples separately from continuous work. */
    const sample = async (kind: BackgroundResourceSample['kind']) => {
        const before = await recoveryDependencies(context).snapshot()
        assert.ok(before?.running)
        const value = await harness.resources((performance.now() - origin) / 1000)
        const after = await recoveryDependencies(context).snapshot()
        assert.deepEqual(
            generationOf(after),
            generationOf(before),
            'Resource sample must have one confirmed process generation',
        )
        const generation = createHash('sha256')
            .update(JSON.stringify(generationOf(before)))
            .digest('hex')
        resources.push({
            kind,
            generation,
            warmup: performance.now() - origin < 5000,
            sample: { ...value, phase: kind },
        })
        return value
    }
    console.log(
        JSON.stringify({
            predeclaredContract: {
                options: { ...options, image: undefined, reportPath: undefined },
                slo: backgroundSlo,
                baseline: 'appliance-1g-2cpu-512pid-100conn',
                evidence:
                    'real appliance worker, PostgreSQL durable observations, controller HTTP probes and controlled fixture upstream; one cadence window is a correctness regression, not a sustained capacity claim',
            },
        }),
    )
    try {
        enter('setup')
        await harness.setup()
        enter('setup')
        imagePlatform = await context.docker([
            'image',
            'inspect',
            '--format',
            '{{.Os}}/{{.Architecture}}',
            harness.imageIdentity,
        ])
        assert.match(imagePlatform, /^linux\/(amd64|arm64)$/u)
        checkBackgroundCancellation(abort.signal)
        await installFixture(context)
        checkBackgroundCancellation(abort.signal)
        await context.synced(await context.fixture('prepare'))
        checkBackgroundCancellation(abort.signal)
        await context.synced(await context.fixture('background-prepare', extra))
        if (options.policies === 1000)
            await context.refreshDns(
                backgroundPolicies(options)
                    .slice(-2)
                    .map((item) => `bg-${item.index}-${item.latency}-${context.domain}`),
            )
        origin = performance.now()
        enter('workload')
        if (options.contention) {
            const result = await runContentionWindow(
                harness,
                options,
                () => sample('continuous'),
                () => {
                    origin = performance.now()
                },
                abort.signal,
                (category) => {
                    cleanupFailures.push(category)
                },
            )
            snapshot = result.snapshot
            contention = result.report
            proxyMs.push(...result.proxyMs)
            managementMs.push(...result.managementMs)
            proxyFailures += result.proxyErrors
        } else {
            snapshot = backgroundSnapshotSchema.parse(
                (await context.fixture('background-start', extra)).background,
            )
            while (performance.now() - origin < options.durationSeconds * 1000) {
                checkBackgroundCancellation(abort.signal)
                const responses = await Promise.allSettled([
                    (async () => {
                        const start = performance.now()
                        const response = await context.http()
                        proxyMs.push(performance.now() - start)
                        assert.equal(response.status, 200)
                        assert.equal(response.body.backend, 'a')
                    })(),
                    (async () => {
                        const start = performance.now()
                        const response = await context.docker(
                            [
                                'exec',
                                context.container,
                                'bun',
                                '--no-env-file',
                                '-e',
                                "const r=await fetch('http://127.0.0.1:3000/login',{signal:AbortSignal.timeout(3000),redirect:'manual'});await r.body?.cancel();if(r.status!==200)process.exit(1)",
                            ],
                            { timeoutMs: 5000 },
                        )
                        assert.equal(response, '')
                        ssrMs.push(performance.now() - start)
                    })(),
                    (async () => {
                        snapshot = backgroundSnapshotSchema.parse(
                            (await context.fixture('background-read', extra)).background,
                        )
                        managementMs.push(snapshot.managementReadMs)
                    })(),
                    sample('continuous'),
                ])
                if (responses[0]!.status === 'rejected') proxyFailures++
                const rejected = responses.find((response) => response.status === 'rejected')
                if (rejected?.status === 'rejected') throw rejected.reason
                assert.ok(snapshot)
                if (
                    snapshot.observations.length >= options.policies &&
                    performance.now() - origin >= 35000
                )
                    break
                await Bun.sleep(500)
            }
        }
        elapsedMs = performance.now() - origin
        assert.ok(snapshot)
        const receipts = backgroundReceiptsSchema.parse(await harness.backgroundReceipts())
        receiptsOverflowed = receipts.overflowed
        monitoring = backgroundMonitoringReport(options, snapshot, receipts.receipts, elapsedMs)
        const quiescent = backgroundSnapshotSchema.parse(
            (await context.fixture('background-quiesce', extra)).background,
        )
        const before = quiescent.observations.length
        enter('fences')
        fences = (
            await withPausedWeb(
                context,
                () => context.fixture('background-fences', extra),
                abort.signal,
            )
        ).fences
        enter('discovery')
        discovery = (await context.fixture('background-discovery', extra)).discovery
        enter('notifications')
        notifications = (
            await withPausedWeb(
                context,
                () => context.fixture('background-notifications', extra),
                abort.signal,
            )
        ).notifications
        for (const component of ['web', 'controller'] as const) {
            const started = performance.now()
            recoveryComponent = component
            enter('recovery-revision')
            await context.synced(await context.fixture('background-read', extra))
            const dependencies = recoveryDependencies(context)
            const deadline = started + backgroundSlo.recoveryMs
            const withinBudget = () => {
                checkBackgroundCancellation(abort.signal)
                if (performance.now() > deadline)
                    throw new ReliabilityError('timeout', 'background component recovery budget')
            }
            const bounded = {
                ...dependencies,
                snapshot: async () => {
                    withinBudget()
                    const value = await dependencies.snapshot()
                    withinBudget()
                    return value
                },
                ready: async () => {
                    withinBudget()
                    const value = await dependencies.ready()
                    withinBudget()
                    return value
                },
            }
            let initialSnapshot = true
            await recoverReliabilityComponent(component, {
                ...bounded,
                snapshot: async () => {
                    if (!initialSnapshot) return bounded.snapshot()
                    initialSnapshot = false
                    enter('recovery-baseline')
                    const baseline = await backgroundRecoveryBaseline(
                        bounded,
                        Math.max(0, deadline - performance.now()),
                    )
                    enter('recovery-owners')
                    return baseline
                },
            })
            enter('recovery-durability')
            await context.synced(await context.fixture('background-read', extra))
            const final = backgroundSnapshotSchema.parse(
                (await context.fixture('background-read', extra)).background,
            )
            assert.equal(
                final.observations.length,
                before + 1,
                'Recovery must retain once-only fenced completions',
            )
            assert.ok(
                final.policies.every((policy) => !policy.enabled && !policy.leased),
                'No disabled work is stranded or restarted',
            )
            const duration = performance.now() - started
            recovery.push({
                component,
                elapsedMs: duration,
                passed: duration <= backgroundSlo.recoveryMs,
            })
            enter('recovery-resources')
            await sample('recovered')
        }
        recoveryComponent = null
        enter('postgres-pause')
        const pauseStarted = performance.now()
        const pause = JSON.parse(
            await context.docker(
                [
                    'exec',
                    context.container,
                    'bun',
                    '--no-env-file',
                    '-e',
                    postgresPauseProbe(context.domain),
                ],
                { timeoutMs: 12000 },
            ),
        ) as { notReady?: boolean; proxyStatus?: number; resumed?: boolean }
        assert.equal(pause.notReady, true)
        assert.equal(pause.proxyStatus, 200)
        assert.equal(pause.resumed, true)
        const pauseElapsedMs = performance.now() - pauseStarted
        if (options.extendedRecovery) {
            enter('extended-recovery')
            const policy = backgroundPolicies(options)[0]!
            const domain = `bg-${policy.index}-${policy.latency}-${context.domain}`
            extendedRecovery = await exerciseExtendedBackgroundRecovery(
                harness,
                options,
                (operation) => withPausedWeb(context, operation, abort.signal, domain),
                () => checkBackgroundCancellation(abort.signal),
                (step) => enter('extended-recovery:' + step),
            )
        }
        checkBackgroundCancellation(abort.signal)
        recovery.push({
            component: 'postgres-unavailable',
            elapsedMs: pauseElapsedMs,
            passed: true,
        })
    } catch (error) {
        failure =
            error instanceof ReliabilityError
                ? error.category
                : error instanceof assert.AssertionError
                  ? 'assertion'
                  : 'unexpected'
        failureContext = {
            stage,
            component: recoveryComponent,
            ...backgroundFailureDiagnostic(error),
        }
        console.error('Background scenarios failed: ' + failure)
        console.error('Background failure location: ' + JSON.stringify(failureContext))
        trafficDiagnostic = await context
            .captureTrafficDiagnostic('background', failure)
            .catch(() => undefined)
    } finally {
        if (origin && !elapsedMs) elapsedMs = performance.now() - origin
        stage = 'cleanup'
        await harness.cleanup().catch(() => {
            failure ??= 'cleanup'
        })
        if (abort.signal.aborted) failure ??= 'cancelled'
        process.off('SIGINT', interrupt)
        process.off('SIGTERM', interrupt)
    }
    const resourceEvidence = backgroundResourceEvidence(resources)
    const interactive = {
        management: {
            ...backgroundDistribution(managementMs),
            measuredIn: options.contention
                ? 'actual authenticated management SSR/auth HTTP read in the web process'
                : 'authorized host-list service in the fixture child',
        },
        ssr: options.contention ? null : backgroundDistribution(ssrMs),
        ssrEvidence: options.contention
            ? 'Same operation as interactive.management; no second independent series'
            : 'Separate public login SSR request',
        proxy: backgroundDistribution(proxyMs),
        proxyFailures,
    }
    const passed =
        !failure &&
        monitoring?.status === 'met' &&
        (!options.contention || contention?.status === 'met') &&
        !receiptsOverflowed &&
        resourceEvidence.status !== 'unmet' &&
        proxyFailures === 0 &&
        interactive.management.p99 !== null &&
        interactive.management.p99 <=
            (options.contention ? 1000 : backgroundSlo.managementReadP99Ms) &&
        interactive.proxy.p99 !== null &&
        interactive.proxy.p99 <= backgroundSlo.proxyP99Ms &&
        (options.contention ||
            (interactive.ssr?.p99 !== null &&
                interactive.ssr?.p99 !== undefined &&
                interactive.ssr.p99 <= backgroundSlo.ssrP99Ms)) &&
        recovery.every((item) => item.passed)
    const report = {
        version: 1,
        targetSha: harness.targetSha,
        imageIdentity: harness.imageIdentity,
        imagePlatform,
        seed: options.seed,
        policyCount: options.policies,
        duePattern: options.pattern,
        latencyProfile: options.profile,
        intervalSeconds: 60,
        observedWorkloadMs: elapsedMs,
        scenarioElapsedMs: performance.now() - scenarioStarted,
        extendedRecoverySelected: options.extendedRecovery === true,
        slo: backgroundSlo,
        monitoring,
        ...(contention ? { contention } : {}),
        interactive,
        fences,
        notifications,
        discovery,
        extendedRecovery,
        ...(trafficDiagnostic ? { trafficDiagnostic } : {}),
        recovery,
        resources: {
            ...resourceEvidence,
            scope: 'Monitoring workload and short component recovery; extended restoration has no continuous resource claim',
            samples: resources,
        },
        summary: {
            passed,
            capacity: 'insufficient',
            reason: 'A bounded first-cadence regression cannot establish sustained production capacity',
        },
        ...(failure ? { failure, failureContext } : {}),
        ...(cleanupFailures.length ? { cleanupFailures } : {}),
    }
    const path = resolve(
        options.reportPath ?? join(import.meta.dir, '../../tmp/runtime-background/report.json'),
    )
    await mkdir(dirname(path), { recursive: true })
    await writeFile(path, JSON.stringify(report, null, 2) + '\n', { mode: 0o600 })
    await chmod(path, 0o600)
    console.log(
        'Runtime background: ' +
            (passed ? 'passed' : 'failed') +
            '; policies=' +
            options.policies +
            '; capacity=insufficient',
    )
    return report
}

if (import.meta.main) {
    try {
        if (
            !(await runBackgroundScenarios(parseBackgroundOptions(process.argv.slice(2)))).summary
                .passed
        )
            process.exitCode = 1
    } catch {
        console.error(
            'Background scenario configuration/setup failed; no sensitive diagnostics exported',
        )
        process.exitCode = 1
    }
}
