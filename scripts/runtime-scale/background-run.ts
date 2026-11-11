// oxlint-disable no-await-in-loop -- Bounded workload observations and fault recovery depend on the preceding durable state.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { chmod, mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { createHarness, ReliabilityError } from '../runtime-reliability/harness.ts'
import { recoveryDependencies } from '../runtime-reliability/run.ts'
import { recoverReliabilityComponent } from '../runtime-reliability/component-recovery.ts'
import { postgresPauseProbe } from '../runtime-reliability/postgres-pause.ts'
import {
    backgroundDistribution,
    backgroundMonitoringReport,
    backgroundResourceEvidence,
    backgroundSlo,
    parseBackgroundOptions,
} from './background.ts'
import { runContentionWindow } from './background-contention-run.ts'
import { exerciseExtendedBackgroundRecovery } from './background-extended.ts'
import { backgroundPolicies } from './background.ts'
import { backgroundReceiptsSchema, backgroundSnapshotSchema } from './background.validation.ts'
import type {
    BackgroundOptions,
    BackgroundResourceSample,
    BackgroundSnapshot,
} from './Types/background.types.ts'
import type { ReliabilityContext } from '../runtime-reliability/Types/harness.types.ts'
import type { TrafficDiagnostic } from '../runtime-reliability/Types/fixture-transport.types.ts'

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

async function installFixture(context: ReliabilityContext) {
    const source = join(context.temp, 'fixture-source')
    for (const relative of [
        'scripts/runtime-scale/background-fixture.ts',
        'scripts/runtime-scale/background-notifications.ts',
        'scripts/runtime-scale/background-receivers.ts',
        'scripts/runtime-scale/background-network.ts',
        'scripts/runtime-scale/background-discovery.ts',
        'scripts/runtime-scale/background-recovery.ts',
        'scripts/runtime-scale/background-contention-fixture.ts',
        'scripts/runtime-scale/background-contention-receiver.ts',
        'scripts/runtime-scale/background-contention-web.ts',
        'scripts/runtime-scale/background.ts',
        'scripts/runtime-scale/background.validation.ts',
        'scripts/runtime-reliability/control.ts',
        'scripts/runtime-reliability/control.config.ts',
    ]) {
        const destination = join(source, relative)
        await mkdir(dirname(destination), { recursive: true })
        await writeFile(destination, await readFile(resolve(import.meta.dir, '../..', relative)))
    }
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

async function withPausedWeb<T>(
    context: ReliabilityContext,
    operation: () => Promise<T>,
    proxyDomain = context.domain,
) {
    const dependencies = recoveryDependencies(context)
    const before = await dependencies.snapshot()
    const web = before?.processes.filter((item) => item.role === 'web')
    assert.ok(web?.length === 1)
    const target = web[0]!
    const identity = `const stat=await Bun.file('/proc/${target.pid}/stat').text();if(stat.slice(stat.lastIndexOf(')')+2).trim().split(/\\s+/)[19]!==${JSON.stringify(target.startTicks)})throw new Error('web identity changed');`
    const guardPath = '/tmp/background-resume-' + context.runId
    await context.docker([
        'exec',
        '--detach',
        context.container,
        'bun',
        '--no-env-file',
        '-e',
        `await Bun.write(${JSON.stringify(guardPath)},'armed');await Bun.sleep(90000);try{${identity}process.kill(${target.pid},'SIGCONT')}catch{}`,
    ])
    await context.waitFor(
        async () =>
            (await context.docker([
                'exec',
                context.container,
                'bun',
                '--no-env-file',
                '-e',
                `try{process.stdout.write(await Bun.file(${JSON.stringify(guardPath)}).text())}catch{}`,
            ])) === 'armed',
        'web resume guard armed',
        5000,
    )
    await context.docker([
        'exec',
        context.container,
        'bun',
        '--no-env-file',
        '-e',
        identity + `process.kill(${target.pid},'SIGSTOP')`,
    ])
    try {
        assert.equal(
            (await context.http(proxyDomain)).status,
            200,
            'Proxy traffic remains available while the Bun owner is hung',
        )
        return await operation()
    } finally {
        await context.docker([
            'exec',
            context.container,
            'bun',
            '--no-env-file',
            '-e',
            identity + `process.kill(${target.pid},'SIGCONT')`,
        ])
        await context.waitFor(
            dependencies.ready,
            'Bun resumes bounded management service after hang',
            10000,
        )
    }
}

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
    process.once('SIGINT', interrupt)
    process.once('SIGTERM', interrupt)
    let snapshot: BackgroundSnapshot | undefined
    let contention: Awaited<ReturnType<typeof runContentionWindow>>['report'] | undefined
    let monitoring: ReturnType<typeof backgroundMonitoringReport> | undefined
    let fences: unknown
    let notifications: unknown
    let discovery: unknown
    let extendedRecovery: unknown = {
        status: 'insufficient',
        reason: 'Explicit --extended-recovery true required for certificate interruption, active-work restore and audit replay',
    }
    let failure: string | undefined
    let trafficDiagnostic: TrafficDiagnostic | undefined
    let imagePlatform: string | undefined
    let proxyFailures = 0
    let origin = 0
    let elapsedMs = 0
    let receiptsOverflowed = false
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
        await harness.setup()
        imagePlatform = await context.docker([
            'image',
            'inspect',
            '--format',
            '{{.Os}}/{{.Architecture}}',
            harness.imageIdentity,
        ])
        assert.match(imagePlatform, /^linux\/(amd64|arm64)$/u)
        await installFixture(context)
        await context.synced(await context.fixture('prepare'))
        await context.synced(await context.fixture('background-prepare', extra))
        origin = performance.now()
        if (options.contention) {
            const result = await runContentionWindow(
                harness,
                options,
                () => sample('continuous'),
                () => {
                    origin = performance.now()
                },
                abort.signal,
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
                if (abort.signal.aborted)
                    throw new ReliabilityError('command', 'background scenario cancelled')
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
        const before = snapshot.observations.length
        fences = (await withPausedWeb(context, () => context.fixture('background-fences', extra)))
            .fences
        discovery = (await context.fixture('background-discovery', extra)).discovery
        notifications = (
            await withPausedWeb(context, () => context.fixture('background-notifications', extra))
        ).notifications
        for (const component of ['web', 'controller'] as const) {
            const started = performance.now()
            await recoverReliabilityComponent(component, recoveryDependencies(context))
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
            await sample('recovered')
        }
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
            const policy = backgroundPolicies(options)[0]!
            const domain = `bg-${policy.index}-${policy.latency}-${context.domain}`
            extendedRecovery = await exerciseExtendedBackgroundRecovery(
                harness,
                options,
                (operation) => withPausedWeb(context, operation, domain),
            )
        }
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
        console.error('Background scenarios failed: ' + failure)
        trafficDiagnostic = await context
            .captureTrafficDiagnostic('background', failure)
            .catch(() => undefined)
    } finally {
        if (origin && !elapsedMs) elapsedMs = performance.now() - origin
        process.off('SIGINT', interrupt)
        process.off('SIGTERM', interrupt)
        await harness.cleanup().catch(() => {
            failure ??= 'cleanup'
        })
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
        ...(failure ? { failure } : {}),
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
