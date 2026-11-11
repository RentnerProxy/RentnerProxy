// oxlint-disable no-await-in-loop -- Each bounded observation is made against the actual overlapping appliance workload.
import assert from 'node:assert/strict'
import { backgroundSnapshotSchema } from './background.validation.ts'
import {
    buildContentionReport,
    contentionContract,
    contentionStateSchema,
    contentionReceiverSchema,
    contentionNativeSchema,
    contentionWebSchema,
} from './background-contention.ts'
import type { ContentionSample } from './background-contention.ts'
import type { createHarness } from '../runtime-reliability/harness.ts'
import { recoveryDependencies } from '../runtime-reliability/run.ts'
import { recoverReliabilityComponent } from '../runtime-reliability/component-recovery.ts'
import type { BackgroundOptions } from './Types/background.types.ts'

/** Stop always runs; diagnostics never replace the original workload or stop failure. */
export async function withContentionCleanup<T>(
    operation: () => Promise<T>,
    stop: () => Promise<unknown>,
    report: (category: 'contention-stop-failed') => void,
): Promise<T> {
    const outcome = await Promise.resolve()
        .then(operation)
        .then(
            (value) => ({ status: 'fulfilled' as const, value }),
            (reason: unknown) => ({ status: 'rejected' as const, reason }),
        )
    let stopFailed = false
    let stopFailure: unknown
    try {
        await stop()
    } catch (error) {
        stopFailed = true
        stopFailure = error
        try {
            report('contention-stop-failed')
        } catch {
            /* Best-effort diagnostics cannot mask either failure. */
        }
    }
    if (outcome.status === 'rejected') throw outcome.reason
    if (stopFailed) throw stopFailure
    return outcome.value
}

export async function runContentionWindow(
    harness: Awaited<ReturnType<typeof createHarness>>,
    options: BackgroundOptions,
    sampleResources: () => Promise<void>,
    started: () => void,
    signal: AbortSignal,
    reportCleanupFailure?: (category: 'contention-stop-failed') => void,
) {
    const context = harness.context
    const extra = {
        policyCount: options.policies,
        seed: options.seed,
        duePattern: options.pattern,
        backgroundProfile: options.profile,
        contention: true,
    }
    console.log(JSON.stringify({ predeclaredContentionContract: contentionContract }))
    const control = async () =>
        contentionReceiverSchema.parse(
            JSON.parse(
                await context.docker(
                    [
                        'exec',
                        context.container,
                        'bun',
                        '--no-env-file',
                        '-e',
                        "const r=await fetch('http://127.0.0.1:9018/status',{signal:AbortSignal.timeout(2000)});if(!r.ok)process.exit(1);process.stdout.write(await r.text())",
                    ],
                    { timeoutMs: 4000 },
                ),
            ),
        )
    const native = async () => {
        const response = await context.controller('/internal/v1/background/status')
        assert.equal(response.status, 200)
        const worker = contentionNativeSchema
            .parse(response.body)
            .workers.find((item) => item.id === 'caddy_recovery')
        assert.ok(worker)
        return {
            attempts: worker.attempts,
            successes: worker.successes,
            observedAt: performance.now(),
        }
    }
    const management = async () => {
        const script = `const state=JSON.parse(await Bun.file('/tmp/rentnerproxy-reliability-fixture.json').text());const start=performance.now();const r=await fetch('http://127.0.0.1:3000/proxy-hosts',{headers:{cookie:'rentnerproxy_session='+state.session},signal:AbortSignal.timeout(3000),redirect:'manual'});if(r.body){const reader=r.body.getReader();let bytes=0;while(true){const part=await reader.read();if(part.done)break;bytes+=part.value.byteLength;if(bytes>4*1024*1024){await reader.cancel();throw new Error('bounded management body exceeded')}}}process.stdout.write(JSON.stringify({status:r.status,elapsedMs:performance.now()-start}));`
        const value = JSON.parse(
            await context.docker(
                ['exec', context.container, 'bun', '--no-env-file', '-e', script],
                { timeoutMs: 5000 },
            ),
        ) as { status?: number; elapsedMs?: number }
        assert.equal(
            value.status,
            200,
            'Authenticated management SSR progresses in the actual web process',
        )
        assert.ok(
            typeof value.elapsedMs === 'number' &&
                Number.isFinite(value.elapsedMs) &&
                value.elapsedMs >= 0,
        )
        return value.elapsedMs
    }
    let seeded = false
    const samples: ContentionSample[] = []
    let proxyErrors = 0
    let snapshot
    const workload = async () => {
        await context.docker([
            'exec',
            '--detach',
            '--workdir',
            '/opt/rentnerproxy/web',
            context.container,
            'bun',
            '--no-env-file',
            '/opt/rentnerproxy/web/background-contention-receiver.js',
            String(options.seed),
        ])
        await context.waitFor(
            async () => {
                await control()
                return true
            },
            'private contention receiver ready',
            10000,
        )
        await context.fixture('background-contention-seed', extra)
        seeded = true
        await recoverReliabilityComponent('web', recoveryDependencies(context))
        started()
        const began = performance.now()
        const begin = await context.fixture('background-start', extra)
        snapshot = backgroundSnapshotSchema.parse(begin.background)
        await context.waitFor(
            async () =>
                contentionStateSchema.parse(
                    (await context.fixture('background-contention-read', extra)).contention,
                ).postgres.monitoringLeases > 0,
            'Actual monitoring leases are active before arming the mixed event burst',
            10000,
        )
        const arm = await context.fixture('background-contention-arm', extra)
        const armedRevision = Number(
            (arm.contention as { discoveryRevision?: number }).discoveryRevision,
        )
        assert.ok(Number.isSafeInteger(armedRevision))
        const baselineNative = await native()
        while (performance.now() - began < options.durationSeconds * 1000) {
            assert.equal(signal.aborted, false, 'Contention scenario was cancelled')
            const captureStarted = performance.now()
            const envelope = await context.fixture('background-read', extra)
            snapshot = backgroundSnapshotSchema.parse(envelope.background)
            const state = contentionStateSchema.parse(envelope.contention)
            const results = await Promise.allSettled([
                management(),
                (async () => {
                    const at = performance.now()
                    const response = await context.http()
                    assert.equal(response.status, 200)
                    assert.equal(response.body.backend, 'a')
                    return performance.now() - at
                })(),
                control(),
                native(),
                context.controller('/internal/v1/proxy/status'),
                sampleResources(),
                context
                    .docker(
                        [
                            'exec',
                            '--workdir',
                            '/opt/rentnerproxy/web',
                            context.container,
                            'bun',
                            '--no-env-file',
                            '/opt/rentnerproxy/web/background-contention-web.js',
                        ],
                        { timeoutMs: 6000 },
                    )
                    .then((value) => contentionWebSchema.parse(JSON.parse(value))),
            ])
            if (results[1]!.status === 'rejected') proxyErrors++
            for (const result of results) if (result.status === 'rejected') throw result.reason
            const [read, proxy, receiver, critical, revision, , web] = results as [
                PromiseFulfilledResult<number>,
                PromiseFulfilledResult<number>,
                PromiseFulfilledResult<ReturnType<typeof contentionReceiverSchema.parse>>,
                PromiseFulfilledResult<{ attempts: number; successes: number; observedAt: number }>,
                PromiseFulfilledResult<Awaited<ReturnType<typeof context.controller>>>,
                PromiseFulfilledResult<void>,
                PromiseFulfilledResult<ReturnType<typeof contentionWebSchema.parse>>,
            ]
            samples.push({
                elapsedMs: performance.now() - began,
                criticalObservedMs: critical.value.observedAt - baselineNative.observedAt,
                captureSpanMs: performance.now() - captureStarted,
                leasedPolicies: state.postgres.monitoringLeases,
                managementMs: read.value,
                proxyMs: proxy.value,
                state,
                receiver: receiver.value,
                critical: critical.value,
                revisionUnchanged: revision.value.body.activeRevision === begin.desiredRevision,
                web: web.value,
            })
            if (
                snapshot.observations.length >= options.policies &&
                performance.now() - began >= 35000
            )
                break
            await Bun.sleep(100)
        }
        const report = buildContentionReport(
            samples,
            { ...baselineNative, discoveryRevision: armedRevision },
            proxyErrors,
        )
        return {
            snapshot,
            report,
            proxyMs: samples.map((item) => item.proxyMs),
            managementMs: samples.map((item) => item.managementMs),
            proxyErrors,
        }
    }
    return withContentionCleanup(
        workload,
        async () => {
            if (seeded) await context.fixture('background-contention-stop', extra)
        },
        (category) => {
            reportCleanupFailure?.(category)
            console.error('Contention cleanup failure: ' + category)
        },
    )
}
