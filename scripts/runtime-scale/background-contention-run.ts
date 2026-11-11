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
import { readMemoryObservation } from '../runtime-reliability/resource-probe.ts'
import type { ResourceSample } from '../runtime-reliability/Types/control.types.ts'
import type { BackgroundOptions } from './Types/background.types.ts'
import { checkBackgroundCancellation } from './background-pause.ts'

/** Add 15 minutes for bounded fixture commands, 60s replacement + 150s readiness, final observation and drain beyond the requested window. */
export function contentionReceiverLifetimeMs(durationSeconds: number): number {
    if (!Number.isInteger(durationSeconds) || durationSeconds < 1 || durationSeconds > 300)
        throw new Error('Invalid contention duration')
    return durationSeconds * 1_000 + 900_000
}

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

/** Measure actual overlapping appliance workers after arming the event burst, retain exact native-progress observation time, and always drain seeded fixture state. */
export async function runContentionWindow(
    harness: Awaited<ReturnType<typeof createHarness>>,
    options: BackgroundOptions,
    sampleResources: () => Promise<ResourceSample>,
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
    const control = async () => {
        checkBackgroundCancellation(signal)
        const response = await context.docker(
            [
                'exec',
                context.container,
                'bun',
                '--no-env-file',
                '-e',
                "const r=await fetch('http://127.0.0.1:9018/status',{signal:AbortSignal.timeout(2000)});if(!r.ok)process.exit(1);process.stdout.write(await r.text())",
            ],
            { timeoutMs: 4000 },
        )
        checkBackgroundCancellation(signal)
        return contentionReceiverSchema.parse(JSON.parse(response))
    }
    /** Read the fixed recovery worker counters and timestamp this observation so later snapshot collection cannot inflate its progress latency. */
    const native = async () => {
        checkBackgroundCancellation(signal)
        const response = await context.controller('/internal/v1/background/status')
        checkBackgroundCancellation(signal)
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
    /** Measure authenticated SSR in the real web process with a bounded response body and deadline, rejecting redirects or unsuccessful responses. */
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
        checkBackgroundCancellation(signal)
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
            String(contentionReceiverLifetimeMs(options.durationSeconds)),
        ])
        await context.waitFor(
            async () => {
                checkBackgroundCancellation(signal)
                await control()
                checkBackgroundCancellation(signal)
                return true
            },
            'private contention receiver ready',
            10000,
        )
        checkBackgroundCancellation(signal)
        await context.fixture('background-contention-seed', extra)
        seeded = true
        checkBackgroundCancellation(signal)
        await recoverReliabilityComponent('web', recoveryDependencies(context))
        checkBackgroundCancellation(signal)
        started()
        const began = performance.now()
        const begin = await context.fixture('background-start', extra)
        checkBackgroundCancellation(signal)
        snapshot = backgroundSnapshotSchema.parse(begin.background)
        await context.waitFor(
            async () => {
                checkBackgroundCancellation(signal)
                const value = await context.fixture('background-contention-read', extra)
                checkBackgroundCancellation(signal)
                return contentionStateSchema.parse(value.contention).postgres.monitoringLeases > 0
            },
            'Actual monitoring leases are active before arming the mixed event burst',
            10000,
        )
        checkBackgroundCancellation(signal)
        const arm = await context.fixture('background-contention-arm', extra)
        checkBackgroundCancellation(signal)
        const armedRevision = Number(
            (arm.contention as { discoveryRevision?: number }).discoveryRevision,
        )
        assert.ok(Number.isSafeInteger(armedRevision))
        const baselineNative = await native()
        while (performance.now() - began < options.durationSeconds * 1000) {
            checkBackgroundCancellation(signal)
            const captureStarted = performance.now()
            const envelope = await context.fixture('background-read', extra)
            checkBackgroundCancellation(signal)
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
            checkBackgroundCancellation(signal)
            if (results[1]!.status === 'rejected') proxyErrors++
            for (const result of results) if (result.status === 'rejected') throw result.reason
            const [read, proxy, receiver, critical, revision, resource, web] = results as [
                PromiseFulfilledResult<number>,
                PromiseFulfilledResult<number>,
                PromiseFulfilledResult<ReturnType<typeof contentionReceiverSchema.parse>>,
                PromiseFulfilledResult<{ attempts: number; successes: number; observedAt: number }>,
                PromiseFulfilledResult<Awaited<ReturnType<typeof context.controller>>>,
                PromiseFulfilledResult<ResourceSample>,
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
                processMemory: readMemoryObservation(resource.value),
            })
            if (
                snapshot.observations.length >= options.policies &&
                performance.now() - began >= 35000
            )
                break
            await Bun.sleep(100)
        }
        checkBackgroundCancellation(signal)
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
