import assert from 'node:assert/strict'
import { join } from 'node:path'
import {
    prepareCertificateFixture,
    exerciseCertificates,
} from '../runtime-reliability/certificates.ts'
import { requestHttp3Client } from '../smoke/http3/client.ts'
import type { createHarness } from '../runtime-reliability/harness.ts'
import type { FixtureResult } from '../runtime-reliability/Types/harness.types.ts'
import type { BackgroundOptions } from './Types/background.types.ts'
import { backgroundSnapshotSchema } from './background.validation.ts'

export async function exerciseExtendedBackgroundRecovery(
    harness: Awaited<ReturnType<typeof createHarness>>,
    options: BackgroundOptions,
    paused: (operation: () => Promise<FixtureResult>) => Promise<FixtureResult>,
    checkCancellation: () => void = () => {},
) {
    checkCancellation()
    const context = harness.context
    const extra = {
        policyCount: options.policies,
        seed: options.seed,
        duePattern: options.pattern,
        backgroundProfile: options.profile,
    }
    const started = performance.now()
    checkCancellation()
    await prepareCertificateFixture(context)
    checkCancellation()
    await exerciseCertificates(context, 0)
    checkCancellation()
    const workload = backgroundSnapshotSchema.parse(
        (await context.fixture('background-start', extra)).background,
    )
    const queued = (await paused(() => context.fixture('background-job-prepare', extra)))
        .recovery as { jobId?: string; queuedForRestore?: boolean }
    assert.ok(queued.jobId && queued.queuedForRestore)
    checkCancellation()
    await harness.backupRestore()
    checkCancellation()
    await context.docker([
        'exec',
        context.container,
        'bun',
        '--no-env-file',
        '-e',
        `await Bun.write(${JSON.stringify('/tmp/background-origin-' + context.runId)},${JSON.stringify(String(workload.origin))});await import('node:fs/promises').then(fs=>fs.chmod(${JSON.stringify('/tmp/background-origin-' + context.runId)},0o600))`,
    ])
    checkCancellation()
    const resumed = (await context.fixture('background-job-resume', extra)).recovery as {
        jobId?: string
        leaseCleared?: boolean
    }
    assert.equal(resumed.jobId, queued.jobId)
    assert.equal(resumed.leaseCleared, true)
    let final: FixtureResult | undefined
    checkCancellation()
    await context.waitFor(
        async () => {
            checkCancellation()
            final = await context.fixture('jobs-read')
            assert.equal(final.currentJob?.id, queued.jobId)
            return final.currentJob?.stage === 'applied'
        },
        'Restored active certificate job reaches its durable applied fence',
        150000,
    )
    assert.ok(final?.currentJob?.certificateId)
    checkCancellation()
    await context.synced(await context.fixture('snapshot'))
    checkCancellation()
    const metadata = await context.controller(
        '/internal/v1/certificates/' + final.currentJob.certificateId,
    )
    assert.equal(metadata.status, 200)
    assert.equal(typeof metadata.body.fingerprint, 'string')
    checkCancellation()
    const tls = await requestHttp3Client(context.command, {
        image: context.http3Image,
        caFile: join(context.temp, 'reliability-issuance-ca.pem'),
        hostname: context.domain,
        port: context.tlsPort,
        address: context.container,
        network: context.network,
        protocol: '--http1.1',
    })
    assert.equal(tls.status, 200)
    assert.equal(
        tls.fingerprint,
        metadata.body.fingerprint,
        'Restored job activates its exact durable certificate',
    )
    checkCancellation()
    const quiescent = backgroundSnapshotSchema.parse(
        (await context.fixture('background-quiesce', extra)).background,
    )
    checkCancellation()
    await Bun.sleep(3500)
    checkCancellation()
    const stable = backgroundSnapshotSchema.parse(
        (await context.fixture('background-read', extra)).background,
    )
    assert.ok(stable.policies.every((policy) => !policy.enabled && !policy.leased))
    assert.equal(
        stable.observations.length,
        quiescent.observations.length,
        'Cancelled pre-maintenance probes cannot append stale outcomes after the final fence',
    )
    const audit = (await paused(() => context.fixture('background-audit-replay', extra))).recovery
    checkCancellation()
    const elapsedMs = performance.now() - started
    assert.ok(
        elapsedMs <= 600000,
        'Extended recovery exceeds its predeclared ten-minute fault budget',
    )
    return {
        status: 'met',
        elapsedMs,
        interruptedCertificateJobs: true,
        activeRestore: {
            jobId: queued.jobId,
            finalStage: 'applied',
            strandedMonitoringLeases: 0,
            noLateMonitoringWrites: true,
        },
        audit,
    }
}
