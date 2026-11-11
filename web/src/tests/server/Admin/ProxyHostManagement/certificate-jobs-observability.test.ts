import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test.each([
    { reason: 'busy', cleanupFails: false },
    { reason: 'deadline', cleanupFails: false },
    { reason: 'busy', cleanupFails: true },
])(
    'critical SQL pressure defers an owned job without changing controller errors or retry intent: %j',
    async ({ reason, cleanupFails }) => {
        const output = await runBunScript(
            `
            import { mock } from 'bun:test'
            process.env.DATABASE_URL = 'postgresql://unit:unit@127.0.0.1:1/rentnerproxy_unit'
            const { ResourceAdmissionError } = await import('./server/Foundation/resource-admission.ts')
            const job = { id: 'job', certificateId: 'certificate', stage: 'issuing', lastErrorCode: 'retained_provider_error', retryRequested: true, leaseToken: 'owned' }
            let claimed = false, network = 0
            const patches = []
            mock.module('./server/Admin/ProxyHostManagement/certificate-jobs.lease.server.ts', () => ({
                claimCertificateJob: async () => claimed ? null : (claimed = true, job),
                withCertificateJobClaim: async () => { throw new ResourceAdmissionError('${reason}') },
                releaseCertificateJob: async (_, changes) => {
                    patches.push(changes)
                    if (${cleanupFails}) throw new ResourceAdmissionError('deadline')
                    Object.assign(job, changes, { leaseToken: null })
                },
            }))
            mock.module('./server/Controller/certificates.server.ts', () => ({
                getControllerCertificate: async () => { network++; throw new Error('unexpected controller call') },
                issueControllerCertificate: async () => { network++ }, renewControllerCertificate: async () => { network++ },
            }))
            const { workerObservability } = await import('./server/Foundation/worker-observability.ts')
            const { observeWorkerOperation } = await import('./server/Foundation/worker-operation.ts')
            const { runCertificateJobsOnce } = await import('./server/Admin/ProxyHostManagement/certificate-jobs.worker.server.ts')
            const outcome = await observeWorkerOperation('certificate_jobs', () => runCertificateJobsOnce(), undefined, value => value)
            const worker = workerObservability.snapshot().workers.find(value => value.id === 'certificate_jobs')
            console.log(JSON.stringify({ outcome, job, patches, network, counters: worker.counters, lastSuccessAt: worker.lastSuccessAt }))
        `,
            { cwd: new URL('../../../..', import.meta.url) },
        )
        const result = JSON.parse(output.trim().split('\n').at(-1)!)
        expect(result).toMatchObject({
            outcome: 'deferred',
            network: 0,
            patches: [{}],
            lastSuccessAt: null,
            job: {
                stage: 'issuing',
                lastErrorCode: 'retained_provider_error',
                retryRequested: true,
                leaseToken: cleanupFails ? 'owned' : null,
            },
            counters: {
                deferred: 1,
                failure: 0,
                success: 0,
                retry: cleanupFails ? 2 : 1,
                admissionRejected: cleanupFails ? 2 : 1,
            },
        })
    },
)

test('saturated real critical admission defers all four claim producers without SQL or controller dispatch', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        process.env.DATABASE_URL = 'postgresql://unit:unit@127.0.0.1:1/rentnerproxy_unit'
        let statements = 0
        mock.module('./server/Auth/Core/database.server.ts', () => ({
            getAuthDatabase: () => ({ transaction: async work => work({ execute: async () => { statements++; return [] } }) }),
        }))
        const { getBackgroundDatabase, getBackgroundDatabaseResourceSnapshot } = await import('./server/Foundation/background-database.server.ts')
        const gate = Promise.withResolvers(), entered = Promise.withResolvers()
        let holders = 0
        const held = Array.from({ length: 2 }, () => getBackgroundDatabase('critical').transaction(async () => { if (++holders === 2) entered.resolve(); await gate.promise }))
        await entered.promise
        const queued = Array.from({ length: 8 }, () => getBackgroundDatabase('critical').transaction(async () => true))
        const { workerObservability } = await import('./server/Foundation/worker-observability.ts')
        const { observeWorkerOperation } = await import('./server/Foundation/worker-operation.ts')
        const { runCertificateJobsOnce } = await import('./server/Admin/ProxyHostManagement/certificate-jobs.worker.server.ts')
        const outcome = await observeWorkerOperation('certificate_jobs', () => runCertificateJobsOnce(), undefined, value => value)
        const pressure = getBackgroundDatabaseResourceSnapshot().critical
        const beforeDrain = statements
        gate.resolve()
        await Promise.all([...held, ...queued])
        const worker = workerObservability.snapshot().workers.find(value => value.id === 'certificate_jobs')
        console.log(JSON.stringify({ outcome, pressure, beforeDrain, counters: worker.counters }))
    `,
        { cwd: new URL('../../../..', import.meta.url) },
    )
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toMatchObject({
        outcome: 'deferred',
        beforeDrain: 2,
        pressure: { running: 2, pending: 8, busy: 4 },
        counters: { deferred: 1, failure: 0, success: 0, retry: 4, admissionRejected: 4 },
    })
})

test('failed claims still await late sibling leases and cancellation cleanup before settling', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        process.env.DATABASE_URL = 'postgresql://unit:unit@127.0.0.1:1/rentnerproxy_unit'
        const lateClaim = Promise.withResolvers(), cleanup = Promise.withResolvers()
        let claims = 0, releases = 0, settled = false
        mock.module('./server/Admin/ProxyHostManagement/certificate-jobs.lease.server.ts', () => ({
            claimCertificateJob: async () => {
                claims++
                if (claims === 1) throw new Error('claim_failed')
                return claims === 2 ? lateClaim.promise : null
            },
            releaseCertificateJob: async () => { releases++; await cleanup.promise },
            withCertificateJobClaim: async () => { throw new Error('late job must not execute') },
        }))
        const { runCertificateJobsOnce } = await import('./server/Admin/ProxyHostManagement/certificate-jobs.worker.server.ts')
        const cancellation = new AbortController()
        const running = runCertificateJobsOnce(undefined, undefined, cancellation.signal)
        void running.then(() => { settled = true }, () => { settled = true })
        const flush = async () => { for (let index = 0; index < 30; index++) await Promise.resolve() }
        await flush()
        const beforeClaim = settled
        cancellation.abort()
        lateClaim.resolve({ id: 'late-job', leaseToken: 'original-token' })
        await flush()
        const beforeCleanup = settled
        cleanup.resolve()
        let error
        try { await running } catch (failure) { error = failure.message }
        console.log(JSON.stringify({ beforeClaim, beforeCleanup, releases, error }))
    `,
        { cwd: new URL('../../../..', import.meta.url) },
    )
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
        beforeClaim: false,
        beforeCleanup: false,
        releases: 1,
        error: 'claim_failed',
    })
})

test('empty certificate batches and caught controller failures never report successful progress', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        mock.module('@tanstack/react-start/server-only', () => ({}))
        let enabled = false, claimed = false
        const released = Promise.withResolvers()
        const job = { id: 'fixture-job', certificateId: 'fixture-certificate' }
        mock.module('./server/Admin/ProxyHostManagement/certificate-jobs.lease.server.ts', () => ({
            claimCertificateJob: async () => enabled && !claimed ? (claimed = true, job) : null,
            releaseCertificateJob: async () => { released.resolve() },
            withCertificateJobClaim: async (current, work) => work({}, current),
        }))
        mock.module('./server/Admin/ProxyHostManagement/certificate-jobs.storage.server.ts', () => ({
            CertificateJobDomainError: class extends Error {},
            certificateJobDigest: () => '', validateCertificateJobContext: async () => {},
        }))
        mock.module('./server/Admin/ProxyHostManagement/certificate-jobs.binding.server.ts', () => ({
            certificateJobRuntime: {}, bindIssuedJobCertificate: async () => false, confirmJobRuntime: async () => false,
        }))
        const { CertificateDomainError } = await import('./server/Admin/CertificateManagement/certificates.errors.ts')
        const unavailable = async () => { throw new CertificateDomainError('controller_unavailable') }
        mock.module('./server/Controller/certificates.server.ts', () => ({
            getControllerCertificate: unavailable, issueControllerCertificate: unavailable, renewControllerCertificate: unavailable,
        }))
        const { workerObservability } = await import('./server/Foundation/worker-observability.ts')
        const { runCertificateJobsOnce, startCertificateJobWorker, stopCertificateJobWorker } = await import('./server/Admin/ProxyHostManagement/certificate-jobs.worker.server.ts')
        const empty = await runCertificateJobsOnce()
        enabled = true
        startCertificateJobWorker()
        await released.promise
        await stopCertificateJobWorker()
        const worker = workerObservability.snapshot().workers.find(item => item.id === 'certificate_jobs')
        console.log(JSON.stringify({ empty, state: worker.state, success: worker.counters.success, failure: worker.counters.failure,
            retry: worker.counters.retry, lastSuccessAt: worker.lastSuccessAt }))
    `,
        { cwd: new URL('../../../..', import.meta.url) },
    )
    const result = JSON.parse(output.trim().split('\n').at(-1)!)
    expect(result).toEqual({
        empty: 'deferred',
        state: 'degraded',
        success: 0,
        failure: 1,
        retry: 1,
        lastSuccessAt: null,
    })
})
