import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test.each([
    ['in-flight', 'admission'],
    ['late claims', 'admission'],
    ['in-flight', 'sql'],
    ['late claims', 'sql'],
] as const)(
    'owned certificate cleanup failure fences restore and recovers only after the original SQL fence: %s / %s',
    async (phase, failureKind) => {
        const output = await runBunScript(
            `
            import assert from 'node:assert/strict'
            import { mock } from 'bun:test'
            process.env.DATABASE_URL = 'postgresql://unit:unit@127.0.0.1:1/rentnerproxy_unit'
            mock.module('@tanstack/react-start/server-only', () => ({}))
            const { ResourceAdmissionError } = await import('./server/Foundation/resource-admission.ts')
            const failure = '${failureKind}' === 'admission' ? new ResourceAdmissionError('busy') : new Error('release SQL failed')
            const entered = Promise.withResolvers(), claimsReady = Promise.withResolvers(), cleanup = Promise.withResolvers()
            const jobs = Array.from({ length: 4 }, (_, index) => ({ id: 'job-' + index, certificateId: 'cert-' + index, leaseToken: 'original-' + index }))
            const leases = new Map(jobs.map(job => [job.id, job.leaseToken]))
            let claims = 0, reads = 0, releases = 0, healthy = false, settled = false
            mock.module('./server/Admin/ProxyHostManagement/certificate-jobs.lease.server.ts', () => ({
                claimCertificateJob: async () => {
                    const job = jobs[claims++] ?? null
                    if ('${phase}' === 'late claims' && claims === 4) entered.resolve()
                    if ('${phase}' === 'late claims') await claimsReady.promise
                    return job
                },
                withCertificateJobClaim: async (job, work) => work({}, job),
                releaseCertificateJob: async (job) => {
                    releases++
                    assert.equal(job, jobs.find(original => original.id === job.id))
                    await cleanup.promise
                    if (job === jobs[0] && !healthy) throw failure
                    if (leases.get(job.id) === job.leaseToken) leases.delete(job.id)
                },
            }))
            mock.module('./server/Admin/ProxyHostManagement/certificate-jobs.storage.server.ts', () => ({
                CertificateJobDomainError: class extends Error {}, certificateJobDigest: () => '', validateCertificateJobContext: async () => {},
            }))
            const unexpected = async () => { throw new Error('Cancelled work must not bind or persist metadata') }
            mock.module('./server/Admin/CertificateManagement/certificate-metadata.persistence.ts', () => ({ persistControllerCertificatesMetadataInTransaction: unexpected }))
            mock.module('./server/Admin/ProxyHostManagement/certificate-jobs.binding.server.ts', () => ({
                certificateJobRuntime: {}, bindIssuedJobCertificate: unexpected, confirmJobRuntime: unexpected,
            }))
            mock.module('./server/Controller/certificates.server.ts', () => ({
                issueControllerCertificate: async () => { throw new Error('unexpected issue') },
                renewControllerCertificate: async () => { throw new Error('unexpected renew') },
                getControllerCertificate: async (_id, signal) => {
                    if (++reads === 4) entered.resolve()
                    return new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }))
                },
            }))
            const { startCertificateJobWorker, stopCertificateJobWorker, runCertificateJobsOnce } = await import('./server/Admin/ProxyHostManagement/certificate-jobs.worker.server.ts')
            startCertificateJobWorker()
            await entered.promise
            const stopped = stopCertificateJobWorker()
            void stopped.then(() => { settled = true }, () => { settled = true })
            claimsReady.resolve()
            await Bun.sleep(0)
            assert.equal(settled, false)
            assert.equal(releases, 4)
            cleanup.resolve()
            await assert.rejects(stopped, error => error.cause === failure)
            await assert.rejects(stopCertificateJobWorker(), error => error.cause === failure)
            assert.deepEqual([...leases], [[jobs[0].id, jobs[0].leaseToken]])
            await assert.rejects(runCertificateJobsOnce(), error => error.cause === failure)
            assert.equal(claims, 4) // No admission while an earlier lease remains owned.
            healthy = true
            leases.set(jobs[0].id, 'newer-token')
            assert.equal(await runCertificateJobsOnce(), 'deferred')
            await stopCertificateJobWorker()
            await assert.rejects(stopped, error => error.cause === failure)
            assert.deepEqual([...leases], [[jobs[0].id, 'newer-token']])
            console.log(JSON.stringify({ claims, releases, originalStopFailed: true }))
        `,
            { cwd: new URL('../../../..', import.meta.url) },
        )
        expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
            claims: 8,
            releases: 6,
            originalStopFailed: true,
        })
    },
)

test.each(['in-flight', 'late claims'] as const)(
    'cancelled certificate cleanup retains SQL admission, claim fences and all sibling drains: %s',
    async (phase) => {
        const output = await runBunScript(
            `
            import assert from 'node:assert/strict'
            import { mock } from 'bun:test'
            import { admitDatabaseTransaction, withBackgroundDatabaseContext } from './db/background-admission.ts'
            import { DATABASE_CONNECTION_BUDGET } from './config/database.config.ts'
            process.env.DATABASE_URL = 'postgresql://unit:unit@127.0.0.1:1/rentnerproxy_unit'
            mock.module('@tanstack/react-start/server-only', () => ({}))
            const policy = await Bun.file('./config/background-resources.config.ts').exists()
                ? (await import('./config/background-resources.config.ts')).BACKGROUND_DATABASE_POLICY : null
            const slots = policy?.classes.critical.running ?? DATABASE_CONNECTION_BUDGET.backgroundTransactions
            const entered = Promise.withResolvers(), claimReply = Promise.withResolvers(), released = Promise.withResolvers(), cleanup = Promise.withResolvers()
            const budgetReady = Promise.withResolvers(), freeBudget = Promise.withResolvers()
            const jobs = Array.from({ length: 4 }, (_, index) => ({ id: 'job-' + index, certificateId: 'certificate-' + index, leaseToken: 'original-token-' + index }))
            const leases = new Map(jobs.map(job => [job.id, job.leaseToken]))
            const releaseSql = [], releaseCalls = [], signals = []
            let claims = 0, reads = 0, validations = 0, holding = 0, stopped = false
            mock.module('./server/Admin/ProxyHostManagement/certificate-jobs.lease.server.ts', () => ({
                claimCertificateJob: async () => {
                    const job = jobs[claims++] ?? null
                    if (claims === 4 && '${phase}' === 'late claims') entered.resolve()
                    if ('${phase}' === 'late claims') await claimReply.promise
                    return job
                },
                withCertificateJobClaim: (job, callback) => admitDatabaseTransaction(async () => {
                    validations++; return callback({}, job)
                }),
                releaseCertificateJob: (job, changes) => {
                    releaseCalls.push(job.id)
                    assert.equal(jobs.find(original => original.id === job.id), job)
                    assert.equal(job.leaseToken, 'original-token-' + jobs.indexOf(job))
                    assert.deepEqual(changes, {})
                    return admitDatabaseTransaction(async () => {
                        releaseSql.push(job.id)
                        if (releaseSql.length === Math.min(4, slots)) released.resolve()
                        await cleanup.promise
                        if (leases.get(job.id) === job.leaseToken) leases.delete(job.id)
                    })
                },
            }))
            mock.module('./server/Admin/ProxyHostManagement/certificate-jobs.storage.server.ts', () => ({
                CertificateJobDomainError: class extends Error {}, certificateJobDigest: () => '', validateCertificateJobContext: async () => true,
            }))
            const unexpected = async () => { throw new Error('Cancelled work must not issue, bind or persist metadata') }
            mock.module('./server/Admin/CertificateManagement/certificate-metadata.persistence.ts', () => ({ persistControllerCertificatesMetadataInTransaction: unexpected }))
            mock.module('./server/Admin/ProxyHostManagement/certificate-jobs.binding.server.ts', () => ({
                certificateJobRuntime: {}, bindIssuedJobCertificate: unexpected, confirmJobRuntime: unexpected,
            }))
            mock.module('./server/Controller/certificates.server.ts', () => ({
                issueControllerCertificate: unexpected, renewControllerCertificate: unexpected,
                getControllerCertificate: async (_id, signal) => {
                    reads++; signals.push(signal)
                    if (reads === 4) entered.resolve()
                    return new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }))
                },
            }))
            const { startCertificateJobWorker, stopCertificateJobWorker } = await import('./server/Admin/ProxyHostManagement/certificate-jobs.worker.server.ts')
            startCertificateJobWorker()
            await entered.promise
            const held = Array.from({ length: slots }, () => withBackgroundDatabaseContext(() => admitDatabaseTransaction(async () => {
                if (++holding === slots) budgetReady.resolve()
                await freeBudget.promise
            })))
            await budgetReady.promise
            const stopping = stopCertificateJobWorker().then(() => { stopped = true })
            claimReply.resolve()
            await Bun.sleep(0)
            assert.equal(releaseSql.length, 0) // Detachment must not bypass the real shared gate.
            assert.equal(stopped, false)
            assert.equal(releaseCalls.length, 4)
            leases.set(jobs[0].id, 'newer-owner-token')
            freeBudget.resolve()
            await released.promise
            await Bun.sleep(0)
            assert.equal(stopped, false) // SQL entry alone is not durable release completion.
            cleanup.resolve()
            await Promise.all([stopping, ...held])
            await stopCertificateJobWorker()
            assert.deepEqual([...leases], [[jobs[0].id, 'newer-owner-token']])
            assert.equal(claims, 4)
            assert.equal(validations, '${phase}' === 'in-flight' ? 4 : 0)
            assert.equal(reads, '${phase}' === 'in-flight' ? 4 : 0)
            assert.equal(signals.every(signal => signal.aborted), true)
            console.log(JSON.stringify({ stopped, released: releaseSql.length, claims, validations, reads }))
        `,
            { cwd: new URL('../../../..', import.meta.url) },
        )
        expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
            stopped: true,
            released: 4,
            claims: 4,
            validations: phase === 'in-flight' ? 4 : 0,
            reads: phase === 'in-flight' ? 4 : 0,
        })
    },
)

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
