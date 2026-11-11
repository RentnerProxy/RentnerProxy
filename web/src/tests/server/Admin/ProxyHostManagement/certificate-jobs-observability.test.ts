import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

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
