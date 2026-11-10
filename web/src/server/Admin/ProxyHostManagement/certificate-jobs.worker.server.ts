import '@tanstack/react-start/server-only'

import { eq } from 'drizzle-orm'
import { certificateJobs } from '@/db/schema.ts'
import { requestCertificateInputSchema } from '@/features/Admin/CertificateManagement/validation.ts'
import { decryptSecret } from '@/server/Auth/Core/encryption.server.ts'
import {
    getControllerCertificate,
    issueControllerCertificate,
    renewControllerCertificate,
} from '@/server/Controller/certificates.server.ts'
import type { ControllerCertificateMetadata } from '@/server/Controller/Types/certificates.types.ts'
import { CertificateDomainError } from '@/server/Admin/CertificateManagement/certificates.errors.ts'
import { persistControllerCertificatesMetadataInTransaction } from '@/server/Admin/CertificateManagement/certificate-metadata.persistence.ts'
import {
    bindIssuedJobCertificate,
    certificateJobRuntime,
    confirmJobRuntime,
} from './certificate-jobs.binding.server.ts'
import {
    claimCertificateJob,
    releaseCertificateJob,
    withCertificateJobClaim,
} from './certificate-jobs.lease.server.ts'
import {
    CertificateJobDomainError,
    certificateJobDigest,
    validateCertificateJobContext,
} from './certificate-jobs.storage.server.ts'
import { getCertificateJobManualDnsState } from './certificate-jobs.manual.ts'
import type { CertificateJobRow } from './Types/certificate-jobs-storage.types.ts'
import { observeWorkerOperation } from '@/server/Foundation/worker-operation.ts'
import { workerObservability } from '@/server/Foundation/worker-observability.ts'
import type { WorkerOutcome } from '@/server/Foundation/Types/worker-observability.types.ts'

const certificateJobController = {
    get: getControllerCertificate,
    issue: issueControllerCertificate,
    renew: renewControllerCertificate,
}

async function saveJobCertificateMetadata(
    job: CertificateJobRow,
    metadata: ControllerCertificateMetadata,
): Promise<void> {
    await withCertificateJobClaim(job, async (transaction, current) => {
        await validateCertificateJobContext(transaction, current)
        const manualState = getCertificateJobManualDnsState(metadata)
        await persistControllerCertificatesMetadataInTransaction(transaction, [metadata], false)
        await transaction
            .update(certificateJobs)
            .set({
                stage:
                    manualState === 'failed' ||
                    (metadata.status === 'failed' &&
                        metadata.operation === 'idle' &&
                        !metadata.candidate)
                        ? 'failed'
                        : 'issuing',
                controllerStage: metadata.currentOperation?.stage ?? null,
                controllerOperationId:
                    metadata.currentOperation?.id ?? current.controllerOperationId,
                lastErrorCode:
                    manualState === 'pending'
                        ? null
                        : (metadata.currentOperation?.manualDns?.lastErrorCode ??
                          metadata.candidate?.lastErrorCode ??
                          metadata.lastErrorCode),
                retryRequested: false,
                updatedAt: new Date(),
            })
            .where(eq(certificateJobs.id, current.id))
    })
}

async function issueJobCertificate(
    job: CertificateJobRow,
    controller: typeof certificateJobController,
): Promise<ControllerCertificateMetadata | null> {
    const dispatch = await withCertificateJobClaim(job, async (transaction, current) => {
        await validateCertificateJobContext(transaction, current)
        if (!current.requestCiphertext || !current.requestIv || !current.certificateId)
            throw new CertificateJobDomainError('dns_credentials_unavailable')
        const plaintext = await decryptSecret(
            { ciphertext: current.requestCiphertext, iv: current.requestIv },
            'certificate-binding-job:' + current.id,
        )
        const request = requestCertificateInputSchema.parse(JSON.parse(plaintext))
        if (
            certificateJobDigest(request.domains.toSorted()) !==
            certificateJobDigest(current.domains)
        )
            throw new CertificateJobDomainError('domain_mismatch')
        await transaction
            .update(certificateJobs)
            .set({ stage: 'issuing', updatedAt: new Date() })
            .where(eq(certificateJobs.id, current.id))
        return { request, certificateId: current.certificateId }
    })
    return dispatch ? controller.issue(dispatch.certificateId, dispatch.request) : null
}

async function loadOrIssueJobCertificate(
    job: CertificateJobRow,
    controller: typeof certificateJobController,
): Promise<ControllerCertificateMetadata | null> {
    if (!job.certificateId) throw new CertificateJobDomainError('certificate_deleted')
    let metadata: ControllerCertificateMetadata
    try {
        metadata = await controller.get(job.certificateId)
    } catch (error) {
        if (!(error instanceof CertificateDomainError) || error.code !== 'certificate_not_found')
            throw error
        if (job.controllerOperationId || job.assignedRevision)
            throw new CertificateJobDomainError('certificate_not_found')
        return issueJobCertificate(job, controller)
    }
    if (metadata.currentOperation?.dnsMode === 'manual' && metadata.operation !== 'idle')
        return metadata
    if (metadata.candidate && job.retryRequested) return controller.renew(job.certificateId)
    if (
        metadata.status !== 'valid' &&
        metadata.operation === 'idle' &&
        !metadata.candidate &&
        job.retryRequested &&
        (metadata.currentOperation?.id ?? null) === job.controllerOperationId
    ) {
        return issueJobCertificate(job, controller)
    }
    return metadata
}

async function processCertificateJob(
    job: CertificateJobRow,
    controller = certificateJobController,
    runtime = certificateJobRuntime,
): Promise<WorkerOutcome> {
    try {
        const ready = await withCertificateJobClaim(job, async (transaction, current) => {
            await validateCertificateJobContext(transaction, current)
            return true
        })
        if (!ready) return 'deferred'
        const metadata = await loadOrIssueJobCertificate(job, controller)
        if (!metadata) return 'deferred'
        const manualState = getCertificateJobManualDnsState(metadata)
        if (
            metadata.status === 'valid' &&
            !metadata.candidate &&
            (manualState === 'not_manual' || manualState === 'complete')
        ) {
            if (!(await bindIssuedJobCertificate(job, metadata))) return 'deferred'
            if (await confirmJobRuntime(job, runtime)) return 'success'
            await releaseCertificateJob(job, {
                stage: 'applying',
                lastErrorCode: 'runtime_apply_failed',
                retryRequested: false,
            })
            return 'failure'
        }
        await saveJobCertificateMetadata(job, metadata)
        await releaseCertificateJob(
            job,
            manualState === 'pending' ? { nextAttemptAt: new Date(Date.now() + 30_000) } : {},
        )
        return manualState === 'failed' || metadata.status === 'failed' ? 'failure' : 'deferred'
    } catch (error) {
        if (error instanceof CertificateJobDomainError) {
            await releaseCertificateJob(job, {
                stage: 'needs_attention',
                lastErrorCode: error.code,
                retryRequested: false,
            })
            workerObservability.count('certificate_jobs', 'exhausted')
            return 'failure'
        }
        const code = error instanceof CertificateDomainError ? error.code : 'controller_unavailable'
        const temporary =
            code === 'controller_unavailable' ||
            code === 'operation_in_progress' ||
            code === 'certificate_store_unavailable' ||
            code === 'runtime_apply_failed'
        await releaseCertificateJob(job, {
            ...(temporary ? {} : { stage: 'failed' }),
            lastErrorCode: code,
        })
        workerObservability.count('certificate_jobs', temporary ? 'retry' : 'exhausted')
        return 'failure'
    }
}

export async function runCertificateJobsOnce(
    controller = certificateJobController,
    runtime = certificateJobRuntime,
): Promise<WorkerOutcome> {
    const claims = await Promise.allSettled(Array.from({ length: 4 }, () => claimCertificateJob()))
    // Admission can defer one slot while a sibling has already acquired a durable lease.
    // Drain every successful claim before reporting the failed admission to the owner.
    const jobs = claims.flatMap((claim) =>
        claim.status === 'fulfilled' && claim.value ? [claim.value] : [],
    )
    const outcomes = await Promise.all(
        jobs.map((job) => processCertificateJob(job, controller, runtime)),
    )
    const failed = claims.find((claim) => claim.status === 'rejected')
    if (failed?.status === 'rejected') throw failed.reason
    return outcomes.includes('failure')
        ? 'failure'
        : outcomes.includes('success')
          ? 'success'
          : 'deferred'
}

let timer: ReturnType<typeof setTimeout> | null = null
let running: Promise<void> | null = null
let stopped = true

export function startCertificateJobWorker(): void {
    if (!stopped) return
    stopped = false
    const tick = () => {
        if (stopped) return
        running = observeWorkerOperation(
            'certificate_jobs',
            runCertificateJobsOnce,
            undefined,
            (outcome) => outcome,
        )
            .then(() => undefined)
            .catch(() => undefined)
            .finally(() => {
                running = null
                if (!stopped) {
                    timer = setTimeout(tick, 5_000)
                    timer.unref()
                }
            })
    }
    tick()
}

export async function stopCertificateJobWorker(): Promise<void> {
    stopped = true
    if (timer) clearTimeout(timer)
    timer = null
    await running
}

workerObservability.sample('certificate_jobs', { concurrency: 4 })
