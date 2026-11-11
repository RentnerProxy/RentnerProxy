import { drainWorker } from '@/server/Foundation/worker-lifecycle.server.ts'
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
    signal?: AbortSignal,
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
    signal?.throwIfAborted()
    return dispatch ? controller.issue(dispatch.certificateId, dispatch.request, signal) : null
}

async function loadOrIssueJobCertificate(
    job: CertificateJobRow,
    controller: typeof certificateJobController,
    signal?: AbortSignal,
): Promise<ControllerCertificateMetadata | null> {
    if (!job.certificateId) throw new CertificateJobDomainError('certificate_deleted')
    let metadata: ControllerCertificateMetadata
    try {
        metadata = await controller.get(job.certificateId, signal)
    } catch (error) {
        if (!(error instanceof CertificateDomainError) || error.code !== 'certificate_not_found')
            throw error
        if (job.controllerOperationId || job.assignedRevision)
            throw new CertificateJobDomainError('certificate_not_found')
        return issueJobCertificate(job, controller, signal)
    }
    if (metadata.currentOperation?.dnsMode === 'manual' && metadata.operation !== 'idle')
        return metadata
    if (metadata.candidate && job.retryRequested) return controller.renew(job.certificateId, signal)
    if (
        metadata.status !== 'valid' &&
        metadata.operation === 'idle' &&
        !metadata.candidate &&
        job.retryRequested &&
        (metadata.currentOperation?.id ?? null) === job.controllerOperationId
    ) {
        return issueJobCertificate(job, controller, signal)
    }
    return metadata
}

/** Fence job context before controller work and count success only after binding/runtime confirmation; release deferred and failed work durably. */
async function processCertificateJob(
    job: CertificateJobRow,
    controller = certificateJobController,
    runtime = certificateJobRuntime,
    signal?: AbortSignal,
): Promise<WorkerOutcome> {
    try {
        const ready = await withCertificateJobClaim(job, async (transaction, current) => {
            await validateCertificateJobContext(transaction, current)
            return true
        })
        if (!ready) return 'deferred'
        signal?.throwIfAborted()
        const metadata = await loadOrIssueJobCertificate(job, controller, signal)
        signal?.throwIfAborted()
        if (!metadata) return 'deferred'
        const manualState = getCertificateJobManualDnsState(metadata)
        if (
            metadata.status === 'valid' &&
            !metadata.candidate &&
            (manualState === 'not_manual' || manualState === 'complete')
        ) {
            if (!(await bindIssuedJobCertificate(job, metadata))) return 'deferred'
            signal?.throwIfAborted()
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
        if (signal?.aborted) {
            await releaseCertificateJob(job, {})
            return 'cancelled'
        }
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

/** Claim up to four jobs concurrently and aggregate failure before success; empty or pending batches remain deferred. */
export async function runCertificateJobsOnce(
    controller = certificateJobController,
    runtime = certificateJobRuntime,
    signal?: AbortSignal,
): Promise<WorkerOutcome> {
    if (signal?.aborted) return 'cancelled'
    const claims = await Promise.allSettled(Array.from({ length: 4 }, () => claimCertificateJob()))
    const results = await Promise.allSettled(
        claims.map(async (claim): Promise<WorkerOutcome> => {
            if (claim.status === 'rejected') throw claim.reason
            const job = claim.value
            if (!job) return 'deferred'
            if (signal?.aborted) {
                await releaseCertificateJob(job, {})
                return 'cancelled'
            }
            return processCertificateJob(job, controller, runtime, signal)
        }),
    )
    const failed = results.find((result) => result.status === 'rejected')
    if (failed?.status === 'rejected') throw failed.reason
    const outcomes = new Set(
        results.flatMap((result) => (result.status === 'fulfilled' ? [result.value] : [])),
    )
    return outcomes.has('failure')
        ? 'failure'
        : outcomes.has('cancelled')
          ? 'cancelled'
          : outcomes.has('success')
            ? 'success'
            : 'deferred'
}

let timer: ReturnType<typeof setTimeout> | null = null
let running: Promise<void> | null = null
let stopped = true
let controller = new AbortController()
let unavailable = false

/** Start one nonoverlapping observed tick chain with five-second spacing after settlement. */
export function startCertificateJobWorker(): void {
    if (!stopped || running) return
    stopped = false
    controller = new AbortController()
    const tick = () => {
        if (stopped) return
        running = observeWorkerOperation(
            'certificate_jobs',
            () =>
                runCertificateJobsOnce(
                    certificateJobController,
                    certificateJobRuntime,
                    controller.signal,
                ),
            controller.signal,
            (outcome) => outcome,
        )
            .then(() => {
                unavailable = false
            })
            .catch(() => {
                if (!unavailable && !controller.signal.aborted)
                    console.warn('[certificates] job worker unavailable')
                unavailable = true
            })
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

/** Prevent rescheduling, cancel the pending timer, and await any in-flight job batch before returning. */
export async function stopCertificateJobWorker(): Promise<void> {
    stopped = true
    controller.abort()
    if (timer) clearTimeout(timer)
    timer = null
    await drainWorker(Promise.resolve(running))
}

workerObservability.sample('certificate_jobs', { concurrency: 4 })
