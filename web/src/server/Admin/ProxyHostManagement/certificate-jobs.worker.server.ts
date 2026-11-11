import { drainWorker } from '@/server/Foundation/worker-lifecycle.server.ts'
import '@tanstack/react-start/server-only'

import { eq } from 'drizzle-orm'
import { withBackgroundDatabaseContext } from '@/db/background-admission.ts'
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
    releaseCertificateJob as releaseClaimedCertificateJob,
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
import { ResourceAdmissionError } from '@/server/Foundation/resource-admission.ts'

const certificateJobController = {
    get: getControllerCertificate,
    issue: issueControllerCertificate,
    renew: renewControllerCertificate,
}

/** SQL pressure defers existing durable work without blaming the controller or changing retry intent. */
function admissionOutcome(error: ResourceAdmissionError): WorkerOutcome {
    if (error.reason === 'cancelled') return 'cancelled'
    workerObservability.count('certificate_jobs', 'admissionRejected')
    workerObservability.count('certificate_jobs', 'retry')
    return 'deferred'
}

/** An owned lease remains unsafe for restore until its original fenced release succeeds. */
class CertificateJobCleanupError extends Error {
    constructor(cause: unknown) {
        super('Certificate job lease cleanup failed', { cause })
        this.name = 'CertificateJobCleanupError'
    }
}

const pendingReleases = new Map<
    CertificateJobRow,
    {
        changes: Parameters<typeof releaseClaimedCertificateJob>[1]
        error: CertificateJobCleanupError
    }
>()

/** Release an owned lease after worker cancellation while retaining SQL admission and the original token fence. */
async function releaseCertificateJob(
    ...args: Parameters<typeof releaseClaimedCertificateJob>
): Promise<void> {
    try {
        await withBackgroundDatabaseContext(() => releaseClaimedCertificateJob(...args), null)
        pendingReleases.delete(args[0])
    } catch (cause) {
        const error = new CertificateJobCleanupError(cause)
        pendingReleases.set(args[0], { changes: args[1], error })
        throw error
    }
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

/** Validate and decrypt only under the current job claim, persist issuing intent, then check cancellation before controller dispatch. */
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

/** Preserve existing operation identity and retry intent while passing cancellation through controller reads, issuance, and renewal. */
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

/** Fence job context before controller work, release deferred/failed/cancelled leases durably, and count success only after binding/runtime confirmation. */
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
        if (error instanceof CertificateJobCleanupError) throw error
        if (signal?.aborted) {
            await releaseCertificateJob(job, {})
            return 'cancelled'
        }
        if (error instanceof ResourceAdmissionError) {
            const outcome = admissionOutcome(error)
            await releaseCertificateJob(job, {})
            return outcome
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

/** Settle all four claim attempts and their late lease cleanup before propagating failure; aggregate real outcomes without inventing empty-batch success. */
export async function runCertificateJobsOnce(
    controller = certificateJobController,
    runtime = certificateJobRuntime,
    signal?: AbortSignal,
): Promise<WorkerOutcome> {
    if (signal?.aborted) return 'cancelled'
    // Reuse the next supervised tick for bounded recovery; never admit more leases
    // while an earlier batch still owns an unconfirmed release.
    const recovered = await Promise.allSettled(
        [...pendingReleases].map(([job, retained]) => releaseCertificateJob(job, retained.changes)),
    )
    const recoveryFailure = recovered.find((result) => result.status === 'rejected')
    if (recoveryFailure?.status === 'rejected') throw recoveryFailure.reason
    if (signal?.aborted) return 'cancelled'
    const claims = await Promise.allSettled(Array.from({ length: 4 }, () => claimCertificateJob()))
    const results = await Promise.allSettled(
        claims.map(async (claim): Promise<WorkerOutcome> => {
            try {
                if (claim.status === 'rejected') throw claim.reason
                const job = claim.value
                if (!job) return 'deferred'
                if (signal?.aborted) {
                    await releaseCertificateJob(job, {})
                    return 'cancelled'
                }
                return await processCertificateJob(job, controller, runtime, signal)
            } catch (error) {
                if (!(error instanceof ResourceAdmissionError)) throw error
                const outcome = admissionOutcome(error)
                return signal?.aborted ? 'cancelled' : outcome
            }
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

/** Start one nonoverlapping observed batch chain with fresh cancellation, five-second spacing after settlement, and bounded unavailable logging. */
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

/** Latch stop, cancel the pending timer and abort controller I/O before awaiting the bounded current batch drain. */
export async function stopCertificateJobWorker(): Promise<void> {
    stopped = true
    controller.abort()
    if (timer) clearTimeout(timer)
    timer = null
    await drainWorker(
        (async () => {
            await running
            const retained = pendingReleases.values().next().value
            if (retained) throw retained.error
        })(),
    )
}

workerObservability.sample('certificate_jobs', { concurrency: 4 })
