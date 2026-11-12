import { createHash, randomUUID } from 'node:crypto'
import { z } from 'zod'
import { runDetachedTask } from '@/lib/Async/runDetachedTask.ts'
import type { HostMonitoringClaim } from './Types/host-monitoring.repository.types.ts'

const timestamp = z.number().finite().nonnegative()
export const hostMonitoringCaptureSchema = z.strictObject({
    version: z.literal(2),
    runId: z.string().regex(/^[a-f0-9]{12}$/u),
    sourceSha: z.string().regex(/^[a-f0-9]{40}$/u),
    pid: z.number().int().positive(),
    processStartTicks: z.string().regex(/^\d+$/u),
    epoch: z.uuid(),
    publication: z.strictObject({
        generation: z.number().int().nonnegative().safe(),
        fd: z.number().int().min(3).max(1_048_575).nullable(),
    }),
    createdAt: timestamp,
    expiresAt: timestamp,
    overflowed: z.boolean(),
    clockRegressed: z.boolean(),
    expired: z.boolean(),
    events: z
        .array(
            z.strictObject({
                kind: z.enum(['admitted', 'started', 'cancelled']),
                policyId: z.uuid(),
                policyVersion: z.uuid(),
                attemptHash: z.string().regex(/^[a-f0-9]{64}$/u),
                dueAt: timestamp,
                at: timestamp,
                monotonicMs: timestamp,
            }),
        )
        .max(20_000),
})
export type HostMonitoringCapture = z.output<typeof hostMonitoringCaptureSchema>

/** Optional bounded observer: claim-return and adapter-start are observations, never SQL-commit or native network timestamps. */
export function createHostMonitoringCapture(options: {
    runId: string
    sourceSha: string
    pid: number
    processStartTicks: string
    write: (capture: HostMonitoringCapture, isCurrent: () => boolean) => Promise<void>
    /** Synchronously revoke only the published lease, never an in-flight candidate descriptor. */
    invalidate?: () => void
    now?: () => number
    monotonic?: () => number
    capacity?: number
    lifetimeMs?: number
}) {
    const now = options.now ?? Date.now
    const monotonic = options.monotonic ?? performance.now.bind(performance)
    const createdAt = now()
    const capture = hostMonitoringCaptureSchema.parse({
        version: 2,
        runId: options.runId,
        sourceSha: options.sourceSha,
        pid: options.pid,
        processStartTicks: options.processStartTicks,
        epoch: randomUUID(),
        publication: { generation: 0, fd: null },
        createdAt,
        expiresAt: createdAt + Math.min(options.lifetimeMs ?? 600_000, 600_000),
        overflowed: false,
        clockRegressed: false,
        expired: false,
        events: [],
    })
    const capacity = Math.max(1, Math.min(options.capacity ?? 20_000, 20_000))
    let lastWall = createdAt
    let lastMonotonic = monotonic()
    let timer: ReturnType<typeof setTimeout> | undefined
    let writing: Promise<void> | undefined
    let dirty = false
    let failed = false
    let generation = 0
    let expiryTimer: ReturnType<typeof setTimeout> | undefined
    /** Publish terminal state before revocation, and never retry a failed close against a possibly reused descriptor. */
    function terminate(revoke = true) {
        const alreadyFailed = failed
        failed = true
        generation++
        capture.publication.generation = generation
        dirty = false
        if (timer) clearTimeout(timer)
        timer = undefined
        if (expiryTimer) clearTimeout(expiryTimer)
        expiryTimer = undefined
        if (revoke && !alreadyFailed) {
            try {
                options.invalidate?.()
            } catch {
                /* Linux close retires the descriptor even on late I/O errors; never retry it. */
            }
        }
    }
    expiryTimer = setTimeout(
        () => {
            capture.expired = true
            terminate()
        },
        Math.max(1, capture.expiresAt - createdAt),
    )
    expiryTimer.unref()
    /** One writer and one dirty bit; disk failure stops capture without affecting worker authority. */
    async function flush(): Promise<void> {
        if (timer) clearTimeout(timer)
        timer = undefined
        if (writing) {
            await writing
            if (dirty) await flush()
            return
        }
        if (!dirty || failed) return
        dirty = false
        const snapshot = structuredClone(capture)
        const writingGeneration = generation
        writing = options
            .write(
                snapshot,
                () =>
                    !failed &&
                    !dirty &&
                    generation === writingGeneration &&
                    !capture.expired &&
                    !capture.overflowed &&
                    !capture.clockRegressed &&
                    now() <= capture.expiresAt,
            )
            .catch(() => terminate())
            .finally(() => {
                writing = undefined
            })
        await writing
        if (dirty) await flush()
    }
    function record(
        kind: HostMonitoringCapture['events'][number]['kind'],
        claim: HostMonitoringClaim,
    ) {
        // A pending or failed flush must never leave an older readable snapshot authoritative.
        generation++
        capture.publication.generation = generation
        if (failed) return
        try {
            options.invalidate?.()
        } catch {
            terminate(false)
            return
        }
        if (capture.overflowed || capture.expired) return
        const at = now(),
            monotonicMs = monotonic()
        capture.clockRegressed ||= at < lastWall || monotonicMs < lastMonotonic
        lastWall = at
        lastMonotonic = monotonicMs
        if (at > capture.expiresAt) capture.expired = true
        else if (capture.events.length >= capacity) capture.overflowed = true
        else
            capture.events.push({
                kind,
                policyId: claim.policy.id,
                policyVersion: claim.policy.version,
                attemptHash: createHash('sha256')
                    .update(options.runId + ':' + claim.leaseToken)
                    .digest('hex'),
                dueAt: claim.policy.nextCheckAt.getTime(),
                at,
                monotonicMs,
            })
        dirty = true
        if (!timer && !writing) {
            timer = setTimeout(() => {
                runDetachedTask(flush, () => terminate())
            }, 25)
            timer.unref()
        }
    }
    return { record, flush }
}
