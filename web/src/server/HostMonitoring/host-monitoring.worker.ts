import { drainWorker, settleWorkerTasks } from '@/server/Foundation/worker-lifecycle.server.ts'
import { WorkerDrainTimeoutError } from '@/server/Foundation/Types/worker-lifecycle.types.ts'
import { HOST_MONITORING_LIMITS } from '@/config/host-monitoring.config.ts'
import type { HostMonitoringWorkerDependencies } from './Types/host-monitoring-worker.types.ts'
import { HostMonitoringCleanupError } from './Types/host-monitoring-worker.types.ts'

/** Refills bounded claim slots independently of slow probes; stop aborts admission and awaits every admitted cleanup. */
export function createHostMonitoringWorker<Claim>(
    dependencies: HostMonitoringWorkerDependencies<Claim>,
) {
    let cancelTimer: (() => void) | null = null
    let admission: Promise<void> | null = null
    let pruning: Promise<void> | null = null
    let stopping: Promise<void> | null = null
    let controller = new AbortController()
    let started = false
    let stopped = false
    let nextPruneAt = 0
    let pollDelayMs: number = HOST_MONITORING_LIMITS.pollIntervalMs
    let wakeRevision = 0
    let unsubscribe: (() => void) | undefined
    let cleanupFailure: { reason: unknown } | undefined
    const failedClaims = new Map<Claim, { reason: unknown }>()
    let recoveryAt = 0
    let recoveryDelay = 1_000
    const running = new Set<Promise<void>>()
    const unavailable = new Set<'claim' | 'due' | 'probe' | 'prune'>()
    const now = dependencies.now ?? Date.now
    const recoveryNow = dependencies.now ?? performance.now.bind(performance)
    const setTimer =
        dependencies.schedule ??
        ((callback, delayMs) => {
            const timer = setTimeout(callback, delayMs)
            timer.unref()
            return () => clearTimeout(timer)
        })

    /** Retain owned failed claims within the existing 64 slots; fence new probes without cancelling healthy siblings. */
    function failCleanup(claim: Claim, error: unknown) {
        const failure = {
            reason: error instanceof HostMonitoringCleanupError ? error.cause : error,
        }
        if (failedClaims.size === 0) {
            recoveryDelay = 1_000
            recoveryAt = recoveryNow() + recoveryDelay
        }
        cleanupFailure ??= failure
        if (!failedClaims.has(claim)) failedClaims.set(claim, failure)
    }

    /** A successful adapter call proves release or supersession in SQL; time alone never releases an owned claim. */
    async function releaseClaims(claims: readonly Claim[]): Promise<void> {
        await Promise.all(
            claims.map(async (claim) => {
                const retained = failedClaims.get(claim)
                try {
                    await dependencies.releaseCancelled(claim)
                    if (failedClaims.get(claim) === retained) failedClaims.delete(claim)
                } catch (error) {
                    failCleanup(claim, error)
                }
            }),
        )
    }

    /** Deduplicates sanitized availability warnings until the failing subsystem recovers. */
    function warn(source: 'claim' | 'due' | 'probe' | 'prune', signal: AbortSignal) {
        if (signal.aborted || unavailable.has(source)) return
        const notify = unavailable.size === 0
        unavailable.add(source)
        if (!notify) return
        try {
            dependencies.unavailable()
        } catch {
            // A failed logger must not terminate periodic monitoring.
        }
    }

    /** Holds a worker slot through the adapter's durable completion or fenced release. */
    function launch(claim: Claim, signal: AbortSignal) {
        const task = (async () => {
            try {
                // runClaim owns completion and release, including abort cleanup.
                await dependencies.runClaim(claim, signal)
                if (!signal.aborted) unavailable.delete('probe')
            } catch (error) {
                if (error instanceof HostMonitoringCleanupError) failCleanup(claim, error)
                warn('probe', signal)
            }
        })().finally(() => running.delete(task))
        running.add(task)
    }

    /** Sweeps history independently of probe progress, with at most one throttled sweep in flight. */
    function prune(signal: AbortSignal) {
        if (pruning || signal.aborted || now() < nextPruneAt) return
        // Failed pruning is retried at the sweep interval, never in a hot loop.
        nextPruneAt = now() + HOST_MONITORING_LIMITS.retentionSweepMs
        pruning = (async () => {
            try {
                await dependencies.prune()
                if (!signal.aborted) unavailable.delete('prune')
            } catch {
                warn('prune', signal)
            }
        })().finally(() => {
            pruning = null
        })
    }

    /** Keeps a single periodic admission wakeup active while existing probes continue. */
    function schedule() {
        if (!started || stopped || cancelTimer) return
        cancelTimer = setTimer(() => {
            cancelTimer = null
            schedule()
            void runOnce()
        }, pollDelayMs)
    }

    /** Coalesces admission ticks and reserves free slots before each storage claim batch. */
    function runOnce(): Promise<void> {
        if (stopped) return Promise.resolve()
        const signal = controller.signal
        prune(signal)
        if (admission) return admission
        admission = (async () => {
            const revision = wakeRevision
            pollDelayMs = HOST_MONITORING_LIMITS.pollIntervalMs
            if (failedClaims.size > 0) {
                if (recoveryNow() < recoveryAt) return
                // One coalesced bounded wave, using the same class-admitted batch release adapter.
                await releaseClaims([...failedClaims.keys()])
                if (failedClaims.size > 0) {
                    recoveryDelay = Math.min(recoveryDelay * 2, 30_000)
                    recoveryAt = recoveryNow() + recoveryDelay
                    return
                }
                cleanupFailure = undefined
                recoveryDelay = 1_000
                unavailable.delete('probe')
            }
            // Cap this tick too: fast completions must not create an endless drain.
            let remaining = HOST_MONITORING_LIMITS.workerConcurrency - running.size
            try {
                while (!signal.aborted && failedClaims.size === 0 && remaining > 0) {
                    const limit = Math.min(
                        remaining,
                        HOST_MONITORING_LIMITS.claimBatchSize,
                        HOST_MONITORING_LIMITS.workerConcurrency - running.size,
                    )
                    // This batch reserves its slots until storage returns. Admission
                    // never overlaps, so running tasks plus reserved slots stay bounded.
                    // eslint-disable-next-line no-await-in-loop -- Each storage batch reserves only currently free capacity.
                    const claims = await dependencies.claim(limit)
                    if (signal.aborted || failedClaims.size > 0) {
                        // eslint-disable-next-line no-await-in-loop -- Shutdown must await every late lease release before returning.
                        await releaseClaims(claims)
                        return
                    }
                    unavailable.delete('claim')
                    let launched = 0
                    for (const claim of claims) {
                        if (signal.aborted || failedClaims.size > 0) break
                        launch(claim, signal)
                        launched++
                    }
                    if (launched < claims.length) {
                        // eslint-disable-next-line no-await-in-loop -- A synchronous cleanup failure fences the remaining admitted batch too.
                        await releaseClaims(claims.slice(launched))
                        return
                    }
                    remaining -= limit
                    if (claims.length < limit) {
                        if (
                            claims.length === 0 &&
                            running.size === 0 &&
                            dependencies.nextAdmissionAt
                        ) {
                            try {
                                // eslint-disable-next-line no-await-in-loop -- Read durable due time only after an empty claim batch.
                                const due = await dependencies.nextAdmissionAt()
                                unavailable.delete('due')
                                if (
                                    !signal.aborted &&
                                    failedClaims.size === 0 &&
                                    running.size === 0 &&
                                    revision === wakeRevision
                                )
                                    pollDelayMs = Math.max(
                                        HOST_MONITORING_LIMITS.pollIntervalMs,
                                        Math.min(
                                            HOST_MONITORING_LIMITS.idlePollMaximumMs,
                                            due === null
                                                ? HOST_MONITORING_LIMITS.idlePollMaximumMs
                                                : due - now(),
                                        ),
                                    )
                            } catch {
                                warn('due', signal)
                            }
                        }
                        return
                    }
                }
            } catch {
                warn('claim', signal)
            }
        })().finally(() => {
            admission = null
            cancelTimer?.()
            cancelTimer = null
            schedule()
        })
        return admission
    }

    return {
        runOnce,
        /** Starts admission, or resumes only after a previous stop has finished draining. */
        start() {
            if (started || stopping) return
            if (stopped) controller = new AbortController()
            started = true
            stopped = false
            unsubscribe = dependencies.subscribe?.(() => {
                wakeRevision++
                if (pollDelayMs === HOST_MONITORING_LIMITS.pollIntervalMs) return
                pollDelayMs = HOST_MONITORING_LIMITS.pollIntervalMs
                cancelTimer?.()
                cancelTimer = null
                schedule()
            })
            schedule()
            void runOnce()
        },
        /** Stops new work and waits for admission, retention, probes and late-lease cleanup. */
        stop(): Promise<void> {
            if (stopping) return stopping
            stopped = true
            started = false
            cancelTimer?.()
            cancelTimer = null
            unsubscribe?.()
            unsubscribe = undefined
            controller.abort()
            stopping = (async () => {
                await drainWorker(
                    (async () => {
                        const admitted = await Promise.allSettled([admission, pruning])
                        await settleWorkerTasks([
                            ...running,
                            ...admitted.map((result) =>
                                result.status === 'rejected' ? Promise.reject(result.reason) : null,
                            ),
                        ])
                        if (cleanupFailure) throw cleanupFailure.reason
                    })(),
                )
            })().then(
                () => {
                    stopping = null
                },
                (error: unknown) => {
                    if (error instanceof WorkerDrainTimeoutError)
                        void error.drained.then(
                            () => {
                                stopping = null
                            },
                            () => undefined,
                        )
                    throw error
                },
            )
            return stopping
        },
    }
}
