import { drainWorker, settleWorkerTasks } from '@/server/Foundation/worker-lifecycle.server.ts'
import { WorkerDrainTimeoutError } from '@/server/Foundation/Types/worker-lifecycle.types.ts'
import { HOST_MONITORING_LIMITS } from '@/config/host-monitoring.config.ts'
import type { HostMonitoringWorkerDependencies } from './Types/host-monitoring-worker.types.ts'

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
    const running = new Set<Promise<void>>()
    const unavailable = new Set<'claim' | 'probe' | 'prune'>()
    const now = dependencies.now ?? Date.now
    const setTimer =
        dependencies.schedule ??
        ((callback, delayMs) => {
            const timer = setTimeout(callback, delayMs)
            timer.unref()
            return () => clearTimeout(timer)
        })

    /** Deduplicates sanitized availability warnings until the failing subsystem recovers. */
    function warn(source: 'claim' | 'probe' | 'prune', signal: AbortSignal) {
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
            } catch {
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
            // Cap this tick too: fast completions must not create an endless drain.
            let remaining = HOST_MONITORING_LIMITS.workerConcurrency - running.size
            while (!signal.aborted && remaining > 0) {
                const limit = Math.min(
                    remaining,
                    HOST_MONITORING_LIMITS.claimBatchSize,
                    HOST_MONITORING_LIMITS.workerConcurrency - running.size,
                )
                // Catch unavailable admission separately: failed cleanup must reach stop().
                let claims: readonly Claim[]
                try {
                    // eslint-disable-next-line no-await-in-loop -- Each storage batch reserves only currently free capacity.
                    claims = await dependencies.claim(limit)
                } catch {
                    warn('claim', signal)
                    return
                }
                if (signal.aborted) {
                    // eslint-disable-next-line no-await-in-loop -- Shutdown must await every late lease release before returning.
                    await settleWorkerTasks(
                        claims.map(async (claim) => dependencies.releaseCancelled(claim)),
                    )
                    return
                }
                unavailable.delete('claim')
                for (const claim of claims) launch(claim, signal)
                remaining -= limit
                if (claims.length < limit) {
                    if (claims.length === 0 && dependencies.nextAdmissionAt) {
                        try {
                            // eslint-disable-next-line no-await-in-loop -- Read durable due time only after an empty claim batch.
                            const due = await dependencies.nextAdmissionAt()
                            if (!signal.aborted && revision === wakeRevision)
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
                            warn('claim', signal)
                        }
                    }
                    return
                }
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
