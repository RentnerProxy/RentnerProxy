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
        }, HOST_MONITORING_LIMITS.pollIntervalMs)
    }

    /** Coalesces admission ticks and reserves free slots before each storage claim batch. */
    function runOnce(): Promise<void> {
        if (stopped) return Promise.resolve()
        const signal = controller.signal
        prune(signal)
        if (admission) return admission
        admission = (async () => {
            // Cap this tick too: fast completions must not create an endless drain.
            let remaining = HOST_MONITORING_LIMITS.workerConcurrency - running.size
            try {
                while (!signal.aborted && remaining > 0) {
                    const limit = Math.min(
                        remaining,
                        HOST_MONITORING_LIMITS.claimBatchSize,
                        HOST_MONITORING_LIMITS.workerConcurrency - running.size,
                    )
                    // This batch reserves its slots until storage returns. Admission
                    // never overlaps, so running tasks plus reserved slots stay bounded.
                    // eslint-disable-next-line no-await-in-loop -- Each storage batch reserves only currently free capacity.
                    const claims = await dependencies.claim(limit)
                    if (signal.aborted) {
                        // eslint-disable-next-line no-await-in-loop -- Shutdown must await every late lease release before returning.
                        await Promise.allSettled(
                            claims.map(async (claim) => dependencies.releaseCancelled(claim)),
                        )
                        return
                    }
                    unavailable.delete('claim')
                    for (const claim of claims) launch(claim, signal)
                    remaining -= limit
                    if (claims.length < limit) return
                }
            } catch {
                warn('claim', signal)
            }
        })().finally(() => {
            admission = null
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
            controller.abort()
            stopping = (async () => {
                await Promise.all([admission, pruning])
                await Promise.all(running)
            })().finally(() => {
                stopping = null
            })
            return stopping
        },
    }
}
