import type {
    ResourceAdmissionClock,
    ResourceAdmissionPolicy,
    ResourceWaiter,
} from './Types/resource-admission.types.ts'
import { runDetachedTask } from '@/lib/Async/runDetachedTask.ts'

export class ResourceAdmissionError extends Error {
    constructor(readonly reason: 'busy' | 'deadline' | 'cancelled') {
        super(`Resource admission ${reason}`)
    }
}

/** Retain a permit until every tracked underlying operation settles, even when the public request already timed out or failed. */
export function releaseAfterSettled(
    release: (() => void) | undefined,
    work: Iterable<Promise<unknown>>,
): void {
    if (release)
        runDetachedTask(
            () => Promise.allSettled(work).then(release),
            () => console.error('[resources] Deferred permit release failed'),
        )
}

/** Bounded FIFO permits, acquired before hydration/SQL. Pending jobs remain in their durable owner. */
export function createResourceAdmission(
    policy: ResourceAdmissionPolicy,
    clock: ResourceAdmissionClock = {},
) {
    let running = 0
    const pending: ResourceWaiter[] = []
    const counters = { busy: 0, deadline: 0, cancelled: 0, admitted: 0 }
    const now = clock.now ?? (() => performance.now())
    const schedule =
        clock.schedule ??
        ((callback, delayMs) => {
            const timer = setTimeout(callback, delayMs)
            return () => clearTimeout(timer)
        })
    /** Keep admission diagnostics within the safe integer range without expanding the fixed reason catalog. */
    const count = (reason: keyof typeof counters) => {
        counters[reason] = Math.min(Number.MAX_SAFE_INTEGER, counters[reason] + 1)
    }

    /** Detach a queued waiter and its timer/abort listener before resolving or rejecting its ownership. */
    function remove(waiter: ResourceWaiter) {
        const index = pending.indexOf(waiter)
        if (index >= 0) pending.splice(index, 1)
        waiter.cancelTimer()
        waiter.signal?.removeEventListener('abort', waiter.abort)
    }

    /** Return an idempotent release that hands capacity to live FIFO waiters and rejects expired/cancelled heads without exceeding the running limit. */
    function permit(): () => void {
        running++
        count('admitted')
        let released = false
        return () => {
            if (released) return
            released = true
            running--
            while (pending.length > 0 && running < policy.running) {
                const waiter = pending[0]
                if (!waiter) break
                remove(waiter)
                const reason = waiter.signal?.aborted
                    ? 'cancelled'
                    : now() - waiter.at >= policy.waitMs
                      ? 'deadline'
                      : null
                if (reason) {
                    count(reason)
                    waiter.reject(new ResourceAdmissionError(reason))
                } else waiter.resolve(permit())
            }
        }
    }

    /** Acquire before work hydration, preserving FIFO order; reject full queues, expired waits, and cancellation without starting the operation. */
    function acquire(signal?: AbortSignal): Promise<() => void> {
        if (signal?.aborted) {
            count('cancelled')
            return Promise.reject(new ResourceAdmissionError('cancelled'))
        }
        if (running < policy.running && pending.length === 0) return Promise.resolve(permit())
        if (pending.length >= policy.pending) {
            count('busy')
            return Promise.reject(new ResourceAdmissionError('busy'))
        }
        return new Promise((resolve, reject) => {
            const waiter: ResourceWaiter = {
                at: now(),
                resolve,
                reject,
                signal,
                cancelTimer: () => {},
                abort: () => {},
            }
            /** Reject only a still-queued waiter so timeout and abort races cannot settle or remove another admission. */
            function fail(reason: 'deadline' | 'cancelled') {
                if (!pending.includes(waiter)) return
                remove(waiter)
                count(reason)
                reject(new ResourceAdmissionError(reason))
            }
            waiter.abort = () => fail('cancelled')
            pending.push(waiter)
            waiter.cancelTimer = schedule(() => fail('deadline'), policy.waitMs)
            signal?.addEventListener('abort', waiter.abort, { once: true })
        })
    }

    return {
        acquire,
        /** Hold one permit through actual operation settlement and release in every outcome, including cancellation immediately after acquisition. */
        async run<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
            const release = await acquire(signal)
            try {
                if (signal?.aborted) {
                    count('cancelled')
                    throw new ResourceAdmissionError('cancelled')
                }
                return await operation()
            } finally {
                release()
            }
        },
        /** Expose only bounded numeric admission evidence; reading metrics never acquires capacity or changes queue ownership. */
        snapshot() {
            const oldest = pending[0]
            return {
                running,
                pending: pending.length,
                oldestPendingAgeMs: oldest
                    ? Math.min(Number.MAX_SAFE_INTEGER, Math.max(0, Math.floor(now() - oldest.at)))
                    : 0,
                ...counters,
            }
        },
    }
}
