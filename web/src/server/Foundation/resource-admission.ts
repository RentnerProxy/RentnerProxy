import type {
    ResourceAdmissionClock,
    ResourceAdmissionPolicy,
    ResourceWaiter,
} from './Types/resource-admission.types.ts'

export class ResourceAdmissionError extends Error {
    constructor(readonly reason: 'busy' | 'deadline' | 'cancelled') {
        super(`Resource admission ${reason}`)
    }
}

export function releaseAfterSettled(
    release: (() => void) | undefined,
    work: Iterable<Promise<unknown>>,
): void {
    if (release) void Promise.allSettled(work).then(release)
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
    const count = (reason: keyof typeof counters) => {
        counters[reason] = Math.min(Number.MAX_SAFE_INTEGER, counters[reason] + 1)
    }

    function remove(waiter: ResourceWaiter) {
        const index = pending.indexOf(waiter)
        if (index >= 0) pending.splice(index, 1)
        waiter.cancelTimer()
        waiter.signal?.removeEventListener('abort', waiter.abort)
    }

    function permit(): () => void {
        running++
        count('admitted')
        let released = false
        return () => {
            if (released) return
            released = true
            running--
            while (pending.length > 0 && running < policy.running) {
                const waiter = pending[0]!
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
        snapshot() {
            return {
                running,
                pending: pending.length,
                oldestPendingAgeMs: pending.length
                    ? Math.min(
                          Number.MAX_SAFE_INTEGER,
                          Math.max(0, Math.floor(now() - pending[0]!.at)),
                      )
                    : 0,
                ...counters,
            }
        },
    }
}
