import '@tanstack/react-start/server-only'
// oxlint-disable no-await-in-loop -- Runtime revisions must be applied serially.

import type { ProxyRuntimeMutationStatus } from '@/lib/ProxyRuntime/Types/proxy-runtime.types.ts'
import { workerObservability } from './worker-observability.ts'
import { withBackgroundDatabaseContext } from '@/db/background-admission.ts'
import { ResourceAdmissionError } from './resource-admission.ts'
import { WorkerDrainTimeoutError } from './Types/worker-lifecycle.types.ts'
import type {
    RuntimeReconcileDependencies,
    RuntimeReconcileOptions,
    RuntimeReconcileWaiter,
    RuntimeReconciler,
} from './Types/runtime-reconciler.types.ts'

const INITIAL_RETRY_DELAY_MS = 1_000
const MAX_RETRY_DELAY_MS = 60_000
const DRIFT_CHECK_INTERVAL_MS = 60_000

/** Serialize revision application with coalesced requests and bounded retries; rejected revisions stay degraded until actual recovery. */
export function createRuntimeReconciler<T>(
    dependencies: RuntimeReconcileDependencies<T>,
    options: RuntimeReconcileOptions,
): RuntimeReconciler {
    let requested = 0
    let completed = 0
    let worker: Promise<void> | null = null
    let driftFlight: Promise<void> | null = null
    let stopped = false
    let driftTimer: ReturnType<typeof setInterval> | null = null
    let wakeResolver: (() => void) | null = null
    let publicFlight: Promise<ProxyRuntimeMutationStatus> | null = null
    let rejectedRevision: string | null = null
    const waiters = new Set<RuntimeReconcileWaiter>()

    function wake(): void {
        const resolve = wakeResolver
        wakeResolver = null
        resolve?.()
    }

    async function waitForWakeOrDelay(delayMs: number): Promise<void> {
        let timer: ReturnType<typeof setTimeout> | undefined
        let localWake: (() => void) | null = null
        const wakePromise = new Promise<void>((resolve) => {
            localWake = resolve
            wakeResolver = resolve
        })
        const delayPromise = new Promise<void>((resolve) => {
            timer = setTimeout(resolve, delayMs)
            timer.unref?.()
        })
        await Promise.race([wakePromise, delayPromise])
        if (timer !== undefined) clearTimeout(timer)
        if (wakeResolver === localWake) wakeResolver = null
    }

    function resolveWaiters(status: ProxyRuntimeMutationStatus): void {
        for (const waiter of waiters) {
            if (status === 'applied' && waiter.target > completed) continue
            clearTimeout(waiter.timer)
            waiters.delete(waiter)
            waiter.resolve(status)
        }
    }

    /** Retain nonretryable rejection evidence across idle checks; only a stable successful apply resolves waiters as applied. */
    async function runWorker(): Promise<void> {
        let retryDelay = INITIAL_RETRY_DELAY_MS
        // oxlint-disable-next-line no-unmodified-loop-condition -- stop() changes this lifecycle flag.
        while (!stopped) {
            if (completed >= requested) {
                await new Promise<void>((resolve) => {
                    wakeResolver = resolve
                })
                continue
            }
            const target = requested
            const finish = options.workerId ? workerObservability.begin(options.workerId) : () => {}
            try {
                const snapshot = await dependencies.load()
                if (stopped) break
                if (rejectedRevision === dependencies.revision(snapshot)) {
                    completed = target
                    finish('deferred', { recoverLane: false })
                    resolveWaiters('pending')
                    continue
                }
                const applied = await dependencies.apply(snapshot)
                if (stopped) break
                if (typeof applied !== 'boolean') {
                    finish('failure')
                    // Only bounded symbolic diagnostics cross into logs; never include response bodies.
                    const code = /^[a-z][a-z0-9_]{0,63}$/u.test(applied.code)
                        ? applied.code
                        : 'apply_failed'
                    console.warn(`[${options.name}] reconcile rejected`, {
                        code,
                        status: applied.status,
                        retryable: applied.retryable,
                    })
                    resolveWaiters('pending')
                    if (!applied.retryable) {
                        rejectedRevision = dependencies.revision(snapshot)
                        completed = target
                        retryDelay = INITIAL_RETRY_DELAY_MS
                        continue
                    }
                    await waitForWakeOrDelay(retryDelay)
                    if (options.workerId) workerObservability.count(options.workerId, 'retry')
                    retryDelay = Math.min(retryDelay * 2, MAX_RETRY_DELAY_MS)
                    continue
                }
                if (!applied) throw new Error('apply_failed')
                rejectedRevision = null
                const latest = await dependencies.load()
                if (stopped) break
                if (
                    target !== requested ||
                    dependencies.revision(latest) !== dependencies.revision(snapshot)
                )
                    continue
                completed = target
                retryDelay = INITIAL_RETRY_DELAY_MS
                finish('success')
                resolveWaiters('applied')
            } catch (error) {
                finish(
                    error instanceof ResourceAdmissionError
                        ? error.reason === 'cancelled'
                            ? 'cancelled'
                            : 'deferred'
                        : 'failure',
                    { recoverLane: false },
                )
                if (stopped) break
                resolveWaiters('pending')
                console.warn(`[${options.name}] reconcile unavailable`)
                await waitForWakeOrDelay(retryDelay)
                retryDelay = Math.min(retryDelay * 2, MAX_RETRY_DELAY_MS)
                if (options.workerId) workerObservability.count(options.workerId, 'retry')
            } finally {
                finish(stopped ? 'cancelled' : 'deferred')
            }
        }
    }

    function enqueue(): void {
        requested += 1
        if (!worker && !stopped) {
            // This shared worker outlives the discovery/request scope that first wakes it.
            worker = withBackgroundDatabaseContext(runWorker, null).finally(() => {
                worker = null
            })
        }
        wake()
    }

    function reconcile(): Promise<ProxyRuntimeMutationStatus> {
        if (stopped || dependencies.enabled === false) return Promise.resolve('pending')
        rejectedRevision = null
        enqueue()
        if (publicFlight) return publicFlight
        const target = requested
        publicFlight = new Promise<ProxyRuntimeMutationStatus>((resolve) => {
            const waiter: RuntimeReconcileWaiter = {
                target,
                resolve,
                timer: setTimeout(() => {
                    waiters.delete(waiter)
                    resolve('pending')
                }, options.timeoutMs),
            }
            if (options.unrefWaiters) waiter.timer.unref?.()
            waiters.add(waiter)
            resolveWaiters('applied')
        }).finally(() => {
            publicFlight = null
        })
        return publicFlight
    }

    function start(): void {
        if (stopped || dependencies.enabled === false) return
        enqueue()
        if (driftTimer === null) {
            driftTimer = setInterval(() => void checkDrift(), DRIFT_CHECK_INTERVAL_MS)
            driftTimer.unref?.()
        }
    }

    /** Share drift ownership so shutdown can drain every admitted observation. */
    function checkDrift(): Promise<void> {
        const drift = dependencies.drift
        if (!drift || stopped || dependencies.enabled === false) return Promise.resolve()
        if (driftFlight) return driftFlight
        driftFlight = withBackgroundDatabaseContext(() => observeDrift(drift), null).finally(() => {
            driftFlight = null
        })
        return driftFlight
    }

    /** Recover only the observation lane after a completed drift scan; apply failures require an actual successful apply. */
    async function observeDrift(
        drift: NonNullable<RuntimeReconcileDependencies<T>['drift']>,
    ): Promise<void> {
        const finish = options.workerId
            ? workerObservability.begin(options.workerId, 'collection')
            : () => {}
        try {
            let hasDrift: boolean
            if ('check' in drift) {
                hasDrift = await drift.check()
            } else {
                const snapshot = await dependencies.load()
                if (stopped) return
                hasDrift = await drift.matches(snapshot)
            }
            finish(stopped ? 'cancelled' : 'deferred')
            if (!stopped && hasDrift) enqueue()
        } catch (error) {
            finish(
                stopped || (error instanceof ResourceAdmissionError && error.reason === 'cancelled')
                    ? 'cancelled'
                    : error instanceof ResourceAdmissionError
                      ? 'deferred'
                      : 'failure',
                { recoverLane: false },
            )
            console.warn(`[${options.name}] drift check unavailable`)
        } finally {
            finish('cancelled')
        }
    }

    /** Fence new work and bound shutdown waiting while retaining the actual apply and drift drain in timeout errors. */
    async function stop(): Promise<void> {
        stopped = true
        if (driftTimer !== null) clearInterval(driftTimer)
        driftTimer = null
        resolveWaiters('pending')
        wake()
        const draining = Promise.allSettled([worker, driftFlight]).then((results) => {
            const failed = results.find((result) => result.status === 'rejected')
            if (failed?.status === 'rejected') throw failed.reason
        })
        if (worker || driftFlight) {
            let timer: ReturnType<typeof setTimeout> | undefined
            try {
                await Promise.race([
                    draining,
                    new Promise<void>((_, reject) => {
                        timer = setTimeout(
                            () => reject(new WorkerDrainTimeoutError(draining)),
                            options.stopTimeoutMs,
                        )
                        timer.unref?.()
                    }),
                ])
            } finally {
                if (timer !== undefined) clearTimeout(timer)
            }
        }
    }

    return Object.assign(reconcile, { checkDrift, start, stop })
}
