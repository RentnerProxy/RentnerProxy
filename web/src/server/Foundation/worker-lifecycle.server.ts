import '@tanstack/react-start/server-only'
// oxlint-disable no-await-in-loop -- Startup attempts and maintenance observations must remain serial.

import type { WorkerLifecycle, WorkerLifecycleOptions } from './Types/worker-lifecycle.types.ts'
import type {
    BackgroundWorkerId,
    BackgroundWorkerState,
} from './Types/worker-observability.types.ts'
import { workerObservability } from './worker-observability.ts'
import { startEventLoopObservation } from './event-loop-observation.ts'
import { readRestoreMaintenanceStatus } from '@/server/Backups/restore-maintenance-status.server.ts'

export class WorkerDrainTimeoutError extends Error {
    constructor() {
        super('worker_drain_timeout')
        this.name = 'WorkerDrainTimeoutError'
    }
}

class WorkerStoppedInstanceError extends Error {
    constructor() {
        super('worker_instance_stopped')
    }
}

/** Wait for every admitted sibling before propagating the first rejection; one failed task cannot establish writer quiescence. */
export async function settleWorkerTasks(tasks: Iterable<Promise<unknown> | null>): Promise<void> {
    const results = await Promise.allSettled(tasks)
    const failed = results.find((result) => result.status === 'rejected')
    if (failed?.status === 'rejected') throw failed.reason
}

/** Bound waiting for a drain and clear the timer; timeout rejects the checkpoint without claiming the underlying work has ended. */
export async function drainWorker(pending: Promise<unknown>, timeoutMs = 15_000): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
        await Promise.race([
            pending,
            new Promise<never>((_, reject) => {
                timer = setTimeout(() => reject(new WorkerDrainTimeoutError()), timeoutMs)
            }),
        ])
    } finally {
        clearTimeout(timer)
    }
}

/** Keep writers fenced for queued/running restores and post-validation failures until the matching restore permits admission. */
const shouldWait = (status: Awaited<ReturnType<typeof readRestoreMaintenanceStatus>>) =>
    status?.operation === 'restore' &&
    (status.state === 'queued' ||
        status.state === 'running' ||
        ((status.state === 'interrupted' || status.state === 'failed') &&
            !['queued', 'preparing', 'validating'].includes(status.phase)))

/** Own deduplicated worker registration, bounded retry/drain, and irreversible maintenance/shutdown admission fences. */
export function createWorkerLifecycleSupervisor() {
    let maintenanceStopped = false
    let maintenanceDrain: Promise<void> | null = null
    let shutdownRequested = false
    const starts = new Map<BackgroundWorkerId, Promise<void>>()
    process.once('rentnerproxy:shutdown', () => {
        shutdownRequested = true
    })

    /** Share startup by stable worker ID and reject new admission once maintenance or shutdown has latched. */
    function startWorkerLifecycle(
        load: (signal: AbortSignal) => Promise<WorkerLifecycle>,
        id?: BackgroundWorkerId,
        options: WorkerLifecycleOptions = {},
    ): Promise<void> {
        if (id) startEventLoopObservation()
        if (id && starts.has(id)) return starts.get(id)!
        if (maintenanceStopped || shutdownRequested) {
            if (id) {
                workerObservability.count(id, 'admissionRejected')
                workerObservability.state(id, maintenanceStopped ? 'maintenance_paused' : 'stopped')
            }
            return Promise.resolve()
        }
        const starting = initializeWorkerLifecycle(load, id, options)
        if (id) starts.set(id, starting)
        return starting
    }

    /** Fence new starts before requesting drain and retain the same drain result for all maintenance callers. */
    async function stopWorkersForRestoreMaintenance(): Promise<void> {
        if (maintenanceDrain) return maintenanceDrain
        maintenanceStopped = true
        const pending: Array<Promise<void>> = []
        process.emit('rentnerproxy:maintenance', pending)
        maintenanceDrain = Promise.all(pending).then(() => undefined)
        return maintenanceDrain
    }

    /** Load with cancellation, clean failed instances before bounded retries, and prevent late startup or reuse of a stopped singleton. */
    async function initializeWorkerLifecycle(
        load: (signal: AbortSignal) => Promise<WorkerLifecycle>,
        id?: BackgroundWorkerId,
        options: WorkerLifecycleOptions = {},
    ): Promise<void> {
        const controller = new AbortController()
        let worker: WorkerLifecycle | undefined
        let stopping: Promise<void> | null = null
        const state = (value: BackgroundWorkerState) => {
            if (id) workerObservability.state(id, value)
        }
        /** Make retry/restore waits interruptible and remove the abort listener after either timer or cancellation settles. */
        const delay = (milliseconds: number) =>
            new Promise<void>((resolve) => {
                if (controller.signal.aborted) return resolve()
                const done = () => {
                    clearTimeout(timer)
                    controller.signal.removeEventListener('abort', done)
                    resolve()
                }
                const timer = setTimeout(done, milliseconds)
                controller.signal.addEventListener('abort', done, { once: true })
            })

        let stoppedWorker: WorkerLifecycle | undefined
        let workerStop: Promise<void> | undefined
        /** Stop each loaded instance at most once and share its actual stop promise across startup and shutdown races. */
        const stopLoaded = () => {
            if (!worker) return Promise.resolve()
            if (stoppedWorker !== worker) {
                stoppedWorker = worker
                const current = worker
                workerStop = Promise.resolve().then(() => current.stop())
            }
            return workerStop!
        }
        /** Bound restore-status observation without treating its timeout as evidence that the underlying operation completed. */
        const readStatus = async () => {
            const reading = readRestoreMaintenanceStatus()
            await drainWorker(reading, options.startupTimeoutMs)
            return reading
        }
        /** Bound worker loading while retaining its completion promise for late-instance cleanup. */
        const drainLoaded = async (loading: Promise<WorkerLifecycle>) => {
            await drainWorker(loading, options.startupTimeoutMs)
            return loading
        }
        const initializing = (async () => {
            if (maintenanceStopped || shutdownRequested || controller.signal.aborted) return
            const attempts = Math.max(1, Math.min(options.maxAttempts ?? 3, 10))
            for (let attempt = 1; attempt <= attempts; attempt++) {
                if (controller.signal.aborted) return
                state('starting')
                try {
                    const loading = load(controller.signal).then((loaded) => {
                        if (loaded === stoppedWorker) throw new WorkerStoppedInstanceError()
                        worker = loaded
                        if (controller.signal.aborted)
                            void stopLoaded().catch(() => state('failed'))
                        return loaded
                    })
                    worker = await drainLoaded(loading)
                    let status = await readStatus()
                    if (shouldWait(status)) {
                        state('maintenance_paused')
                        const restoreId = status!.id
                        while (
                            !controller.signal.aborted &&
                            (status?.id !== restoreId || shouldWait(status))
                        ) {
                            await delay(250)
                            if (!controller.signal.aborted) status = await readStatus()
                        }
                    }
                    if (maintenanceStopped || shutdownRequested || controller.signal.aborted) {
                        void stopLoaded().catch(() => state('failed'))
                        return
                    }
                    state('idle')
                    await drainWorker(
                        Promise.resolve(worker.start(controller.signal)),
                        options.startupTimeoutMs,
                    )
                    return
                } catch (error) {
                    if (controller.signal.aborted) return
                    if (error instanceof WorkerDrainTimeoutError) controller.abort()
                    if (worker) {
                        await drainWorker(stopLoaded(), options.drainTimeoutMs)
                        worker = undefined
                    }
                    if (
                        error instanceof WorkerDrainTimeoutError ||
                        error instanceof WorkerStoppedInstanceError ||
                        attempt === attempts ||
                        options.classifyFailure?.(error) === 'terminal'
                    ) {
                        state('failed')
                        if (id) workerObservability.count(id, 'exhausted')
                        throw error
                    }
                    state('retrying')
                    if (id) workerObservability.count(id, 'retry')
                    await delay(
                        Math.min((options.retryDelayMs ?? 1_000) * 2 ** (attempt - 1), 10_000),
                    )
                }
            }
        })()
        /** Abort initialization first, drain both existing and late-loaded workers, and record success only after every admitted task settles. */
        const stop = (pending: Array<Promise<void>>) => {
            controller.abort()
            if (!stopping) state(maintenanceStopped ? 'maintenance_paused' : 'stopping')
            stopping ??= drainWorker(
                settleWorkerTasks([stopLoaded(), initializing.then(stopLoaded)]),
                options.drainTimeoutMs,
            ).then(
                () => state(maintenanceStopped ? 'maintenance_paused' : 'stopped'),
                (error: unknown) => {
                    state('failed')
                    if (id && error instanceof WorkerDrainTimeoutError)
                        workerObservability.count(id, 'drainTimeout')
                    throw error
                },
            )
            pending.push(stopping)
        }
        process.once('rentnerproxy:shutdown', stop)
        process.once('rentnerproxy:maintenance', stop)
        try {
            await initializing
        } catch (error) {
            controller.abort()
            void stopLoaded().catch(() => state('failed'))
            state('failed')
            console.warn('[workers] startup unavailable', { workerId: id ?? 'unregistered' })
            throw error
        }
        if (maintenanceStopped) {
            const pending: Array<Promise<void>> = []
            if (!stopping) stop(pending)
            await stopping
        }
    }

    return { startWorkerLifecycle, stopWorkersForRestoreMaintenance }
}

const supervisor = createWorkerLifecycleSupervisor()
export const startWorkerLifecycle = supervisor.startWorkerLifecycle
export const stopWorkersForRestoreMaintenance = supervisor.stopWorkersForRestoreMaintenance
