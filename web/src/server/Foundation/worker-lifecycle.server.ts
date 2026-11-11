import '@tanstack/react-start/server-only'
// oxlint-disable no-await-in-loop -- Startup attempts and maintenance observations must remain serial.

import type { WorkerLifecycle, WorkerLifecycleOptions } from './Types/worker-lifecycle.types.ts'
import type {
    BackgroundWorkerId,
    BackgroundWorkerState,
} from './Types/worker-observability.types.ts'
import { workerObservability } from './worker-observability.ts'
import { startEventLoopObservation } from './event-loop-observation.ts'
import { readRestoreMaintenanceObservation } from '@/server/Backups/restore-maintenance-status.server.ts'
import { WorkerDrainTimeoutError, drainWorker, settleWorkerTasks } from './worker-drain.ts'
export { WorkerDrainTimeoutError, drainWorker, settleWorkerTasks } from './worker-drain.ts'

type RestoreObservation = Awaited<ReturnType<typeof readRestoreMaintenanceObservation>>

class WorkerStoppedInstanceError extends Error {
    constructor() {
        super('worker_instance_stopped')
    }
}

/** Keep writers fenced for queued/running restores and post-validation failures until the matching restore permits admission. */
const shouldWait = (
    status: RestoreObservation['maintenance'] | RestoreObservation['restoreResults'][number],
) =>
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

    /** Share startup by stable worker ID, wait for restore readiness and reject admission once maintenance or shutdown latches. */
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

    /** Fence new starts before requesting listener drains and retain the same success or failure for every maintenance caller. */
    async function stopWorkersForRestoreMaintenance(): Promise<void> {
        if (maintenanceDrain) return maintenanceDrain
        maintenanceStopped = true
        const pending: Array<Promise<void>> = []
        process.emit('rentnerproxy:maintenance', pending)
        maintenanceDrain = Promise.all(pending).then(() => undefined)
        return maintenanceDrain
    }

    /** Wait for restore permission, load with cancellation and bounded retries, and share drain ownership without reusing stopped instances. */
    async function initializeWorkerLifecycle(
        load: (signal: AbortSignal) => Promise<WorkerLifecycle>,
        id?: BackgroundWorkerId,
        options: WorkerLifecycleOptions = {},
    ): Promise<void> {
        const controller = new AbortController()
        let worker: WorkerLifecycle | undefined
        let startAttempted = false
        // Startup deadlines bound the waiter; the load and any late cleanup stay owned.
        let loading: Promise<void> | undefined
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
            const reading = readRestoreMaintenanceObservation()
            await drainWorker(reading, options.startupTimeoutMs)
            return reading
        }
        /** Bound worker loading while retaining its completion promise for late-instance cleanup. */
        const drainLoaded = async (
            pendingLoad: Promise<WorkerLifecycle>,
            ownedLoad: Promise<void>,
        ) => {
            try {
                await drainWorker(pendingLoad, options.startupTimeoutMs)
            } catch (error) {
                if (error instanceof WorkerDrainTimeoutError)
                    throw new WorkerDrainTimeoutError(ownedLoad)
                throw error
            }
            return pendingLoad
        }
        // A retry may replace the worker, but cannot forget a restore whose safe result is unconfirmed.
        const waiting = new Set<string>()
        const initializing = (async () => {
            if (maintenanceStopped || shutdownRequested || controller.signal.aborted) return
            const attempts = Math.max(1, Math.min(options.maxAttempts ?? 3, 10))
            for (let attempt = 1; attempt <= attempts; attempt++) {
                if (controller.signal.aborted) return
                state('starting')
                try {
                    if (!worker) {
                        const pendingLoad = load(controller.signal).then((loaded) => {
                            if (loaded === stoppedWorker) throw new WorkerStoppedInstanceError()
                            worker = loaded
                            return loaded
                        })
                        const ownedLoad = pendingLoad.then(async () => {
                            if (controller.signal.aborted) await stopLoaded()
                        })
                        loading = ownedLoad
                        const settledLoad = () => {
                            if (loading === ownedLoad) loading = undefined
                        }
                        void ownedLoad.then(settledLoad, settledLoad)
                        worker = await drainLoaded(pendingLoad, ownedLoad)
                    }
                    let observation = await readStatus()
                    if (waiting.size > 0 || shouldWait(observation.maintenance)) {
                        state('maintenance_paused')
                        while (!controller.signal.aborted) {
                            const status = observation.maintenance
                            if (shouldWait(status)) {
                                // The root journal retains 100 IDs. Exhaustion cannot become admission proof.
                                if (!waiting.has(status!.id) && waiting.size === 100)
                                    throw new Error('restore_admission_history_exhausted')
                                waiting.add(status!.id)
                            }
                            for (const restoreId of waiting) {
                                const result =
                                    status?.id === restoreId
                                        ? status
                                        : observation.restoreResults.find(
                                              (entry) => entry.id === restoreId,
                                          )
                                if (
                                    result?.operation === 'restore' &&
                                    !['queued', 'running'].includes(result.state) &&
                                    !shouldWait(result)
                                )
                                    waiting.delete(restoreId)
                            }
                            if (waiting.size === 0) break
                            await delay(250)
                            if (!controller.signal.aborted) observation = await readStatus()
                        }
                    }
                    if (maintenanceStopped || shutdownRequested || controller.signal.aborted) {
                        void stopLoaded().catch(() => state('failed'))
                        return
                    }
                    state('idle')
                    startAttempted = true
                    await drainWorker(
                        Promise.resolve(worker.start(controller.signal)),
                        options.startupTimeoutMs,
                    )
                    return
                } catch (error) {
                    if (controller.signal.aborted) return
                    if (error instanceof WorkerDrainTimeoutError) controller.abort()
                    const terminal =
                        error instanceof WorkerDrainTimeoutError ||
                        error instanceof WorkerStoppedInstanceError ||
                        attempt === attempts ||
                        options.classifyFailure?.(error) === 'terminal'
                    // Retry transient restore reads with the same never-started instance.
                    // A failed start or terminal attempt still owns mandatory cleanup.
                    if (worker && (startAttempted || terminal)) {
                        await drainWorker(stopLoaded(), options.drainTimeoutMs)
                        worker = undefined
                        startAttempted = false
                    }
                    if (terminal) {
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
            if (!stopping) state('stopping')
            stopping ??= drainWorker(
                settleWorkerTasks([stopLoaded(), initializing.then(stopLoaded), loading ?? null]),
                options.drainTimeoutMs,
            ).then(
                () => state(maintenanceStopped ? 'maintenance_paused' : 'stopped'),
                (error: unknown) => {
                    if (error instanceof WorkerDrainTimeoutError) {
                        if (id) workerObservability.count(id, 'drainTimeout')
                        void error.drained.then(
                            () => state(maintenanceStopped ? 'maintenance_paused' : 'stopped'),
                            () => state('failed'),
                        )
                    } else state('failed')
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
