import '@tanstack/react-start/server-only'
// oxlint-disable no-await-in-loop, no-unmodified-loop-condition -- Poll root completion serially; shutdown changes the listener-owned stop flag.

import type { WorkerLifecycle } from './Types/worker-lifecycle.types.ts'
import { WorkerDrainTimeoutError } from './Types/worker-lifecycle.types.ts'
import type { BackgroundWorkerId } from './Types/worker-observability.types.ts'
import { workerObservability } from './worker-observability.ts'
import { startEventLoopObservation } from './event-loop-observation.ts'
import { readRestoreMaintenanceStatus } from '@/server/Backups/restore-maintenance-status.server.ts'

let maintenanceStopped = false
let maintenanceDrain: Promise<void> | null = null

/** Fence future starts before collecting listener drain promises; propagate any failure to stop an existing writer. */
export async function stopWorkersForRestoreMaintenance(): Promise<void> {
    if (maintenanceDrain) return maintenanceDrain
    maintenanceStopped = true
    const pending: Array<Promise<void>> = []
    process.emit('rentnerproxy:maintenance', pending)
    maintenanceDrain = Promise.all(pending).then(() => undefined)
    return maintenanceDrain
}

/** Delay writer startup until restore state permits it and share shutdown/maintenance drain ownership across initialization races. */
export async function startWorkerLifecycle(
    load: () => Promise<WorkerLifecycle>,
    id?: BackgroundWorkerId,
): Promise<void> {
    if (id) {
        startEventLoopObservation()
        workerObservability.state(id, 'starting')
    }
    let stoppingRequested = false
    const initializing = load().then(async (worker) => {
        let status = await readRestoreMaintenanceStatus()
        if (
            status?.operation === 'restore' &&
            (status.state === 'queued' ||
                status.state === 'running' ||
                ((status.state === 'interrupted' || status.state === 'failed') &&
                    !['queued', 'preparing', 'validating'].includes(status.phase)))
        ) {
            if (id) workerObservability.state(id, 'maintenance_paused')
            const restoreId = status.id
            // The new web can prove readiness, but writers wait for confirmed completion.
            while (
                !stoppingRequested &&
                (status?.id !== restoreId ||
                    (status.state !== 'succeeded' &&
                        !(
                            (status.state === 'failed' || status.state === 'interrupted') &&
                            ['queued', 'preparing', 'validating'].includes(status.phase)
                        )))
            ) {
                await new Promise<void>((resolve) => setTimeout(resolve, 250))
                status = await readRestoreMaintenanceStatus()
            }
        }
        if (!maintenanceStopped && !stoppingRequested) {
            if (id) workerObservability.state(id, 'idle')
            worker.start()
        }
        return worker
    })
    let stopping: Promise<void> | null = null
    /** Publish a terminal lifecycle state only after the worker's real execution has settled. */
    const drained = () => {
        if (id) workerObservability.state(id, maintenanceStopped ? 'maintenance_paused' : 'stopped')
    }
    /** Latch stop intent before initialization completes and share one drain promise while retaining truthful terminal diagnostics. */
    const stop = (pending: Array<Promise<void>>) => {
        stoppingRequested = true
        if (id && !stopping) workerObservability.state(id, 'stopping')
        stopping ??= initializing.then(async (worker) => {
            try {
                await worker.stop()
                drained()
            } catch (error) {
                if (error instanceof WorkerDrainTimeoutError) {
                    if (id) workerObservability.count(id, 'drainTimeout')
                    void error.drained.then(drained).catch(() => {
                        if (id) workerObservability.state(id, 'failed')
                    })
                } else if (id) workerObservability.state(id, 'failed')
                throw error
            }
        })
        pending.push(stopping)
    }
    process.once('rentnerproxy:shutdown', stop)
    process.once('rentnerproxy:maintenance', stop)
    try {
        await initializing
    } catch (error) {
        if (id) workerObservability.state(id, 'failed')
        throw error
    }
    if (maintenanceStopped) {
        if (!stopping) stop([])
        await stopping
    }
}
