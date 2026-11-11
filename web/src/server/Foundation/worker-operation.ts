import { workerObservability } from './worker-observability.ts'
import type {
    BackgroundWorkerId,
    WorkerOutcome,
    BackgroundWorkerLane,
} from './Types/worker-observability.types.ts'

/** Preserve operation results and error identity while recording its lane outcome; deadline aborts are failures and other aborts cancellations. */
export async function observeWorkerOperation<T>(
    id: BackgroundWorkerId,
    operation: () => Promise<T>,
    signal?: AbortSignal,
    outcome?: (value: T) => WorkerOutcome,
    lane?: BackgroundWorkerLane,
): Promise<T> {
    const finish = workerObservability.begin(id, lane)
    try {
        const value = await operation()
        finish(outcome?.(value) ?? 'success')
        return value
    } catch (error) {
        const timedOut =
            signal?.reason instanceof DOMException && signal.reason.name === 'TimeoutError'
        finish(signal?.aborted && !timedOut ? 'cancelled' : 'failure')
        throw error
    }
}
