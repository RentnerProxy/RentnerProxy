import { workerObservability } from './worker-observability.ts'
import { withBackgroundDatabaseContext } from '@/db/background-admission.ts'
import { ResourceAdmissionError } from './resource-admission.ts'
import type {
    BackgroundWorkerId,
    WorkerOutcome,
    BackgroundWorkerLane,
    WorkerOperationCompletion,
} from './Types/worker-observability.types.ts'

/** Preserve operation results and error identity while recording its lane outcome; deadline aborts are failures and other aborts cancellations. */
export async function observeWorkerOperation<T>(
    id: BackgroundWorkerId,
    operation: () => Promise<T>,
    signal?: AbortSignal,
    outcome?: (value: T) => WorkerOutcome | WorkerOperationCompletion,
    lane?: BackgroundWorkerLane,
): Promise<T> {
    const finish = workerObservability.begin(id, lane)
    try {
        const value = await withBackgroundDatabaseContext(operation, signal)
        const completion = outcome?.(value) ?? 'success'
        finish(
            typeof completion === 'string' ? completion : completion.outcome,
            typeof completion === 'string' ? undefined : completion,
        )
        return value
    } catch (error) {
        if (error instanceof ResourceAdmissionError) {
            finish(error.reason === 'cancelled' ? 'cancelled' : 'deferred', {
                recoverLane: false,
            })
            throw error
        }
        const timedOut =
            signal?.reason instanceof DOMException && signal.reason.name === 'TimeoutError'
        finish(signal?.aborted && !timedOut ? 'cancelled' : 'failure')
        throw error
    }
}
