import { WorkerDrainTimeoutError } from './Types/worker-lifecycle.types.ts'
export { WorkerDrainTimeoutError } from './Types/worker-lifecycle.types.ts'

/** Await all admitted siblings before propagating the first failure so rejection cannot falsely establish quiescence. */
export async function settleWorkerTasks(tasks: Iterable<Promise<unknown> | null>): Promise<void> {
    const results = await Promise.allSettled(
        Array.from(tasks, async (task) => {
            try {
                await task
            } catch (error) {
                // A nested checkpoint can time out while its admitted execution still owns resources.
                if (error instanceof WorkerDrainTimeoutError) return error.drained
                throw error
            }
        }),
    )
    const failed = results.find((result) => result.status === 'rejected')
    if (failed?.status === 'rejected') throw failed.reason
}

/** Reject a missed drain deadline without claiming underlying work stopped; always clear the watchdog timer. */
export async function drainWorker(pending: Promise<unknown>, timeoutMs = 15_000): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
        await Promise.race([
            pending,
            new Promise<never>((_, reject) => {
                timer = setTimeout(
                    () => reject(new WorkerDrainTimeoutError(settleWorkerTasks([pending]))),
                    timeoutMs,
                )
            }),
        ])
    } finally {
        clearTimeout(timer)
    }
}
