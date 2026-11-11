export class WorkerDrainTimeoutError extends Error {
    constructor() {
        super('worker_drain_timeout')
        this.name = 'WorkerDrainTimeoutError'
    }
}

export async function settleWorkerTasks(tasks: Iterable<Promise<unknown> | null>): Promise<void> {
    const results = await Promise.allSettled(tasks)
    const failed = results.find((result) => result.status === 'rejected')
    if (failed?.status === 'rejected') throw failed.reason
}

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
