export type WorkerLifecycle = {
    start: () => void
    stop: () => Promise<void>
}

/** A deadline bounds shutdown waiting, while drained retains ownership of the actual execution. */
export class WorkerDrainTimeoutError extends Error {
    constructor(readonly drained: Promise<void>) {
        super('Worker drain deadline exceeded')
        this.name = 'WorkerDrainTimeoutError'
    }
}
