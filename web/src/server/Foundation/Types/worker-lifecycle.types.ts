export type WorkerLifecycle = {
    start: (signal?: AbortSignal) => void | Promise<unknown>
    stop: () => Promise<void>
}

export interface WorkerLifecycleOptions {
    maxAttempts?: number
    retryDelayMs?: number
    startupTimeoutMs?: number
    drainTimeoutMs?: number
    classifyFailure?: (error: unknown) => 'retryable' | 'terminal'
}
