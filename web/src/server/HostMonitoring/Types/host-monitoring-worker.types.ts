/** A failed fenced release retains the original cause so worker drain cannot acknowledge unresolved lease ownership. */
export class HostMonitoringCleanupError extends Error {
    constructor(cause: unknown) {
        super('host_monitoring_cleanup_failed', { cause })
    }
}

export interface HostMonitoringWorkerDependencies<Claim> {
    // Storage must return at most the requested number of claims.
    readonly claim: (limit: number) => Promise<readonly Claim[]>
    // Owns durable completion and cancellation cleanup; unresolved releases reject HostMonitoringCleanupError.
    readonly runClaim: (claim: Claim, signal: AbortSignal) => Promise<void>
    readonly releaseCancelled: (claim: Claim) => Promise<void>
    readonly prune: () => Promise<void>
    // Deliberately receives no exception or target data.
    readonly unavailable: () => void
    readonly now?: () => number
    readonly schedule?: (callback: () => void, delayMs: number) => () => void
}
