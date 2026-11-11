export interface HostMonitoringWorkerDependencies<Claim> {
    // Storage must return at most the requested number of claims.
    readonly claim: (limit: number) => Promise<readonly Claim[]>
    // Owns durable completion and cancellation cleanup for an admitted probe.
    readonly runClaim: (claim: Claim, signal: AbortSignal) => Promise<void>
    readonly releaseCancelled: (claim: Claim) => Promise<void>
    readonly prune: () => Promise<void>
    readonly nextAdmissionAt?: () => Promise<number | null>
    readonly subscribe?: (wake: () => void) => () => void
    // Deliberately receives no exception or target data.
    readonly unavailable: () => void
    readonly now?: () => number
    readonly schedule?: (callback: () => void, delayMs: number) => () => void
}
