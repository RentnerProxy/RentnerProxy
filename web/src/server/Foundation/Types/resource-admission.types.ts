export interface ResourceAdmissionPolicy {
    readonly running: number
    readonly pending: number
    readonly waitMs: number
}

export interface ResourceAdmissionClock {
    readonly now?: () => number
    readonly schedule?: (callback: () => void, delayMs: number) => () => void
}

export interface ResourceWaiter {
    readonly at: number
    readonly resolve: (release: () => void) => void
    readonly reject: (reason: Error) => void
    readonly signal: AbortSignal | undefined
    cancelTimer: () => void
    abort: () => void
}
