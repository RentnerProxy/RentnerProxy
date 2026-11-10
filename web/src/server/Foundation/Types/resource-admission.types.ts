import type { BACKGROUND_DATABASE_POLICY } from '@/config/background-resources.config.ts'

export type BackgroundDatabaseClass = keyof typeof BACKGROUND_DATABASE_POLICY.classes

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
