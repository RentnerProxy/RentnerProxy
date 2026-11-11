import type { ProxyRuntimeMutationStatus } from '@/lib/ProxyRuntime/Types/proxy-runtime.types.ts'
import type { BackgroundWorkerId } from './worker-observability.types.ts'

export interface RuntimeReconcileDependencies<T> {
    readonly enabled?: boolean
    readonly load: (signal?: AbortSignal) => Promise<T>
    readonly revision: (snapshot: T) => string
    readonly apply: (snapshot: T, signal?: AbortSignal) => Promise<boolean | RuntimeApplyFailure>
    readonly drift?:
        | { readonly check: (signal?: AbortSignal) => Promise<boolean> }
        | { readonly matches: (snapshot: T, signal?: AbortSignal) => Promise<boolean> }
}

export interface RuntimeApplyFailure {
    readonly applied: false
    readonly retryable: boolean
    readonly code: string
    readonly status: number | null
}

export interface RuntimeReconcileOptions {
    readonly workerId?: BackgroundWorkerId
    readonly name: string
    readonly timeoutMs: number
    readonly stopTimeoutMs: number
    readonly unrefWaiters?: boolean
}

export interface RuntimeReconcileWaiter {
    readonly target: number
    readonly resolve: (status: ProxyRuntimeMutationStatus) => void
    readonly timer: ReturnType<typeof setTimeout>
}

export interface RuntimeReconciler {
    (): Promise<ProxyRuntimeMutationStatus>
    readonly start: () => void
    readonly stop: () => Promise<void>
    readonly checkDrift: () => Promise<void>
}
