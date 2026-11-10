import type { ProxyRuntimeMutationStatus } from '@/lib/ProxyRuntime/Types/proxy-runtime.types.ts'

export interface RuntimeReconcileDependencies<T> {
    readonly enabled?: boolean
    readonly load: () => Promise<T>
    readonly revision: (snapshot: T) => string
    readonly apply: (snapshot: T) => Promise<boolean | RuntimeApplyFailure>
    readonly drift?:
        | { readonly check: () => Promise<boolean> }
        | { readonly matches: (snapshot: T) => Promise<boolean> }
}

export interface RuntimeApplyFailure {
    readonly applied: false
    readonly retryable: boolean
    readonly code: string
    readonly status: number | null
}

export interface RuntimeReconcileOptions {
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
