import type {
    ProxyRuntimeSnapshot,
    ProxyRuntimeApplyResponse,
    ProxyRuntimeApplyResult,
} from './proxy-runtime.types.ts'

import type { RuntimeReconciler } from '@/server/Foundation/Types/runtime-reconciler.types.ts'

export interface ReconcileDependencies {
    readonly enabled?: boolean
    readonly loadSnapshot: () => Promise<ProxyRuntimeSnapshot>
    readonly checkDrift?: () => Promise<boolean>
    readonly applySnapshot: (
        snapshot: ProxyRuntimeSnapshot,
        timeoutMs: number,
    ) => Promise<ProxyRuntimeApplyResponse | ProxyRuntimeApplyResult | null>
}

export type ProxyReconciler = RuntimeReconciler
