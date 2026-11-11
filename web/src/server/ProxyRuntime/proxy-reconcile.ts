// oxlint-disable-next-line import/no-unassigned-import -- Reconcile must never execute in the browser.
import '@tanstack/react-start/server-only'

import type { ReconcileDependencies, ProxyReconciler } from './Types/proxy-reconcile.types.ts'
import { createRuntimeReconciler } from '@/server/Foundation/runtime-reconciler.server.ts'

const CONTROLLER_APPLY_TIMEOUT_MS = 20_000

/** Propagate reconciler cancellation into each bounded controller apply and require the acknowledged active revision to match the desired snapshot. */
export function createProxyReconciler(
    dependencies: ReconcileDependencies,
    timeoutMs = 25_000,
): ProxyReconciler {
    return createRuntimeReconciler(
        {
            enabled: dependencies.enabled !== false,
            load: dependencies.loadSnapshot,
            revision: (snapshot) => snapshot.revision,
            apply: async (snapshot, signal) => {
                const applied = await dependencies.applySnapshot(
                    snapshot,
                    CONTROLLER_APPLY_TIMEOUT_MS,
                    signal,
                )
                if (applied === null) return false
                if ('ok' in applied) {
                    if (!applied.ok)
                        return {
                            applied: false,
                            retryable: applied.retryable,
                            code: applied.code ?? applied.kind,
                            status: applied.status,
                        }
                    return applied.value.activeRevision === snapshot.revision
                }
                return applied.activeRevision === snapshot.revision
            },
            ...(dependencies.checkDrift ? { drift: { check: dependencies.checkDrift } } : {}),
        },
        {
            name: 'proxy-runtime',
            workerId: 'proxy_reconciliation',
            timeoutMs,
            stopTimeoutMs: CONTROLLER_APPLY_TIMEOUT_MS + 5_000,
        },
    )
}
