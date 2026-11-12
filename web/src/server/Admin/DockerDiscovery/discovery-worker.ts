// oxlint-disable-next-line import/no-unassigned-import -- The lifecycle must only run on the server.
import '@tanstack/react-start/server-only'
import { sql } from 'drizzle-orm'
import { getBackgroundDatabase } from '@/server/Foundation/background-database.server.ts'
import { lockProxyRuntimeSettings } from '@/server/ProxyRuntime/proxy-runtime-settings.ts'
import { reconcileProxyConfigurationService } from '@/server/ProxyRuntime/proxy-runtime.service.ts'
import { dockerDiscoveryController } from '@/server/Controller/docker-discovery.server.ts'
import { publishApplicationChange } from '@/server/WebSockets/applicationChanges.ts'
import { subscribeToDiscoveryChanges } from '@/server/WebSockets/discoveryChanges.ts'
import { markDiscoveryReconciliation } from '@/server/WebSockets/committedChanges.ts'
import { readDiscoveryRegistry, writeDiscoveryRegistry } from './discovery-storage.server.ts'
import { reconcileDiscoveryInTransaction } from './discovery-reconcile.server.ts'
import { appendAuditEventInTransactionService } from '@/server/Audit/audit.service.ts'
import {
    createDiscoveryObserver,
    DiscoveryFinalizationError,
    DiscoverySuperseded,
} from './discovery-observer.ts'
import { DockerDiscoveryError } from './docker-discovery.errors.ts'
import { observeWorkerOperation } from '@/server/Foundation/worker-operation.ts'
import { withBackgroundDatabaseContext } from '@/db/background-admission.ts'
import { ResourceAdmissionError } from '@/server/Foundation/resource-admission.ts'
import { DOCKER_DISCOVERY_WORKER } from '@/config/docker-discovery.config.ts'
import type { DockerDiscoverySnapshot } from '@/lib/DockerDiscovery/Types/docker-discovery.types.ts'

/** Retry only unadmitted receipts; every attempt keeps the shared SQL gate and revision fence. */
async function finishAppliedReceipt(commit: () => Promise<void>): Promise<void> {
    await withBackgroundDatabaseContext(async () => {
        // At most seven seconds of admission/backoff plus a five-second SQL transaction.
        for (let attempt = 1; ; attempt++) {
            try {
                // oxlint-disable-next-line no-await-in-loop -- Receipt attempts are serial and never hold a permit during backoff.
                await commit()
                return
            } catch (error) {
                if (
                    !(error instanceof ResourceAdmissionError) ||
                    error.reason === 'cancelled' ||
                    attempt === 5
                )
                    throw new DiscoveryFinalizationError(error)
                // oxlint-disable-next-line no-await-in-loop -- Bounded retry remains owned by the worker drain after collection stops.
                await Bun.sleep(DOCKER_DISCOVERY_WORKER.coalesceMs)
            }
        }
    }, null)
}

/** Persist and audit one discovery failure under its SQL class, then announce only after commit. */
async function recordDiscoveryFailure(): Promise<void> {
    await getBackgroundDatabase('discovery').transaction(async (transaction) => {
        markDiscoveryReconciliation(transaction)
        await transaction.execute(sql`set local lock_timeout = '5s'`)
        await lockProxyRuntimeSettings(transaction)
        const registry = await readDiscoveryRegistry(transaction)
        if (!registry.enabled || registry.error === 'reconcile_failed') return
        registry.error = 'reconcile_failed'
        registry.revision += 1
        await writeDiscoveryRegistry(transaction, registry)
        await appendAuditEventInTransactionService(transaction, {
            actorUserId: registry.actorUserId,
            actorKind: 'user',
            action: 'update',
            resource: 'security-settings',
            targetId: null,
            result: 'failure',
            metadata: { changedFields: ['dockerDiscovery'] },
        })
    })
    publishApplicationChange()
}

export function createDockerDiscoveryWorker() {
    let afterKey: string | null = null
    let remaining: number | null = null
    let versioned = false

    async function readObservation(signal: AbortSignal) {
        try {
            const observation = await dockerDiscoveryController.watch(null, signal)
            if (!observation.snapshot) throw new DockerDiscoveryError('controller_unavailable')
            versioned = true
            return { cursor: observation.cursor, snapshot: observation.snapshot }
        } catch (error) {
            // Old controllers retain bounded polling compatibility. Once negotiated, never
            // downgrade to an unversioned snapshot during a transient watch failure.
            if (signal.aborted || versioned) throw error
            return { cursor: null, snapshot: await dockerDiscoveryController.status(signal) }
        }
    }

    /** Admit SQL by discovery class, preserve fair cursor progress, and perform controller reconciliation outside the transaction permit. */
    async function runDiscoveryCycle(
        snapshot: DockerDiscoverySnapshot,
        signal: AbortSignal,
        assertCurrent: () => void,
        resetBudget: boolean,
    ): Promise<{ enabled: boolean; again: boolean; receiptSettled: boolean }> {
        const result = await getBackgroundDatabase('discovery').transaction(async (transaction) => {
            markDiscoveryReconciliation(transaction)
            await transaction.execute(sql`set local lock_timeout = '5s'`)
            await transaction.execute(sql`set local statement_timeout = '20s'`)
            await lockProxyRuntimeSettings(transaction)
            const registry = await readDiscoveryRegistry(transaction)
            // This is generation admission. A newer hint arriving after admission gets a
            // follow-up; it cannot starve already admitted fair batches during an event flood.
            assertCurrent()
            const progress = { afterKey, attempted: 0, total: 0 }
            const changed = await reconcileDiscoveryInTransaction(
                transaction,
                registry,
                snapshot,
                progress,
            )
            if (signal.aborted) throw new DiscoverySuperseded()
            return {
                revision: registry.revision,
                enabled: registry.enabled,
                pending: changed || registry.runtimeStatus === 'pending',
                ...progress,
            }
        })
        afterKey = result.afterKey
        remaining = Math.max(
            0,
            (resetBudget || remaining === null ? result.total : remaining) - result.attempted,
        )
        if (!result.enabled || !result.pending || signal.aborted)
            return {
                enabled: result.enabled,
                again: result.enabled && remaining > 0,
                receiptSettled: !result.enabled || !result.pending,
            }
        const runtimeStatus = await reconcileProxyConfigurationService()
        if (runtimeStatus === 'applied') {
            // Finish an admitted apply under the same SQL budget after stop cancels collection.
            await finishAppliedReceipt(() =>
                getBackgroundDatabase('discovery').transaction(async (transaction) => {
                    markDiscoveryReconciliation(transaction)
                    await transaction.execute(
                        sql`select set_config('lock_timeout', '5s', true), set_config('statement_timeout', '5s', true), set_config('transaction_timeout', '5s', true)`,
                    )
                    await lockProxyRuntimeSettings(transaction)
                    const registry = await readDiscoveryRegistry(transaction)
                    if (registry.revision !== result.revision) return
                    registry.runtimeStatus = 'applied'
                    registry.revision += 1
                    await writeDiscoveryRegistry(transaction, registry)
                    await appendAuditEventInTransactionService(transaction, {
                        actorUserId: registry.actorUserId,
                        actorKind: 'user',
                        action: 'apply',
                        resource: 'proxy-runtime',
                        targetId: null,
                        result: 'success',
                        metadata: { runtimeStatus: 'applied' },
                    })
                }),
            )
        }
        publishApplicationChange()
        return {
            enabled: result.enabled,
            again: remaining > 0 || runtimeStatus !== 'applied',
            receiptSettled: runtimeStatus === 'applied',
        }
    }

    return createDiscoveryObserver({
        enabled: async () =>
            (
                await getBackgroundDatabase('discovery').transaction(readDiscoveryRegistry, {
                    accessMode: 'read only',
                })
            ).enabled,
        read: (signal) =>
            observeWorkerOperation(
                'docker_discovery',
                () => readObservation(signal),
                signal,
                undefined,
                'collection',
            ),
        watch: (cursor, signal) =>
            observeWorkerOperation(
                'docker_discovery',
                () => dockerDiscoveryController.watch(cursor, signal),
                signal,
                (reply) => (reply.snapshot ? 'success' : 'deferred'),
                'collection',
            ),
        reconcile: (snapshot, signal, assertCurrent, resetBudget) =>
            observeWorkerOperation(
                'docker_discovery',
                () => runDiscoveryCycle(snapshot, signal, assertCurrent, resetBudget),
                signal,
            ),
        subscribe: subscribeToDiscoveryChanges,
        failed: recordDiscoveryFailure,
    })
}

export const dockerDiscoveryWorker = createDockerDiscoveryWorker()
