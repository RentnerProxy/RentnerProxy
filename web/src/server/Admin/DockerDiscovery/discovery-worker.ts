// oxlint-disable-next-line import/no-unassigned-import -- The lifecycle must only run on the server.
import '@tanstack/react-start/server-only'
import { sql } from 'drizzle-orm'
import { getAuthDatabase } from '@/server/Auth/Core/database.server.ts'
import { lockProxyRuntimeSettings } from '@/server/ProxyRuntime/proxy-runtime-settings.ts'
import { reconcileProxyConfigurationService } from '@/server/ProxyRuntime/proxy-runtime.service.ts'
import { dockerDiscoveryController } from '@/server/Controller/docker-discovery.server.ts'
import { publishApplicationChange } from '@/server/WebSockets/applicationChanges.ts'
import { subscribeToDiscoveryChanges } from '@/server/WebSockets/discoveryChanges.ts'
import { markDiscoveryReconciliation } from '@/server/WebSockets/committedChanges.ts'
import { readDiscoveryRegistry, writeDiscoveryRegistry } from './discovery-storage.server.ts'
import { reconcileDiscoveryInTransaction } from './discovery-reconcile.server.ts'
import { appendAuditEventInTransactionService } from '@/server/Audit/audit.service.ts'
import { createDiscoveryObserver, DiscoverySuperseded } from './discovery-observer.ts'
import { DockerDiscoveryError } from './docker-discovery.errors.ts'
import type { DockerDiscoverySnapshot } from '@/lib/DockerDiscovery/Types/docker-discovery.types.ts'

async function recordDiscoveryFailure(): Promise<void> {
    await getAuthDatabase().transaction(async (transaction) => {
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

    async function runDiscoveryCycle(
        snapshot: DockerDiscoverySnapshot,
        signal: AbortSignal,
        assertCurrent: () => void,
        resetBudget: boolean,
    ): Promise<{ enabled: boolean; again: boolean }> {
        const result = await getAuthDatabase().transaction(async (transaction) => {
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
            return { enabled: result.enabled, again: result.enabled && remaining > 0 }
        const runtimeStatus = await reconcileProxyConfigurationService()
        if (runtimeStatus === 'applied') {
            // An admitted apply can finish during stop; preserve its truthful receipt.
            await getAuthDatabase().transaction(async (transaction) => {
                markDiscoveryReconciliation(transaction)
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
            })
        }
        publishApplicationChange()
        return { enabled: result.enabled, again: remaining > 0 || runtimeStatus !== 'applied' }
    }

    return createDiscoveryObserver({
        enabled: async () =>
            (
                await getAuthDatabase().transaction(readDiscoveryRegistry, {
                    accessMode: 'read only',
                })
            ).enabled,
        read: readObservation,
        watch: (cursor, signal) => dockerDiscoveryController.watch(cursor, signal),
        reconcile: runDiscoveryCycle,
        subscribe: subscribeToDiscoveryChanges,
        failed: recordDiscoveryFailure,
    })
}

export const dockerDiscoveryWorker = createDockerDiscoveryWorker()
