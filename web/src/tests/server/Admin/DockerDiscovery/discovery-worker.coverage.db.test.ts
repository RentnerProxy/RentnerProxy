// oxlint-disable no-await-in-loop -- Settle each virtual timer turn before advancing the next interval.
import { afterAll, afterEach, beforeAll, beforeEach, expect, jest, mock, spyOn } from 'bun:test'
import { and, eq, inArray } from 'drizzle-orm'
import { DATABASE_CONNECTION_BUDGET } from '@/config/database.config.ts'
import {
    admitDatabaseTransaction,
    withBackgroundDatabaseContext,
} from '@/db/background-admission.ts'
import { auditEvents, proxyHosts, systemSettings, userRoles, users } from '@/db/schema.ts'
import { DiscoveryFinalizationError } from '@/server/Admin/DockerDiscovery/discovery-observer.ts'
import {
    discoveryCandidate,
    discoveryRegistry,
    discoverySnapshot,
} from '@/tests/Helpers/discoveryFixture.ts'
import { dockerDiscoveryController } from '@/server/Controller/docker-discovery.server.ts'
import * as runtime from '@/server/ProxyRuntime/proxy-runtime.service.ts'
import {
    lockProxyRuntimeSettings,
    PROXY_RUNTIME_SETTINGS_KEY,
} from '@/server/ProxyRuntime/proxy-runtime-settings.ts'
import {
    readDiscoveryRegistry,
    writeDiscoveryRegistry,
} from '@/server/Admin/DockerDiscovery/discovery-storage.server.ts'
import { discoveryEndpointHash } from '@/server/Admin/DockerDiscovery/discovery-plan.ts'
import {
    createDockerDiscoveryWorker,
    dockerDiscoveryWorker,
} from '@/server/Admin/DockerDiscovery/discovery-worker.ts'
import { createDiscoveryWorker } from '@/server/Admin/DockerDiscovery/discovery-scheduler.ts'
import * as storage from '@/server/Admin/DockerDiscovery/discovery-storage.server.ts'
import type { DockerDiscoveryObservation } from '@/lib/DockerDiscovery/Types/docker-discovery.types.ts'
import {
    backendCoverageEnabled,
    cleanupBackendCoverage,
    createBackendActor,
    database,
    initializeBackendRegistry,
    integrationTest,
} from '@/tests/Helpers/backendCoverageFixture.ts'
const registryKey = 'docker_discovery_management_v1'
const savedSettings: (typeof systemSettings.$inferSelect)[] = []
const ownedHosts: string[] = []
let generation = 0
const observed = (snapshot: ReturnType<typeof discoverySnapshot>) => ({
    cursor: { runtimeId: 'a'.repeat(32), generation: ++generation },
    snapshot,
})
beforeAll(async () => {
    if (backendCoverageEnabled) await initializeBackendRegistry()
})
beforeEach(async () => {
    if (backendCoverageEnabled)
        savedSettings.push(
            ...(await database
                .select()
                .from(systemSettings)
                .where(inArray(systemSettings.key, [registryKey, PROXY_RUNTIME_SETTINGS_KEY]))),
        )
})
afterEach(async () => {
    jest.useRealTimers()
    mock.restore()
    if (!backendCoverageEnabled) return
    if (ownedHosts.length)
        await database.delete(proxyHosts).where(inArray(proxyHosts.id, ownedHosts.splice(0)))
    await database
        .delete(systemSettings)
        .where(inArray(systemSettings.key, [registryKey, PROXY_RUNTIME_SETTINGS_KEY]))
    if (savedSettings.length) await database.insert(systemSettings).values(savedSettings.splice(0))
    await cleanupBackendCoverage()
})
afterAll(async () => {
    await dockerDiscoveryWorker.stop()
})
async function registry() {
    return database.transaction(readDiscoveryRegistry)
}
async function configure(
    actorUserId: string,
    overrides: Partial<ReturnType<typeof discoveryRegistry>> = {},
) {
    await database.transaction(async (transaction) => {
        await lockProxyRuntimeSettings(transaction)
        await writeDiscoveryRegistry(
            transaction,
            discoveryRegistry({
                enabled: true,
                actorUserId,
                connectorRevision: 1,
                endpointHash: discoveryEndpointHash('http://docker-proxy:2375'),
                ...overrides,
            }),
        )
    })
}

integrationTest(
    'discovery worker remains dormant when disabled and reconciles a real managed host exactly once',
    async () => {
        const actor = await createBackendActor()
        const status = spyOn(dockerDiscoveryController, 'watch').mockResolvedValue(
            observed(
                discoverySnapshot([
                    discoveryCandidate({ domains: [`worker-${actor}.example.com`] }),
                ]),
            ),
        )
        const reconcile = spyOn(runtime, 'reconcileProxyConfigurationService').mockResolvedValue(
            'applied',
        )
        await configure(actor, { enabled: false })
        await dockerDiscoveryWorker.trigger()
        expect(status).not.toHaveBeenCalled()
        await configure(actor)
        await dockerDiscoveryWorker.trigger()
        const managed = await registry()
        ownedHosts.push(...managed.owners.map((owner) => owner.hostId))
        expect(managed.owners).toHaveLength(1)
        expect(managed.runtimeStatus).toBe('applied')
        expect(managed.error).toBeNull()
        const host = await database
            .select()
            .from(proxyHosts)
            .where(eq(proxyHosts.id, managed.owners[0]!.hostId))
        expect(host).toMatchObject([
            { forwardHost: '172.22.0.5', forwardPort: 8080, enabled: true },
        ])
        expect(reconcile).toHaveBeenCalledTimes(1)
        await dockerDiscoveryWorker.trigger()
        expect((await registry()).owners).toHaveLength(1)
        expect(reconcile).toHaveBeenCalledTimes(1)
    },
)

integrationTest.each(['revoked', 'disabled', 'endpoint', 'revision', 'incomplete'] as const)(
    'a snapshot cannot mutate hosts after %s changes during collection',
    async (change) => {
        const actor = await createBackendActor()
        await configure(actor)
        const snapshot = discoverySnapshot([
            discoveryCandidate({ domains: [`fenced-${actor}.example.com`] }),
        ])
        const watch = spyOn(dockerDiscoveryController, 'watch').mockImplementation(async () => {
            if (change === 'revoked')
                await database.delete(userRoles).where(eq(userRoles.userId, actor))
            if (change === 'disabled')
                await database.update(users).set({ status: 'disabled' }).where(eq(users.id, actor))
            if (change === 'endpoint')
                await configure(actor, {
                    endpointHash: discoveryEndpointHash('http://replacement:2375'),
                })
            if (change === 'revision') await configure(actor, { connectorRevision: 2 })
            if (change === 'incomplete') snapshot.stale = true
            return observed(snapshot)
        })
        const apply = spyOn(runtime, 'reconcileProxyConfigurationService').mockResolvedValue(
            'applied',
        )
        const worker = createDockerDiscoveryWorker()
        try {
            await worker.trigger()
            const current = await registry()
            ownedHosts.push(...current.owners.map((owner) => owner.hostId))
            expect(current.owners).toHaveLength(0)
            expect(current.error).toBe(
                ['revoked', 'disabled'].includes(change) ? 'authorization_required' : null,
            )
            expect(watch).toHaveBeenCalledTimes(1)
            expect(apply).not.toHaveBeenCalled()
        } finally {
            await worker.stop()
        }
    },
)

integrationTest(
    'discovery failures persist one outage transition and recover without exposing provider error details',
    async () => {
        const actor = await createBackendActor()
        await configure(actor)
        const warning = spyOn(console, 'warn').mockImplementation(() => undefined)
        const status = spyOn(dockerDiscoveryController, 'watch').mockRejectedValue(
            new Error('private controller transport detail'),
        )
        await dockerDiscoveryWorker.trigger()
        const failed = await registry()
        expect(failed.error).toBe('reconcile_failed')
        await dockerDiscoveryWorker.trigger()
        expect((await registry()).revision).toBe(failed.revision)
        expect(warning).toHaveBeenCalledTimes(1)
        expect(JSON.stringify(warning.mock.calls)).not.toContain('private')
        status.mockResolvedValue(observed(discoverySnapshot([])))
        await dockerDiscoveryWorker.trigger()
        expect((await registry()).error).toBeNull()
    },
)

integrationTest(
    'pending discovery reconciliation cannot overwrite a newer registry revision',
    async () => {
        const actor = await createBackendActor()
        await configure(actor, { runtimeStatus: 'pending' })
        spyOn(dockerDiscoveryController, 'watch').mockResolvedValue(observed(discoverySnapshot([])))
        const originalReconciler = runtime.reconcileProxyConfigurationService
        const reconcile = spyOn(runtime, 'reconcileProxyConfigurationService').mockImplementation(
            Object.assign(
                async (): Promise<'applied'> => {
                    await database.transaction(async (transaction) => {
                        await lockProxyRuntimeSettings(transaction)
                        const current = await readDiscoveryRegistry(transaction)
                        current.revision += 1
                        current.enabled = false
                        await writeDiscoveryRegistry(transaction, current)
                    })
                    return 'applied'
                },
                {
                    start: originalReconciler.start,
                    stop: originalReconciler.stop,
                    checkDrift: originalReconciler.checkDrift,
                },
            ),
        )
        await dockerDiscoveryWorker.trigger()
        const latest = await registry()
        expect(latest.enabled).toBe(false)
        expect(latest.runtimeStatus).toBe('pending')
        expect(reconcile).toHaveBeenCalledTimes(1)
    },
)

integrationTest(
    'discovery start installs one interval and stop prevents all later work',
    async () => {
        jest.useFakeTimers()
        const step = mock(async () => undefined)
        const worker = createDiscoveryWorker(step, 2000)
        try {
            worker.start()
            worker.start()
            await Promise.resolve()
            await Promise.resolve()
            await Promise.resolve()
            expect(step).toHaveBeenCalledTimes(1)
            jest.advanceTimersByTime(2000)
            await Promise.resolve()
            await Promise.resolve()
            await Promise.resolve()
            expect(step).toHaveBeenCalledTimes(2)
            await worker.stop()
            jest.advanceTimersByTime(6000)
            await worker.trigger()
            expect(step).toHaveBeenCalledTimes(2)
            worker.start()
            expect(step).toHaveBeenCalledTimes(2)
        } finally {
            await worker.stop()
            jest.useRealTimers()
        }
    },
)

integrationTest.each([false, true])(
    'real PostgreSQL idle work is bounded to the one-minute fallback, enabled: %s',
    async (enabled) => {
        const actor = await createBackendActor()
        await configure(actor, { enabled })
        const original = storage.readDiscoveryRegistry
        const reads = spyOn(storage, 'readDiscoveryRegistry').mockImplementation(original)
        const stable = observed(discoverySnapshot([]))
        const watch = spyOn(dockerDiscoveryController, 'watch').mockImplementation(
            async (cursor, signal) => {
                if (!cursor) return stable
                return new Promise<DockerDiscoveryObservation>((resolve, reject) => {
                    const cancel = () => {
                        clearTimeout(timer)
                        reject(new Error('cancelled'))
                    }
                    const timer = setTimeout(() => {
                        signal.removeEventListener('abort', cancel)
                        resolve({ cursor, snapshot: null })
                    }, 25_000)
                    signal.addEventListener('abort', cancel, { once: true })
                })
            },
        )
        const apply = spyOn(runtime, 'reconcileProxyConfigurationService').mockResolvedValue(
            'applied',
        )
        const worker = createDockerDiscoveryWorker()
        jest.useFakeTimers()
        try {
            await worker.start()
            const startupReads = reads.mock.calls.length
            const startupRequests = watch.mock.calls.length
            for (let second = 0; second < 60; second += 2) {
                jest.advanceTimersByTime(2000)
                await Promise.resolve()
                await Promise.resolve()
                await Promise.resolve()
            }
            // Joins the already-started safety pass; the extra dirty hint is cancelled by stop below.
            await worker.trigger()
            expect(reads.mock.calls.length - startupReads).toBe(enabled ? 2 : 1)
            expect(watch.mock.calls.length - startupRequests).toBeLessThanOrEqual(enabled ? 4 : 0)
            expect(apply).not.toHaveBeenCalled()
            console.info(
                JSON.stringify({
                    fixture: 'discovery-idle',
                    enabled,
                    windowMs: 60_000,
                    startupExcluded: true,
                    registryReads: reads.mock.calls.length - startupReads,
                    controllerRequestsStarted: watch.mock.calls.length - startupRequests,
                    reconciliationPasses: enabled ? 1 : 0,
                }),
            )
        } finally {
            await worker.stop()
            jest.useRealTimers()
        }
    },
)

integrationTest.each(['waiting', 'deadline', 'busy', 'superseded', 'exhausted'] as const)(
    'shutdown finalization keeps its SQL budget through %s admission and drains the applied receipt',
    async (admission) => {
        const actor = await createBackendActor()
        await configure(actor, { runtimeStatus: 'pending' })
        const inventory = observed(discoverySnapshot([]))
        spyOn(dockerDiscoveryController, 'watch').mockResolvedValue(inventory)
        const applyEntered = Promise.withResolvers<void>()
        const applied = Promise.withResolvers<void>()
        const originalApply = runtime.reconcileProxyConfigurationService
        spyOn(runtime, 'reconcileProxyConfigurationService').mockImplementation(
            Object.assign(
                async (): Promise<'applied'> => {
                    applyEntered.resolve()
                    await applied.promise
                    return 'applied'
                },
                {
                    start: originalApply.start,
                    stop: originalApply.stop,
                    checkDrift: originalApply.checkDrift,
                },
            ),
        )
        const receiptEntered = Promise.withResolvers<void>()
        const finishReceipt = Promise.withResolvers<void>()
        const originalWrite = storage.writeDiscoveryRegistry
        spyOn(storage, 'writeDiscoveryRegistry').mockImplementation(async (transaction, value) => {
            if (value.runtimeStatus === 'applied') {
                receiptEntered.resolve()
                await finishReceipt.promise
            }
            return originalWrite(transaction, value)
        })
        const worker = createDockerDiscoveryWorker()
        const running = worker.start()
        const budgetReady = Promise.withResolvers<void>()
        const releaseBudget = Promise.withResolvers<void>()
        let occupied = 0
        let receiptStarted = false
        void receiptEntered.promise.then(() => {
            receiptStarted = true
        })
        let held: Array<Promise<void>> = []
        const queued: Array<Promise<void>> = []
        let deadline: ReturnType<typeof setTimeout> | undefined
        const expired = new Promise<never>((_, reject) => {
            deadline = setTimeout(
                () => reject(new Error('Receipt finalization fixture stalled')),
                10_000,
            )
        })
        try {
            await Promise.race([applyEntered.promise, expired])
            held = Array.from({ length: DATABASE_CONNECTION_BUDGET.backgroundTransactions }, () =>
                withBackgroundDatabaseContext(() =>
                    admitDatabaseTransaction(async () => {
                        if (++occupied === DATABASE_CONNECTION_BUDGET.backgroundTransactions)
                            budgetReady.resolve()
                        await releaseBudget.promise
                    }),
                ),
            )
            await Promise.race([budgetReady.promise, expired])
            if (admission === 'busy') {
                for (let index = 0; index < DATABASE_CONNECTION_BUDGET.backgroundPending; index++) {
                    const pending = withBackgroundDatabaseContext(() =>
                        admitDatabaseTransaction(async () => undefined),
                    )
                    void pending.catch(() => undefined)
                    queued.push(pending)
                }
                await expect(
                    withBackgroundDatabaseContext(() =>
                        admitDatabaseTransaction(async () => undefined),
                    ),
                ).rejects.toMatchObject({ reason: 'busy' })
            }
            let drained = false
            const stopping = worker.stop().then(() => {
                drained = true
            })
            void stopping.catch(() => undefined)
            applied.resolve()
            await Bun.sleep(0)
            const successfulReceipts = () =>
                database
                    .select({ id: auditEvents.id })
                    .from(auditEvents)
                    .where(
                        and(
                            eq(auditEvents.actorUserId, actor),
                            eq(auditEvents.resource, 'proxy-runtime'),
                            eq(auditEvents.action, 'apply'),
                            eq(auditEvents.result, 'success'),
                        ),
                    )
            if (admission === 'exhausted') {
                await expect(Promise.race([stopping, expired])).rejects.toBeInstanceOf(
                    DiscoveryFinalizationError,
                )
                expect(drained).toBe(false)
                expect((await registry()).runtimeStatus).toBe('pending')
                expect(await successfulReceipts()).toHaveLength(0)
                await expect(worker.stop()).rejects.toBeInstanceOf(DiscoveryFinalizationError)
                return
            }
            if (admission !== 'waiting')
                await Bun.sleep(DATABASE_CONNECTION_BUDGET.backgroundWaitMs + 100)
            expect(receiptStarted).toBe(false)
            expect(drained).toBe(false)
            if (admission === 'superseded')
                await database.transaction(async (transaction) => {
                    await lockProxyRuntimeSettings(transaction)
                    const newer = await readDiscoveryRegistry(transaction)
                    newer.revision += 1
                    newer.enabled = false
                    await writeDiscoveryRegistry(transaction, newer)
                })
            releaseBudget.resolve()
            if (admission === 'superseded') {
                await Promise.all([running, stopping])
                expect(await registry()).toMatchObject({ runtimeStatus: 'pending', enabled: false })
                expect(await successfulReceipts()).toHaveLength(0)
                expect(receiptStarted).toBe(false)
                return
            }
            await Promise.race([receiptEntered.promise, expired])
            expect(drained).toBe(false)
            finishReceipt.resolve()
            await Promise.all([running, stopping])
            expect((await registry()).runtimeStatus).toBe('applied')
            expect(await successfulReceipts()).toHaveLength(1)
            expect(drained).toBe(true)
        } finally {
            if (deadline) clearTimeout(deadline)
            applied.resolve()
            releaseBudget.resolve()
            finishReceipt.resolve()
            await Promise.allSettled([running, worker.stop(), ...held, ...queued])
        }
    },
    15_000,
)

integrationTest(
    'an inventory hint commits a host before apply admission and shutdown drains its truthful receipt',
    async () => {
        const actor = await createBackendActor()
        await configure(actor)
        const first = observed(discoverySnapshot([]))
        const next = Promise.withResolvers<DockerDiscoveryObservation>()
        spyOn(dockerDiscoveryController, 'watch').mockImplementation(async (cursor, signal) => {
            if (!cursor) return first
            return new Promise((resolve, reject) => {
                const cancel = () => reject(new Error('cancelled'))
                signal.addEventListener('abort', cancel, { once: true })
                void next.promise.then((value) => {
                    signal.removeEventListener('abort', cancel)
                    resolve(value)
                })
            })
        })
        const applied = Promise.withResolvers<void>()
        let eventAt = 0
        let applyAdmissionMs = 0
        const original = runtime.reconcileProxyConfigurationService
        const apply = spyOn(runtime, 'reconcileProxyConfigurationService').mockImplementation(
            Object.assign(
                async (): Promise<'applied'> => {
                    applyAdmissionMs = performance.now() - eventAt
                    const committed = await registry()
                    ownedHosts.push(...committed.owners.map((owner) => owner.hostId))
                    expect(committed.owners).toHaveLength(1)
                    expect(
                        (
                            await database
                                .select()
                                .from(proxyHosts)
                                .where(eq(proxyHosts.id, committed.owners[0]!.hostId))
                        )[0]?.forwardHost,
                    ).toBe('172.22.0.5')
                    applied.resolve()
                    return 'applied'
                },
                { start: original.start, stop: original.stop, checkDrift: original.checkDrift },
            ),
        )
        const worker = createDockerDiscoveryWorker()
        let deadline: ReturnType<typeof setTimeout> | undefined
        try {
            await worker.start()
            const began = performance.now()
            eventAt = began
            next.resolve(
                observed(
                    discoverySnapshot([
                        discoveryCandidate({ domains: [`event-${actor}.example.com`] }),
                    ]),
                ),
            )
            await Promise.race([
                applied.promise,
                new Promise<never>((_resolve, reject) => {
                    deadline = setTimeout(
                        () =>
                            reject(
                                new Error('Discovery event-to-apply exceeded its reference bound'),
                            ),
                        3000,
                    )
                }),
            ])
            expect(performance.now() - began).toBeLessThan(3000)
            await worker.stop()
            expect((await registry()).runtimeStatus).toBe('applied')
            expect(apply).toHaveBeenCalledTimes(1)
            console.info(
                JSON.stringify({
                    fixture: 'discovery-event',
                    eventToApplyAdmissionMs: applyAdmissionMs,
                    committedHostVerifiedBeforeApply: true,
                    boundMs: 3000,
                    runtimeApply: 'injected',
                }),
            )
        } finally {
            if (deadline) clearTimeout(deadline)
            await worker.stop()
        }
    },
)

integrationTest(
    'a real fair batch continues past sixteen hosts without another inventory hint',
    async () => {
        const actor = await createBackendActor()
        await configure(actor)
        const inventory = observed(
            discoverySnapshot(
                Array.from({ length: 20 }, (_unused, index) =>
                    discoveryCandidate({
                        containerId: index.toString(16).padStart(64, '0'),
                        sourceId: `batch-${index}`,
                        domains: [`batch-${index}-${actor}.example.com`],
                    }),
                ),
            ),
        )
        const watch = spyOn(dockerDiscoveryController, 'watch').mockImplementation(
            async (cursor, signal) => {
                if (!cursor) return inventory
                return new Promise((_resolve, reject) => {
                    const cancel = () => reject(new Error('cancelled'))
                    signal.addEventListener('abort', cancel, { once: true })
                    if (signal.aborted) cancel()
                })
            },
        )
        const complete = Promise.withResolvers<void>()
        const original = runtime.reconcileProxyConfigurationService
        const apply = spyOn(runtime, 'reconcileProxyConfigurationService').mockImplementation(
            Object.assign(
                async (): Promise<'applied'> => {
                    const committed = await registry()
                    ownedHosts.push(...committed.owners.map((owner) => owner.hostId))
                    if (committed.owners.length === 20) complete.resolve()
                    return 'applied'
                },
                { start: original.start, stop: original.stop, checkDrift: original.checkDrift },
            ),
        )
        const worker = createDockerDiscoveryWorker()
        let deadline: ReturnType<typeof setTimeout> | undefined
        try {
            await worker.start()
            expect((await registry()).owners).toHaveLength(16)
            await Promise.race([
                complete.promise,
                new Promise<never>((_resolve, reject) => {
                    deadline = setTimeout(
                        () => reject(new Error('Fair batch continuation did not converge')),
                        5000,
                    )
                }),
            ])
            await worker.stop()
            expect((await registry()).owners).toHaveLength(20)
            expect((await registry()).runtimeStatus).toBe('applied')
            expect(apply).toHaveBeenCalledTimes(2)
            expect(watch.mock.calls.filter(([cursor]) => cursor === null)).toHaveLength(1)
        } finally {
            if (deadline) clearTimeout(deadline)
            await worker.stop()
        }
    },
    10_000,
)
