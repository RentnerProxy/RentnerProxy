// oxlint-disable no-await-in-loop -- Discrete lease faults and final durable readbacks are intentionally sequential.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { startBackgroundNotificationReceivers } from './background-receivers.ts'
import {
    NOTIFICATION_LIMITS,
    NOTIFICATION_SETTINGS_KEY,
    NOTIFICATION_STATE_KEY,
} from '../../web/src/config/notifications.config.ts'
import { systemSettings } from '../../web/src/db/schema.ts'
import { inArray } from 'drizzle-orm'
import { observeNotificationSignals } from '../../web/src/server/Notifications/incident-engine.ts'
import * as repository from '../../web/src/server/Notifications/notification-repository.ts'
import * as storage from '../../web/src/server/Notifications/notification-storage.ts'
import { deliverNotification } from '../../web/src/server/Notifications/delivery.service.ts'
import { sendNotificationEmailService } from '../../web/src/server/Mail/mail.service.ts'
import type { FixtureContext } from '../runtime-reliability/Types/fixture.types.ts'
import type { StoredNotificationConfiguration } from '../../web/src/server/Notifications/Types/notification-internal.types.ts'
import { backgroundDistribution, backgroundSlo } from './background.ts'
import type { NotificationDeliveryResult } from '../../web/src/lib/Notifications/Types/notifications.types.ts'
import { backgroundDeferredNotifications } from './background-notification-identities.ts'

/** Produce deterministic per-seed notification identities with a private sequence independent from global random state. */
function idFactory(seed: number) {
    let sequence = 0
    return () => {
        const value = createHash('sha256')
            .update(`notification:${seed}:${sequence++}`)
            .digest('hex')
        return `${value.slice(0, 8)}-${value.slice(8, 12)}-4${value.slice(13, 16)}-8${value.slice(17, 20)}-${value.slice(20, 32)}`
    }
}

/** Exercise real delivery transports and durable lease faults only against unused fixture settings; restore settings, clock, and receivers afterward. */
export async function runBackgroundNotifications(
    context: Pick<FixtureContext, 'database' | 'command'>,
) {
    const database = context.database
    const makeId = idFactory(context.command.seed)
    const keys = [NOTIFICATION_SETTINGS_KEY, NOTIFICATION_STATE_KEY]
    const originalRows = await database
        .select({ key: systemSettings.key })
        .from(systemSettings)
        .where(inArray(systemSettings.key, keys))
    const originalConfiguration = await database.transaction(storage.readNotificationConfiguration)
    const originalRuntime = await database.transaction(storage.readNotificationRuntime)
    assert.ok(
        !originalConfiguration.enabled && originalConfiguration.channels.length === 0,
        'Only unused fixture notification configuration is permitted',
    )
    const receivers = await startBackgroundNotificationReceivers()
    const { webhook, accepted, receiverAttempts, receiverIdentity } = receivers
    const configuration: StoredNotificationConfiguration = {
        version: 1,
        revision: 1,
        enabled: true,
        pausedAfterRestore: false,
        channels: Array.from({ length: 8 }, (_, index) => ({
            id: makeId(),
            name: 'Fixture ' + index,
            revision: 1,
            enabled: true,
            kind: index % 2 === 0 ? ('email' as const) : ('webhook' as const),
            events: [index % 4 < 2 ? ('ddns' as const) : ('proxy_configuration' as const)],
            minimumSeverity: 'error' as const,
            thresholdSeconds: 0,
            recovery: true,
            destination: { ciphertext: 'a'.repeat(32), iv: 'b'.repeat(16) },
            hasAuthorization: false,
        })),
    }
    const start = Date.now()
    const wall = performance.now()
    let now = start
    const realNow = Date.now
    const admitted = new Set<string>(),
        acknowledged = new Set<string>(),
        permanent = new Set<string>()
    const claimMs: number[] = [],
        completionMs: number[] = [],
        transportMs: number[] = []
    const attempts = new Map<string, number>()
    const slow = { email: 0, webhook: 0 }
    const faults: { fault: string; deliveryId: string; passed: boolean }[] = []
    let initialAdmitted: string[] = [],
        initialDeferred = 0,
        finalPending = -1
    let initiallyDeferredIdentities: ReturnType<typeof backgroundDeferredNotifications> = []
    const admittedIdentities = new Map<
        string,
        {
            incidentId: string
            channelId: string
            channelRevision: number
            type: string
            deliveryId: string
        }
    >()
    const runtime = () => database.transaction(storage.readNotificationRuntime)
    try {
        Date.now = () => now
        await database.transaction(async (transaction) => {
            await storage.lockNotificationConfiguration(transaction)
            await storage.writeNotificationConfiguration(transaction, configuration)
            const value = await storage.lockNotificationRuntime(transaction)
            const engine = observeNotificationSignals(
                value.engine,
                configuration,
                Array.from({ length: 80 }, (_, index) => ({
                    source: 'event' as const,
                    category:
                        index % 2 === 0 ? ('ddns' as const) : ('proxy_configuration' as const),
                    resourceId: 'fixture-' + index,
                    health: 'failing' as const,
                    severity: 'error' as const,
                    occurredAt: new Date(now).toISOString(),
                    evidence: 'seed-' + context.command.seed,
                    failures: 1,
                })),
                now,
                makeId,
            )
            initialAdmitted = engine.deliveries.map((item) => item.id)
            initialDeferred = engine.deferredCount
            initiallyDeferredIdentities = backgroundDeferredNotifications(
                engine,
                configuration,
                now,
            )
            assert.equal(initiallyDeferredIdentities.length, initialDeferred)
            assert.ok(
                initialDeferred > 0 && initialAdmitted.length <= NOTIFICATION_LIMITS.deliveries,
            )
            await storage.writeNotificationRuntime(transaction, { ...value, engine })
        })
        for (let step = 0; step < 20000; step++) {
            assert.ok(
                performance.now() - wall < backgroundSlo.notifications.wallBudgetMs,
                'Notification fixture exceeded its predeclared 45-second wall budget',
            )
            const started = performance.now()
            const claim = await repository.claimNextNotificationDelivery()
            claimMs.push(performance.now() - started)
            if (!claim) {
                const state = await runtime()
                finalPending =
                    state.engine.deliveries.filter(
                        (item) => item.status === 'pending' || item.status === 'inflight',
                    ).length + state.engine.deferredCount
                if (!finalPending) break
                const next = await repository.readNextNotificationDeliveryAt()
                assert.ok(
                    next !== null && Number.isFinite(next),
                    'Pending work must have a bounded next wake',
                )
                now = Math.max(now + NOTIFICATION_LIMITS.wakeCoalesceMs, next)
                continue
            }
            admitted.add(claim.id)
            admittedIdentities.set(claim.id, {
                incidentId: claim.payload.incidentId,
                channelId: claim.channelId,
                channelRevision: claim.channelRevision,
                type: claim.payload.type,
                deliveryId: claim.id,
            })
            attempts.set(claim.id, (attempts.get(claim.id) ?? 0) + 1)
            const channel = configuration.channels.findIndex((item) => item.id === claim.channelId)
            if (channel !== 0 && attempts.get(claim.id) === 1 && slow[claim.channel.kind] < 2) {
                receivers.slowPayloadIds.add(claim.payload.id)
                slow[claim.channel.kind]++
            }
            const fault =
                channel !== 0 && attempts.get(claim.id) === 1 && faults.length < 3
                    ? ['crash-before-send', 'crash-after-receiver-ack', 'crash-after-durable-ack'][
                          faults.length
                      ]
                    : undefined
            if (fault === 'crash-before-send') {
                now += NOTIFICATION_LIMITS.leaseMs + 1
                await repository.acknowledgeNotificationDelivery(claim.leaseToken, {
                    success: true,
                })
                const entry = (await runtime()).engine.deliveries.find(
                    (item) => item.id === claim.id,
                )
                assert.ok(
                    entry && entry.status !== 'succeeded',
                    'Expired token cannot commit a send which never occurred',
                )
                faults.push({ fault, deliveryId: claim.id, passed: true })
                continue
            }
            const transportStarted = performance.now()
            receiverIdentity.set(
                createHash('sha256').update(claim.payload.id).digest('hex'),
                claim.payload.id,
            )
            const result: NotificationDeliveryResult =
                claim.channel.kind === 'webhook'
                    ? await deliverNotification(
                          {
                              kind: 'webhook',
                              url: `http://127.0.0.1:${webhook.port}/`,
                              authorizationToken: null,
                              allowedPrivateCidrs: ['127.0.0.1/32'],
                          },
                          claim.payload,
                      )
                    : await sendNotificationEmailService(
                          [
                              channel === 0
                                  ? 'permanent@fixture.invalid'
                                  : 'accepted@fixture.invalid',
                          ],
                          claim.payload,
                          undefined,
                          {
                              createClient: receivers.createMailClient,
                          },
                      )
            transportMs.push(performance.now() - transportStarted)
            if (fault === 'crash-after-receiver-ack') {
                assert.ok(result.success)
                now += NOTIFICATION_LIMITS.leaseMs + 1
                await repository.acknowledgeNotificationDelivery(claim.leaseToken, result)
                const entry = (await runtime()).engine.deliveries.find(
                    (item) => item.id === claim.id,
                )
                assert.ok(
                    entry && entry.status !== 'succeeded',
                    'Receiver acceptance is distinct from a durable acknowledgement',
                )
                faults.push({ fault, deliveryId: claim.id, passed: true })
                continue
            }
            await repository.acknowledgeNotificationDelivery(claim.leaseToken, result)
            const entry = (await runtime()).engine.deliveries.find((item) => item.id === claim.id)
            assert.ok(entry)
            if (result.success) {
                assert.equal(entry.status, 'succeeded')
                acknowledged.add(claim.id)
            } else if (!result.retryable) {
                assert.equal(entry.status, 'failed')
                permanent.add(claim.id)
            }
            if (fault === 'crash-after-durable-ack') {
                const before = JSON.stringify(entry)
                await repository.acknowledgeNotificationDelivery(claim.leaseToken, result)
                assert.equal(
                    JSON.stringify(
                        (await runtime()).engine.deliveries.find((item) => item.id === claim.id),
                    ),
                    before,
                    'Replayed acknowledgement cannot mutate final state',
                )
                faults.push({ fault, deliveryId: claim.id, passed: true })
            }
            completionMs.push(now - start)
        }
        assert.equal(finalPending, 0)
        assert.equal(admitted.size, 320)
        assert.equal(acknowledged.size, 280)
        assert.equal(permanent.size, 40)
        const deferredAdmissions = initiallyDeferredIdentities.map((identity) => {
            const matches = [...admittedIdentities.values()].filter(
                (item) =>
                    item.incidentId === identity.incidentId &&
                    item.channelId === identity.channelId &&
                    item.channelRevision === identity.channelRevision &&
                    item.type === identity.type,
            )
            assert.equal(
                matches.length,
                1,
                'Each deferred obligation has one subsequently allocated delivery identity',
            )
            return matches[0]!
        })
        assert.equal(faults.length, 3)
        assert.deepEqual(slow, { email: 2, webhook: 2 })
        assert.ok(
            [...receivers.slowPayloadIds].every((id) => accepted.has(id)),
            'Both slow transport lanes reach the deduplicating receiver',
        )
        assert.ok(now - start <= backgroundSlo.notifications.schedulerDrainMs)
        assert.ok(performance.now() - wall <= backgroundSlo.notifications.wallBudgetMs)
        const final = await runtime()
        assert.equal(final.engine.incidents.length, 80, 'Retries cannot create duplicate incidents')
        assert.ok(
            final.engine.deliveries.every(
                (item) =>
                    item.status !== 'inflight' && item.status !== 'pending' && !item.leaseToken,
            ),
            'All final leases and deferred work drain',
        )
        const collection = await repository.claimNotificationCollection()
        assert.ok(collection)
        const cursor = Bun.randomUUIDv7() + ':1'
        await repository.commitNotificationCollection(collection, {
            observations: [],
            certificateCursor: cursor,
        })
        assert.equal((await runtime()).certificateCursor, cursor)
        const committed = JSON.stringify(await runtime())
        assert.equal(
            await repository.commitNotificationCollection(collection, {
                observations: [],
                certificateCursor: Bun.randomUUIDv7() + ':2',
            }),
            false,
        )
        assert.equal(
            JSON.stringify(await runtime()),
            committed,
            'Consumed collection token cannot replay or advance its durable cursor',
        )
        const staleCollection = await repository.claimNotificationCollection()
        assert.ok(staleCollection)
        await database.transaction(async (transaction) => {
            await storage.lockNotificationConfiguration(transaction)
            await storage.writeNotificationConfiguration(transaction, {
                ...configuration,
                revision: 2,
                enabled: false,
                pausedAfterRestore: true,
            })
        })
        assert.equal(
            await repository.commitNotificationCollection(staleCollection, {
                observations: [],
                certificateCursor: Bun.randomUUIDv7() + ':3',
            }),
            false,
        )
        assert.equal(
            (await runtime()).certificateCursor,
            cursor,
            'Restore pause fences active collection progress',
        )
        assert.equal(await repository.claimNotificationCollection(), null)
        assert.equal(await repository.claimNextNotificationDelivery(), null)
        assert.equal(await repository.readNextNotificationDeliveryAt(), null)
        return {
            seed: context.command.seed,
            timingModel:
                'discrete scheduling/lease expiry; real PostgreSQL and production webhook/SMTP transports against local slow deduplicating receivers; no appliance throughput claim',
            attempted: 320,
            admissionLimit: NOTIFICATION_LIMITS.deliveries,
            initiallyAdmittedIds: initialAdmitted,
            initiallyDeferred: initialDeferred,
            initiallyDeferredIdentities,
            deferredAdmissions,
            admittedIds: [...admitted],
            receiverAcceptedIds: [...accepted],
            acknowledgedIds: [...acknowledged],
            permanentlyFailedIds: [...permanent],
            faults,
            collectionFences: {
                replayRejected: true,
                restorePauseRejected: true,
                finalCursor: cursor,
                admissionWhilePaused: 0,
            },
            duplicateReceiverAttempts: [...receiverAttempts.values()].reduce(
                (sum, value) => sum + Math.max(0, value - 1),
                0,
            ),
            remainingDuplicateRisk:
                'Receiver deduplication prevents repeated fixture effects; external delivery remains at-least-once across acceptance/durable-ack crashes',
            finalPending,
            finalIncidentCount: final.engine.incidents.length,
            wallElapsedMs: performance.now() - wall,
            schedulerElapsedMs: now - start,
            claimWallMs: backgroundDistribution(claimMs),
            completionSchedulerMs: backgroundDistribution(completionMs),
            transportWallMs: backgroundDistribution(transportMs),
            receiverDelaysMs: { healthyWebhook: 15, healthySmtp: 30, slow: 500 },
            slowReceiverCases: slow,
            status: 'met',
        }
    } finally {
        Date.now = realNow
        await receivers.close()
        await database.transaction(async (transaction) => {
            await storage.lockNotificationConfiguration(transaction)
            await storage.writeNotificationConfiguration(transaction, originalConfiguration)
            await storage.lockNotificationRuntime(transaction)
            await storage.writeNotificationRuntime(transaction, originalRuntime)
        })
        const created = keys.filter((key) => !originalRows.some((row) => row.key === key))
        if (created.length)
            await database.delete(systemSettings).where(inArray(systemSettings.key, created))
    }
}
