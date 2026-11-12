import { randomUUID } from 'node:crypto'
import { createServer } from 'node:http'
import { afterEach, beforeAll, beforeEach, describe, expect, test } from 'bun:test'
import { eq, inArray, sql } from 'drizzle-orm'
import { NOTIFICATION_SETTINGS_KEY, NOTIFICATION_STATE_KEY } from '@/config/notifications.config.ts'
import { systemSettings } from '@/db/schema.ts'
import { getAuthDatabase } from '@/server/Auth/Core/database.server.ts'
import { getDatabaseUrl } from '@/server/env.server.ts'
import { ResourceAdmissionError } from '@/server/Foundation/resource-admission.ts'
import { getBackgroundDatabaseResourceSnapshot } from '@/server/Foundation/background-database.server.ts'
import {
    createNotificationState,
    observeNotificationSignals,
} from '@/server/Notifications/incident-engine.ts'
import {
    acknowledgeNotificationDelivery,
    claimNextNotificationDelivery,
    claimNotificationCollection,
    commitNotificationCollection,
    authorizeNotificationDelivery,
    readNextNotificationDeliveryAt,
} from '@/server/Notifications/notification-repository.ts'
import {
    lockNotificationConfiguration,
    lockNotificationRuntime,
    readNotificationConfiguration,
    readNotificationRuntime,
    writeNotificationConfiguration,
    writeNotificationRuntime,
} from '@/server/Notifications/notification-storage.ts'
import type {
    NotificationRuntime,
    StoredNotificationConfiguration,
} from '@/server/Notifications/Types/notification-internal.types.ts'
import type { NotificationCollectedBatch } from '@/server/Notifications/Types/notification-worker.types.ts'
import { createNotificationWorker } from '@/server/Notifications/notification-worker.ts'
import { createAuthorizedNotificationDelivery } from '@/server/Notifications/notification-delivery.server.ts'
import { deliverWebhookNotification } from '@/server/Notifications/webhook-delivery.ts'
import { sendNotificationEmailService } from '@/server/Mail/mail.service.ts'

const enabled = process.env.RENTNERPROXY_DATABASE_INTEGRATION === '1' && getDatabaseUrl() !== null
const integrationTest = enabled ? test : test.skip
const keys = [NOTIFICATION_SETTINGS_KEY, NOTIFICATION_STATE_KEY]
const channelId = '00000000-0000-4000-8000-000000000237'
const initialCursor = '019b1234-1234-7000-8000-000000000001:1'
const nextCursor = '019b1234-1234-7000-8000-000000000001:2'
let dedicatedDatabaseVerified = false

function configuration(): StoredNotificationConfiguration {
    return {
        version: 1,
        revision: 1,
        enabled: true,
        pausedAfterRestore: false,
        channels: [
            {
                id: channelId,
                name: 'Repository fixture',
                kind: 'webhook',
                revision: 1,
                enabled: true,
                events: ['ddns'],
                minimumSeverity: 'error',
                thresholdSeconds: 0,
                recovery: true,
                // Shape-valid sealed fixtures only: repository operations never decrypt or deliver them.
                destination: { ciphertext: 'a'.repeat(32), iv: 'b'.repeat(16) },
                hasAuthorization: false,
            },
        ],
    }
}

function batch(resourceId = 'repository-resource'): NotificationCollectedBatch {
    return {
        certificateCursor: nextCursor,
        observations: [
            {
                source: 'snapshot',
                category: 'ddns',
                resourceId,
                health: 'failing',
                severity: 'error',
                occurredAt: new Date(Date.now() - 1_000).toISOString(),
                evidence: `operation-${resourceId}`,
                failures: 1,
            },
        ],
    }
}

async function readRuntime() {
    return getAuthDatabase().transaction(readNotificationRuntime, { accessMode: 'read only' })
}

async function runtimeVersion() {
    const [row] = await getAuthDatabase()
        .select({ xmin: sql<string>`xmin::text` })
        .from(systemSettings)
        .where(eq(systemSettings.key, NOTIFICATION_STATE_KEY))
    return row?.xmin
}

async function mutateRuntime(change: (runtime: NotificationRuntime) => void) {
    await getAuthDatabase().transaction(async (transaction) => {
        await lockNotificationConfiguration(transaction)
        const runtime = await lockNotificationRuntime(transaction)
        change(runtime)
        await writeNotificationRuntime(transaction, runtime)
    })
}

async function seedFailure() {
    const claim = await claimNotificationCollection()
    expect(claim).not.toBeNull()
    expect(await commitNotificationCollection(claim!, batch())).toBe(true)
}

async function changeConfiguration(
    change: (current: StoredNotificationConfiguration) => StoredNotificationConfiguration,
) {
    await getAuthDatabase().transaction(async (transaction) => {
        const current = await lockNotificationConfiguration(transaction)
        const next = change(current)
        const runtime = await lockNotificationRuntime(transaction)
        runtime.engine = observeNotificationSignals(
            runtime.engine,
            next,
            [],
            Date.now(),
            randomUUID,
        )
        await writeNotificationConfiguration(transaction, next)
        await writeNotificationRuntime(transaction, runtime)
    })
}

async function cleanup() {
    if (!dedicatedDatabaseVerified)
        throw new Error('Disposable notification database was not verified.')
    await getAuthDatabase().delete(systemSettings).where(inArray(systemSettings.key, keys))
}

beforeAll(() => {
    if (!enabled) return
    if (new URL(getDatabaseUrl()!).pathname !== '/rentnerproxy_upstream_test')
        throw new Error(
            'Notification repository integration requires its disposable test database.',
        )
    dedicatedDatabaseVerified = true
})

beforeEach(async () => {
    if (!enabled) return
    await cleanup()
    await getAuthDatabase().transaction(async (transaction) => {
        await lockNotificationConfiguration(transaction)
        await writeNotificationConfiguration(transaction, configuration())
        const runtime = await lockNotificationRuntime(transaction)
        await writeNotificationRuntime(transaction, {
            ...runtime,
            certificateCursor: initialCursor,
        })
    })
})

afterEach(async () => {
    if (enabled && dedicatedDatabaseVerified) await cleanup()
})

describe('notification repository PostgreSQL durability and concurrency', () => {
    for (const absent of [false, true])
        integrationTest(
            `locking ${absent ? 'absent' : 'existing'} settings waits for and reads the committed configuration and cursor`,
            async () => {
                if (absent) await cleanup()
                const locked = Promise.withResolvers<void>()
                const release = Promise.withResolvers<void>()
                const applicationName = `notification-lock-${randomUUID()}`
                const first = getAuthDatabase().transaction(async (transaction) => {
                    const current = await lockNotificationConfiguration(transaction)
                    await writeNotificationConfiguration(transaction, {
                        ...current,
                        revision: 2,
                    })
                    const runtime = await lockNotificationRuntime(transaction)
                    await writeNotificationRuntime(transaction, {
                        ...runtime,
                        certificateCursor: nextCursor,
                    })
                    locked.resolve()
                    await release.promise
                })
                let second: Promise<unknown> | undefined
                try {
                    await locked.promise
                    second = getAuthDatabase().transaction(async (transaction) => {
                        await transaction.execute(
                            sql`select set_config('application_name', ${applicationName}, true)`,
                        )
                        const current = await lockNotificationConfiguration(transaction)
                        const runtime = await lockNotificationRuntime(transaction)
                        expect(current.revision).toBe(2)
                        expect(runtime.certificateCursor).toBe(nextCursor)
                    })
                    const deadline = performance.now() + 5_000
                    let waiting = false
                    while (!waiting && performance.now() < deadline) {
                        // oxlint-disable-next-line no-await-in-loop -- Observe the competing transaction's actual lock wait before releasing its owner.
                        const [state] = await getAuthDatabase().execute<{ waiting: boolean }>(
                            sql`select exists(select 1 from pg_stat_activity where application_name = ${applicationName} and wait_event_type = 'Lock') as waiting`,
                        )
                        waiting = state?.waiting === true
                        // oxlint-disable-next-line no-await-in-loop -- This bounded poll waits only for the real PostgreSQL lock barrier.
                        if (!waiting) await Bun.sleep(10)
                    }
                    expect(waiting).toBe(true)
                } finally {
                    release.resolve()
                    await first
                    await second
                }
            },
            10_000,
        )

    integrationTest('initializing both notification rows rolls back with the transaction', async () => {
        await cleanup()
        await expect(
            getAuthDatabase().transaction(async (transaction) => {
                await lockNotificationConfiguration(transaction)
                const runtime = await lockNotificationRuntime(transaction)
                await writeNotificationRuntime(transaction, {
                    ...runtime,
                    certificateCursor: nextCursor,
                })
                throw new Error('Fixture rollback')
            }),
        ).rejects.toThrow('Fixture rollback')
        expect(
            await getAuthDatabase()
                .select({ key: systemSettings.key })
                .from(systemSettings)
                .where(inArray(systemSettings.key, keys)),
        ).toHaveLength(0)
    })

    integrationTest('a waiting initializer replaces rolled-back defaults without retaining their cursor', async () => {
        await cleanup()
        const locked = Promise.withResolvers<void>()
        const release = Promise.withResolvers<void>()
        const applicationName = `notification-rollback-${randomUUID()}`
        const rollback = new Error('Fixture initializer rollback')
        const first = getAuthDatabase().transaction(async (transaction) => {
            await lockNotificationConfiguration(transaction)
            const runtime = await lockNotificationRuntime(transaction)
            await writeNotificationRuntime(transaction, { ...runtime, certificateCursor: nextCursor })
            locked.resolve()
            await release.promise
            throw rollback
        }).catch((error: unknown) => { expect(error).toBe(rollback) })
        let second: Promise<unknown> | undefined
        try {
            await locked.promise
            second = getAuthDatabase().transaction(async (transaction) => {
                await transaction.execute(sql`select set_config('application_name', ${applicationName}, true)`)
                const current = await lockNotificationConfiguration(transaction)
                const runtime = await lockNotificationRuntime(transaction)
                expect(current.revision).toBe(0)
                expect(runtime.certificateCursor).toBeNull()
                await writeNotificationRuntime(transaction, { ...runtime, certificateCursor: initialCursor })
            })
            const deadline = performance.now() + 5_000
            let waiting = false
            while (!waiting && performance.now() < deadline) {
                // oxlint-disable-next-line no-await-in-loop -- Require an observed PostgreSQL wait before rolling back the first initializer.
                const [state] = await getAuthDatabase().execute<{ waiting: boolean }>(sql`
                    select exists(select 1 from pg_stat_activity
                    where application_name = ${applicationName} and wait_event_type = 'Lock') as waiting
                `)
                waiting = state?.waiting === true
                // oxlint-disable-next-line no-await-in-loop -- Bound the real lock barrier without timing the transaction release.
                if (!waiting) await Bun.sleep(10)
            }
            expect(waiting).toBe(true)
        } finally {
            release.resolve()
            await first
            await second
        }
        expect((await readRuntime()).certificateCursor).toBe(initialCursor)
    }, 10_000)

    integrationTest('locking malformed existing runtime fails without replacing it with defaults', async () => {
        await getAuthDatabase().update(systemSettings)
            .set({ value: { version: 999 } })
            .where(eq(systemSettings.key, NOTIFICATION_STATE_KEY))
        await expect(getAuthDatabase().transaction(async (transaction) => {
            await lockNotificationConfiguration(transaction)
            await lockNotificationRuntime(transaction)
        })).rejects.toMatchObject({ code: 'configuration_unavailable' })
        const [row] = await getAuthDatabase().select({ value: systemSettings.value })
            .from(systemSettings).where(eq(systemSettings.key, NOTIFICATION_STATE_KEY))
        expect(row?.value).toEqual({ version: 999 })
    })

    integrationTest(
        'real healthy webhooks drain behind stalled SMTP, and stop persists every cancellation before returning',
        async () => {
            const base = configuration()
            const config = {
                ...base,
                channels: Array.from({ length: 8 }, (_, index) => ({
                    ...base.channels[0]!,
                    id: randomUUID(),
                    kind: index % 2 === 0 ? ('email' as const) : ('webhook' as const),
                    events: [index % 4 < 2 ? ('ddns' as const) : ('proxy_configuration' as const)],
                })),
            }
            await getAuthDatabase().transaction(async (transaction) => {
                await lockNotificationConfiguration(transaction)
                await writeNotificationConfiguration(transaction, config)
                const runtime = await lockNotificationRuntime(transaction)
                runtime.engine = observeNotificationSignals(
                    runtime.engine,
                    config,
                    Array.from({ length: 8 }, (_, index) => ({
                        ...batch(`mixed-${index}`).observations[0]!,
                        category: index % 2 === 0 ? 'ddns' : 'proxy_configuration',
                    })),
                    Date.now(),
                    randomUUID,
                )
                await writeNotificationRuntime(transaction, runtime)
            })
            const received = new Set<string>()
            const allReceived = Promise.withResolvers<void>()
            let smtpOpened = 0
            let smtpClosed = 0
            let unavailable = false
            const server = createServer((request, response) => {
                const key = request.headers['idempotency-key']
                if (typeof key === 'string') received.add(key)
                if (received.size === 16) allReceived.resolve()
                request.resume()
                response.writeHead(204)
                response.end()
            })
            await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
            const address = server.address()
            if (!address || typeof address === 'string') throw new Error('Invalid receiver fixture')
            const deliver = createAuthorizedNotificationDelivery({
                authorize: authorizeNotificationDelivery,
                open: async (channel) =>
                    channel.kind === 'email'
                        ? { kind: 'email', recipients: ['fixture@example.test'] }
                        : {
                              kind: 'webhook',
                              url: `http://127.0.0.1:${address.port}/`,
                              authorizationToken: null,
                              allowedPrivateCidrs: ['127.0.0.1/32'],
                          },
                deliver: async (destination, payload, signal) =>
                    destination.kind === 'webhook'
                        ? deliverWebhookNotification(destination, payload, signal)
                        : sendNotificationEmailService(destination.recipients, payload, signal, {
                              createClient: () => {
                                  smtpOpened++
                                  return {
                                      from: 'fixture@example.test',
                                      close() {
                                          smtpClosed++
                                      },
                                      sendMail: () => new Promise(() => {}),
                                  }
                              },
                          }),
            })
            const worker = createNotificationWorker({
                claimCollection: async () => null,
                collect: async () => ({ observations: [], certificateCursor: null }),
                commitCollection: async () => false,
                claimDelivery: claimNextNotificationDelivery,
                nextDeliveryAt: readNextNotificationDeliveryAt,
                acknowledge: acknowledgeNotificationDelivery,
                deliver,
                changed() {},
                unavailable() {
                    unavailable = true
                },
            })
            const started = performance.now()
            let deadline: ReturnType<typeof setTimeout> | undefined
            try {
                worker.start()
                await Promise.race([
                    allReceived.promise,
                    new Promise<never>((_resolve, reject) => {
                        deadline = setTimeout(
                            () => reject(new Error('Healthy destinations starved')),
                            4000,
                        )
                    }),
                ])
                expect(performance.now() - started).toBeLessThan(4000)
                expect(smtpOpened).toBeGreaterThan(0)
                expect(smtpOpened).toBeLessThanOrEqual(4)
                // Let received responses finish before testing cancellation of the stalled SMTP lane.
                const waitForAcknowledgements = async (): Promise<void> => {
                    const runtime = await readRuntime()
                    if (
                        runtime.engine.deliveries.filter((item) => item.status === 'succeeded')
                            .length === 16
                    )
                        return
                    if (performance.now() - started > 4000)
                        throw new Error('Acknowledgements did not drain')
                    await new Promise((resolve) => setTimeout(resolve, 20))
                    return waitForAcknowledgements()
                }
                await waitForAcknowledgements()
            } finally {
                if (deadline) clearTimeout(deadline)
                await worker.stop()
                server.closeAllConnections()
                server.close()
            }
            const runtime = await readRuntime()
            expect(received.size).toBe(16)
            expect(unavailable).toBe(false)
            expect(smtpClosed).toBe(smtpOpened)
            expect(
                runtime.engine.deliveries.filter((item) => item.status === 'inflight'),
            ).toHaveLength(0)
            expect(
                runtime.engine.deliveries.filter((item) => item.status === 'succeeded'),
            ).toHaveLength(16)
            expect(
                runtime.engine.deliveries
                    .filter((item) => item.error === 'cancelled')
                    .every((item) => item.status === 'pending'),
            ).toBe(true)
        },
        8000,
    )

    integrationTest(
        'concurrent processes enforce the shared budgets and acknowledge independent claims without lost writes',
        async () => {
            const base = configuration()
            const config = {
                ...base,
                channels: Array.from({ length: 16 }, (_, index) => ({
                    ...base.channels[0]!,
                    id: randomUUID(),
                    kind: index % 2 === 0 ? ('email' as const) : ('webhook' as const),
                    events: [index % 4 < 2 ? ('ddns' as const) : ('proxy_configuration' as const)],
                })),
            }
            await getAuthDatabase().transaction(async (transaction) => {
                await lockNotificationConfiguration(transaction)
                await writeNotificationConfiguration(transaction, config)
                const runtime = await lockNotificationRuntime(transaction)
                runtime.engine = observeNotificationSignals(
                    runtime.engine,
                    config,
                    Array.from({ length: 8 }, (_, index) => ({
                        ...batch(`storm-${index}`).observations[0]!,
                        category: index % 2 === 0 ? 'ddns' : 'proxy_configuration',
                    })),
                    Date.now(),
                    randomUUID,
                )
                await writeNotificationRuntime(transaction, runtime)
            })
            const beforePressure = getBackgroundDatabaseResourceSnapshot().notifications!.busy
            const attempts = await Promise.allSettled(
                Array.from({ length: 32 }, () => claimNextNotificationDelivery()),
            )
            const rejected = attempts.filter((attempt) => attempt.status === 'rejected')
            expect(rejected).toHaveLength(18)
            expect(
                rejected.every(
                    (attempt) =>
                        attempt.reason instanceof ResourceAdmissionError &&
                        attempt.reason.reason === 'busy',
                ),
            ).toBe(true)
            expect(
                getBackgroundDatabaseResourceSnapshot().notifications!.busy - beforePressure,
            ).toBe(18)
            const claims = attempts.flatMap((attempt) =>
                attempt.status === 'fulfilled' && attempt.value ? [attempt.value] : [],
            )
            expect(claims).toHaveLength(8)
            expect(new Set(claims.map((claim) => claim.id)).size).toBe(8)
            expect(claims.filter((claim) => claim.channel.kind === 'email')).toHaveLength(4)
            const version = await runtimeVersion()
            expect(await readNextNotificationDeliveryAt()).toBeGreaterThan(Date.now())
            expect(await runtimeVersion()).toBe(version)
            await Promise.all(
                claims.map((claim) =>
                    acknowledgeNotificationDelivery(claim.leaseToken, { success: true }),
                ),
            )
            const persisted = await readRuntime()
            expect(
                persisted.engine.deliveries.filter((delivery) => delivery.status === 'succeeded'),
            ).toHaveLength(8)
            expect(
                persisted.engine.deliveries.filter((delivery) => delivery.status === 'inflight'),
            ).toHaveLength(0)
        },
    )

    integrationTest(
        'dispatch authorization checks the live lease and a restore pause still records admitted acceptance',
        async () => {
            await seedFailure()
            const claim = (await claimNextNotificationDelivery())!
            expect(await authorizeNotificationDelivery(claim)).not.toBeNull()
            expect(
                await authorizeNotificationDelivery({ ...claim, leaseToken: randomUUID() }),
            ).toBeNull()
            await changeConfiguration((current) => ({
                ...current,
                revision: current.revision + 1,
                enabled: false,
                pausedAfterRestore: true,
            }))
            expect(await authorizeNotificationDelivery(claim)).toBeNull()
            await acknowledgeNotificationDelivery(claim.leaseToken, { success: true })
            expect((await readRuntime()).engine.deliveries[0]?.status).toBe('succeeded')
            expect(await claimNextNotificationDelivery()).toBeNull()
        },
    )

    integrationTest(
        'empty polls and unknown acknowledgements preserve the stored row and collector lease',
        async () => {
            const beforeClaim = await runtimeVersion()
            const collection = await claimNotificationCollection()
            expect(collection).not.toBeNull()
            const leasedVersion = await runtimeVersion()
            expect(leasedVersion).not.toBe(beforeClaim)
            const leased = await readRuntime()
            expect(await claimNextNotificationDelivery()).toBeNull()
            expect(await runtimeVersion()).toBe(leasedVersion)
            await acknowledgeNotificationDelivery(randomUUID(), { success: true })
            expect(await runtimeVersion()).toBe(leasedVersion)
            expect(await readRuntime()).toEqual(leased)
        },
    )

    integrationTest.each(['poll', 'acknowledgement'] as const)(
        '%s still persists expired-delivery reconciliation without claiming a delivery',
        async (operation) => {
            await seedFailure()
            expect(await claimNextNotificationDelivery()).not.toBeNull()
            await mutateRuntime((runtime) => {
                runtime.engine.deliveries[0]!.leaseExpiresAt = Date.now() - 1
            })
            const expiredVersion = await runtimeVersion()
            if (operation === 'poll') expect(await claimNextNotificationDelivery()).toBeNull()
            else await acknowledgeNotificationDelivery(randomUUID(), { success: true })
            expect(await runtimeVersion()).not.toBe(expiredVersion)
            const persisted = await readRuntime()
            expect(persisted.engine.deliveries[0]).toMatchObject({
                status: 'pending',
                attempts: 1,
                leaseToken: null,
                leaseExpiresAt: null,
            })
            expect(persisted.engine.deliveries[0]!.nextAttemptAt).toBeGreaterThan(Date.now())
        },
    )

    integrationTest('row locks admit exactly one concurrent collector lease', async () => {
        const claims = await Promise.all(
            Array.from({ length: 8 }, () => claimNotificationCollection()),
        )
        const winners = claims.filter((claim) => claim !== null)
        expect(winners).toHaveLength(1)
        const persisted = await readRuntime()
        expect(persisted.collectionLease?.token).toBe(winners[0]!.token)
        expect(persisted.certificateCursor).toBe(initialCursor)
        expect(persisted.engine).toEqual(createNotificationState())
    })

    integrationTest(
        'commits cursor and engine together, preserving the exact validated JSON runtime on reread',
        async () => {
            const claim = await claimNotificationCollection()
            expect(claim).not.toBeNull()
            const leased = await readRuntime()
            const invalid = { ...batch(), certificateCursor: 'invalid-cursor' }
            await expect(commitNotificationCollection(claim!, invalid)).rejects.toMatchObject({
                code: 'configuration_unavailable',
            })
            // The invalid cursor rejects the whole write, including the incident and lease release.
            expect(await readRuntime()).toEqual(leased)
            expect(await commitNotificationCollection(claim!, batch())).toBe(true)
            const persisted = await readRuntime()
            expect(persisted.certificateCursor).toBe(nextCursor)
            expect(persisted.collectionLease).toBeNull()
            expect(persisted.engine.incidents).toHaveLength(1)
            expect(persisted.engine.deliveries).toHaveLength(1)
            const rows = await getAuthDatabase()
                .select({ key: systemSettings.key, value: systemSettings.value })
                .from(systemSettings)
                .where(inArray(systemSettings.key, keys))
            const rawRuntime = rows.find((row) => row.key === NOTIFICATION_STATE_KEY)?.value
            expect(typeof rawRuntime).toBe('object')
            expect(rawRuntime).toEqual(JSON.parse(JSON.stringify(persisted)))
            const storedConfiguration = await getAuthDatabase().transaction(
                readNotificationConfiguration,
                { accessMode: 'read only' },
            )
            expect(storedConfiguration).toEqual(configuration())
            expect(JSON.stringify(persisted)).not.toContain('ciphertext')
            expect(JSON.stringify(persisted)).not.toContain('destination')
        },
    )

    integrationTest(
        'an expired collector cannot overwrite cursor or engine after a replacement claim',
        async () => {
            const first = await claimNotificationCollection()
            expect(first).not.toBeNull()
            await mutateRuntime((runtime) => {
                runtime.collectionLease = { token: first!.token, expiresAt: Date.now() - 1 }
            })
            const replacement = await claimNotificationCollection()
            expect(replacement).not.toBeNull()
            expect(replacement?.token).not.toBe(first?.token)
            const beforeStaleCommit = await readRuntime()
            expect(await commitNotificationCollection(first!, batch('stale-source'))).toBe(false)
            expect(await readRuntime()).toEqual(beforeStaleCommit)
            expect(
                await commitNotificationCollection(replacement!, batch('replacement-source')),
            ).toBe(true)
            const persisted = await readRuntime()
            expect(persisted.engine.incidents.map((incident) => incident.resourceId)).toEqual([
                'replacement-source',
            ])
            expect(persisted.certificateCursor).toBe(nextCursor)
        },
    )

    integrationTest(
        'expired collection and changed configuration reject late results without source changes',
        async () => {
            const expired = await claimNotificationCollection()
            expect(expired).not.toBeNull()
            await mutateRuntime((runtime) => {
                runtime.collectionLease = { token: expired!.token, expiresAt: Date.now() - 1 }
            })
            expect(await commitNotificationCollection(expired!, batch('expired-source'))).toBe(
                false,
            )
            let persisted = await readRuntime()
            expect(persisted.certificateCursor).toBe(initialCursor)
            expect(persisted.engine).toEqual(createNotificationState())
            expect(persisted.collectionLease).toBeNull()
            const staleConfiguration = await claimNotificationCollection()
            expect(staleConfiguration).not.toBeNull()
            await changeConfiguration((current) => ({
                ...current,
                revision: current.revision + 1,
                enabled: false,
            }))
            expect(
                await commitNotificationCollection(staleConfiguration!, batch('disabled-source')),
            ).toBe(false)
            persisted = await readRuntime()
            expect(persisted.certificateCursor).toBe(initialCursor)
            expect(persisted.engine).toEqual(createNotificationState())
            expect(persisted.collectionLease).toBeNull()
        },
    )

    integrationTest(
        'persists one delivery claim across readers and uses lease-token CAS after expiry and retry',
        async () => {
            await seedFailure()
            const concurrent = await Promise.all([
                claimNextNotificationDelivery(),
                claimNextNotificationDelivery(),
            ])
            const claims = concurrent.filter((claim) => claim !== null)
            expect(claims).toHaveLength(1)
            const first = claims[0]!
            const persistedClaim = await readRuntime()
            expect(persistedClaim.engine.deliveries[0]).toMatchObject({
                id: first.id,
                status: 'inflight',
                leaseToken: first.leaseToken,
                attempts: 1,
            })
            // A new database transaction reconstructs all state without relying on process-local memory.
            expect(await readRuntime()).toEqual(persistedClaim)
            await acknowledgeNotificationDelivery(randomUUID(), { success: true })
            expect(await readRuntime()).toEqual(persistedClaim)
            await mutateRuntime((runtime) => {
                runtime.engine.deliveries[0]!.leaseExpiresAt = Date.now() - 1
            })
            await acknowledgeNotificationDelivery(first.leaseToken, { success: true })
            expect((await readRuntime()).engine.deliveries[0]).toMatchObject({
                status: 'pending',
                leaseToken: null,
                attempts: 1,
            })
            await mutateRuntime((runtime) => {
                runtime.engine.deliveries[0]!.nextAttemptAt = Date.now() - 1
            })
            const retry = await claimNextNotificationDelivery()
            expect(retry).not.toBeNull()
            expect(retry?.id).toBe(first.id)
            expect(retry?.leaseToken).not.toBe(first.leaseToken)
            const retriedRuntime = await readRuntime()
            await acknowledgeNotificationDelivery(first.leaseToken, { success: true })
            expect(await readRuntime()).toEqual(retriedRuntime)
            await acknowledgeNotificationDelivery(retry!.leaseToken, { success: true })
            const acknowledged = await readRuntime()
            expect(acknowledged.engine.deliveries[0]).toMatchObject({
                id: first.id,
                status: 'succeeded',
                attempts: 2,
                leaseToken: null,
            })
            expect(acknowledged.engine.incidents[0]?.channels[0]?.failure).toBe('succeeded')
            expect(await claimNextNotificationDelivery()).toBeNull()
        },
    )

    integrationTest(
        'disabling configuration cancels queued work atomically and prevents subsequent claims',
        async () => {
            await seedFailure()
            await changeConfiguration((current) => ({
                ...current,
                revision: current.revision + 1,
                enabled: false,
            }))
            expect(await claimNextNotificationDelivery()).toBeNull()
            expect(await claimNotificationCollection()).toBeNull()
            const runtime = await readRuntime()
            expect(runtime.engine.deliveries[0]?.status).toBe('cancelled')
            expect(runtime.engine.incidents[0]?.channels[0]?.failure).toBe('failed')
        },
    )

    integrationTest(
        'a revised channel cancels the stale job and only claims the current channel revision',
        async () => {
            await seedFailure()
            const original = (await readRuntime()).engine.deliveries[0]!
            await changeConfiguration((current) => ({
                ...current,
                revision: current.revision + 1,
                channels: current.channels.map((channel) => ({
                    ...channel,
                    revision: channel.revision + 1,
                })),
            }))
            const claimed = await claimNextNotificationDelivery()
            expect(claimed?.channelRevision).toBe(2)
            expect(claimed?.id).not.toBe(original.id)
            const runtime = await readRuntime()
            expect(
                runtime.engine.deliveries.find((delivery) => delivery.id === original.id)?.status,
            ).toBe('cancelled')
            expect(
                runtime.engine.incidents[0]?.channels.map((channel) => channel.channelRevision),
            ).toEqual([2])
        },
    )
})
