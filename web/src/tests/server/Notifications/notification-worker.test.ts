// oxlint-disable no-await-in-loop -- Flush promise turns between deterministic timer advances.
import { describe, expect, jest, test } from 'bun:test'
import { createNotificationWorker } from '@/server/Notifications/notification-worker.ts'
import { NOTIFICATION_LIMITS } from '@/config/notifications.config.ts'
import type { NotificationWorkerDependencies } from '@/server/Notifications/Types/notification-worker.types.ts'
import type { NotificationDeliveryClaim } from '@/server/Notifications/Types/notification-internal.types.ts'
import { PAYLOAD } from './Types/transport-test.types.ts'

const claim: NotificationDeliveryClaim = {
    id: PAYLOAD.id,
    channelId: '00000000-0000-4000-8000-000000000001',
    channelRevision: 1,
    payload: PAYLOAD,
    leaseToken: '00000000-0000-4000-8000-000000000002',
    channel: {
        id: '00000000-0000-4000-8000-000000000001',
        name: 'Test',
        kind: 'email',
        revision: 1,
        enabled: true,
        events: ['ddns'],
        minimumSeverity: 'error',
        thresholdSeconds: 60,
        recovery: true,
        hasAuthorization: false,
        destination: { ciphertext: 'sealed', iv: 'sealed' },
    },
}

function dependencies(
    overrides: Partial<NotificationWorkerDependencies> = {},
): NotificationWorkerDependencies {
    return {
        claimCollection: async () => null,
        collect: async () => ({ observations: [], certificateCursor: null }),
        commitCollection: async () => false,
        claimDelivery: async () => null,
        deliver: async () => ({ success: true }),
        acknowledge: async () => {},
        changed() {},
        unavailable() {},
        ...overrides,
    }
}

describe('notification worker lifecycle and isolation', () => {
    test('does not collect or send without an admitted durable claim', async () => {
        let effects = 0
        const worker = createNotificationWorker(
            dependencies({
                collect: async () => {
                    effects++
                    throw new Error('unexpected')
                },
                deliver: async () => {
                    effects++
                    throw new Error('unexpected')
                },
            }),
        )
        await worker.runOnce()
        await worker.stop()
        expect(effects).toBe(0)
    })

    test('delivers independently of a pending source poll and prevents overlapping ticks', async () => {
        const collecting = Promise.withResolvers<void>()
        const delivered = Promise.withResolvers<void>()
        let collections = 0
        let claims = 0
        let acknowledgements = 0
        const worker = createNotificationWorker(
            dependencies({
                claimCollection: async () => ({
                    token: 'collection-token',
                    configurationRevision: 1,
                    certificateCursor: null,
                }),
                collect: async () => {
                    collections++
                    await collecting.promise
                    return { observations: [], certificateCursor: null }
                },
                claimDelivery: async () => (++claims === 1 ? claim : null),
                deliver: async () => {
                    delivered.resolve()
                    return { success: true }
                },
                acknowledge: async (token, result) => {
                    expect(token).toBe(claim.leaseToken)
                    expect(result.success).toBeTrue()
                    acknowledgements++
                },
            }),
        )
        const first = worker.runOnce()
        expect(worker.runOnce()).toBe(first)
        await delivered.promise
        expect(collections).toBe(1)
        collecting.resolve()
        await first
        await worker.stop()
        expect(acknowledgements).toBe(1)
    })

    test('bounds delivery bursts and stores safe terminal results without recursive events', async () => {
        let deliveries = 0
        let acknowledgements = 0
        let collections = 0
        const worker = createNotificationWorker(
            dependencies({
                claimCollection: async () => {
                    collections++
                    return null
                },
                claimDelivery: async () => claim,
                deliver: async () => {
                    deliveries++
                    return { success: false, code: 'redirect_denied', retryable: false }
                },
                acknowledge: async (_token, result) => {
                    expect(result).toEqual({
                        success: false,
                        code: 'redirect_denied',
                        retryable: false,
                    })
                    acknowledgements++
                },
            }),
        )
        await worker.runOnce()
        await worker.stop()
        expect(deliveries).toBe(NOTIFICATION_LIMITS.deliveriesPerTick)
        expect(acknowledgements).toBe(deliveries)
        expect(collections).toBe(1)
    })

    test('aborts an in-flight transport, acknowledges cancellation and stops further claims', async () => {
        const sending = Promise.withResolvers<void>()
        let claims = 0
        let acknowledgements = 0
        const worker = createNotificationWorker(
            dependencies({
                claimDelivery: async () => {
                    claims++
                    return claim
                },
                deliver: async (_claim, signal) => {
                    sending.resolve()
                    await new Promise<void>((resolve) =>
                        signal.addEventListener('abort', () => resolve(), { once: true }),
                    )
                    return { success: false, code: 'cancelled', retryable: false }
                },
                acknowledge: async (_token, result) => {
                    expect(result).toEqual({ success: false, code: 'cancelled', retryable: true })
                    acknowledgements++
                },
            }),
        )
        const running = worker.runOnce()
        await sending.promise
        await worker.stop()
        await running
        await worker.runOnce()
        expect(claims).toBeGreaterThan(0)
        expect(claims).toBeLessThanOrEqual(NOTIFICATION_LIMITS.concurrency)
        expect(acknowledgements).toBe(claims)
    })

    test('acknowledges a durable claim returned after shutdown without starting its transport', async () => {
        const claiming = Promise.withResolvers<void>()
        const admitted = Promise.withResolvers<NotificationDeliveryClaim>()
        let sends = 0
        let acknowledgements = 0
        const worker = createNotificationWorker(
            dependencies({
                claimDelivery: async () => {
                    claiming.resolve()
                    return admitted.promise
                },
                deliver: async () => {
                    sends++
                    return { success: true }
                },
                acknowledge: async (token, result) => {
                    expect(token).toBe(claim.leaseToken)
                    expect(result).toEqual({ success: false, code: 'cancelled', retryable: true })
                    acknowledgements++
                },
            }),
        )
        const running = worker.runOnce()
        await claiming.promise
        const stopped = worker.stop()
        admitted.resolve(claim)
        await stopped
        await running
        expect(sends).toBe(0)
        expect(acknowledgements).toBe(1)
    })

    test('preserves terminal security failures when shutdown races with their result', async () => {
        const sending = Promise.withResolvers<void>()
        const result = Promise.withResolvers<{
            readonly success: false
            readonly code: 'destination_blocked'
            readonly retryable: false
        }>()
        let acknowledgements = 0
        const worker = createNotificationWorker(
            dependencies({
                claimDelivery: async () => claim,
                deliver: async () => {
                    sending.resolve()
                    return result.promise
                },
                acknowledge: async (_token, delivered) => {
                    expect(delivered).toEqual({
                        success: false,
                        code: 'destination_blocked',
                        retryable: false,
                    })
                    acknowledgements++
                },
            }),
        )
        const running = worker.runOnce()
        await sending.promise
        const stopped = worker.stop()
        result.resolve({ success: false, code: 'destination_blocked', retryable: false })
        await stopped
        await running
        expect(acknowledgements).toBe(1)
    })

    test('releases a collection lease returned after shutdown without changing the cursor', async () => {
        const claiming = Promise.withResolvers<void>()
        const admitted = Promise.withResolvers<{
            token: string
            configurationRevision: number
            certificateCursor: string | null
        }>()
        let collections = 0
        let commits = 0
        const worker = createNotificationWorker(
            dependencies({
                claimCollection: async () => {
                    claiming.resolve()
                    return admitted.promise
                },
                collect: async () => {
                    collections++
                    throw new Error('unexpected')
                },
                commitCollection: async (_claim, batch) => {
                    expect(batch).toEqual({
                        observations: [],
                        certificateCursor: 'original-cursor',
                    })
                    commits++
                    return false
                },
            }),
        )
        const running = worker.runOnce()
        await claiming.promise
        const stopped = worker.stop()
        admitted.resolve({
            token: 'token',
            configurationRevision: 1,
            certificateCursor: 'original-cursor',
        })
        await stopped
        await running
        expect(collections).toBe(0)
        expect(commits).toBe(1)
    })

    test('reports storage unavailability once per outage and keeps queued delivery independent', async () => {
        let warnings = 0
        let delivered = 0
        let broken = true
        const worker = createNotificationWorker(
            dependencies({
                claimCollection: async () => {
                    if (broken) throw new Error('private database error')
                    return null
                },
                claimDelivery: async () => (delivered === 0 ? claim : null),
                deliver: async () => {
                    delivered++
                    return { success: true }
                },
                unavailable: () => {
                    warnings++
                },
            }),
        )
        await worker.runOnce()
        await worker.runOnce()
        expect(warnings).toBe(1)
        expect(delivered).toBe(1)
        broken = false
        await worker.runOnce()
        broken = true
        await worker.runOnce()
        await worker.stop()
        expect(warnings).toBe(2)
    })
})

async function settle() {
    for (let index = 0; index < 80; index++) await Promise.resolve()
}

test('scheduled delivery refills free slots while other transports and collection remain stalled', async () => {
    jest.useFakeTimers({ now: 1000 })
    const pending = new Set<
        (
            result: { success: false; code: 'cancelled'; retryable: false } | { success: true },
        ) => void
    >()
    let sends = 0
    let claims = 0
    let collecting = 0
    let active = 0
    let maximum = 0
    let acknowledged = 0
    const worker = createNotificationWorker(
        dependencies({
            claimCollection: async () => ({
                token: 'token',
                configurationRevision: 1,
                certificateCursor: null,
            }),
            collect: async (_cursor, signal) => {
                collecting++
                await new Promise<void>((resolve) =>
                    signal.addEventListener('abort', () => resolve(), { once: true }),
                )
                return { observations: [], certificateCursor: null }
            },
            claimDelivery: async () => {
                claims++
                return claim
            },
            deliver: async (_claim, signal) => {
                sends++
                active++
                maximum = Math.max(maximum, active)
                const result = await new Promise<
                    { success: false; code: 'cancelled'; retryable: false } | { success: true }
                >((resolve) => {
                    pending.add(resolve)
                    signal.addEventListener(
                        'abort',
                        () => resolve({ success: false, code: 'cancelled', retryable: false }),
                        { once: true },
                    )
                })
                active--
                return result
            },
            acknowledge: async () => {
                acknowledged++
            },
        }),
    )
    try {
        worker.start()
        await settle()
        expect(sends).toBe(8)
        expect(collecting).toBe(1)
        pending.values().next().value!({ success: true })
        await settle()
        jest.advanceTimersByTime(50)
        await settle()
        expect(sends).toBe(9)
        expect(maximum).toBe(8)
        await worker.stop()
        expect(acknowledged).toBe(claims)
        expect(active).toBe(0)
    } finally {
        await worker.stop()
        jest.useRealTimers()
    }
})

test('due timers, coalesced hints and periodic replay recover work independently of Valkey', async () => {
    jest.useFakeTimers({ now: 1000 })
    let wake: (() => void) | undefined
    let pending = false
    let claims = 0
    let sends = 0
    let unsubscribed = false
    let due: number | null = 2000
    const worker = createNotificationWorker(
        dependencies({
            subscribe: (listener) => {
                wake = listener
                return () => {
                    unsubscribed = true
                }
            },
            nextDeliveryAt: async () => due,
            claimDelivery: async () => {
                claims++
                if (!pending) return null
                pending = false
                return claim
            },
            deliver: async () => {
                sends++
                return { success: true }
            },
        }),
    )
    try {
        worker.start()
        await settle()
        pending = true
        jest.advanceTimersByTime(999)
        await settle()
        expect(sends).toBe(0)
        due = null
        jest.advanceTimersByTime(1)
        await settle()
        expect(sends).toBe(1)
        jest.advanceTimersByTime(50)
        await settle()
        const beforeHints = claims
        pending = true
        for (let index = 0; index < 1000; index++) wake!()
        jest.advanceTimersByTime(49)
        await settle()
        expect(claims).toBe(beforeHints)
        jest.advanceTimersByTime(1)
        await settle()
        expect(sends).toBe(2)
        jest.advanceTimersByTime(50)
        await settle()
        // A durable commit with a lost hint still recovers on the authoritative PostgreSQL poll.
        pending = true
        jest.advanceTimersByTime(15_000)
        await settle()
        expect(sends).toBe(3)
        await worker.stop()
        expect(unsubscribed).toBe(true)
        const afterStop = claims
        wake!()
        jest.advanceTimersByTime(60_000)
        await settle()
        expect(claims).toBe(afterStop)
    } finally {
        await worker.stop()
        jest.useRealTimers()
    }
})

test('stop records a receiver acceptance that races cancellation and drains acknowledgement before returning', async () => {
    const sending = Promise.withResolvers<void>()
    const accepted = Promise.withResolvers<{ success: true }>()
    const acknowledge = Promise.withResolvers<void>()
    let claims = 0
    let recorded = false
    const worker = createNotificationWorker(
        dependencies({
            claimDelivery: async () => (++claims === 1 ? claim : null),
            deliver: async () => {
                sending.resolve()
                return accepted.promise
            },
            acknowledge: async (_token, result) => {
                expect(result).toEqual({ success: true })
                await acknowledge.promise
                recorded = true
            },
        }),
    )
    const running = worker.runOnce()
    await sending.promise
    let drained = false
    const stop = worker.stop().then(() => {
        drained = true
    })
    accepted.resolve({ success: true })
    await settle()
    expect(drained).toBe(false)
    acknowledge.resolve()
    await stop
    await running
    expect(recorded).toBe(true)
})
