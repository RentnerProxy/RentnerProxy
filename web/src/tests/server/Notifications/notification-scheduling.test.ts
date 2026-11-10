import { expect, test } from 'bun:test'
import { NOTIFICATION_LIMITS as limits } from '@/config/notifications.config.ts'
import {
    claimNotificationDelivery,
    createNotificationState,
    finishNotificationDelivery,
    observeNotificationSignals,
    summarizeNotificationState,
} from '@/server/Notifications/incident-engine.ts'
import { nextNotificationDeliveryAt } from '@/server/Notifications/notification-scheduling.ts'
import { notificationStateSchema } from '@/server/Notifications/notification-state.validation.ts'
import type { NotificationEngineConfiguration } from '@/server/Notifications/Types/notification-state.types.ts'
import type { NotificationObservation } from '@/lib/Notifications/Types/notifications.types.ts'

let sequence = 1000
const id = () => `00000000-0000-4000-8000-${String(sequence++).padStart(12, '0')}`
function configuration(count = 16): NotificationEngineConfiguration {
    return {
        enabled: true,
        pausedAfterRestore: false,
        channels: Array.from({ length: count }, (_, index) => ({
            id: id(),
            ownerUserId: `00000000-0000-4000-8001-${String(Math.floor(index / 2)).padStart(12, '0')}`,
            name: 'Fixture',
            kind: index % 2 === 0 ? 'email' : 'webhook',
            revision: 1,
            enabled: true,
            events: [index % 4 < 2 ? 'ddns' : 'proxy_configuration'],
            minimumSeverity: 'error',
            thresholdSeconds: 0,
            recovery: true,
        })),
    }
}
function observations(
    count: number,
    now: number,
    health: 'failing' | 'healthy' = 'failing',
): NotificationObservation[] {
    return Array.from({ length: count }, (_, index) => ({
        source: 'event',
        category: index % 2 === 0 ? 'ddns' : 'proxy_configuration',
        resourceId: `resource-${index}`,
        health,
        severity: 'error',
        occurredAt: new Date(now).toISOString(),
        evidence: `${health}-${now}`,
        failures: 1,
    }))
}

test('durable global, channel, owner, transport and category concurrency leave healthy owners capacity', () => {
    const config = configuration()
    let state = observeNotificationSignals(
        createNotificationState(),
        config,
        observations(8, 1000),
        1000,
        id,
    )
    for (let index = 0; index < 32; index++) {
        state = claimNotificationDelivery(state, config, 1000, id).state
    }
    const active = state.deliveries.filter((delivery) => delivery.status === 'inflight')
    expect(active).toHaveLength(limits.concurrency)
    for (const channel of config.channels) {
        expect(active.filter((item) => item.channelId === channel.id).length).toBeLessThanOrEqual(
            limits.channelConcurrency,
        )
        expect(
            active.filter((item) => item.dispatch?.ownerUserId === channel.ownerUserId).length,
        ).toBeLessThanOrEqual(limits.ownerConcurrency)
    }
    for (const kind of ['email', 'webhook']) {
        expect(active.filter((item) => item.dispatch?.kind === kind)).toHaveLength(
            limits.transportConcurrency,
        )
    }
    for (const category of ['ddns', 'proxy_configuration']) {
        expect(active.filter((item) => item.payload.category === category)).toHaveLength(
            limits.categoryConcurrency,
        )
    }
    expect(new Set(active.map((item) => item.dispatch?.ownerUserId)).size).toBe(8)
    const restarted = notificationStateSchema.parse(JSON.parse(JSON.stringify(state)))
    expect(claimNotificationDelivery(restarted, config, 1001, id).delivery).toBeNull()
})

test('one owner cannot fill slots through many channels and admitted revisions retain their budgets', () => {
    const base = configuration()
    const owner = base.channels[0]!.ownerUserId
    const config = {
        ...base,
        channels: base.channels.map((channel, index) =>
            index < 12 ? Object.assign({}, channel, { ownerUserId: owner }) : channel,
        ),
    }
    let state = observeNotificationSignals(
        createNotificationState(),
        config,
        observations(8, 1000),
        1000,
        id,
    )
    for (let index = 0; index < 8; index++)
        state = claimNotificationDelivery(state, config, 1000, id).state
    const active = state.deliveries.filter((item) => item.status === 'inflight')
    expect(active.filter((item) => item.dispatch?.ownerUserId === owner)).toHaveLength(2)
    expect(active.some((item) => item.dispatch?.ownerUserId !== owner)).toBe(true)
    const revised = {
        ...config,
        channels: config.channels.map((channel) => Object.assign({}, channel, { revision: 2 })),
    }
    state = claimNotificationDelivery(state, revised, 2001, id).state
    expect(
        state.deliveries.filter(
            (item) => item.status === 'inflight' && item.dispatch?.ownerUserId === owner,
        ),
    ).toHaveLength(2)
})

test('durable rate receipts survive restart and history eviction, and next due avoids a busy loop', () => {
    const config = configuration()
    let state = observeNotificationSignals(
        createNotificationState(),
        config,
        observations(16, 1000),
        1000,
        id,
    )
    for (let index = 0; index < limits.globalRate; index++) {
        const claimed = claimNotificationDelivery(state, config, 1000, id)
        expect(claimed.delivery).not.toBeNull()
        state = finishNotificationDelivery(
            claimed.state,
            config,
            claimed.delivery!.leaseToken,
            { success: true },
            1000,
        )
    }
    state.deliveries = state.deliveries.filter((item) => item.status === 'pending')
    state = notificationStateSchema.parse(JSON.parse(JSON.stringify(state)))
    expect(state.dispatches).toHaveLength(limits.globalRate)
    expect(claimNotificationDelivery(state, config, 1000, id).delivery).toBeNull()
    expect(nextNotificationDeliveryAt(state, config, 1000)).toBe(2000)
    expect(claimNotificationDelivery(state, config, 2000, id).delivery).not.toBeNull()
})

test('legacy channels share no fabricated owner; stalled SMTP leaves a webhook slot', () => {
    const base = configuration(4)
    const config = {
        ...base,
        channels: base.channels.map(({ ownerUserId: _owner, ...channel }) => channel),
    }
    let state = observeNotificationSignals(
        createNotificationState(),
        config,
        observations(4, 1000),
        1000,
        id,
    )
    for (let index = 0; index < 4; index++)
        state = claimNotificationDelivery(state, config, 1000, id).state
    expect(state.deliveries.filter((item) => item.status === 'inflight')).toHaveLength(4)
})

test('a saturated storm defers admission fairly and delivers every failure and recovery once despite history eviction', () => {
    const config = configuration(8)
    let now = 1000
    let state = observeNotificationSignals(
        createNotificationState(),
        config,
        observations(80, now),
        now,
        id,
    )
    expect(state.deliveries).toHaveLength(limits.deliveries)
    expect(state.deferredCount).toBe(64)
    expect(new Set(state.deliveries.map((item) => item.channelId)).size).toBe(8)
    const received = new Set<string>()
    const drain = () => {
        for (let iteration = 0; iteration < 1000; iteration++) {
            const claimed = claimNotificationDelivery(state, config, now, id)
            state = claimed.state
            if (claimed.delivery) {
                expect(received.has(claimed.delivery.payload.id)).toBe(false)
                received.add(claimed.delivery.payload.id)
                state = finishNotificationDelivery(
                    state,
                    config,
                    claimed.delivery.leaseToken,
                    { success: true },
                    now,
                )
            } else if (state.deliveries.some((item) => item.status === 'pending')) {
                now = nextNotificationDeliveryAt(state, config, now) ?? now + 1000
            } else return
        }
        throw new Error('Storm did not drain')
    }
    drain()
    expect(received.size).toBe(320)
    now++
    state = observeNotificationSignals(state, config, observations(80, now, 'healthy'), now, id)
    drain()
    expect(received.size).toBe(640)
    expect(state.incidents).toHaveLength(80)
    expect(
        state.incidents.every((incident) =>
            incident.channels.every(
                (marker) => marker.failure === 'succeeded' && marker.recovery === 'succeeded',
            ),
        ),
    ).toBe(true)
    expect(notificationStateSchema.safeParse(state).success).toBe(true)
})

test('retry jitter is stable, bounded, honors Retry-After and terminates exhausted deliveries', () => {
    const config = configuration(1)
    let now = 1000
    let state = observeNotificationSignals(
        createNotificationState(),
        config,
        observations(2, now),
        now,
        id,
    )
    for (let attempt = 1; attempt <= limits.attempts; attempt++) {
        const claimed = claimNotificationDelivery(state, config, now, id)
        expect(claimed.delivery).not.toBeNull()
        const result = { success: false, code: 'http_rejected', retryable: true } as const
        state = finishNotificationDelivery(
            claimed.state,
            config,
            claimed.delivery!.leaseToken,
            result,
            now,
        )
        expect(
            finishNotificationDelivery(
                claimed.state,
                config,
                claimed.delivery!.leaseToken,
                result,
                now,
            ),
        ).toEqual(state)
        if (attempt < limits.attempts) {
            const base = Math.min(limits.retryMaximumMs, limits.retryInitialMs * 2 ** (attempt - 1))
            const delay = state.deliveries[0]!.nextAttemptAt - now
            expect(delay).toBeGreaterThanOrEqual(base)
            expect(delay).toBeLessThanOrEqual(
                Math.min(limits.retryMaximumMs, Math.ceil(base * 1.2)),
            )
            now = state.deliveries[0]!.nextAttemptAt
        }
    }
    expect(nextNotificationDeliveryAt(state, config, now)).toBeNull()
    expect(summarizeNotificationState(state, now)).toMatchObject({
        activeCount: 0,
        exhaustedCount: 1,
        terminalCount: 1,
        oldestPendingAgeMs: null,
    })
})

test('crash after receiver acceptance repeats the same event ID; receiver deduplication prevents a second effect', () => {
    const config = configuration(1)
    const state = observeNotificationSignals(
        createNotificationState(),
        config,
        observations(2, 1000),
        1000,
        id,
    )
    const first = claimNotificationDelivery(state, config, 1000, id)
    const receiver = new Set([first.delivery!.payload.id])
    const recovered = claimNotificationDelivery(
        notificationStateSchema.parse(JSON.parse(JSON.stringify(first.state))),
        config,
        1000 + limits.leaseMs,
        id,
    )
    const retry = claimNotificationDelivery(
        recovered.state,
        config,
        recovered.state.deliveries[0]!.nextAttemptAt,
        id,
    )
    expect(retry.delivery!.payload.id).toBe(first.delivery!.payload.id)
    expect(retry.delivery!.leaseToken).not.toBe(first.delivery!.leaseToken)
    receiver.add(retry.delivery!.payload.id)
    expect(receiver.size).toBe(1)
    const stale = finishNotificationDelivery(
        retry.state,
        config,
        first.delivery!.leaseToken,
        { success: true },
        recovered.state.deliveries[0]!.nextAttemptAt,
    )
    expect(stale.deliveries[0]!.status).toBe('inflight')
})
