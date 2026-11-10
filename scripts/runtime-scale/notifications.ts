// oxlint-disable no-await-in-loop -- Discrete transport events drive real, fenced PostgreSQL queue transactions.
import { inArray, sql } from 'drizzle-orm'
import {
    NOTIFICATION_LIMITS,
    NOTIFICATION_SETTINGS_KEY,
    NOTIFICATION_STATE_KEY,
} from '../../web/src/config/notifications.config.ts'
import { systemSettings } from '../../web/src/db/schema.ts'
import { closeDatabaseForRestoreMaintenance } from '../../web/src/db/index.ts'
import { getAuthDatabase } from '../../web/src/server/Auth/Core/database.server.ts'
import { observeNotificationSignals } from '../../web/src/server/Notifications/incident-engine.ts'
import * as repository from '../../web/src/server/Notifications/notification-repository.ts'
import {
    lockNotificationConfiguration,
    lockNotificationRuntime,
    readNotificationRuntime,
    writeNotificationConfiguration,
    writeNotificationRuntime,
} from '../../web/src/server/Notifications/notification-storage.ts'
import type {
    NotificationDeliveryClaim,
    StoredNotificationConfiguration,
} from '../../web/src/server/Notifications/Types/notification-internal.types.ts'
import type { NotificationDeliveryResult } from '../../web/src/lib/Notifications/Types/notifications.types.ts'

const mode = process.argv[2]
if (!['candidate', 'legacy'].includes(mode ?? '')) {
    throw new Error(
        'Use: RENTNERPROXY_DATABASE_INTEGRATION=1 DATABASE_URL=<empty loopback rentnerproxy_upstream_test> bun --no-env-file scripts/runtime-scale/notifications.ts candidate|legacy. Discrete scheduling time, real PostgreSQL transactions; no external sends. Legacy models four serial attempts plus a 15-second completion-relative pause on the checked-out queue implementation. Run the same fixture at the baseline revision for an original-code comparison. Requires unused notification setting rows; emits JSON and removes only its own fixture rows.',
    )
}
const url = new URL(process.env.DATABASE_URL ?? 'http://invalid')
if (
    process.env.RENTNERPROXY_DATABASE_INTEGRATION !== '1' ||
    !['postgres:', 'postgresql:'].includes(url.protocol) ||
    !['127.0.0.1', '[::1]', 'localhost'].includes(url.hostname) ||
    url.pathname !== '/rentnerproxy_upstream_test'
)
    throw new Error('Requires the explicitly isolated loopback notification database')

const database = getAuthDatabase()
let sequence = 1
const makeId = () => `00000393-0000-4000-8000-${String(sequence++).padStart(12, '0')}`
const keys = [NOTIFICATION_SETTINGS_KEY, NOTIFICATION_STATE_KEY]
if (
    (
        await database
            .select({ key: systemSettings.key })
            .from(systemSettings)
            .where(inArray(systemSettings.key, keys))
    ).length
)
    throw new Error(
        'Notification fixture rows must be absent; refusing to replace existing settings',
    )

const configuration: StoredNotificationConfiguration = {
    version: 1,
    revision: 1,
    enabled: true,
    pausedAfterRestore: false,
    channels: Array.from({ length: 8 }, (_, index) => ({
        id: makeId(),
        name: `Fixture ${index}`,
        revision: 1,
        enabled: true,
        kind: index % 2 === 0 ? 'email' : 'webhook',
        events: [index % 4 < 2 ? 'ddns' : 'proxy_configuration'],
        minimumSeverity: 'error',
        thresholdSeconds: 0,
        recovery: true,
        destination: { ciphertext: 'a'.repeat(32), iv: 'b'.repeat(16) },
        hasAuthorization: false,
    })),
}
const start = Date.now()
const wallStart = performance.now()
let now = start
const realNow = Date.now
const claimTimes: number[] = []
const ackTimes: number[] = []
const healthyStarts: number[] = []
const receiver = new Set<string>()
const attempts = new Map<string, number>()
let admissionMs = 0
let oldestQueueAgeMs = 0
let maxStateBytes = 0
let maximumActive = 0
let maxDeferred = 0
let peakRssBytes = process.memoryUsage().rss
let walStart = ''
let owned = false
const active: {
    claim: NotificationDeliveryClaim
    due: number
    result: NotificationDeliveryResult
}[] = []
const p = (values: number[], quantile: number) =>
    values.toSorted((a, b) => a - b)[Math.max(0, Math.ceil(values.length * quantile) - 1)] ?? 0

async function claim() {
    const began = performance.now()
    const value = await repository.claimNextNotificationDelivery()
    claimTimes.push(performance.now() - began)
    if (!value) return null
    const count = (attempts.get(value.id) ?? 0) + 1
    attempts.set(value.id, count)
    const channel = configuration.channels.findIndex((item) => item.id === value.channelId)
    if (channel !== 0 && count === 1) healthyStarts.push(now - start)
    return {
        claim: value,
        due: now + (channel === 0 ? 15_000 : channel % 2 === 0 ? 25 : 10),
        result:
            channel === 0
                ? ({ success: false, code: 'smtp_unavailable', retryable: false } as const)
                : channel === 1 && count === 1
                  ? ({
                        success: false,
                        code: 'http_rejected',
                        retryable: true,
                        retryAfterMs: 2_000,
                    } as const)
                  : ({ success: true } as const),
    }
}

async function acknowledge(item: (typeof active)[number]) {
    if (item.result.success) receiver.add(item.claim.payload.id)
    const began = performance.now()
    await repository.acknowledgeNotificationDelivery(item.claim.leaseToken, item.result)
    ackTimes.push(performance.now() - began)
}

try {
    Date.now = () => now
    const before = await database.execute<{ lsn: string }>(
        sql`select pg_current_wal_lsn()::text as lsn`,
    )
    walStart = before[0]!.lsn
    const began = performance.now()
    await database.transaction(async (transaction) => {
        await lockNotificationConfiguration(transaction)
        await writeNotificationConfiguration(transaction, configuration)
        const runtime = await lockNotificationRuntime(transaction)
        const engine = observeNotificationSignals(
            runtime.engine,
            configuration,
            Array.from({ length: 80 }, (_, index) => ({
                source: 'event',
                category: index % 2 === 0 ? 'ddns' : 'proxy_configuration',
                resourceId: `fixture-${index}`,
                health: 'failing',
                severity: 'error',
                occurredAt: new Date(now).toISOString(),
                evidence: 'seed-393',
                failures: 1,
            })),
            now,
            makeId,
        )
        maxDeferred = engine.deferredCount
        await writeNotificationRuntime(transaction, { ...runtime, engine })
    })
    owned = true
    admissionMs = performance.now() - began
    for (let step = 0; step < 20_000; step++) {
        if (mode === 'legacy') {
            for (let index = 0; index < 4; index++) {
                const item = await claim()
                if (!item) break
                maximumActive = 1
                now = item.due
                await acknowledge(item)
            }
        } else {
            while (active.length < NOTIFICATION_LIMITS.concurrency) {
                const item = await claim()
                if (!item) break
                active.push(item)
                maximumActive = Math.max(maximumActive, active.length)
            }
        }
        const runtime = await database.transaction(readNotificationRuntime, {
            accessMode: 'read only',
        })
        peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss)
        const pending = runtime.engine.deliveries.filter((item) => item.status === 'pending')
        maxStateBytes = Math.max(maxStateBytes, Buffer.byteLength(JSON.stringify(runtime)))
        maxDeferred = Math.max(maxDeferred, runtime.engine.deferredCount)
        oldestQueueAgeMs = Math.max(
            oldestQueueAgeMs,
            ...pending.map((item) => now - item.createdAt),
        )
        if (pending.length === 0 && active.length === 0 && runtime.engine.deferredCount === 0) break
        if (mode === 'legacy') now += 15_000
        else {
            const due = await repository.readNextNotificationDeliveryAt()
            now = Math.max(
                now + NOTIFICATION_LIMITS.wakeCoalesceMs,
                Math.min(due ?? Infinity, ...active.map((item) => item.due)),
            )
            if (!Number.isFinite(now)) throw new Error('Stranded work')
            const completed = active.filter((item) => item.due <= now)
            for (const item of completed) {
                active.splice(active.indexOf(item), 1)
                await acknowledge(item)
            }
        }
    }
    const wal = await database.execute<{ bytes: string }>(
        sql`select pg_wal_lsn_diff(pg_current_wal_lsn(), ${walStart}::pg_lsn)::text as bytes`,
    )
    const report = {
        mode,
        seed: 393,
        timingModel:
            'discrete transport/scheduler time; real PostgreSQL transactions, no SSR or external network',
        burstPotential: 320,
        seededIncidents: 80,
        collectionAdmissionWallMs: admissionMs,
        deferred: maxDeferred,
        drainSchedulerMs: now - start,
        healthyStartP95Ms: p(healthyStarts, 0.95),
        healthyStartP99Ms: p(healthyStarts, 0.99),
        oldestPendingSchedulerMs: oldestQueueAgeMs,
        attempts: ackTimes.length,
        retries: [...attempts.values()].reduce((sum, count) => sum + count - 1, 0),
        acceptedIds: receiver.size,
        maximumActive,
        fixtureWallMs: performance.now() - wallStart,
        peakRssBytes,
        runtimeBytes: maxStateBytes,
        database: {
            claims: claimTimes.length,
            acknowledgements: ackTimes.length,
            claimP95WallMs: p(claimTimes, 0.95),
            claimP99WallMs: p(claimTimes, 0.99),
            ackP95WallMs: p(ackTimes, 0.95),
            walBytes: Number(wal[0]!.bytes),
        },
        slo: {
            healthyStartP99SchedulerMs: 60_000,
            drainSchedulerMs: 700_000,
            admissionWallMs: 5_000,
            acceptedIds: 280,
        },
    }
    console.log(JSON.stringify(report, null, 2))
    if (
        receiver.size !== 280 ||
        attempts.size !== 320 ||
        (mode === 'candidate' &&
            (report.healthyStartP99Ms > 60_000 ||
                report.drainSchedulerMs > 700_000 ||
                admissionMs > 5_000))
    )
        process.exitCode = 1
} finally {
    Date.now = realNow
    if (owned) await database.delete(systemSettings).where(inArray(systemSettings.key, keys))
    await closeDatabaseForRestoreMaintenance()
}
