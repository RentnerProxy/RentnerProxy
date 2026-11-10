import { describe, expect, test } from 'bun:test'
import { createHash } from 'node:crypto'
import {
    backgroundDistribution,
    backgroundMonitoringReport,
    backgroundPolicies,
    backgroundResourceEvidence,
    parseBackgroundOptions,
} from '../../../../../../scripts/runtime-scale/background.ts'
import {
    backgroundReceiptsSchema,
    backgroundSnapshotSchema,
} from '../../../../../../scripts/runtime-scale/background.validation.ts'
import { startBackgroundNotificationReceivers } from '../../../../../../scripts/runtime-scale/background-receivers.ts'
import { startFixtureServers } from '../../../../../../scripts/runtime-reliability/fixture-transport.ts'
import { sendNotificationEmailService } from '../../../../server/Mail/mail.service.ts'
import { deliverNotification } from '../../../../server/Notifications/delivery.service.ts'
import { startBackgroundDockerEngine } from '../../../../../../scripts/runtime-scale/background-discovery.ts'
import { withInFlightBackgroundHead } from '../../../../../../scripts/runtime-scale/background-network.ts'
import type { BackgroundResourceSample } from '../../../../../../scripts/runtime-scale/Types/background.types.ts'
import type { NotificationPayload } from '../../../../lib/Notifications/Types/notifications.types.ts'

describe('bounded seeded background workload', () => {
    test('keeps large cases explicit, separates monitoring count, rejects unsafe/unbounded options', () => {
        expect(parseBackgroundOptions([])).toMatchObject({ policies: 100, durationSeconds: 120 })
        expect(
            parseBackgroundOptions(['--policies', '1000', '--pattern', 'staggered']),
        ).toMatchObject({ policies: 1000, pattern: 'staggered' })
        expect(parseBackgroundOptions(['--extended-recovery', 'true']).extendedRecovery).toBe(true)
        expect(parseBackgroundOptions([]).extendedRecovery).toBeUndefined()
        for (const args of [
            ['--policies', '101'],
            ['--hosts', '1000'],
            ['--duration-seconds', '1800'],
            ['--policies', '500', '--policies', '1000'],
            ['--seed', '-1'],
            ['--image', 'bad image'],
            ['--pattern', 'random'],
            ['--report', 'bad\npath'],
        ])
            expect(() => parseBackgroundOptions(args)).toThrow()
    })
    test('reproduces each policy ID, latency and due time without global random or clock state', () => {
        const options = parseBackgroundOptions([
            '--policies',
            '1000',
            '--pattern',
            'staggered',
            '--seed',
            '397',
        ])
        const first = backgroundPolicies(options)
        expect(backgroundPolicies(options)).toEqual(first)
        expect(new Set(first.map((item) => item.id)).size).toBe(1000)
        expect(new Set(first.map((item) => item.latency)).size).toBe(4)
        expect(first.every((item) => item.offsetMs >= 0 && item.offsetMs < 10000)).toBe(true)
        expect(backgroundPolicies({ ...options, seed: 398 })).not.toEqual(first)
        expect(
            backgroundPolicies({ ...options, pattern: 'simultaneous' }).every(
                (item) => item.offsetMs === 0,
            ),
        ).toBe(true)
    })
    test('uses nearest rank tails, represents empty evidence, rejects nonfinite metrics', () => {
        expect(backgroundDistribution([])).toEqual({
            count: 0,
            p50: null,
            p95: null,
            p99: null,
            max: null,
        })
        expect(
            backgroundDistribution(Array.from({ length: 100 }, (_, index) => index + 1)),
        ).toEqual({ count: 100, p50: 50, p95: 95, p99: 99, max: 100 })
        expect(() => backgroundDistribution([NaN])).toThrow()
        expect(() => backgroundDistribution([-1])).toThrow()
    })
    test('classifies missing start telemetry, partial and incorrect durable work independently', () => {
        const options = parseBackgroundOptions(['--profile', 'healthy'])
        const policies = backgroundPolicies(options)
        const snapshot = backgroundSnapshotSchema.parse({
            origin: 1000,
            managementReadMs: 1,
            policies: policies.map((item) => ({
                id: item.id,
                version: item.id,
                index: item.index,
                enabled: true,
                leased: false,
                dueAt: 1000,
            })),
            observations: policies.map((item) => ({
                policyId: item.id,
                policyVersion: item.id,
                checkedAt: 2000,
                status: 'healthy',
                code: 'expected_status',
            })),
        })
        expect(backgroundMonitoringReport(options, snapshot, [], 35000).status).toBe('insufficient')
        const receipts = policies.map((item) => ({
            index: item.index,
            latency: item.latency,
            startedAt: 1100,
            finishedAt: 1200,
        }))
        const report = backgroundMonitoringReport(options, snapshot, receipts, 35000)
        expect(report).toMatchObject({
            attempted: 100,
            networkAccepted: 100,
            committed: 100,
            schedulerAdmitted: null,
            status: 'met',
        })
        expect(
            backgroundMonitoringReport(
                options,
                { ...snapshot, observations: snapshot.observations.slice(1) },
                receipts,
                35000,
            ).status,
        ).toBe('unmet')
        expect(
            backgroundMonitoringReport(
                options,
                {
                    ...snapshot,
                    observations: [...snapshot.observations, snapshot.observations[0]!],
                },
                receipts,
                35000,
            ).status,
        ).toBe('unmet')
        expect(backgroundMonitoringReport(options, snapshot, receipts, 100000).status).toBe('unmet')
        expect(
            backgroundSnapshotSchema.safeParse({ ...snapshot, environment: 'private' }).success,
        ).toBe(false)
        expect(
            backgroundReceiptsSchema.safeParse({ receipts, overflowed: false, token: 'private' })
                .success,
        ).toBe(false)
    })
})

function resource(
    index: number,
    generation = 'same',
    kind: BackgroundResourceSample['kind'] = 'continuous',
): BackgroundResourceSample {
    return {
        kind,
        generation,
        warmup: false,
        sample: {
            elapsedSeconds: index * 5,
            phase: kind,
            memoryBytes: 100,
            cpuPercent: 1,
            pids: 20,
            postgresConnections: 10,
            webFds: 3,
            controllerFds: 3,
            caddyFds: 3,
            restartCount: 0,
            oomKilled: false,
        },
    }
}

test('Docker bursts use deterministic events and bounded inventory while requiring stream reconnects', async () => {
    const engine = startBackgroundDockerEngine(397)
    const other = startBackgroundDockerEngine(397)
    try {
        expect(engine.events).toEqual(other.events)
        expect(new Set(engine.events.map((item) => item.Actor.ID)).size).toBe(8)
        const base = `http://127.0.0.1:${engine.server.port}/v1.47`
        const inventory = (await (await fetch(base + '/containers/json')).json()) as {
            Id: string
        }[]
        expect(inventory).toHaveLength(8)
        for (let index = 0; index < 3; index++) {
            // oxlint-disable-next-line no-await-in-loop -- Each closed stream establishes one actual reconnect boundary.
            const text = await (await fetch(base + '/events')).text()
            expect(text.trim().split('\n')).toHaveLength(64)
        }
        const last = await fetch(base + '/events')
        const reader = last.body!.getReader()
        const chunk = await reader.read()
        expect(new TextDecoder().decode(chunk.value).trim().split('\n')).toHaveLength(64)
        await reader.cancel()
        expect(engine.stats()).toMatchObject({ streams: 4, emitted: 256, inventoryCount: 8 })
    } finally {
        engine.stop()
        other.stop()
    }
})

test('in-flight mutation barrier performs a real HEAD and cleans up when its mutation fails', async () => {
    let mutated = false
    expect(
        await withInFlightBackgroundHead(async () => {
            mutated = true
        }),
    ).toMatchObject({ status: 503 })
    expect(mutated).toBe(true)
    await expect(
        withInFlightBackgroundHead(async () => {
            throw new Error('fixture mutation failure')
        }),
    ).rejects.toThrow('fixture mutation failure')
})

test('continuous evidence requires sufficient elapsed time in the same process; recovered samples cannot fill its gaps', () => {
    expect(backgroundResourceEvidence([])).toMatchObject({
        status: 'insufficient',
        trendEvidence: 'insufficient',
        stabilityClaimed: false,
    })
    expect(
        backgroundResourceEvidence(Array.from({ length: 8 }, (_, index) => resource(index))),
    ).toMatchObject({ status: 'met', trendEvidence: 'comparable' })
    expect(
        backgroundResourceEvidence(
            Array.from({ length: 8 }, (_, index) => resource(index, String(index))),
        ),
    ).toMatchObject({ trendEvidence: 'insufficient', processGenerations: 8 })
    expect(
        backgroundResourceEvidence(
            Array.from({ length: 8 }, (_, index) => resource(index, 'same', 'recovered')),
        ),
    ).toMatchObject({ continuous: 0, recovered: 8, trendEvidence: 'insufficient' })
    const exceeded = resource(1)
    exceeded.sample.memoryBytes = 1024 ** 3 + 1
    expect(backgroundResourceEvidence([exceeded]).status).toBe('unmet')
    const missing = resource(0)
    missing.sample.webFds = NaN
    expect(backgroundResourceEvidence([missing])).toMatchObject({
        status: 'insufficient',
        missingTelemetry: 1,
    })
    expect(
        backgroundResourceEvidence(Array.from({ length: 8 }, () => resource(0))).trendEvidence,
    ).toBe('insufficient')
})

test('controlled native HEAD mixes record start/finish receipts while unrelated proxy GETs continue', async () => {
    const servers = startFixtureServers({
        primaryPort: 0,
        secondaryPort: 0,
        authPort: 0,
        controlPort: 0,
        tlsPort: 0,
    })
    const url = 'http://127.0.0.1:' + servers.primary.port
    try {
        const slow = fetch(url, { method: 'HEAD', headers: { host: 'bg-3-slow-fixture.test' } })
        expect((await fetch(url)).status).toBe(200)
        expect(
            (await fetch(url, { method: 'HEAD', headers: { host: 'bg-4-failure-fixture.test' } }))
                .status,
        ).toBe(503)
        expect((await slow).status).toBe(200)
        const receiptResponse = await fetch(
            'http://127.0.0.1:' + servers.control.port + '/background-receipts',
        )
        const value = backgroundReceiptsSchema.parse(await receiptResponse.json())
        expect(value.overflowed).toBe(false)
        expect(value.receipts.map((item) => item.index)).toEqual([3, 4])
        expect(
            value.receipts.every(
                (item) => item.finishedAt !== null && item.finishedAt >= item.startedAt,
            ),
        ).toBe(true)
    } finally {
        servers.stop()
    }
})

test('private slow receivers exercise production SMTP/webhook sends and deduplicate retry effects', async () => {
    const original = process.env.PUBLIC_ORIGIN
    process.env.PUBLIC_ORIGIN = 'https://fixture.invalid'
    const receiver = await startBackgroundNotificationReceivers()
    const payload: NotificationPayload = {
        schemaVersion: 1,
        id: '397-receipt',
        incidentId: '397-incident',
        type: 'incident',
        category: 'ddns',
        severity: 'error',
        resourceId: 'fixture',
        code: 'ddns_update_failed',
        occurredAt: '2026-11-11T00:00:00Z',
        startedAt: '2026-11-11T00:00:00Z',
        failures: 1,
    }
    receiver.receiverIdentity.set(createHash('sha256').update(payload.id).digest('hex'), payload.id)
    try {
        const destination = {
            kind: 'webhook' as const,
            url: `http://127.0.0.1:${receiver.webhook.port}/`,
            authorizationToken: null,
            allowedPrivateCidrs: ['127.0.0.1/32'],
        }
        expect(await deliverNotification(destination, payload)).toEqual({ success: true })
        expect(await deliverNotification(destination, payload)).toEqual({ success: true })
        const createClient = receiver.createMailClient
        expect(
            await sendNotificationEmailService(['accepted@fixture.invalid'], payload, undefined, {
                createClient,
            }),
        ).toEqual({ success: true })
        expect(
            await sendNotificationEmailService(['permanent@fixture.invalid'], payload, undefined, {
                createClient,
            }),
        ).toMatchObject({ success: false, retryable: false })
        expect(receiver.accepted.size).toBe(1)
        expect(receiver.receiverAttempts.get(payload.id)).toBe(3)
    } finally {
        await receiver.close()
        if (original === undefined) delete process.env.PUBLIC_ORIGIN
        else process.env.PUBLIC_ORIGIN = original
    }
}, 20000)
