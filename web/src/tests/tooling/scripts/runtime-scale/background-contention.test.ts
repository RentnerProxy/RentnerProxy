import { expect, test } from 'bun:test'
import {
    buildContentionReport,
    contentionContract,
    contentionStateSchema,
    contentionWebSchema,
} from '../../../../../../scripts/runtime-scale/background-contention.ts'
import type { ContentionSample } from '../../../../../../scripts/runtime-scale/background-contention.ts'
import { parseBackgroundOptions } from '../../../../../../scripts/runtime-scale/background.ts'
import { startBackgroundDockerEngine } from '../../../../../../scripts/runtime-scale/background-discovery.ts'
import { startBackgroundNotificationReceivers } from '../../../../../../scripts/runtime-scale/background-receivers.ts'
import { createTransport } from 'nodemailer'
import { createServer as createNetServer } from 'node:net'
import { TLS_TEST_CA } from '../../../server/Notifications/Types/tls-test.types.ts'
import { readBunWorkerReport } from '../../../../lib/RuntimeDiagnostics/backgroundWorkerReport.ts'
import { createWorkerObservability } from '../../../../server/Foundation/worker-observability.ts'
import { findSupportRpcId } from '../../../../../../scripts/runtime-scale/background-contention-web.ts'

function sample(): ContentionSample {
    const workers = createWorkerObservability({ now: () => 1000 })
    workers.eventLoopDelay(2)
    const usage = {
        running: 1,
        pending: 0,
        oldestPendingAgeMs: 0,
        busy: 0,
        deadline: 0,
        cancelled: 0,
        admitted: 1,
    }
    return {
        web: {
            source: 'authenticated actual web support RPC',
            resources: {
                sampledAt: 1000,
                database: {
                    monitoring: { ...usage },
                    notifications: { ...usage },
                    discovery: { ...usage },
                    housekeeping: { ...usage },
                    critical: { ...usage },
                },
            },
            bun: readBunWorkerReport(workers.snapshot()),
        },
        processMemory: {
            memoryBytes: 100000,
            cgroupMemoryCurrentBytes: 200000,
            webRssBytes: 300000,
            controllerRssBytes: 40000,
            caddyRssBytes: 50000,
        },
        elapsedMs: 1000,
        criticalObservedMs: 500,
        captureSpanMs: 250,
        leasedPolicies: 64,
        managementMs: 100,
        proxyMs: 20,
        revisionUnchanged: true,
        critical: { attempts: 2, successes: 2 },
        state: {
            notification: {
                pending: 248,
                inflight: 8,
                succeeded: 8,
                failed: 0,
                cancelled: 0,
                deferred: 64,
            },
            discovery: { revision: 3, error: null, detached: 8 },
            postgres: {
                connections: 20,
                active: 8,
                lockWaiters: 0,
                idleTransactions: 0,
                monitoringLeases: 64,
            },
        },
        receiver: {
            notifications: {
                accepted: 8,
                emailAccepted: 4,
                webhookAccepted: 4,
                attempts: 8,
                activeWebhooks: 4,
                smtpConnections: 4,
            },
            discovery: { streams: 4, emitted: 256, lists: 4, inspections: 32, inventoryCount: 8 },
        },
    }
}

test('contention is an explicit bounded option independent of policy capacity and extended recovery', () => {
    expect(parseBackgroundOptions([]).contention).toBeUndefined()
    expect(parseBackgroundOptions(['--contention', 'true', '--policies', '1000'])).toMatchObject({
        contention: true,
        policies: 1000,
    })
    expect(() => parseBackgroundOptions(['--contention', 'yes'])).toThrow()
    expect(() =>
        parseBackgroundOptions(['--contention', 'true', '--contention', 'false']),
    ).toThrow()
    expect(
        Object.values(contentionContract.sql.classes).reduce((sum, item) => sum + item.running, 0),
    ).toBe(8)
    expect(contentionContract.connectionPool).toEqual({
        maxConnections: 16,
        backgroundTransactions: 8,
        interactiveHeadroom: 8,
    })
})

const metadata = (id: string, name = 'exportRuntimeSupportReportHandler') =>
    `createServerRpc({id:"${id}",name:"${name}",filename:"middleware.ts"})`

test('existing support RPC discovery selects only exact built metadata and refuses ambiguous IDs', () => {
    const first = 'a'.repeat(64),
        second = 'b'.repeat(64)
    expect(findSupportRpcId([metadata(first)])).toBe(first)
    expect(findSupportRpcId([metadata(first), metadata(first)])).toBe(first)
    expect(findSupportRpcId([metadata(first), metadata(second)])).toBeNull()
    expect(findSupportRpcId([metadata(first, 'differentHandler')])).toBeNull()
    expect(findSupportRpcId(['private unrelated source'])).toBeNull()
})

test('mixed evidence requires genuine overlapping progress and keeps unavailable web singleton values unknown', () => {
    const previous = sample()
    previous.critical = { attempts: 1, successes: 1 }
    const report = buildContentionReport(
        [previous, sample()],
        { attempts: 1, successes: 1, discoveryRevision: 2 },
        0,
    )
    expect(report).toMatchObject({
        status: 'met',
        simultaneousSamples: 2,
        capacity: 'insufficient',
        resourceEvidence: { complete: true, boundsMet: true },
    })
    expect(report.progress.nativeCriticalProgressMs).toBe(500)
    expect(
        buildContentionReport([], { attempts: 1, successes: 1, discoveryRevision: 2 }, 0).status,
    ).toBe('insufficient')
    const missing = sample()
    missing.leasedPolicies = 0
    expect(
        buildContentionReport([missing], { attempts: 1, successes: 1, discoveryRevision: 2 }, 0)
            .status,
    ).toBe('unmet')
    missing.leasedPolicies = 64
    missing.receiver.notifications.emailAccepted = 0
    expect(
        buildContentionReport([missing], { attempts: 1, successes: 1, discoveryRevision: 2 }, 0)
            .status,
    ).toBe('unmet')
})

test('missing real web/event-loop data stays insufficient and each observed SQL class is bounded', () => {
    const base = { attempts: 1, successes: 1, discoveryRevision: 2 }
    const missing = sample()
    delete missing.web
    expect(buildContentionReport([missing], base, 0)).toMatchObject({
        status: 'insufficient',
        webProcessMetrics: { singletonAdmissionCounters: null, eventLoopLag: null },
    })
    const noLoop = sample()
    noLoop.web!.bun = null
    expect(buildContentionReport([noLoop], base, 0).status).toBe('insufficient')
    const running = sample()
    running.web!.resources!.database.monitoring.running = 3
    expect(buildContentionReport([running], base, 0)).toMatchObject({
        status: 'unmet',
        resourceEvidence: { boundsMet: false },
    })
    const pending = sample()
    pending.web!.resources!.database.discovery.pending = 2
    expect(buildContentionReport([pending], base, 0).status).toBe('unmet')
})

test('idle timers, stale registry writes, data-plane errors, admission overflow and slow management fail separately', () => {
    const base = { attempts: 1, successes: 1, discoveryRevision: 2 }
    for (const mutate of [
        (value: ContentionSample) => {
            value.critical.successes = 1
        },
        (value: ContentionSample) => {
            value.criticalObservedMs = 15001
        },
        (value: ContentionSample) => {
            value.state.discovery.revision = 2
        },
        (value: ContentionSample) => {
            value.revisionUnchanged = false
        },
        (value: ContentionSample) => {
            value.state.notification.inflight = 9
        },
        (value: ContentionSample) => {
            value.managementMs = 1001
        },
    ]) {
        const value = sample()
        mutate(value)
        expect(buildContentionReport([value], base, 0).status).toBe('unmet')
    }
    expect(buildContentionReport([sample()], base, 1).status).toBe('unmet')
    expect(
        contentionStateSchema.safeParse({ ...sample().state, singleton: { running: 0 } }).success,
    ).toBe(false)
})

test('native progress must advance between samples taken during mixed overlap', () => {
    const base = { attempts: 1, successes: 1, discoveryRevision: 2 }
    const before = sample()
    before.critical = { attempts: 2, successes: 2 }
    const after = sample()
    after.critical = { attempts: 3, successes: 3 }
    expect(buildContentionReport([before, after], base, 0).status).toBe('met')
    const idle = sample()
    idle.leasedPolicies = 0
    idle.critical = { attempts: 3, successes: 3 }
    const afterGap = sample()
    afterGap.critical = { attempts: 3, successes: 3 }
    expect(buildContentionReport([before, idle, afterGap], base, 0).status).toBe('unmet')
    after.critical = { attempts: 2, successes: 2 }
    expect(buildContentionReport([before, after], base, 0).status).toBe('unmet')
})

test('registry revision without emitted discovery events is not positive discovery evidence', () => {
    const noEvents = sample()
    noEvents.receiver.discovery.emitted = 0
    expect(
        buildContentionReport([noEvents], { attempts: 1, successes: 1, discoveryRevision: 2 }, 0)
            .status,
    ).toBe('unmet')
})

test('controlled Docker events wait for the overlap barrier and change only detached candidate metadata', async () => {
    const engine = startBackgroundDockerEngine(400, {
        initiallyPaused: true,
        mutateDetachedInventory: true,
    })
    try {
        const response = await fetch(`http://127.0.0.1:${engine.server.port}/v1.47/events`)
        const reader = response.body!.getReader()
        await reader.read()
        expect(engine.stats().emitted).toBe(0)
        engine.arm()
        const chunk = await reader.read()
        expect(new TextDecoder().decode(chunk.value).trim().split('\n')).toHaveLength(64)
        expect(engine.stats().emitted).toBe(64)
        const candidate = (await (
            await fetch(
                `http://127.0.0.1:${engine.server.port}/v1.47/containers/${engine.ids[0]}/json`,
            )
        ).json()) as { Config: { Labels: Record<string, string> } }
        expect(candidate.Config.Labels['rentnerproxy.port']).toBe('9001')
        await reader.cancel()
    } finally {
        engine.stop()
    }
})

test('cancelling one paused Docker stream preserves the other stream for the arm barrier', async () => {
    const engine = startBackgroundDockerEngine(419, { initiallyPaused: true })
    try {
        const url = `http://127.0.0.1:${engine.server.port}/v1.47/events`
        const cancelled = (await fetch(url)).body!.getReader()
        await cancelled.read()
        const survivor = (await fetch(url)).body!.getReader()
        await survivor.read()
        expect(engine.waitingStreams()).toBe(2)
        await cancelled.cancel()
        const deadline = performance.now() + 1000
        while (engine.waitingStreams() !== 1 && performance.now() < deadline) {
            // oxlint-disable-next-line no-await-in-loop -- Wait for the actual server-side cancel acknowledgment before arming.
            await Bun.sleep(1)
        }
        expect(engine.waitingStreams()).toBe(1)
        engine.arm()
        const burst = await survivor.read()
        expect(new TextDecoder().decode(burst.value).trim().split('\n')).toHaveLength(64)
        expect(engine.stats()).toMatchObject({ streams: 2, emitted: 64 })
        expect(engine.waitingStreams()).toBe(0)
        await survivor.cancel()
        for (let index = 0; index < 2; index++) {
            // oxlint-disable-next-line no-await-in-loop -- Each positive event batch must finish before the next reconnect.
            const response = await fetch(url)
            // oxlint-disable-next-line no-await-in-loop -- These first three batches close; only the final batch retains its stream.
            expect((await response.text()).trim().split('\n')).toHaveLength(64)
        }
        const final = (await fetch(url)).body!.getReader()
        const remaining = await final.read()
        expect(new TextDecoder().decode(remaining.value).trim().split('\n')).toHaveLength(64)
        expect(engine.stats()).toMatchObject({ streams: 5, emitted: 256 })
        await final.cancel()
    } finally {
        engine.stop()
    }
})

test('actual private TLS SMTP AUTH and webhook receivers retain slow in-flight work and allowlisted receipt counts', async () => {
    const receiver = await startBackgroundNotificationReceivers({
        delayMs: 0,
        holdDeliveries: true,
    })
    const transport = createTransport({
        host: '127.0.0.1',
        port: receiver.smtpPort,
        secure: true,
        auth: { user: 'reliability', pass: 'fixture-only' },
        tls: { servername: 'notification-tls.invalid', ca: TLS_TEST_CA, minVersion: 'TLSv1.2' },
    })
    try {
        const mail = transport.sendMail({
            from: 'fixture@fixture.invalid',
            to: 'accepted@fixture.invalid',
            messageId: '<' + 'a'.repeat(64) + '@notifications.rentnerproxy>',
            text: 'private generated fixture',
        })
        const webhook = fetch(`http://127.0.0.1:${receiver.webhook.port}/`, {
            method: 'POST',
            body: JSON.stringify({ id: 'contention-webhook' }),
        })
        await Promise.all([receiver.webhookReady, receiver.smtpDeliveryReady])
        expect(receiver.stats()).toMatchObject({
            emailAccepted: 0,
            webhookAccepted: 0,
            activeWebhooks: 1,
        })
        expect(receiver.stats().smtpConnections).toBeGreaterThan(0)
        receiver.releaseDeliveries()
        await mail
        expect((await webhook).status).toBe(204)
        expect(receiver.stats()).toMatchObject({
            emailAccepted: 1,
            webhookAccepted: 1,
            activeWebhooks: 0,
        })
    } finally {
        transport.close()
        await receiver.close()
    }
})

test('SMTP startup failure closes the already-started webhook listener', async () => {
    const webhookPortProbe = createNetServer()
    await new Promise<void>((resolve) => webhookPortProbe.listen(0, '127.0.0.1', resolve))
    const webhookPort = (webhookPortProbe.address() as { port: number }).port
    await new Promise<void>((resolve) => webhookPortProbe.close(() => resolve()))
    const smtpPortProbe = createNetServer()
    await new Promise<void>((resolve) => smtpPortProbe.listen(0, '127.0.0.1', resolve))
    const smtpPort = (smtpPortProbe.address() as { port: number }).port
    try {
        await expect(
            startBackgroundNotificationReceivers({ webhookPort, smtpPort }),
        ).rejects.toThrow()
        const reclaimed = createNetServer()
        await new Promise<void>((resolve, reject) => {
            reclaimed.once('error', reject)
            reclaimed.listen(webhookPort, '127.0.0.1', resolve)
        })
        await new Promise<void>((resolve) => reclaimed.close(() => resolve()))
    } finally {
        await new Promise<void>((resolve) => smtpPortProbe.close(() => resolve()))
    }
})

test('contention memory evidence requires every real serving owner and never substitutes cgroup memory for RSS', () => {
    const base = { attempts: 1, successes: 1, discoveryRevision: 2 }
    for (const field of [
        'webRssBytes',
        'controllerRssBytes',
        'caddyRssBytes',
        'cgroupMemoryCurrentBytes',
    ] as const) {
        const missing = sample()
        missing.processMemory![field] = null
        expect(buildContentionReport([missing], base, 0)).toMatchObject({
            status: 'insufficient',
            resourceEvidence: { complete: false, processMemoryComplete: false },
        })
    }
    const old = sample()
    delete old.processMemory
    expect(buildContentionReport([old], base, 0).status).toBe('insufficient')
    const mixed = sample()
    mixed.processMemory!.webRssBytes = null
    expect(buildContentionReport([sample(), mixed], base, 0).status).toBe('insufficient')
    const complete = sample()
    complete.processMemory = {
        ...complete.processMemory!,
        command: 'private secret',
    } as NonNullable<ContentionSample['processMemory']>
    expect(JSON.stringify(buildContentionReport([complete], base, 0))).not.toContain(
        'private secret',
    )
})

test.each([
    'missing-web',
    'missing-resource',
    'invalid-resource',
    'missing-workers',
    'invalid-workers',
] as const)('one valid web sample cannot hide another sample with %s evidence', (fault) => {
    const invalid = sample()
    if (fault === 'missing-web') delete invalid.web
    else {
        const raw = structuredClone(invalid.web!)
        if (fault === 'missing-resource') raw.resources = null
        if (fault === 'invalid-resource') raw.resources!.database.monitoring.running = 3
        if (fault === 'missing-workers') raw.bun = null
        if (fault === 'invalid-workers') raw.bun!.workers.pop()
        invalid.web = contentionWebSchema.parse(raw)
    }
    const report = buildContentionReport(
        [sample(), invalid],
        { attempts: 1, successes: 1, discoveryRevision: 2 },
        0,
    )
    expect(report).toMatchObject({ status: 'insufficient', resourceEvidence: { complete: false } })
    expect(report.samples).toHaveLength(2)
})
