// oxlint-disable no-await-in-loop -- Bounded polling observes successive native connector reconnect states.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { createSeededRandom } from '../runtime-reliability/control.ts'
import type { DockerDiscoverySnapshot } from '../../web/src/lib/DockerDiscovery/Types/docker-discovery.types.ts'

/** Serve eight seeded containers and 256 events with controlled reconnects, delayed arming and detached-only mutation; caller owns shutdown. */
export function startBackgroundDockerEngine(
    seed: number,
    options: {
        port?: number
        initiallyPaused?: boolean
        mutateDetachedInventory?: boolean
        finalEventBarrier?: boolean
    } = {},
) {
    const random = createSeededRandom(seed)
    const inventory = Array.from({ length: 8 }, (_, index) => ({
        Id: createHash('sha256').update(`docker:${seed}:${index}`).digest('hex'),
        Name: '/background-' + index,
        State: { Running: true, Paused: false, Restarting: false },
        Config: {
            Labels: {
                'rentnerproxy.enable': 'true',
                'rentnerproxy.domains': `discovered-${index}.fixture.test`,
                'rentnerproxy.id': 'background-' + index,
                'rentnerproxy.network': 'fixture',
                'rentnerproxy.port': '9000',
            },
            ExposedPorts: {},
        },
        HostConfig: { NetworkMode: 'fixture' },
        NetworkSettings: {
            Networks: { fixture: { IPAddress: '192.0.2.' + (index + 1), GlobalIPv6Address: '' } },
        },
    }))
    const actions = ['create', 'start', 'update', 'die', 'destroy']
    const events = Array.from({ length: 256 }, (_, index) => ({
        Type: 'container',
        Action: actions[Math.floor(random() * actions.length)]!,
        Actor: { ID: inventory[Math.floor(random() * inventory.length)]!.Id },
        timeNano: 1794355200000000000 + index * 1000,
    }))
    if (options.finalEventBarrier)
        events[255] = { ...events[255]!, Action: 'update', Actor: { ID: inventory[0]!.Id } }
    let streams = 0,
        emitted = 0,
        lists = 0,
        inspections = 0
    let armed = !options.initiallyPaused
    const waiting = new Set<ReadableStreamDefaultController<Uint8Array>>()
    let finalController: ReadableStreamDefaultController<Uint8Array> | undefined
    let finalListSeen = false
    const finalInspections = new Set<string>()
    /** Emit the next 64 events, optionally vary detached inventory, and force reconnects until the final batch is delivered. */
    const send = (controller: ReadableStreamDefaultController<Uint8Array>) => {
        const end = options.finalEventBarrier ? Math.min(emitted + 64, 255) : emitted + 64
        const batch = events.slice(emitted, end)
        emitted += batch.length
        if (options.mutateDetachedInventory)
            for (const item of inventory)
                item.Config.Labels['rentnerproxy.port'] = String(
                    9000 + (Math.floor(emitted / 64) % 2),
                )
        controller.enqueue(
            new TextEncoder().encode(batch.map((item) => JSON.stringify(item)).join('\n') + '\n'),
        )
        if (emitted < 255 || (!options.finalEventBarrier && emitted < events.length))
            controller.close()
        else finalController = controller
    }
    const server = Bun.serve({
        hostname: '127.0.0.1',
        port: options.port ?? 0,
        /** Expose only read-only fixture Docker routes and close the first three event streams to force bounded reconnect evidence. */
        fetch(request) {
            const path = new URL(request.url).pathname
            if (request.method !== 'GET') return new Response(null, { status: 403 })
            if (path === '/v1.47/events') {
                streams++
                let own: ReadableStreamDefaultController<Uint8Array> | undefined
                return new Response(
                    new ReadableStream<Uint8Array>({
                        start(controller) {
                            own = controller
                            if (armed) send(controller)
                            else {
                                controller.enqueue(new TextEncoder().encode('\n'))
                                waiting.add(controller)
                            }
                        },
                        cancel() {
                            if (own) waiting.delete(own)
                            if (own === finalController) finalController = undefined
                        },
                    }),
                    { headers: { 'content-type': 'application/json' } },
                )
            }
            if (path === '/v1.47/containers/json') {
                lists++
                if (finalController) {
                    finalListSeen = true
                    finalInspections.clear()
                }
                return Response.json(
                    inventory.map((item) => ({ Id: item.Id, Labels: item.Config.Labels })),
                )
            }
            const candidate = inventory.find(
                (item) => path === '/v1.47/containers/' + item.Id + '/json',
            )
            if (candidate) {
                inspections++
                if (finalController && finalListSeen) finalInspections.add(candidate.Id)
                return Response.json(candidate)
            }
            return new Response(null, { status: 404 })
        },
    })
    return {
        server,
        ids: inventory.map((item) => item.Id),
        events,
        waitingStreams: () => waiting.size,
        /** Release waiting event streams once the measured workload is active and clear their retained controllers. */
        arm() {
            armed = true
            for (const controller of waiting) send(controller)
            waiting.clear()
        },
        /** Final-event evidence requires the live fourth stream's complete inventory before changing anything. */
        finalEventReady: () =>
            Boolean(
                options.finalEventBarrier &&
                streams === 4 &&
                emitted === 255 &&
                finalController &&
                finalListSeen &&
                finalInspections.size === inventory.length,
            ),
        /** Change one detached candidate and release event 256 on its existing stream; never reconnect to prove the change. */
        armFinalEvent() {
            assert.ok(
                options.finalEventBarrier &&
                    streams === 4 &&
                    emitted === 255 &&
                    finalController &&
                    finalListSeen &&
                    finalInspections.size === inventory.length,
                'The final open stream must finish its initial inventory before the final event',
            )
            const baseline = { streams, lists }
            inventory[0]!.Config.Labels['rentnerproxy.port'] = '9001'
            finalController.enqueue(new TextEncoder().encode(JSON.stringify(events[255]) + '\n'))
            emitted++
            return { ...baseline, containerId: inventory[0]!.Id, port: 9001 }
        },
        stats: () => ({ streams, emitted, lists, inspections, inventoryCount: inventory.length }),
        stop: () => server.stop(true),
    }
}

/** Candidate identity convergence alone is insufficient: the final live-stream event must change its target. */
export function assertBackgroundDiscoveryEventChange(
    snapshot: Pick<DockerDiscoverySnapshot, 'candidates'>,
    change: { streams: number; lists: number; containerId: string; port: number },
    stats: { streams: number; lists: number },
) {
    assert.equal(stats.streams, change.streams, 'A reconnect cannot establish final event evidence')
    assert.ok(
        stats.lists > change.lists,
        'The final live-stream event must trigger a fresh inventory scan',
    )
    assert.equal(
        snapshot.candidates.find((candidate) => candidate.containerId === change.containerId)
            ?.target?.port,
        change.port,
        'The final live-stream event must change its actual candidate',
    )
}

/** Require unused discovery configuration, verify bounded real connector reconnects, and restore configuration before stopping the fixture engine. */
export async function runBackgroundDiscovery(seed: number) {
    const { dockerDiscoveryController } =
        await import('../../web/src/server/Controller/docker-discovery.server.ts')
    const original = await dockerDiscoveryController.status()
    assert.equal(
        original.configuration.settings.enabled,
        false,
        'Only the isolated unused discovery connector may be configured',
    )
    const engine = startBackgroundDockerEngine(seed, { finalEventBarrier: true })
    const started = performance.now()
    let configured = false
    try {
        await dockerDiscoveryController.save({
            actorUserId: Bun.randomUUIDv7(),
            expectedRevision: original.configuration.revision,
            configuration: { enabled: true, endpoint: `http://127.0.0.1:${engine.server.port}` },
            acknowledgeRestore: false,
        })
        configured = true
        let final = await dockerDiscoveryController.status()
        let quietSince = performance.now(),
            quietLists = -1
        while (performance.now() - started < 45000) {
            final = await dockerDiscoveryController.status()
            const stats = engine.stats()
            if (stats.lists !== quietLists) {
                quietLists = stats.lists
                quietSince = performance.now()
            }
            if (
                engine.finalEventReady() &&
                !final.stale &&
                final.status === 'ready' &&
                performance.now() - quietSince >= 1000
            )
                break
            // oxlint-disable-next-line no-await-in-loop -- Observe real bounded connector reconnects without retrying a failed acceptance assertion.
            await Bun.sleep(100)
        }
        assert.ok(
            engine.finalEventReady() &&
                !final.stale &&
                final.status === 'ready' &&
                performance.now() - quietSince >= 1000,
            'The final stream must complete and settle its initial inventory',
        )
        assert.ok(
            final.candidates.every((candidate) => candidate.target?.port === 9000),
            'The final event requires an unchanged candidate baseline',
        )
        const change = engine.armFinalEvent()
        const finalEventDeadline = Math.max(started + 45000, performance.now() + 15000)
        while (performance.now() < finalEventDeadline) {
            final = await dockerDiscoveryController.status()
            if (
                engine.stats().streams !== change.streams ||
                (!final.stale &&
                    final.status === 'ready' &&
                    final.candidates.find(
                        (candidate) => candidate.containerId === change.containerId,
                    )?.target?.port === change.port)
            )
                break
            await Bun.sleep(100)
        }
        assert.equal(engine.stats().emitted, 256)
        assert.ok(
            engine.stats().streams >= 4,
            'Three closed event streams require actual reconnects',
        )
        assert.equal(final.status, 'ready')
        assert.equal(final.stale, false)
        assert.deepEqual(
            final.candidates.map((item) => item.containerId).toSorted(),
            engine.ids.toSorted(),
            'Reconnect converges to the exact bounded final inventory',
        )
        assert.ok(final.candidates.every((item) => item.issues.length === 0))
        assertBackgroundDiscoveryEventChange(final, change, engine.stats())
        return {
            seed,
            model: 'Controlled Docker API event workload against real native discovery connector; no host Docker socket',
            attemptedEvents: 256,
            inventoryLimit: 256,
            ...engine.stats(),
            elapsedMs: performance.now() - started,
            finalCandidateIds: final.candidates.map((item) => item.containerId),
            finalEvent: {
                containerId: change.containerId,
                observedPort: change.port,
                sameStream: true,
            },
            status: 'met',
        }
    } finally {
        try {
            if (configured) {
                const latest = await dockerDiscoveryController.status()
                await dockerDiscoveryController.save({
                    actorUserId: Bun.randomUUIDv7(),
                    expectedRevision: latest.configuration.revision,
                    configuration: original.configuration.settings,
                    acknowledgeRestore: false,
                })
            }
        } finally {
            engine.stop()
        }
    }
}
