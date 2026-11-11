// oxlint-disable no-await-in-loop -- Bounded polling observes successive native connector reconnect states.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { createSeededRandom } from '../runtime-reliability/control.ts'

/** Serve the bounded seeded Docker inventory with optional delayed event arming and detached-only mutations; caller owns shutdown. */
export function startBackgroundDockerEngine(
    seed: number,
    options: { port?: number; initiallyPaused?: boolean; mutateDetachedInventory?: boolean } = {},
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
    let streams = 0,
        emitted = 0,
        lists = 0,
        inspections = 0
    let armed = !options.initiallyPaused
    const waiting = new Set<ReadableStreamDefaultController<Uint8Array>>()
    /** Emit the next 64 events, optionally vary detached inventory, and force reconnects until the final batch is delivered. */
    const send = (controller: ReadableStreamDefaultController<Uint8Array>) => {
        const batch = events.slice(emitted, emitted + 64)
        emitted += batch.length
        if (options.mutateDetachedInventory)
            for (const item of inventory)
                item.Config.Labels['rentnerproxy.port'] = String(
                    9000 + (Math.floor(emitted / 64) % 2),
                )
        controller.enqueue(
            new TextEncoder().encode(batch.map((item) => JSON.stringify(item)).join('\n') + '\n'),
        )
        if (emitted < events.length) controller.close()
    }
    const server = Bun.serve({
        hostname: '127.0.0.1',
        port: options.port ?? 0,
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
                        },
                    }),
                    { headers: { 'content-type': 'application/json' } },
                )
            }
            if (path === '/v1.47/containers/json') {
                lists++
                return Response.json(
                    inventory.map((item) => ({ Id: item.Id, Labels: item.Config.Labels })),
                )
            }
            const candidate = inventory.find(
                (item) => path === '/v1.47/containers/' + item.Id + '/json',
            )
            if (candidate) {
                inspections++
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
        stats: () => ({ streams, emitted, lists, inspections, inventoryCount: inventory.length }),
        stop: () => server.stop(true),
    }
}

export async function runBackgroundDiscovery(seed: number) {
    const { dockerDiscoveryController } =
        await import('../../web/src/server/Controller/docker-discovery.server.ts')
    const original = await dockerDiscoveryController.status()
    assert.equal(
        original.configuration.settings.enabled,
        false,
        'Only the isolated unused discovery connector may be configured',
    )
    const engine = startBackgroundDockerEngine(seed)
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
        while (performance.now() - started < 45000) {
            final = await dockerDiscoveryController.status()
            if (
                engine.stats().streams >= 4 &&
                engine.stats().emitted === 256 &&
                !final.stale &&
                final.status === 'ready'
            )
                break
            // oxlint-disable-next-line no-await-in-loop -- Observe real bounded connector reconnects without retrying a failed acceptance assertion.
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
        return {
            seed,
            model: 'Controlled Docker API event workload against real native discovery connector; no host Docker socket',
            attemptedEvents: 256,
            inventoryLimit: 256,
            ...engine.stats(),
            elapsedMs: performance.now() - started,
            finalCandidateIds: final.candidates.map((item) => item.containerId),
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
