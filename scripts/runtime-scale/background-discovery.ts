// oxlint-disable no-await-in-loop -- Bounded polling observes successive native connector reconnect states.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { createSeededRandom } from '../runtime-reliability/control.ts'

/** Serve eight deterministic containers and 256 seeded events with forced stream reconnects; the caller owns server shutdown. */
export function startBackgroundDockerEngine(seed: number) {
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
    const server = Bun.serve({
        hostname: '127.0.0.1',
        port: 0,
        /** Expose only read-only fixture Docker routes and close the first three event streams to force bounded reconnect evidence. */
        fetch(request) {
            const path = new URL(request.url).pathname
            if (request.method !== 'GET') return new Response(null, { status: 403 })
            if (path === '/v1.47/events') {
                streams++
                const batch = events.slice(emitted, emitted + 64)
                emitted += batch.length
                const close = streams <= 3
                return new Response(
                    new ReadableStream({
                        start(controller) {
                            controller.enqueue(
                                new TextEncoder().encode(
                                    batch.map((item) => JSON.stringify(item)).join('\n') + '\n',
                                ),
                            )
                            if (close) controller.close()
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
        stats: () => ({ streams, emitted, lists, inspections, inventoryCount: inventory.length }),
        stop: () => server.stop(true),
    }
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
