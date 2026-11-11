// oxlint-disable no-await-in-loop -- Each EOF establishes a distinct reconnect before the live final-event proof.
import { expect, test } from 'bun:test'
import {
    assertBackgroundDiscoveryEventChange,
    startBackgroundDockerEngine,
} from '../../../../../../scripts/runtime-scale/background-discovery.ts'

type Container = {
    Id: string
    Config: { Labels: Record<string, string> }
    NetworkSettings: { Networks: { fixture: { IPAddress: string } } }
}
function snapshot(containers: Container[]) {
    return {
        candidates: containers.map((item) => ({
            containerId: item.Id,
            sourceId: item.Config.Labels['rentnerproxy.id']!,
            domains: [item.Config.Labels['rentnerproxy.domains']!],
            target: {
                scheme: 'http' as const,
                address: item.NetworkSettings.Networks.fixture.IPAddress,
                port: Number(item.Config.Labels['rentnerproxy.port']),
                network: 'fixture',
            },
            certificateId: null,
            accessPolicyId: null,
            upstreamTlsServerName: null,
            issues: [],
        })),
    }
}

test('reconnect-only inventory cannot satisfy the controlled final event on the still-open fourth stream', async () => {
    const engine = startBackgroundDockerEngine(397, { finalEventBarrier: true })
    const base = `http://127.0.0.1:${engine.server.port}/v1.47`
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
    const inspect = () =>
        Promise.all(
            engine.ids.map(
                async (id) =>
                    (await (await fetch(base + '/containers/' + id + '/json')).json()) as Container,
            ),
        )
    try {
        expect(() => engine.armFinalEvent()).toThrow()
        for (let index = 0; index < 3; index++) {
            expect((await (await fetch(base + '/events')).text()).trim().split('\n')).toHaveLength(
                64,
            )
            await fetch(base + '/containers/json')
            await inspect()
        }
        reader = (await fetch(base + '/events')).body!.getReader()
        const initial = await reader.read()
        expect(new TextDecoder().decode(initial.value).trim().split('\n')).toHaveLength(63)
        expect(engine.stats()).toMatchObject({ streams: 4, emitted: 255, inventoryCount: 8 })
        expect(engine.finalEventReady()).toBe(false)
        await fetch(base + '/containers/json')
        expect(() => engine.armFinalEvent()).toThrow()
        const ignoredEventsSnapshot = snapshot(await inspect())
        expect(engine.finalEventReady()).toBe(true)
        expect(
            ignoredEventsSnapshot.candidates.every((candidate) => candidate.target.port === 9000),
        ).toBe(true)
        const change = engine.armFinalEvent()
        const final = await reader.read()
        expect(final.done).toBe(false)
        const delivered = new TextDecoder()
            .decode(final.value)
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as unknown)
        expect(delivered).toEqual([engine.events[255]])
        expect(engine.events[255]).toMatchObject({
            Action: 'update',
            Actor: { ID: change.containerId },
        })
        expect(() => engine.armFinalEvent()).toThrow()
        // Even the same eight correct identities fail when the connector ignores their final update.
        expect(() =>
            assertBackgroundDiscoveryEventChange(ignoredEventsSnapshot, change, engine.stats()),
        ).toThrow('fresh inventory scan')
        await fetch(base + '/containers/json')
        const changedSnapshot = snapshot(await inspect())
        expect(() =>
            assertBackgroundDiscoveryEventChange(ignoredEventsSnapshot, change, engine.stats()),
        ).toThrow('change its actual candidate')
        expect(() =>
            assertBackgroundDiscoveryEventChange(changedSnapshot, change, engine.stats()),
        ).not.toThrow()
        expect(() =>
            assertBackgroundDiscoveryEventChange(changedSnapshot, change, {
                streams: 5,
                lists: engine.stats().lists,
            }),
        ).toThrow('reconnect')
        const pending = reader.read()
        expect(
            await Promise.race([pending.then(() => false), Bun.sleep(20).then(() => true)]),
        ).toBe(true)
        expect(engine.stats()).toMatchObject({ streams: 4, emitted: 256, inventoryCount: 8 })
    } finally {
        await reader?.cancel()
        engine.stop()
    }
})
