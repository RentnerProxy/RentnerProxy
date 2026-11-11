import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test('arm and seed increment the registry observed after acquiring the runtime lock', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        import * as fs from 'node:fs/promises'
        process.env.DATABASE_URL = 'postgresql://unit:unit@127.0.0.1:1/rentnerproxy_unit'
        mock.module('node:fs/promises', () => ({ ...fs, writeFile: async () => {} }))
        const events = [], writes = []
        let locked = false, transactionId = 0
        mock.module('./web/src/server/ProxyRuntime/proxy-runtime-settings.ts', () => ({
            lockProxyRuntimeSettings: async transaction => { locked = true; events.push(['lock', transaction.id]) },
        }))
        mock.module('./web/src/server/Admin/DockerDiscovery/discovery-storage.server.ts', () => ({
            readDiscoveryRegistry: async transaction => { events.push(['read', transaction.id, locked]); return { enabled: false, revision: locked ? 8 : 3, marker: locked ? 'latest' : 'stale' } },
            writeDiscoveryRegistry: async (transaction, registry) => { events.push(['write', transaction.id]); writes.push(registry) },
        }))
        mock.module('./web/src/server/Controller/docker-discovery.server.ts', () => ({ dockerDiscoveryController: {
            status: async () => ({ configuration: { revision: 1, settings: { enabled: false } } }),
            save: async () => ({ configuration: { revision: 2 } }),
        } }))
        mock.module('./web/src/server/Notifications/notification-settings.ts', () => ({ buildNotificationConfiguration: async () => ({ enabled: true }) }))
        mock.module('./web/src/server/Notifications/incident-engine.ts', () => ({ observeNotificationSignals: () => ({ deferredCount: 64 }) }))
        mock.module('./web/src/server/Notifications/notification-storage.ts', () => ({
            readNotificationConfiguration: async () => ({ enabled: false, channels: [], revision: 1 }),
            readNotificationRuntime: async () => ({ engine: {} }),
            lockNotificationConfiguration: async () => {}, writeNotificationConfiguration: async () => {},
            lockNotificationRuntime: async () => ({ engine: {} }), writeNotificationRuntime: async () => {},
        }))
        globalThis.fetch = async () => new Response(null, { status: 204 })
        const { backgroundContentionFixture } = await import('./scripts/runtime-scale/background-contention-fixture.ts')
        const database = {
            transaction: async operation => operation({ id: ++transactionId }),
            select: () => ({ from: () => ({ where: async () => [{ id: 'fixture-actor' }] }) }),
        }
        const command = { phase: 'background-contention-arm', seed: 1, runId: 'fixture' }
        const armed = await backgroundContentionFixture({ database, command })
        const arm = { events: [...events], value: writes[0], armed }
        events.length = 0; locked = false; writes.length = 0
        command.phase = 'background-contention-seed'
        await backgroundContentionFixture({ database, command })
        console.log(JSON.stringify({ arm, seed: { events, value: writes[0] } }))
    `,
        { cwd: new URL('../../../../../..', import.meta.url) },
    )
    const result = JSON.parse(output.trim().split('\n').at(-1)!)
    expect(result.arm).toEqual({
        events: [
            ['lock', 1],
            ['read', 1, true],
            ['write', 1],
        ],
        value: { enabled: false, revision: 9, marker: 'latest', error: 'reconcile_failed' },
        armed: { discoveryRevision: 9 },
    })
    expect(result.seed.value).toMatchObject({ enabled: true, revision: 9, marker: 'latest' })
    const last = result.seed.events.slice(-3)
    expect(last).toEqual([
        ['lock', 6],
        ['read', 6, true],
        ['write', 6],
    ])
})
