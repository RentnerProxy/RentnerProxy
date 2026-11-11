// oxlint-disable no-await-in-loop -- Stop admission before observing settlement of admitted real appliance sends.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { readFile, writeFile, rm } from 'node:fs/promises'
import { eq, sql } from 'drizzle-orm'
import { users } from '../../web/src/db/schema.ts'
import { buildNotificationConfiguration } from '../../web/src/server/Notifications/notification-settings.ts'
import * as notifications from '../../web/src/server/Notifications/notification-storage.ts'
import { observeNotificationSignals } from '../../web/src/server/Notifications/incident-engine.ts'
import { dockerDiscoveryController } from '../../web/src/server/Controller/docker-discovery.server.ts'
import {
    readDiscoveryRegistry,
    writeDiscoveryRegistry,
} from '../../web/src/server/Admin/DockerDiscovery/discovery-storage.server.ts'
import {
    discoveryEndpointHash,
    discoverySourceKey,
} from '../../web/src/server/Admin/DockerDiscovery/discovery-plan.ts'
import { lockProxyRuntimeSettings } from '../../web/src/server/ProxyRuntime/proxy-runtime-settings.ts'
import type { FixtureContext } from '../runtime-reliability/Types/fixture.types.ts'
import type {
    StoredNotificationConfiguration,
    NotificationRuntime,
} from '../../web/src/server/Notifications/Types/notification-internal.types.ts'
import type { DiscoveryRegistry } from '../../web/src/lib/DockerDiscovery/Types/management.types.ts'

const endpoint = 'http://127.0.0.1:9017'
const control = 'http://127.0.0.1:9018'

/** Derive eight stable detached discovery identities from the seed so event reconciliation cannot adopt extra proxy routes. */
export function contentionDetachedOwners(seed: number) {
    return Array.from({ length: 8 }, (_, index) => {
        const id = createHash('sha256').update(`docker:${seed}:${index}`).digest('hex')
        return {
            key: discoverySourceKey(endpoint, 'label:background-' + index),
            sourceId: 'label:background-' + index,
            hostId: `${id.slice(0, 8)}-${id.slice(8, 12)}-7${id.slice(13, 16)}-8${id.slice(17, 20)}-${id.slice(20, 32)}`,
            containerId: id,
            domains: [`discovered-${index}.fixture.test`],
            state: 'detached' as const,
            error: null,
        }
    })
}

type Saved = {
    configuration: StoredNotificationConfiguration
    runtime: NotificationRuntime
    registry: DiscoveryRegistry
    controller: Awaited<
        ReturnType<typeof dockerDiscoveryController.status>
    >['configuration']['settings']
}

/** Require unused disposable settings, save prior state, seed real workers, and restore only after notification admission and in-flight sends drain. */
export async function backgroundContentionFixture(context: FixtureContext) {
    const database = context.database
    const path = '/tmp/background-contention-' + context.command.runId + '.json'
    if (context.command.phase === 'background-contention-seed') {
        const [configuration, runtime, registry, controller] = await Promise.all([
            database.transaction(notifications.readNotificationConfiguration),
            database.transaction(notifications.readNotificationRuntime),
            database.transaction(readDiscoveryRegistry),
            dockerDiscoveryController.status(),
        ])
        assert.ok(
            !configuration.enabled &&
                configuration.channels.length === 0 &&
                !registry.enabled &&
                !controller.configuration.settings.enabled,
            'Contention requires unused disposable notification and discovery settings',
        )
        await writeFile(
            path,
            JSON.stringify({
                configuration,
                runtime,
                registry,
                controller: controller.configuration.settings,
            }),
            { mode: 0o600 },
        )
        const [actor] = await database
            .select({ id: users.id })
            .from(users)
            .where(eq(users.email, context.command.runId + '@reliability.invalid'))
        assert.ok(actor)
        const connected = await dockerDiscoveryController.save({
            actorUserId: actor.id,
            expectedRevision: controller.configuration.revision,
            configuration: { enabled: true, endpoint },
            acknowledgeRestore: false,
        })
        let sequence = 0
        const makeId = () => {
            const value = createHash('sha256')
                .update(`contention:${context.command.seed}:${sequence++}`)
                .digest('hex')
            return `${value.slice(0, 8)}-${value.slice(8, 12)}-7${value.slice(13, 16)}-8${value.slice(17, 20)}-${value.slice(20, 32)}`
        }
        const configured = await buildNotificationConfiguration(configuration, {
            expectedRevision: configuration.revision,
            enabled: true,
            channels: Array.from({ length: 8 }, (_, index) => ({
                id: Bun.randomUUIDv7(),
                name: 'Contention fixture ' + index,
                kind: index % 2 === 0 ? ('email' as const) : ('webhook' as const),
                enabled: true,
                events: [index % 4 < 2 ? ('ddns' as const) : ('proxy_configuration' as const)],
                minimumSeverity: 'error' as const,
                thresholdSeconds: 0,
                recovery: false,
                destination:
                    index % 2 === 0
                        ? { kind: 'email' as const, recipients: ['accepted@fixture.invalid'] }
                        : {
                              kind: 'webhook' as const,
                              url: 'http://127.0.0.1:9016/',
                              authorizationToken: null,
                              allowedPrivateCidrs: ['127.0.0.1/32'],
                          },
            })),
        })
        await database.transaction(async (transaction) => {
            await notifications.lockNotificationConfiguration(transaction)
            await notifications.writeNotificationConfiguration(transaction, configured)
            const state = await notifications.lockNotificationRuntime(transaction)
            const engine = observeNotificationSignals(
                state.engine,
                configured,
                Array.from({ length: 80 }, (_, index) => ({
                    source: 'event' as const,
                    category:
                        index % 2 === 0 ? ('ddns' as const) : ('proxy_configuration' as const),
                    resourceId: `contention-${context.command.seed}-${index}`,
                    health: 'failing' as const,
                    severity: 'error' as const,
                    occurredAt: new Date().toISOString(),
                    evidence: 'private-mixed-load-fixture',
                    failures: 1,
                })),
                Date.now(),
                makeId,
            )
            assert.ok(engine.deferredCount > 0)
            await notifications.writeNotificationRuntime(transaction, { ...state, engine })
        })
        await database.transaction(async (transaction) => {
            await lockProxyRuntimeSettings(transaction)
            const current = await readDiscoveryRegistry(transaction)
            assert.ok(!current.enabled, 'Contention requires unused disposable discovery settings')
            await writeDiscoveryRegistry(transaction, {
                ...current,
                revision: current.revision + 1,
                enabled: true,
                actorUserId: actor.id,
                connectorRevision: connected.configuration.revision,
                endpointHash: discoveryEndpointHash(endpoint),
                error: 'reconcile_failed',
                owners: contentionDetachedOwners(context.command.seed),
                results: [],
            })
        })
        return { status: 'seeded', potentialDeliveries: 320, inventory: 8, receiverDelayMs: 1500 }
    }
    if (context.command.phase === 'background-contention-arm') {
        const discoveryRevision = await database.transaction(async (transaction) => {
            await lockProxyRuntimeSettings(transaction)
            const registry = await readDiscoveryRegistry(transaction)
            const revision = registry.revision + 1
            await writeDiscoveryRegistry(transaction, {
                ...registry,
                revision,
                error: 'reconcile_failed',
            })
            return revision
        })
        const response = await fetch(control + '/arm', {
            method: 'POST',
            signal: AbortSignal.timeout(2000),
        })
        assert.equal(response.status, 204)
        return { discoveryRevision }
    }
    if (context.command.phase === 'background-contention-stop') {
        const saved = JSON.parse(await readFile(path, 'utf8')) as Saved
        await database.transaction(async (transaction) => {
            const current = await notifications.lockNotificationConfiguration(transaction)
            await notifications.writeNotificationConfiguration(transaction, {
                ...current,
                enabled: false,
                revision: current.revision + 1,
            })
        })
        const deadline = Date.now() + 20000
        let inflight = 1
        while (Date.now() < deadline) {
            const state = await database.transaction(notifications.readNotificationRuntime)
            inflight = state.engine.deliveries.filter((item) => item.status === 'inflight').length
            if (!inflight) break
            // oxlint-disable-next-line no-await-in-loop -- Stop admission before waiting for real admitted transports to settle.
            await Bun.sleep(100)
        }
        assert.equal(
            inflight,
            0,
            'Actual appliance notification worker drains before fixture settings are restored',
        )
        await database.transaction(async (transaction) => {
            await notifications.lockNotificationConfiguration(transaction)
            await notifications.writeNotificationConfiguration(transaction, saved.configuration)
            await notifications.lockNotificationRuntime(transaction)
            await notifications.writeNotificationRuntime(transaction, saved.runtime)
        })
        const latest = await dockerDiscoveryController.status()
        await dockerDiscoveryController.save({
            actorUserId: Bun.randomUUIDv7(),
            expectedRevision: latest.configuration.revision,
            configuration: saved.controller,
            acknowledgeRestore: false,
        })
        await database.transaction(async (transaction) => {
            await lockProxyRuntimeSettings(transaction)
            await writeDiscoveryRegistry(transaction, saved.registry)
        })
        await fetch(control + '/stop', { method: 'POST', signal: AbortSignal.timeout(2000) })
        await rm(path)
        return { drained: true }
    }
    assert.equal(context.command.phase, 'background-contention-read')
    const [runtime, registry, rows] = await Promise.all([
        database.transaction(notifications.readNotificationRuntime),
        database.transaction(readDiscoveryRegistry),
        database.execute<{
            connections: number
            active: number
            lockWaiters: number
            idleTransactions: number
            monitoringLeases: number
        }>(
            sql`select count(*)::int as connections, count(*) filter (where state = 'active' and pid <> pg_backend_pid())::int as active, count(*) filter (where wait_event_type = 'Lock')::int as "lockWaiters", count(*) filter (where state = 'idle in transaction')::int as "idleTransactions", (select count(*)::int from rentnerproxy.host_check_policies where lease_expires_at > clock_timestamp()) as "monitoringLeases" from pg_stat_activity where datname = current_database()`,
        ),
    ])
    const totals = { pending: 0, inflight: 0, succeeded: 0, failed: 0, cancelled: 0 }
    for (const delivery of runtime.engine.deliveries) totals[delivery.status]++
    return {
        notification: { ...totals, deferred: runtime.engine.deferredCount },
        discovery: {
            revision: registry.revision,
            error: registry.error,
            detached: registry.owners.filter((owner) => owner.state === 'detached').length,
        },
        postgres: rows[0],
    }
}
