import { expect, test } from 'bun:test'
import { inArray } from 'drizzle-orm'
import { getAuthDatabase } from '../../../../server/Auth/Core/database.server.ts'
import { getDatabaseUrl } from '../../../../server/env.server.ts'
import { hostCheckPolicies, hostDomains, proxyHosts } from '../../../../db/schema.ts'
import { commandSchema } from '../../../../../../scripts/runtime-reliability/fixture.validation.ts'
import { runBackgroundNotifications } from '../../../../../../scripts/runtime-scale/background-notifications.ts'
import { backgroundFences } from '../../../../../../scripts/runtime-scale/background-fixture.ts'
import {
    backgroundPolicies,
    parseBackgroundOptions,
} from '../../../../../../scripts/runtime-scale/background.ts'

const url = getDatabaseUrl()
const enabled =
    process.env.RENTNERPROXY_DATABASE_INTEGRATION === '1' &&
    url !== null &&
    new URL(url).hostname === '127.0.0.1' &&
    new URL(url).pathname === '/rentnerproxy_upstream_test'
const integrationTest = enabled ? test : test.skip
const command = commandSchema.parse({
    runId: 'background397',
    phase: 'background-fences',
    upstreamPort: 9000,
    secondaryPort: 9001,
    seed: 397,
})

integrationTest(
    'storm drains the actual PostgreSQL queue behind expired tokens and receiver/durable-ack crashes',
    async () => {
        const previous = process.env.PUBLIC_ORIGIN
        process.env.PUBLIC_ORIGIN = 'https://fixture.invalid'
        try {
            const result = await runBackgroundNotifications({
                database: getAuthDatabase(),
                command,
            })
            expect(result).toMatchObject({
                status: 'met',
                attempted: 320,
                finalPending: 0,
                finalIncidentCount: 80,
            })
            expect(result.initiallyDeferred).toBeGreaterThan(0)
            expect(result.initiallyDeferredIdentities).toHaveLength(result.initiallyDeferred)
            expect(result.deferredAdmissions).toHaveLength(result.initiallyDeferred)
            expect(
                result.deferredAdmissions.every(
                    (item) =>
                        result.admittedIds.includes(item.deliveryId) &&
                        !result.initiallyAdmittedIds.includes(item.deliveryId),
                ),
            ).toBe(true)
            expect(result.admittedIds).toHaveLength(320)
            expect(result.acknowledgedIds).toHaveLength(280)
            expect(result.permanentlyFailedIds).toHaveLength(40)
            expect(result.duplicateReceiverAttempts).toBe(1)
            expect(result.slowReceiverCases).toEqual({ email: 2, webhook: 2 })
            expect(result.transportWallMs.max).toBeGreaterThan(400)
            expect(result.collectionFences).toMatchObject({
                replayRejected: true,
                restorePauseRejected: true,
                admissionWhilePaused: 0,
            })
            expect(result.faults.map((item) => item.fault)).toEqual([
                'crash-before-send',
                'crash-after-receiver-ack',
                'crash-after-durable-ack',
            ])
        } finally {
            if (previous === undefined) delete process.env.PUBLIC_ORIGIN
            else process.env.PUBLIC_ORIGIN = previous
        }
    },
    60000,
)

integrationTest(
    'monitoring faults reject stale results and leave actual durable observations and leases fenced',
    async () => {
        const database = getAuthDatabase()
        const inventory = backgroundPolicies(parseBackgroundOptions(['--seed', '397']))
        expect(
            await database.select({ id: hostCheckPolicies.id }).from(hostCheckPolicies).limit(1),
        ).toHaveLength(0)
        const ids = inventory.map((item) => item.id)
        try {
            await database.insert(proxyHosts).values(
                inventory.map((item) => ({
                    id: item.id,
                    forwardScheme: 'http' as const,
                    forwardHost: 'fixture-upstream',
                    forwardPort: 9000,
                })),
            )
            await database.insert(hostDomains).values(
                inventory.map((item) => ({
                    proxyHostId: item.id,
                    domain: `bg-${item.index}-${item.latency}-fixture.test`,
                })),
            )
            await database.insert(hostCheckPolicies).values(
                inventory.map((item) => ({
                    id: item.id,
                    proxyHostId: item.id,
                    domain: `bg-${item.index}-${item.latency}-fixture.test`,
                    enabled: false,
                    failureThreshold: 1,
                })),
            )
            const result = await backgroundFences({ database, command })
            expect(result.cases).toHaveLength(9)
            expect(
                result.cases.every(
                    (item) =>
                        item.passed &&
                        item.staleWrites === 0 &&
                        item.strandedLeases === 0 &&
                        item.activeIncidents <= 1,
                ),
            ).toBe(true)
        } finally {
            await database.delete(proxyHosts).where(inArray(proxyHosts.id, ids))
        }
    },
    15000,
)
