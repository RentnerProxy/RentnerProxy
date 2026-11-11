// oxlint-disable no-await-in-loop -- Each isolated transaction proves a distinct boundary or rollback case.
import { expect, test } from 'bun:test'
import { eq } from 'drizzle-orm'
import {
    accessPolicies,
    hostCheckPolicies,
    hostDomains,
    proxyHosts,
    redirectHosts,
} from '../../../../db/schema.ts'
import { getAuthDatabase } from '../../../../server/Auth/Core/database.server.ts'
import type { AuthTransaction } from '../../../../server/Auth/Core/Types/database.types.ts'
import { getDatabaseUrl } from '../../../../server/env.server.ts'
import { readProxyRuntimeSnapshot } from '../../../../server/ProxyRuntime/proxy-runtime-data.ts'
import { commandSchema } from '../../../../../../scripts/runtime-reliability/fixture.validation.ts'
import { prepareBackgroundInventory } from '../../../../../../scripts/runtime-scale/background-fixture.ts'
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

async function rolledBack(operation: (transaction: AuthTransaction) => Promise<void>) {
    const rollback = new Error('rollback private boundary fixture')
    try {
        await getAuthDatabase().transaction(async (transaction) => {
            expect(await transaction.select({ id: proxyHosts.id }).from(proxyHosts)).toHaveLength(0)
            expect(
                await transaction.select({ id: redirectHosts.id }).from(redirectHosts),
            ).toHaveLength(0)
            await operation(transaction)
            throw rollback
        })
    } catch (error) {
        if (error !== rollback) throw error
    }
}

async function fixture(
    database: AuthTransaction,
    count: 100 | 500 | 1000,
    domain = 'reliability-boundary397.test',
) {
    const command = commandSchema.parse({
        runId: 'boundary397',
        phase: 'background-prepare',
        policyCount: count,
        upstreamHost: 'fixture-upstream',
        upstreamPort: 9000,
        secondaryPort: 9001,
        seed: 397,
    })
    const state = {
        runId: command.runId,
        domain,
        proxyId: Bun.randomUUIDv7(),
        policyHostId: Bun.randomUUIDv7(),
        policyId: Bun.randomUUIDv7(),
        redirectId: Bun.randomUUIDv7(),
    }
    await database
        .insert(accessPolicies)
        .values({ id: state.policyId, name: 'Reliability ' + command.runId, mode: 'public' })
    await database.insert(proxyHosts).values([
        {
            id: state.proxyId,
            forwardScheme: 'http',
            forwardHost: command.upstreamHost,
            forwardPort: command.upstreamPort,
        },
        {
            id: state.policyHostId,
            forwardScheme: 'http',
            forwardHost: command.upstreamHost,
            forwardPort: command.upstreamPort,
            accessPolicyId: state.policyId,
        },
    ])
    await database.insert(redirectHosts).values({
        id: state.redirectId,
        destination: 'http://' + domain + '/initial',
        statusCode: 302,
        preserveRequestUri: true,
    })
    await database.insert(hostDomains).values([
        { proxyHostId: state.proxyId, domain },
        { proxyHostId: state.policyHostId, domain: 'policy-' + domain },
        { redirectHostId: state.redirectId, domain: 'redirect-' + domain },
    ])
    const saved: (typeof state)[] = []
    return {
        database,
        command,
        domain,
        state,
        saved,
        save: async () => {
            saved.push(structuredClone(state))
        },
    }
}

integrationTest(
    '100/500 preserve their original routes; 1000 policies share the exact runtime routing budget and persist usable traffic owners',
    async () => {
        for (const count of [100, 500, 1000] as const)
            await rolledBack(async (transaction) => {
                const context = await fixture(transaction, count)
                const original = { ...context.state }
                const inventory = backgroundPolicies(
                    parseBackgroundOptions(['--policies', String(count), '--seed', '397']),
                )
                await prepareBackgroundInventory(context)
                const snapshot = await readProxyRuntimeSnapshot(transaction)
                const policies = await transaction.select().from(hostCheckPolicies)
                expect(policies).toHaveLength(count)
                expect(policies.map((policy) => policy.id).toSorted()).toEqual(
                    inventory.map((item) => item.id).toSorted(),
                )
                expect(
                    policies.every(
                        (policy) =>
                            policy.id === policy.proxyHostId &&
                            policy.intervalSeconds === 60 &&
                            !policy.enabled,
                    ),
                ).toBe(true)
                expect(snapshot.proxyHosts).toHaveLength(count === 1000 ? 1000 : count + 2)
                expect(snapshot.redirectHosts).toHaveLength(count === 1000 ? 0 : 1)
                const traffic = snapshot.proxyHosts.find((host) =>
                    host.domains.includes(context.domain),
                )!
                const control = snapshot.proxyHosts.find((host) =>
                    host.domains.includes('policy-' + context.domain),
                )!
                expect(traffic.id).toBe(context.state.proxyId)
                expect(control.id).toBe(context.state.policyHostId)
                expect(control.accessPolicy).toMatchObject({
                    id: original.policyId,
                    mode: 'public',
                })
                const first = snapshot.proxyHosts.find((host) => host.id === inventory[0]!.id)!
                expect(first.domains).toEqual([`bg-0-${inventory[0]!.latency}-${context.domain}`])
                expect(first.accessPolicy).toBeUndefined()
                if (count === 1000) {
                    expect(traffic.id).toBe(inventory.at(-2)!.id)
                    expect(control.id).toBe(inventory.at(-1)!.id)
                    expect(context.saved).toEqual([context.state])
                    expect(
                        await transaction
                            .select()
                            .from(proxyHosts)
                            .where(eq(proxyHosts.id, original.proxyId)),
                    ).toHaveLength(0)
                    // Later fixture mutations use the persisted owner of the original traffic domain.
                    const restoredState = JSON.parse(
                        JSON.stringify(context.saved[0]),
                    ) as typeof original
                    const updated = await transaction
                        .update(proxyHosts)
                        .set({ forwardPort: context.command.secondaryPort })
                        .where(eq(proxyHosts.id, restoredState.proxyId))
                        .returning({ id: proxyHosts.id })
                    expect(updated).toEqual([{ id: traffic.id }])
                    expect(
                        (await readProxyRuntimeSnapshot(transaction)).proxyHosts.find((host) =>
                            host.domains.includes(context.domain),
                        )?.forwardPort,
                    ).toBe(9001)
                    await transaction
                        .update(redirectHosts)
                        .set({ enabled: true })
                        .where(eq(redirectHosts.id, original.redirectId))
                    await expect(readProxyRuntimeSnapshot(transaction)).rejects.toThrow(
                        'host limit exceeded',
                    )
                } else {
                    expect(context.state).toEqual(original)
                    expect(context.saved).toHaveLength(0)
                }
            })
    },
    15000,
)

integrationTest(
    '1000-policy replacement rejects unexpected fixture ownership before changing routes or state',
    async () => {
        for (const change of [
            'foreign-host',
            'changed-stub',
            'changed-alias',
            'nonpublic-policy',
            'wrong-state',
            'wrong-run',
        ] as const)
            await rolledBack(async (transaction) => {
                const context = await fixture(transaction, 1000)
                if (change === 'foreign-host')
                    await transaction.insert(proxyHosts).values({
                        forwardScheme: 'http',
                        forwardHost: 'unowned.invalid',
                        forwardPort: 9000,
                    })
                if (change === 'changed-stub')
                    await transaction
                        .update(proxyHosts)
                        .set({ forwardPort: 9001 })
                        .where(eq(proxyHosts.id, context.state.proxyId))
                if (change === 'changed-alias')
                    await transaction
                        .update(hostDomains)
                        .set({ domain: 'unowned.invalid' })
                        .where(eq(hostDomains.proxyHostId, context.state.proxyId))
                if (change === 'nonpublic-policy')
                    await transaction
                        .update(accessPolicies)
                        .set({ mode: 'ip-restricted' })
                        .where(eq(accessPolicies.id, context.state.policyId))
                if (change === 'wrong-state') context.state.proxyId = Bun.randomUUIDv7()
                if (change === 'wrong-run') context.state.runId = 'another-private-fixture'
                const state = { ...context.state }
                const hosts = await transaction.select().from(proxyHosts)
                const domains = await transaction.select().from(hostDomains)
                await expect(prepareBackgroundInventory(context)).rejects.toThrow()
                expect(await transaction.select().from(proxyHosts)).toEqual(hosts)
                expect(await transaction.select().from(hostDomains)).toEqual(domains)
                expect((await transaction.select().from(redirectHosts))[0]?.enabled).toBe(true)
                expect(await transaction.select().from(hostCheckPolicies)).toHaveLength(0)
                expect(context.state).toEqual(state)
                expect(context.saved).toHaveLength(0)
            })
    },
    15000,
)

integrationTest(
    'a late inventory insert failure rolls back deleted original routes and never publishes rebound state',
    async () => {
        await rolledBack(async (transaction) => {
            // All original aliases fit varchar(253), but generated bg-* names exceed it.
            const domain = [40, 63, 63, 63, 11].map((length) => 'a'.repeat(length)).join('.')
            const context = await fixture(transaction, 1000, domain)
            const original = { ...context.state }
            const hosts = await transaction.select().from(proxyHosts)
            const domains = await transaction.select().from(hostDomains)
            await expect(prepareBackgroundInventory(context)).rejects.toThrow()
            expect(await transaction.select().from(proxyHosts)).toEqual(hosts)
            expect(await transaction.select().from(hostDomains)).toEqual(domains)
            expect((await transaction.select().from(redirectHosts))[0]?.enabled).toBe(true)
            expect(await transaction.select().from(hostCheckPolicies)).toHaveLength(0)
            expect(context.state).toEqual(original)
            expect(context.saved).toHaveLength(0)
        })
    },
    15000,
)
