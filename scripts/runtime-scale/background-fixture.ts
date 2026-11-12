// oxlint-disable no-await-in-loop -- Faults mutate and then verify the exact durable claim before the next case.
import assert from 'node:assert/strict'
import { readFile, writeFile } from 'node:fs/promises'
import { randomUUID, X509Certificate, createHash } from 'node:crypto'
import { and, eq, inArray, sql } from 'drizzle-orm'
import {
    hostCheckPolicies,
    hostCheckObservations,
    hostCheckIncidents,
    hostDomains,
    proxyHosts,
    trustedCas,
    accessPolicies,
    redirectHosts,
} from '../../web/src/db/schema.ts'
import type { AuthTransaction } from '../../web/src/server/Auth/Core/Types/database.types.ts'
import { readProxyRuntimeSnapshot } from '../../web/src/server/ProxyRuntime/proxy-runtime-data.ts'
import * as repository from '../../web/src/server/HostMonitoring/host-monitoring.repository.ts'
import { runFixture } from '../runtime-reliability/fixture-core.ts'
import { executeFixture } from '../runtime-reliability/fixture.ts'
import type { FixtureContext } from '../runtime-reliability/Types/fixture.types.ts'
import { backgroundPolicies } from './background.ts'
import { withInFlightBackgroundHead } from './background-network.ts'
import { backgroundSnapshotSchema } from './background.validation.ts'
import { backgroundRecoveryFixture } from './background-recovery.ts'
import { backgroundContentionFixture } from './background-contention-fixture.ts'
import { runBackgroundDiscovery } from './background-discovery.ts'
import { runBackgroundNotifications } from './background-notifications.ts'
import { readBackgroundCapture } from './background-capture.ts'
import type { BackgroundOptions } from './Types/background.types.ts'
import {
    TLS_TEST_CA,
    TLS_TEST_CERTIFICATE,
} from '../../web/src/tests/server/Notifications/Types/tls-test.types.ts'

/** Project validated fixture command fields into the fixed background workload without reading process-global configuration. */
function options(context: Pick<FixtureContext, 'command'>): BackgroundOptions {
    return {
        policies: context.command.policyCount,
        seed: context.command.seed,
        profile: context.command.backgroundProfile,
        pattern: context.command.duePattern,
        durationSeconds: 120,
    }
}

/** Measure an authorized management read and return validated durable policy/observation evidence for the seeded fixture IDs. */
async function snapshot(context: FixtureContext) {
    const started = performance.now()
    await context.authorized(context.hosts.getProxyHostsService)
    const managementReadMs = performance.now() - started
    const inventory = backgroundPolicies(options(context))
    const ids = inventory.map((item) => item.id)
    const [policies, observations] = await Promise.all([
        context.database
            .select()
            .from(hostCheckPolicies)
            .where(inArray(hostCheckPolicies.proxyHostId, ids)),
        context.database
            .select()
            .from(hostCheckObservations)
            .innerJoin(hostCheckPolicies, eq(hostCheckPolicies.id, hostCheckObservations.policyId))
            .where(inArray(hostCheckPolicies.proxyHostId, ids))
            .orderBy(hostCheckObservations.checkedAt),
    ])
    const origin = Number(await readFile('/tmp/background-origin-' + context.command.runId, 'utf8'))
    return backgroundSnapshotSchema.parse({
        origin,
        managementReadMs,
        schedulerCapture: await readBackgroundCapture(context.command.runId),
        policies: inventory.flatMap((item) => {
            const policy = policies.find((candidate) => candidate.proxyHostId === item.id)
            return policy
                ? [
                      {
                          id: policy.id,
                          index: item.index,
                          dueAt: origin + item.offsetMs,
                          version: policy.version,
                          enabled: policy.enabled,
                          leased: policy.leaseToken !== null,
                      },
                  ]
                : []
        }),
        observations: observations.map((item) => ({
            policyId: item.host_check_observations.policyId,
            policyVersion: item.host_check_observations.policyVersion,
            checkedAt: item.host_check_observations.checkedAt.getTime(),
            status: item.host_check_observations.status,
            code: item.host_check_observations.code,
        })),
    })
}

/** Mutate one owned in-flight claim across stale-context faults and verify durable write fences, restoring temporary host configuration afterward. */
export async function backgroundFences(context: Pick<FixtureContext, 'database' | 'command'>) {
    const inventory = backgroundPolicies(options(context))
    const hostId = inventory[0]!.id
    const database = context.database
    const ids = inventory.map((item) => item.id)
    await database
        .update(hostCheckPolicies)
        .set({ enabled: false, leaseToken: null, leaseExpiresAt: null })
        .where(inArray(hostCheckPolicies.proxyHostId, ids))
    const [original] = await database.select().from(proxyHosts).where(eq(proxyHosts.id, hostId))
    assert.ok(original)
    const caId = Bun.randomUUIDv7(),
        accessId = Bun.randomUUIDv7()
    const ca = new X509Certificate(TLS_TEST_CA)
    try {
        await database.insert(trustedCas).values({
            id: caId,
            name: 'Background fixture CA',
            pem: TLS_TEST_CA,
            fingerprintSha256: 'sha256:' + createHash('sha256').update(TLS_TEST_CA).digest('hex'),
            subject: ca.subject,
            issuer: ca.issuer,
            notBefore: new Date(ca.validFrom),
            notAfter: new Date(ca.validTo),
        })
        await database
            .insert(accessPolicies)
            .values({ id: accessId, name: 'Background fixture policy', mode: 'public' })
        const cases = []
        for (const fault of [
            'policy-version',
            'lease-expiry',
            'host-change',
            'host-disabled',
            'access-policy-change',
            'trusted-ca-change',
            'database-lock',
            'cancelled',
            'duplicate-completion',
        ] as const) {
            await database
                .update(proxyHosts)
                .set({
                    enabled: true,
                    forwardPort: original.forwardPort,
                    forwardScheme: fault === 'trusted-ca-change' ? 'https' : 'http',
                    trustedCaId: fault === 'trusted-ca-change' ? caId : null,
                    accessPolicyId: fault === 'access-policy-change' ? accessId : null,
                })
                .where(eq(proxyHosts.id, hostId))
            await database
                .update(hostCheckPolicies)
                .set({
                    enabled: true,
                    version: randomUUID(),
                    nextCheckAt: new Date(0),
                    leaseToken: null,
                    leaseExpiresAt: null,
                })
                .where(eq(hostCheckPolicies.proxyHostId, hostId))
            const claim = await repository.claimDueHostMonitoringPolicy()
            assert.ok(claim)
            assert.equal(claim.policy.proxyHostId, hostId)
            const [before] = await repository.readHostMonitoringClaimContexts([claim])
            assert.ok(before)
            const count = async () =>
                (
                    await database
                        .select({ id: hostCheckObservations.id })
                        .from(hostCheckObservations)
                        .where(eq(hostCheckObservations.policyId, claim.policy.id))
                ).length
            const prior = await count()
            const network = await withInFlightBackgroundHead(async () => {
                if (fault === 'policy-version')
                    await database
                        .update(hostCheckPolicies)
                        .set({ version: randomUUID(), leaseToken: null, leaseExpiresAt: null })
                        .where(eq(hostCheckPolicies.id, claim.policy.id))
                if (fault === 'lease-expiry')
                    await database
                        .update(hostCheckPolicies)
                        .set({ leaseExpiresAt: new Date(0) })
                        .where(eq(hostCheckPolicies.id, claim.policy.id))
                if (fault === 'host-change')
                    await database
                        .update(proxyHosts)
                        .set({ forwardPort: original.forwardPort + 1 })
                        .where(eq(proxyHosts.id, hostId))
                if (fault === 'host-disabled')
                    await database
                        .update(proxyHosts)
                        .set({ enabled: false })
                        .where(eq(proxyHosts.id, hostId))
                if (fault === 'access-policy-change')
                    await database
                        .update(accessPolicies)
                        .set({
                            mode: 'ip-restricted',
                            ipRules: { defaultAction: 'deny', allow: [], deny: [] },
                        })
                        .where(eq(accessPolicies.id, accessId))
                if (fault === 'trusted-ca-change')
                    await database
                        .update(trustedCas)
                        .set({ pem: TLS_TEST_CERTIFICATE })
                        .where(eq(trustedCas.id, caId))
            })
            const completion = {
                claim,
                outcome: {
                    status: 'unhealthy' as const,
                    code: 'unexpected_status' as const,
                    durationMs: network.durationMs,
                    httpStatus: network.status,
                },
                before,
                signal: fault === 'cancelled' ? AbortSignal.abort() : new AbortController().signal,
            }
            if (fault === 'database-lock') {
                const locked = Promise.withResolvers<void>()
                const holder = database.transaction(async (transaction) => {
                    await transaction
                        .select()
                        .from(hostCheckPolicies)
                        .where(eq(hostCheckPolicies.id, claim.policy.id))
                        .for('update')
                    locked.resolve()
                    await Bun.sleep(2500)
                })
                await Promise.race([locked.promise, holder])
                const began = performance.now()
                try {
                    await assert.rejects(repository.completeHostMonitoringClaims([completion]))
                    assert.ok(
                        performance.now() - began < 4000,
                        'Lock contention must be bounded by the worker transaction timeout',
                    )
                } finally {
                    await holder
                }
            } else {
                const [result] = await repository.completeHostMonitoringClaims([completion])
                assert.equal(result?.status, 'fulfilled')
                assert.equal(
                    result?.status === 'fulfilled' && result.value,
                    fault === 'duplicate-completion',
                )
            }
            if (fault === 'duplicate-completion') {
                const [retry] = await repository.completeHostMonitoringClaims([completion])
                assert.ok(retry?.status === 'fulfilled' && !retry.value)
            }
            assert.equal(
                await count(),
                prior + (fault === 'duplicate-completion' ? 1 : 0),
                'Only a fresh, once-only completion changes durable observations',
            )
            await repository.releaseHostMonitoringClaim(claim)
            await database
                .update(hostCheckPolicies)
                .set({ enabled: false, leaseToken: null, leaseExpiresAt: null })
                .where(eq(hostCheckPolicies.id, claim.policy.id))
            const [final] = await database
                .select()
                .from(hostCheckPolicies)
                .where(eq(hostCheckPolicies.id, claim.policy.id))
            assert.ok(final && !final.leaseToken && !final.leaseExpiresAt && !final.enabled)
            const incidents = await database
                .select({ id: hostCheckIncidents.id })
                .from(hostCheckIncidents)
                .where(
                    and(
                        eq(hostCheckIncidents.policyId, claim.policy.id),
                        sql`${hostCheckIncidents.closedReason} is null`,
                    ),
                )
            assert.ok(incidents.length <= 1, 'No duplicate active incidents')
            cases.push({
                fault,
                passed: true,
                networkModel: 'controlled in-flight HEAD, real durable context/lease fences',
                staleWrites: 0,
                strandedLeases: 0,
                observationsAdded: fault === 'duplicate-completion' ? 1 : 0,
                activeIncidents: incidents.length,
            })
        }
        return {
            cases,
            extendedRecoveryRequired: ['active-certificate-restore', 'audit-cursor-replay'],
        }
    } finally {
        await database
            .update(proxyHosts)
            .set({
                enabled: original.enabled,
                forwardPort: original.forwardPort,
                forwardScheme: original.forwardScheme,
                trustedCaId: original.trustedCaId,
                accessPolicyId: original.accessPolicyId,
            })
            .where(eq(proxyHosts.id, hostId))
        await database.delete(trustedCas).where(eq(trustedCas.id, caId))
        await database.delete(accessPolicies).where(eq(accessPolicies.id, accessId))
    }
}

/** Fixture-only inventory setup; the explicit 1000-policy case also owns the traffic routes. */
export async function prepareBackgroundInventory(
    context: Pick<FixtureContext, 'command' | 'domain' | 'state' | 'save'> & {
        database: {
            transaction<T>(operation: (transaction: AuthTransaction) => Promise<T>): Promise<T>
        }
    },
) {
    const inventory = backgroundPolicies(options(context))
    const bindings = await context.database.transaction(async (transaction) => {
        assert.equal(
            (
                await transaction
                    .select({ id: hostCheckPolicies.id })
                    .from(hostCheckPolicies)
                    .limit(1)
            ).length,
            0,
            'No preexisting monitoring policies',
        )
        let rebound: { proxyId: string; policyHostId: string } | null = null
        if (inventory.length === 1000) {
            const { state, domain, command } = context
            assert.equal(
                state.runId,
                command.runId,
                'Boundary fixture must match persisted run ownership',
            )
            assert.equal(
                state.domain,
                domain,
                'Boundary fixture domain must match persisted ownership',
            )
            assert.ok(
                state.proxyId && state.policyHostId && state.policyId && state.redirectId,
                'Boundary fixture must be prepared',
            )
            assert.notEqual(
                state.proxyId,
                state.policyHostId,
                'Boundary traffic owners must be distinct',
            )
            const hosts = await transaction.select().from(proxyHosts).for('update')
            const redirects = await transaction.select().from(redirectHosts).for('update')
            const domains = await transaction.select().from(hostDomains).for('update')
            const [policy] = await transaction
                .select()
                .from(accessPolicies)
                .where(eq(accessPolicies.id, state.policyId))
                .for('update')
            assert.equal(
                hosts.length,
                2,
                'Only the two original fixture proxy hosts may be replaced',
            )
            assert.equal(redirects.length, 1, 'Only the original fixture redirect may be disabled')
            assert.equal(domains.length, 3, 'Only the three original fixture domains may exist')
            assert.ok(
                policy &&
                    policy.mode === 'public' &&
                    policy.name === 'Reliability ' + command.runId,
                'Boundary control route retains its original public policy',
            )
            for (const [hostId, alias, accessPolicyId] of [
                [state.proxyId, domain, null],
                [state.policyHostId, 'policy-' + domain, state.policyId],
            ] as const) {
                const host = hosts.find((item) => item.id === hostId)
                assert.ok(
                    host &&
                        host.enabled &&
                        host.forwardScheme === 'http' &&
                        host.forwardHost === command.upstreamHost &&
                        host.forwardPort === command.upstreamPort &&
                        host.certificateId === null &&
                        !host.forceHttps &&
                        host.upstreamPool === null &&
                        host.trustedCaId === null &&
                        host.upstreamTlsServerName === null &&
                        host.accessPolicyId === accessPolicyId,
                    'Only an unchanged owned fixture stub may be replaced',
                )
                assert.ok(
                    domains.some((item) => item.proxyHostId === hostId && item.domain === alias),
                    'Boundary traffic alias ownership must match',
                )
                assert.ok(
                    !inventory.some((item) => item.id === hostId),
                    'Boundary inventory identities must be fresh',
                )
            }
            const redirect = redirects[0]!
            assert.ok(
                redirect.id === state.redirectId &&
                    redirect.enabled &&
                    redirect.destination === 'http://' + domain + '/initial' &&
                    redirect.statusCode === 302 &&
                    redirect.preserveRequestUri &&
                    redirect.certificateId === null &&
                    domains.some(
                        (item) =>
                            item.redirectHostId === redirect.id &&
                            item.domain === 'redirect-' + domain,
                    ),
                'Only the unused original fixture redirect may be disabled',
            )
            await transaction
                .delete(proxyHosts)
                .where(inArray(proxyHosts.id, [state.proxyId, state.policyHostId]))
            await transaction
                .update(redirectHosts)
                .set({ enabled: false })
                .where(eq(redirectHosts.id, state.redirectId))
            rebound = { proxyId: inventory.at(-2)!.id, policyHostId: inventory.at(-1)!.id }
        }
        await transaction.insert(proxyHosts).values(
            inventory.map((item) => ({
                id: item.id,
                forwardScheme: 'http' as const,
                forwardHost: context.command.upstreamHost,
                forwardPort: context.command.upstreamPort,
                ...(item.id === rebound?.policyHostId
                    ? { accessPolicyId: context.state.policyId }
                    : {}),
            })),
        )
        await transaction.insert(hostDomains).values(
            inventory.map((item) => ({
                proxyHostId: item.id,
                domain: `bg-${item.index}-${item.latency}-${context.domain}`,
            })),
        )
        await transaction.insert(hostCheckPolicies).values(
            inventory.map((item) => ({
                proxyHostId: item.id,
                id: item.id,
                domain: `bg-${item.index}-${item.latency}-${context.domain}`,
                enabled: false,
                intervalSeconds: 60,
                timeoutMs: 3000,
                failureThreshold: 1,
            })),
        )
        if (rebound) {
            await transaction.insert(hostDomains).values([
                { proxyHostId: rebound.proxyId, domain: context.domain },
                { proxyHostId: rebound.policyHostId, domain: 'policy-' + context.domain },
            ])
            const runtime = await readProxyRuntimeSnapshot(transaction)
            assert.equal(
                runtime.proxyHosts.length,
                1000,
                'Boundary inventory must fit the actual runtime host limit',
            )
            assert.equal(
                runtime.redirectHosts.length,
                0,
                'Boundary redirect must remain outside the routing budget',
            )
        }
        return rebound
    })
    if (bindings) {
        Object.assign(context.state, bindings)
        await context.save()
    }
}

/** Dispatch validated background fixture phases; use the existing fixture executor for phases outside this workload. */
async function execute(context: FixtureContext): Promise<Record<string, unknown>> {
    if (!context.command.phase.startsWith('background-')) return executeFixture(context)
    if (context.command.phase.startsWith('background-contention-')) {
        const detail = await backgroundContentionFixture(context)
        return {
            contention: detail,
            desiredRevision: (await context.runtime.getProxyRuntimeSnapshotService()).revision,
        }
    }
    const inventory = backgroundPolicies(options(context))
    if (context.command.phase === 'background-prepare') {
        await prepareBackgroundInventory(context)
        await writeFile('/tmp/background-origin-' + context.command.runId, '0', { mode: 0o600 })
    }
    if (context.command.phase === 'background-start') {
        const origin = Date.now() + 1000
        await writeFile('/tmp/background-origin-' + context.command.runId, String(origin), {
            mode: 0o600,
        })
        await context.database.transaction(async (transaction) => {
            for (const item of inventory)
                await transaction
                    .update(hostCheckPolicies)
                    .set({ enabled: true, nextCheckAt: new Date(origin + item.offsetMs) })
                    .where(eq(hostCheckPolicies.proxyHostId, item.id))
        })
    }
    if (context.command.phase === 'background-quiesce')
        await context.database
            .update(hostCheckPolicies)
            .set({ enabled: false, leaseToken: null, leaseExpiresAt: null })
            .where(
                inArray(
                    hostCheckPolicies.proxyHostId,
                    inventory.map((item) => item.id),
                ),
            )
    const detail = [
        'background-job-prepare',
        'background-job-resume',
        'background-audit-replay',
    ].includes(context.command.phase)
        ? { recovery: await backgroundRecoveryFixture(context) }
        : context.command.phase === 'background-fences'
          ? { fences: await backgroundFences(context) }
          : context.command.phase === 'background-discovery'
            ? { discovery: await runBackgroundDiscovery(context.command.seed) }
            : context.command.phase === 'background-notifications'
              ? { notifications: await runBackgroundNotifications(context) }
              : { background: await snapshot(context) }
    const desired = await context.runtime.getProxyRuntimeSnapshotService()
    return {
        ...detail,
        ...(context.command.contention && context.command.phase === 'background-read'
            ? {
                  contention: await backgroundContentionFixture({
                      ...context,
                      command: { ...context.command, phase: 'background-contention-read' },
                  }),
              }
            : {}),
        desiredRevision: desired.revision,
    }
}

if (import.meta.main) await runFixture(execute)
