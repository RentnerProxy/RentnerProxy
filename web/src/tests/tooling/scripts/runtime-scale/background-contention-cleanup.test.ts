// oxlint-disable no-await-in-loop -- Each failure-identity case owns its own workload and cleanup outcome.
import { expect, test } from 'bun:test'
import { withContentionCleanup } from '../../../../../../scripts/runtime-scale/background-contention-run.ts'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test.each(['entry', 'receiver', 'leases', 'sample'])(
    'cancellation during %s preserves its category and drains seeded state',
    async (phase) => {
        const output = await runBunScript(
            `
            import { mock } from 'bun:test'
            mock.module('./scripts/runtime-reliability/run.ts', () => ({ recoveryDependencies: () => ({}) }))
            mock.module('./scripts/runtime-reliability/component-recovery.ts', () => ({ recoverReliabilityComponent: async () => {} }))
            const { runContentionWindow } = await import('./scripts/runtime-scale/background-contention-run.ts')
            const cancellation = new AbortController(), phases = []
            let dockerCalls = 0, lifetime
            const receiver = { notifications: { accepted: 0, emailAccepted: 0, webhookAccepted: 0, attempts: 0, activeWebhooks: 0, smtpConnections: 0 }, discovery: { streams: 0, emitted: 0, lists: 0, inspections: 0, inventoryCount: 8 } }
            const state = { notification: { pending: 0, inflight: 0, succeeded: 0, failed: 0, cancelled: 0, deferred: 0 }, discovery: { revision: 9, error: null, detached: 8 }, postgres: { connections: 1, active: 1, lockWaiters: 0, idleTransactions: 0, monitoringLeases: 1 } }
            const context = {
                container: 'owned-fixture',
                docker: async args => {
                    dockerCalls++
                    if (args.includes('--detach')) { lifetime = Number(args.at(-1)); return '' }
                    if ('${phase}' === 'receiver') { cancellation.abort(); return 'invalid after cancellation' }
                    return JSON.stringify(receiver)
                },
                waitFor: async predicate => { await predicate() },
                fixture: async current => {
                    phases.push(current)
                    if (current === 'background-contention-read') {
                        if ('${phase}' === 'leases') cancellation.abort()
                        return { contention: '${phase}' === 'leases' ? null : state }
                    }
                    if (current === 'background-read') { cancellation.abort(); return { background: null } }
                    if (current === 'background-start') return { background: { origin: 0, policies: [], observations: [], managementReadMs: 0 }, desiredRevision: 'fixture' }
                    if (current === 'background-contention-arm') return { contention: { discoveryRevision: 9 } }
                    return {}
                },
                controller: async () => ({ status: 200, body: { workers: [{ id: 'caddy_recovery', attempts: 1, successes: 1 }] } }),
            }
            if ('${phase}' === 'entry') cancellation.abort()
            const result = await runContentionWindow({ context }, { seed: 1, policies: 100, pattern: 'simultaneous', profile: 'mixed', durationSeconds: 300 }, async () => { throw new Error('unexpected sample') }, () => {}, cancellation.signal).then(
                () => ({ category: 'unexpected success' }), error => ({ category: error.category, message: error.message }))
            console.log(JSON.stringify({ ...result, dockerCalls, lifetime, phases }))
        `,
            { cwd: new URL('../../../../../..', import.meta.url) },
        )
        const result = JSON.parse(output.trim().split('\n').at(-1)!)
        expect(result.category).toBe('cancelled')
        expect(result.message).toBe('background scenario cancelled')
        if (phase === 'entry') expect(result.dockerCalls).toBe(0)
        else expect(result.lifetime).toBe(1_200_000)
        if (phase === 'leases' || phase === 'sample')
            expect(result.phases.at(-1)).toBe('background-contention-stop')
        else expect(result.phases).toEqual([])
    },
)

test('cleanup cannot replace the original workload failure, including falsy thrown values', async () => {
    for (const primary of [
        new Error('synthetic workload failure'),
        undefined,
        null,
        false,
        0,
        '',
    ]) {
        let stringified = false
        const stopFailure = {
            toString() {
                stringified = true
                return 'untrusted stop details'
            },
        }
        const categories: string[] = []
        let stops = 0
        const outcome = await withContentionCleanup(
            async () => {
                throw primary
            },
            async () => {
                stops++
                throw stopFailure
            },
            (category) => {
                categories.push(category)
            },
        ).then(
            (value) => ({ status: 'fulfilled' as const, value }),
            (reason: unknown) => ({ status: 'rejected' as const, reason }),
        )
        expect(outcome.status).toBe('rejected')
        if (outcome.status === 'rejected') expect(outcome.reason).toBe(primary)
        expect(stops).toBe(1)
        expect(categories).toEqual(['contention-stop-failed'])
        expect(JSON.stringify(categories)).not.toContain('untrusted stop details')
        expect(stringified).toBe(false)
    }
})

test('a cleanup-only failure still rejects with its original identity, including null/undefined', async () => {
    for (const stopFailure of [new Error('synthetic stop failure'), null, undefined]) {
        const categories: string[] = []
        const outcome = await withContentionCleanup(
            async () => 'completed',
            async () => {
                throw stopFailure
            },
            (category) => {
                categories.push(category)
            },
        ).then(
            (value) => ({ status: 'fulfilled' as const, value }),
            (reason: unknown) => ({ status: 'rejected' as const, reason }),
        )
        expect(outcome.status).toBe('rejected')
        if (outcome.status === 'rejected') expect(outcome.reason).toBe(stopFailure)
        expect(categories).toEqual(['contention-stop-failed'])
    }
})

test('successful cleanup preserves workload results and a diagnostic failure cannot mask failures', async () => {
    const order: string[] = []
    const value = {}
    expect(
        await withContentionCleanup(
            async () => {
                order.push('workload')
                return value
            },
            async () => {
                order.push('stop')
            },
            () => {
                throw new Error('unexpected diagnostic')
            },
        ),
    ).toBe(value)
    expect(order).toEqual(['workload', 'stop'])
    const primary = new Error('synthetic workload failure')
    const stopFailure = new Error('synthetic stop failure')
    const outcome = await withContentionCleanup(
        async () => {
            throw primary
        },
        async () => {
            throw stopFailure
        },
        () => {
            throw new Error('synthetic diagnostic failure')
        },
    ).then(
        () => undefined,
        (reason: unknown) => reason,
    )
    expect(outcome).toBe(primary)
})
