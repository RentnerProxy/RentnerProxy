import { expect, mock, test } from 'bun:test'
import type { AuditEventInput } from '@/lib/Admin/AuditLogs/Types/audit-events.types.ts'
import {
    affectsDockerDiscovery,
    subscribeToDiscoveryChanges,
} from '@/server/WebSockets/discoveryChanges.ts'
import {
    markAuditedApplicationMutation,
    markDiscoveryReconciliation,
    withCommittedApplicationChanges,
} from '@/server/WebSockets/committedChanges.ts'
import { getApplicationRevision } from '@/server/WebSockets/applicationChanges.ts'

const event: AuditEventInput = {
    actorUserId: '0198d98a-0000-7000-8000-000000000001',
    actorKind: 'user',
    action: 'update',
    resource: 'proxy-host',
    targetId: null,
    result: 'success',
    metadata: { changedFields: ['upstream'] },
}

test('only relevant committed inventory, ownership and authorization changes wake discovery', () => {
    expect(affectsDockerDiscovery(event)).toBe(true)
    expect(affectsDockerDiscovery({ ...event, result: 'denied' })).toBe(false)
    expect(affectsDockerDiscovery({ ...event, actorKind: 'anonymous', actorUserId: null })).toBe(
        false,
    )
    expect(affectsDockerDiscovery({ ...event, metadata: { changedFields: [] } })).toBe(false)
    expect(
        affectsDockerDiscovery({
            ...event,
            resource: 'user',
            metadata: { changedFields: ['displayName'] },
        }),
    ).toBe(false)
    expect(
        affectsDockerDiscovery({
            ...event,
            resource: 'user',
            metadata: { changedFields: ['roles'] },
        }),
    ).toBe(true)
    expect(affectsDockerDiscovery({ ...event, resource: 'role' })).toBe(true)
    for (const action of ['disable', 'enable', 'delete'] as const)
        expect(
            affectsDockerDiscovery({
                actorUserId: event.actorUserId,
                actorKind: 'user',
                resource: 'user',
                targetId: null,
                result: 'success',
                action,
            }),
        ).toBe(true)
    expect(affectsDockerDiscovery({ ...event, resource: 'session', action: 'login' })).toBe(false)
    expect(
        affectsDockerDiscovery({
            ...event,
            resource: 'security-settings',
            metadata: { changedFields: ['ddns'] },
        }),
    ).toBe(false)
    expect(
        affectsDockerDiscovery({
            ...event,
            resource: 'security-settings',
            result: 'failure',
            metadata: { changedFields: ['dockerDiscovery'] },
        }),
    ).toBe(true)
})

test('nested transactions publish one hint only after the outer commit, never on rollback', async () => {
    const notified = mock(() => {})
    const unsubscribe = subscribeToDiscoveryChanges(notified)
    const outer = {}
    try {
        await withCommittedApplicationChanges(
            async (run) => {
                const result = await run(outer)
                expect(notified).not.toHaveBeenCalled()
                return result
            },
            async (transaction) => {
                await withCommittedApplicationChanges(
                    (run) => run({}),
                    async (nested) => {
                        markAuditedApplicationMutation(nested, event)
                    },
                    transaction,
                )
                markAuditedApplicationMutation(transaction, event)
            },
        )
        expect(notified).toHaveBeenCalledTimes(1)
        await expect(
            withCommittedApplicationChanges(
                async (run) => {
                    await run({})
                    throw new Error('rollback')
                },
                async (transaction) => {
                    markAuditedApplicationMutation(transaction, event)
                },
            ),
        ).rejects.toThrow('rollback')
        expect(notified).toHaveBeenCalledTimes(1)
        await withCommittedApplicationChanges(
            (run) => run({}),
            async () => {},
        )
        expect(notified).toHaveBeenCalledTimes(1)
    } finally {
        unsubscribe()
    }
})

test('listener failures cannot turn an already committed mutation into an apparent failure', async () => {
    const stopBroken = subscribeToDiscoveryChanges(() => {
        throw new Error('listener failure')
    })
    const healthy = mock(() => {})
    const stopHealthy = subscribeToDiscoveryChanges(healthy)
    try {
        await withCommittedApplicationChanges(
            (run) => run({}),
            async (transaction) => {
                markAuditedApplicationMutation(transaction, event)
            },
        )
        expect(healthy).toHaveBeenCalledTimes(1)
    } finally {
        stopBroken()
        stopHealthy()
    }
})

test('discovery-owned commits preserve application publication without feeding their own invalidation loop', async () => {
    const notified = mock(() => {})
    const unsubscribe = subscribeToDiscoveryChanges(notified)
    try {
        const revision = getApplicationRevision()
        await withCommittedApplicationChanges(
            (run) => run({}),
            async (transaction) => {
                markDiscoveryReconciliation(transaction)
                markAuditedApplicationMutation(transaction, event)
            },
        )
        expect(notified).not.toHaveBeenCalled()
        expect(getApplicationRevision()).not.toBe(revision)
        await withCommittedApplicationChanges(
            (run) => run({}),
            async (transaction) => {
                markAuditedApplicationMutation(transaction, event)
            },
        )
        expect(notified).toHaveBeenCalledTimes(1)
    } finally {
        unsubscribe()
    }
})
