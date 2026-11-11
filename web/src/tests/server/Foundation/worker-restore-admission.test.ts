// oxlint-disable no-await-in-loop -- Observe the serialized status polls before changing the root fixture.
import { afterEach, expect, mock, test } from 'bun:test'
import type { ManagedBackupMaintenance } from '@/lib/Backups/Types/backups.types.ts'

const original: ManagedBackupMaintenance = {
    id: '01900000-0000-7000-8000-000000000001',
    requestId: '01900000-0000-7000-8000-000000000002',
    actorId: '01900000-0000-7000-8000-000000000003',
    actorName: 'Restore operator',
    artifactId: '01900000-0000-7000-8000-000000000004',
    metadataSha256: 'a'.repeat(64),
    operation: 'restore',
    state: 'running',
    phase: 'preparing',
    progressPercent: 0,
    requestedAt: 100,
    startedAt: 100,
    finishedAt: null,
    error: null,
}
type Result = Pick<ManagedBackupMaintenance, 'id' | 'operation' | 'state' | 'phase'>
let maintenance: ManagedBackupMaintenance | null = original
let restoreResults: Result[] = []
const read = mock(async () => ({ maintenance, restoreResults }))
mock.module('@/server/Backups/restore-maintenance-status.server.ts', () => ({
    readRestoreMaintenanceObservation: read,
}))
const { createWorkerLifecycleSupervisor } =
    await import('@/server/Foundation/worker-lifecycle.server.ts')
const shutdownListeners = new Set(process.listeners('rentnerproxy:shutdown'))
const maintenanceListeners = new Set(process.listeners('rentnerproxy:maintenance'))
afterEach(() => {
    for (const listener of process.listeners('rentnerproxy:shutdown'))
        if (!shutdownListeners.has(listener))
            process.removeListener('rentnerproxy:shutdown', listener)
    for (const listener of process.listeners('rentnerproxy:maintenance'))
        if (!maintenanceListeners.has(listener))
            process.removeListener('rentnerproxy:maintenance', listener)
})

async function waitForRead() {
    const previous = read.mock.calls.length
    const until = Date.now() + 2_000
    while (read.mock.calls.length === previous && Date.now() < until) await Bun.sleep(1)
    expect(read.mock.calls.length).toBeGreaterThan(previous)
}

test('an overwritten pre-destructive failure admits startup using its original root receipt', async () => {
    maintenance = original
    restoreResults = []
    const start = mock(() => undefined)
    const initializing = createWorkerLifecycleSupervisor().startWorkerLifecycle(async () => ({
        start,
        stop: async () => undefined,
    }))
    await waitForRead()
    maintenance = {
        ...original,
        id: '01900000-0000-7000-8000-000000000005',
        operation: 'delete',
        state: 'succeeded',
    }
    restoreResults = [{ ...original, state: 'failed' }]
    await initializing
    expect(start).toHaveBeenCalledTimes(1)
})

test.each(['active', 'destructive failure', 'missing original'] as const)(
    '%s keeps admission fenced even when a newer operation hides the original result',
    async (scenario) => {
        maintenance = original
        restoreResults = []
        const start = mock(() => undefined)
        const stop = mock(async () => undefined)
        const initializing = createWorkerLifecycleSupervisor().startWorkerLifecycle(async () => ({
            start,
            stop,
        }))
        await waitForRead()
        const nextId = '01900000-0000-7000-8000-000000000005'
        maintenance = {
            ...original,
            id: nextId,
            operation: scenario === 'active' ? 'restore' : 'delete',
            state: scenario === 'active' ? 'running' : 'succeeded',
        }
        restoreResults =
            scenario === 'missing original'
                ? []
                : [
                      {
                          ...original,
                          state: 'failed',
                          phase: scenario === 'destructive failure' ? 'database' : 'preparing',
                      },
                  ]
        await waitForRead()
        if (scenario === 'active') {
            // The new restore's destructive failure must remain latched after another overwrite.
            maintenance = {
                ...maintenance,
                operation: 'delete',
                id: '01900000-0000-7000-8000-000000000006',
                state: 'succeeded',
            }
            restoreResults.push({ ...original, id: nextId, state: 'failed', phase: 'database' })
            await waitForRead()
        }
        expect(start).not.toHaveBeenCalled()
        const pending: Array<Promise<void>> = []
        process.emit('rentnerproxy:shutdown', pending)
        await Promise.all([initializing, ...pending])
        expect(start).not.toHaveBeenCalled()
        expect(stop).toHaveBeenCalledTimes(1)
    },
)

test('a transient status read retains the unstarted worker and original restore fence until its safe receipt arrives', async () => {
    maintenance = original
    restoreResults = []
    const start = mock(() => undefined)
    const stop = mock(async () => undefined)
    const load = mock(async () => ({ start, stop }))
    const initializing = createWorkerLifecycleSupervisor().startWorkerLifecycle(load, undefined, {
        retryDelayMs: 1,
    })
    await waitForRead()
    maintenance = {
        ...original,
        id: '01900000-0000-7000-8000-000000000005',
        operation: 'delete',
        state: 'succeeded',
    }
    read.mockRejectedValueOnce(
        Object.assign(new Error('fixture status read failure'), { code: 'EIO' }),
    )
    // Observe both the rejected read and its retry before checking retained ownership.
    await waitForRead()
    await waitForRead()
    expect(load).toHaveBeenCalledTimes(1)
    expect(stop).not.toHaveBeenCalled()
    expect(start).not.toHaveBeenCalled()

    restoreResults = [{ ...original, state: 'failed', phase: 'validating' }]
    await initializing
    expect(start).toHaveBeenCalledTimes(1)
    const pending: Array<Promise<void>> = []
    process.emit('rentnerproxy:shutdown', pending)
    await Promise.all(pending)
    expect(stop).toHaveBeenCalledTimes(1)
})
