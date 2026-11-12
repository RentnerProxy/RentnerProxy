import { expect, mock, test } from 'bun:test'
import * as fs from 'node:fs/promises'
import { join } from 'node:path'
import * as storage from '../../../../../../docker/production/backups/storage.ts'
import type { ManagedBackupMaintenance } from '@/lib/Backups/Types/backups.types.ts'
import { managedRestoreResultSchema } from '@/lib/Backups/validation.ts'

const publish = mock(async (_path: string, _value: unknown, _permissions: unknown) => undefined)
await mock.module('node:fs/promises', () => ({
    ...fs,
    mkdir: async () => undefined,
    lstat: async () => ({ uid: 0, isDirectory: () => true, isSymbolicLink: () => false }),
    chown: async () => undefined,
    chmod: async () => undefined,
}))
await mock.module('../../../../../../docker/production/backups/storage.ts', () => ({
    ...storage,
    writePrivateJson: publish,
}))
const { writeMaintenanceProgress } =
    await import('../../../../../../docker/production/backups/restore.ts')

test('publishes the bounded root journal as allowlisted receipts with latest progress in one protected snapshot under 16KiB', async () => {
    const current: ManagedBackupMaintenance = {
        id: '01900000-0000-7000-8000-000000000001',
        requestId: '01900000-0000-7000-8000-000000000002',
        actorId: '01900000-0000-7000-8000-000000000003',
        actorName: '\u0800'.repeat(160),
        artifactId: '01900000-0000-7000-8000-000000000004',
        metadataSha256: 'a'.repeat(64),
        operation: 'restore',
        state: 'running',
        phase: 'restarting',
        progressPercent: 99,
        requestedAt: Number.MAX_SAFE_INTEGER,
        startedAt: Number.MAX_SAFE_INTEGER,
        finishedAt: null,
        error: null,
    }
    const phases: ManagedBackupMaintenance['phase'][] = [
        'queued',
        'preparing',
        'validating',
        'quiescing',
        'database',
        'controller',
        'crowdsec',
        'secrets',
        'restarting',
        'complete',
    ]
    const states = ['succeeded', 'failed', 'interrupted'] as const
    const history = Array.from({ length: 100 }, (_, index): ManagedBackupMaintenance => ({
        ...current,
        id: '01900000-0000-7000-8000-' + String(index + 10).padStart(12, '0'),
        state: states[index % states.length]!,
        phase: phases[index % phases.length]!,
    }))
    await writeMaintenanceProgress(current, history)
    const snapshot = publish.mock.calls[0]![1] as {
        maintenance: ManagedBackupMaintenance
        restoreResults: unknown[]
    }
    expect(publish).toHaveBeenCalledTimes(1)
    expect(publish.mock.calls[0]![0]).toBe(join('/run/rentnerproxy/maintenance', 'progress.json'))
    expect(publish.mock.calls[0]![2]).toEqual({ gid: 10002, mode: 0o440 })
    expect(snapshot.maintenance).toEqual(current)
    expect(snapshot.restoreResults).toHaveLength(100)
    for (const entry of snapshot.restoreResults)
        expect(entry).toEqual(managedRestoreResultSchema.parse(entry))
    expect(Buffer.byteLength(JSON.stringify(snapshot) + '\n')).toBeLessThanOrEqual(16_384)

    publish.mockClear()
    await writeMaintenanceProgress(
        current,
        history.map((entry) => ({ ...entry, state: 'interrupted', phase: 'validating' })),
    )
    expect(Buffer.byteLength(JSON.stringify(publish.mock.calls[0]![1]) + '\n')).toBeLessThanOrEqual(
        16_384,
    )

    publish.mockClear()
    await writeMaintenanceProgress(null, [
        current,
        { ...history[0]!, operation: 'delete' },
        history[1]!,
    ])
    expect(publish.mock.calls[0]![1]).toEqual({
        maintenance: null,
        restoreResults: [
            {
                id: history[1]!.id,
                operation: 'restore',
                state: history[1]!.state,
                phase: history[1]!.phase,
            },
        ],
    })
})
