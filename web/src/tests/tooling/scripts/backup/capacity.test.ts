import { describe, expect, test } from 'bun:test'
import { assertBackupCapacity } from '../../../../../../scripts/backup/capacity.ts'
import { BACKUP_RUNTIME_POLICY } from '../../../../config/backup-runtime.config.ts'

const reserve = BACKUP_RUNTIME_POLICY.diskReserveBytes

describe('backup capacity admission', () => {
    test('aggregates snapshot, extraction, SQL and database headroom on one device', async () => {
        const requirements = [
            { path: '/backup', bytes: 100 },
            { path: '/tmp', bytes: 200 },
            { path: '/data', bytes: 400 },
        ]
        await expect(assertBackupCapacity(requirements, async () => ({
            device: 1, availableBytes: reserve + 699,
        }))).rejects.toThrow('backup capacity unavailable')
        await assertBackupCapacity(requirements, async () => ({
            device: 1, availableBytes: reserve + 700,
        }))
    })

    test('checks each mounted filesystem and uses its least recent free-space observation', async () => {
        const requirements = [{ path: '/backup', bytes: 100 }, { path: '/tmp', bytes: 200 }]
        await expect(assertBackupCapacity(requirements, async (path) => ({
            device: path === '/backup' ? 1 : 2,
            availableBytes: reserve + 150,
        }))).rejects.toThrow('backup capacity unavailable')
        await expect(assertBackupCapacity(requirements, async (path) => ({
            device: 1, availableBytes: reserve + (path === '/backup' ? 300 : 299),
        }))).rejects.toThrow('backup capacity unavailable')
    })

    test('fails closed on unsafe arithmetic and unavailable capacity probes', async () => {
        for (const bytes of [-1, Infinity, Number.MAX_SAFE_INTEGER]) {
            // oxlint-disable-next-line no-await-in-loop -- Exercise each invalid admission independently.
            await expect(assertBackupCapacity([{ path: '/backup', bytes }], async () => ({
                device: 1, availableBytes: Number.MAX_SAFE_INTEGER,
            }))).rejects.toThrow()
        }
        await expect(assertBackupCapacity([{ path: '/backup', bytes: 1 }], async () => {
            throw new Error('synthetic statfs failure')
        })).rejects.toThrow('synthetic statfs failure')
    })

    test('outer deadlines leave cleanup room after the bounded four-hour operation', () => {
        expect(BACKUP_RUNTIME_POLICY.maximumArtifactBytes).toBe(8 * 1024 ** 3)
        expect(BACKUP_RUNTIME_POLICY.captureTimeoutMs).toBeLessThanOrEqual(BACKUP_RUNTIME_POLICY.operationTimeoutMs)
        expect(BACKUP_RUNTIME_POLICY.databaseTimeoutMs).toBeLessThanOrEqual(BACKUP_RUNTIME_POLICY.operationTimeoutMs)
        expect(BACKUP_RUNTIME_POLICY.supervisorTimeoutSeconds * 1000).toBeGreaterThan(BACKUP_RUNTIME_POLICY.operationTimeoutMs)
        expect(BACKUP_RUNTIME_POLICY.agentTimeoutMs).toBeGreaterThan(BACKUP_RUNTIME_POLICY.supervisorTimeoutSeconds * 1000)
    })
})
