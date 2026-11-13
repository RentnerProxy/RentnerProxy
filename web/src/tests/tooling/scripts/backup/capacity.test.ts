import { describe, expect, test } from 'bun:test'
import { assertBackupCapacity, stateArchiveAllocationBytes, stateArchiveAllocationEvidence } from '../../../../../../scripts/backup/capacity.ts'
import { BACKUP_RUNTIME_POLICY } from '../../../../config/backup-runtime.config.ts'

const reserve = BACKUP_RUNTIME_POLICY.diskReserveBytes

describe('backup capacity admission', () => {
    test('counts explicit and implicit directories once across sorted path boundaries', () => {
        const entries = [
            { name: 'a/b/c', directory: false, size: 7, bodyOffset: 0 },
            { name: 'a-b/d', directory: false, size: 9, bodyOffset: 0 },
            { name: '', directory: true, size: 0, bodyOffset: 0 },
            { name: 'a', directory: true, size: 0, bodyOffset: 0 },
            { name: 'a/b/cc', directory: false, size: 0, bodyOffset: 0 },
            { name: 'a/b', directory: true, size: 0, bodyOffset: 0 },
        ]
        expect(stateArchiveAllocationEvidence(entries)).toEqual({ payloadBytes: 16, nodes: 11 })
        expect(entries[0]!.name).toBe('a/b/c')
    })

    test('small-file allocation on a 64 KiB filesystem can exceed the operational reserve', async () => {
        const archive = stateArchiveAllocationEvidence(Array.from({ length: 20_000 }, (_, index) => ({
            name: `files/${index}`, directory: false, size: 1, bodyOffset: 0,
        })))
        const allocated = stateArchiveAllocationBytes(archive, 65_536)
        expect(archive).toEqual({ payloadBytes: 20_000, nodes: 20_006 })
        expect(allocated).toBeGreaterThan(reserve)
        await expect(assertBackupCapacity([
            { path: '/data', bytes: 100, archive },
            { path: '/tmp', bytes: 200 },
        ], async () => ({ device: 1, availableBytes: reserve + allocated + 299, allocationUnitBytes: 65_536 }))).rejects.toThrow('backup capacity unavailable')
        await assertBackupCapacity([
            { path: '/data', bytes: 100, archive },
            { path: '/tmp', bytes: 200 },
        ], async () => ({ device: 1, availableBytes: reserve + allocated + 300, allocationUnitBytes: 65_536 }))
    })

    test('a few large files retain a small allocation allowance and missing allocation evidence fails closed', async () => {
        const payloadBytes = 3 * 1024 ** 3
        const archive = stateArchiveAllocationEvidence([{ name: 'large.bin', directory: false, size: payloadBytes, bodyOffset: 0 }])
        expect(stateArchiveAllocationBytes(archive, 65_536) - payloadBytes).toBeLessThan(1024 ** 2)
        await expect(assertBackupCapacity([{ path: '/data', bytes: 0, archive }], async () => ({
            device: 1, availableBytes: Number.MAX_SAFE_INTEGER,
        }))).rejects.toThrow('invalid state archive capacity evidence')
        expect(() => stateArchiveAllocationBytes({ payloadBytes: 1, nodes: Number.MAX_SAFE_INTEGER }, 65_536)).toThrow('invalid state archive capacity evidence')
    })

    test('aggregates snapshot, extraction, SQL and database headroom on one device', async () => {
        const requirements = [
            { path: '/backup', bytes: 100 },
            { path: '/tmp', bytes: 200 },
            { path: '/data', bytes: 400 },
        ]
        await expect(
            assertBackupCapacity(requirements, async () => ({
                device: 1,
                availableBytes: reserve + 699,
            })),
        ).rejects.toThrow('backup capacity unavailable')
        await assertBackupCapacity(requirements, async () => ({
            device: 1,
            availableBytes: reserve + 700,
        }))
    })

    test('checks each mounted filesystem and uses its least recent free-space observation', async () => {
        const requirements = [
            { path: '/backup', bytes: 100 },
            { path: '/tmp', bytes: 200 },
        ]
        await expect(
            assertBackupCapacity(requirements, async (path) => ({
                device: path === '/backup' ? 1 : 2,
                availableBytes: reserve + 150,
            })),
        ).rejects.toThrow('backup capacity unavailable')
        await expect(
            assertBackupCapacity(requirements, async (path) => ({
                device: 1,
                availableBytes: reserve + (path === '/backup' ? 300 : 299),
            })),
        ).rejects.toThrow('backup capacity unavailable')
    })

    test('fails closed on unsafe arithmetic and unavailable capacity probes', async () => {
        for (const bytes of [-1, Infinity, Number.MAX_SAFE_INTEGER]) {
            // oxlint-disable-next-line no-await-in-loop -- Exercise each invalid admission independently.
            await expect(
                assertBackupCapacity([{ path: '/backup', bytes }], async () => ({
                    device: 1,
                    availableBytes: Number.MAX_SAFE_INTEGER,
                })),
            ).rejects.toThrow()
        }
        await expect(
            assertBackupCapacity([{ path: '/backup', bytes: 1 }], async () => {
                throw new Error('synthetic statfs failure')
            }),
        ).rejects.toThrow('synthetic statfs failure')
    })

    test('outer deadlines leave cleanup room after the bounded four-hour operation', () => {
        expect(BACKUP_RUNTIME_POLICY.maximumArtifactBytes).toBe(8 * 1024 ** 3)
        expect(BACKUP_RUNTIME_POLICY.captureTimeoutMs).toBeLessThanOrEqual(
            BACKUP_RUNTIME_POLICY.operationTimeoutMs,
        )
        expect(BACKUP_RUNTIME_POLICY.databaseTimeoutMs).toBeLessThanOrEqual(
            BACKUP_RUNTIME_POLICY.operationTimeoutMs,
        )
        expect(BACKUP_RUNTIME_POLICY.supervisorTimeoutSeconds * 1000).toBeGreaterThan(
            BACKUP_RUNTIME_POLICY.operationTimeoutMs,
        )
        expect(BACKUP_RUNTIME_POLICY.agentTimeoutMs).toBeGreaterThan(
            BACKUP_RUNTIME_POLICY.supervisorTimeoutSeconds * 1000,
        )
    })
})
