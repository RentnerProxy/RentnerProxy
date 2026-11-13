import { stat, statfs } from 'node:fs/promises'
import { BACKUP_RUNTIME_POLICY } from '../../web/src/config/backup-runtime.config.ts'
import type { BackupCapacityProbe, BackupCapacityRequirement } from './Types/capacity.types.ts'

const probeCapacity: BackupCapacityProbe = async (path) => {
    const [info, space] = await Promise.all([stat(path), statfs(path)])
    return { device: info.dev, availableBytes: space.bavail * space.bsize }
}

export async function assertBackupCapacity(
    requirements: readonly BackupCapacityRequirement[],
    probe: BackupCapacityProbe = probeCapacity,
): Promise<void> {
    const devices = new Map<number, { required: number; available: number }>()
    await Promise.all(
        requirements.map(async ({ path, bytes }) => {
            if (!Number.isSafeInteger(bytes) || bytes < 0)
                throw new Error('invalid backup capacity requirement')
            const space = await probe(path)
            if (!Number.isFinite(space.availableBytes) || space.availableBytes < 0)
                throw new Error('backup capacity unavailable')
            const previous = devices.get(space.device)
            devices.set(space.device, {
                required: (previous?.required ?? BACKUP_RUNTIME_POLICY.diskReserveBytes) + bytes,
                available: Math.min(previous?.available ?? Infinity, space.availableBytes),
            })
        }),
    )
    for (const { required, available } of devices.values())
        if (!Number.isSafeInteger(required) || available < required)
            throw new Error('backup capacity unavailable')
}

export function backupCapacityGuard(paths: readonly string[]) {
    // Reserve the next bounded write batch in addition to the operational reserve.
    return () =>
        assertBackupCapacity(
            paths.map((path) => ({ path, bytes: BACKUP_RUNTIME_POLICY.capacityCheckBytes })),
        )
}
