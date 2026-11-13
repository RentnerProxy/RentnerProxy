import { stat, statfs } from 'node:fs/promises'
import { BACKUP_RUNTIME_POLICY } from '../../web/src/config/backup-runtime.config.ts'
import type { BackupCapacityProbe, BackupCapacityRequirement, StateArchiveAllocationEvidence } from './Types/capacity.types.ts'
import type { StateArchiveFileEntry } from './Types/format.types.ts'

const probeCapacity: BackupCapacityProbe = async (path) => {
    const [info, space] = await Promise.all([stat(path), statfs(path)])
    return { device: info.dev, availableBytes: space.bavail * space.bsize, allocationUnitBytes: space.bsize }
}

/** Count unique extracted paths and implicit parents without reading file bodies. */
export function stateArchiveAllocationEvidence(entries: readonly StateArchiveFileEntry[]): StateArchiveAllocationEvidence {
    let payloadBytes = 0
    // Include the extraction root and small restore-created pause/ownership paths.
    let nodes = 5
    let previous = ''
    // Order a directory's complete subtree before similarly prefixed sibling names.
    // Ordinary lexical ordering can separate `a` from `a/b` with `a-b/file`.
    const ordered = entries.toSorted((left, right) => {
        for (let index = 0; index < Math.min(left.name.length, right.name.length); index++) {
            const first = left.name[index] === '/' ? 0 : left.name.charCodeAt(index)
            const second = right.name[index] === '/' ? 0 : right.name.charCodeAt(index)
            if (first !== second) return first - second
        }
        return left.name.length - right.name.length
    })
    for (const entry of ordered) {
        if (!entry.directory) payloadBytes += entry.size
        const name = entry.name
        let common = 0
        while (common < previous.length && common < name.length && previous[common] === name[common]) common++
        const sharedParent = common === previous.length && name[common] === '/'
            ? common
            : name.lastIndexOf('/', common - 1)
        for (let index = 0; index < name.length; index++)
            if ((name[index] === '/' || index === name.length - 1) &&
                (name[index] === '/' ? index : index + 1) > sharedParent) nodes++
        previous = name
    }
    if (!Number.isSafeInteger(payloadBytes) || payloadBytes < 0 || !Number.isSafeInteger(nodes))
        throw new Error('invalid state archive capacity evidence')
    return { payloadBytes, nodes }
}

export function stateArchiveAllocationBytes(evidence: StateArchiveAllocationEvidence, allocationUnitBytes: number): number {
    if (!Number.isSafeInteger(evidence.payloadBytes) || evidence.payloadBytes < 0 ||
        !Number.isSafeInteger(evidence.nodes) || evidence.nodes < 1 ||
        !Number.isSafeInteger(allocationUnitBytes) || allocationUnitBytes <= 0)
        throw new Error('invalid state archive capacity evidence')
    // One unit covers file tail rounding; another covers inode/directory allocation.
    // Keep at least 4 KiB of metadata allowance even on filesystems with smaller units.
    const bytes = evidence.payloadBytes + evidence.nodes * 2 * Math.max(4096, allocationUnitBytes)
    if (!Number.isSafeInteger(bytes)) throw new Error('invalid state archive capacity evidence')
    return bytes
}

export async function assertBackupCapacity(
    requirements: readonly BackupCapacityRequirement[],
    probe: BackupCapacityProbe = probeCapacity,
): Promise<void> {
    const devices = new Map<number, { required: number; available: number }>()
    await Promise.all(
        requirements.map(async ({ path, bytes, archive }) => {
            if (!Number.isSafeInteger(bytes) || bytes < 0)
                throw new Error('invalid backup capacity requirement')
            const space = await probe(path)
            const requiredBytes = bytes + (archive ? stateArchiveAllocationBytes(archive, space.allocationUnitBytes ?? 0) : 0)
            if (!Number.isFinite(space.availableBytes) || space.availableBytes < 0)
                throw new Error('backup capacity unavailable')
            const previous = devices.get(space.device)
            devices.set(space.device, {
                required: (previous?.required ?? BACKUP_RUNTIME_POLICY.diskReserveBytes) + requiredBytes,
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
