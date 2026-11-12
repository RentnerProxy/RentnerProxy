import { open, stat } from 'node:fs/promises'
import { constants } from 'node:fs'
import type { BigIntStats } from 'node:fs'
import { hostMonitoringCaptureSchema } from '../../web/src/server/HostMonitoring/host-monitoring.capture.ts'
import { readRuntimeProcesses } from '../smoke/appliance-processes.ts'
import type { HostMonitoringCapture } from '../../web/src/server/HostMonitoring/host-monitoring.capture.ts'

type Owner = { pid: number; startTicks: string; state: string }
/** Open once without following links or blocking on a FIFO, then bound every byte read from that same regular inode. */
async function withBoundedBackgroundCapture<T>(path: string, observe: (text: string, identity: BigIntStats) => Promise<T>) {
    const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK)
    try {
        const identity = await file.stat({ bigint: true })
        const maximum = 8 * 1024 * 1024
        if (!identity.isFile() || identity.size > BigInt(maximum)) return null
        const fileSize = Number(identity.size)
        const buffer = Buffer.alloc(Math.min(fileSize + 1, maximum + 1))
        let size = 0
        while (size < buffer.length) {
            // oxlint-disable-next-line no-await-in-loop -- Read only from the opened inode under one fixed byte bound.
            const { bytesRead } = await file.read(buffer, size, buffer.length - size, size)
            if (!bytesRead) break
            size += bytesRead
        }
        if (size > fileSize || size > maximum) return null
        return await observe(buffer.subarray(0, size).toString('utf8'), identity)
    } finally {
        await file.close()
    }
}

/** Preserve the bounded raw-file helper; authority requires the separate live descriptor lease validation below. */
export function readBoundedBackgroundCapture(path: string) {
    return withBoundedBackgroundCapture(path, async (text) => text)
}

async function ownerHoldsPublication(capture: HostMonitoringCapture, opened: BigIntStats) {
    const fd = capture.publication.fd
    if (fd === null || capture.publication.generation === 0) return false
    try {
        const held = await stat('/proc/' + capture.pid + '/fd/' + fd, { bigint: true })
        return held.isFile() && held.dev === opened.dev && held.ino === opened.ino
    } catch { return false }
}
/** Compare both census brackets; neither PID reuse nor a child-process capture establishes serving-owner evidence. */
export function backgroundCaptureMatchesOwner(
    capture: HostMonitoringCapture,
    expected: { runId: string; sourceSha: string | undefined },
    before: Owner[],
    after: Owner[],
    now: number,
) {
    return (
        before.length === 1 &&
        after.length === 1 &&
        !['Z', 'X'].includes(before[0]!.state) &&
        !['Z', 'X'].includes(after[0]!.state) &&
        capture.runId === expected.runId &&
        capture.sourceSha === expected.sourceSha &&
        capture.pid === before[0]!.pid &&
        capture.processStartTicks === before[0]!.startTicks &&
        before[0]!.pid === after[0]!.pid &&
        before[0]!.startTicks === after[0]!.startTicks &&
        now >= capture.createdAt &&
        now <= capture.expiresAt
    )
}

/** Hold the opened artifact through both lease checks; pathname cleanup failure and descriptor reuse cannot revive an obsolete inode. */
export async function readLeasedBackgroundCapture(path: string, expected: { runId: string; sourceSha: string | undefined }, owners: () => Promise<Owner[]> = async () => (await readRuntimeProcesses()).filter((owner) => owner.role === 'web' && owner.pid !== process.pid), now: () => number = Date.now) {
    try {
        const before = await owners()
        return await withBoundedBackgroundCapture(path, async (text, opened) => {
            const capture = hostMonitoringCaptureSchema.parse(JSON.parse(text))
            if (!await ownerHoldsPublication(capture, opened)) return null
            const after = await owners()
            if (!backgroundCaptureMatchesOwner(capture, expected, before, after, now()) || !await ownerHoldsPublication(capture, opened)) return null
            return capture
        })
    } catch {
        return null
    }
}

/** Missing, stale, unleased or mismatched capture is unavailable; no network receipt can substitute for it. */
export function readBackgroundCapture(runId: string) {
    if (!/^[a-f0-9]{12}$/u.test(runId)) return Promise.resolve(null)
    return readLeasedBackgroundCapture('/tmp/rentnerproxy-background-capture-' + runId + '.json', { runId, sourceSha: process.env.RENTNERPROXY_BACKGROUND_CAPTURE_SOURCE_SHA })
}
