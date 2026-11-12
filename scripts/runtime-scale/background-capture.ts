import { open } from 'node:fs/promises'
import { constants } from 'node:fs'
import { hostMonitoringCaptureSchema } from '../../web/src/server/HostMonitoring/host-monitoring.capture.ts'
import { readRuntimeProcesses } from '../smoke/appliance-processes.ts'
import type { HostMonitoringCapture } from '../../web/src/server/HostMonitoring/host-monitoring.capture.ts'

type Owner = { pid: number; startTicks: string; state: string }
/** Open once without following links or blocking on a FIFO, then bound every byte read from that same regular inode. */
export async function readBoundedBackgroundCapture(path: string) {
    const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK)
    try {
        const stat = await file.stat()
        const maximum = 8 * 1024 * 1024
        if (!stat.isFile() || stat.size > maximum) return null
        const buffer = Buffer.alloc(Math.min(stat.size + 1, maximum + 1))
        let size = 0
        while (size < buffer.length) {
            // oxlint-disable-next-line no-await-in-loop -- Read only from the opened inode under one fixed byte bound.
            const { bytesRead } = await file.read(buffer, size, buffer.length - size, size)
            if (!bytesRead) break
            size += bytesRead
        }
        if (size > stat.size || size > maximum) return null
        return buffer.subarray(0, size).toString('utf8')
    } finally { await file.close() }
}
/** Compare both census brackets; neither PID reuse nor a child-process capture establishes serving-owner evidence. */
export function backgroundCaptureMatchesOwner(capture: HostMonitoringCapture, expected: { runId: string; sourceSha: string | undefined }, before: Owner[], after: Owner[], now: number) {
    return before.length === 1 && after.length === 1 && !['Z', 'X'].includes(before[0]!.state) && !['Z', 'X'].includes(after[0]!.state) && capture.runId === expected.runId && capture.sourceSha === expected.sourceSha && capture.pid === before[0]!.pid && capture.processStartTicks === before[0]!.startTicks && before[0]!.pid === after[0]!.pid && before[0]!.startTicks === after[0]!.startTicks && now >= capture.createdAt && now <= capture.expiresAt
}

/** Missing, replaced, oversized, nonregular or mismatched capture is unavailable; no network receipt can substitute for it. */
export async function readBackgroundCapture(runId: string) {
    if (!/^[a-f0-9]{12}$/u.test(runId)) return null
    try {
        const path = '/tmp/rentnerproxy-background-capture-' + runId + '.json'
        const before = (await readRuntimeProcesses()).filter((owner) => owner.role === 'web')
        const text = await readBoundedBackgroundCapture(path)
        if (text === null) return null
        const capture = hostMonitoringCaptureSchema.parse(JSON.parse(text))
        const after = (await readRuntimeProcesses()).filter((owner) => owner.role === 'web')
        if (!backgroundCaptureMatchesOwner(capture, { runId, sourceSha: process.env.RENTNERPROXY_BACKGROUND_CAPTURE_SOURCE_SHA }, before, after, Date.now())) return null
        return capture
    } catch { return null }
}
