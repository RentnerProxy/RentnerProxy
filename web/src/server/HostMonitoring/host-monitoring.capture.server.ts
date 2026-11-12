import '@tanstack/react-start/server-only'
import { writeFile, rename, unlink } from 'node:fs/promises'
import { readFileSync } from 'node:fs'
import { createHostMonitoringCapture } from './host-monitoring.capture.ts'

/** Only the isolated harness opts in; normal operation performs no capture I/O and accepts no user-selected path. */
function captureFromEnvironment() {
    const runId = process.env.RENTNERPROXY_BACKGROUND_CAPTURE_RUN
    const sourceSha = process.env.RENTNERPROXY_BACKGROUND_CAPTURE_SOURCE_SHA
    if (
        !runId ||
        !/^[a-f0-9]{12}$/u.test(runId) ||
        !sourceSha ||
        !/^[a-f0-9]{40}$/u.test(sourceSha)
    )
        return undefined
    const path = '/tmp/rentnerproxy-background-capture-' + runId + '.json'
    let processStartTicks: string
    try {
        const stat = readFileSync('/proc/self/stat', 'utf8')
        processStartTicks = stat
            .slice(stat.lastIndexOf(')') + 2)
            .trim()
            .split(/\s+/u)[19]!
        if (!/^\d+$/u.test(processStartTicks)) return undefined
    } catch {
        return undefined
    }
    return createHostMonitoringCapture({
        runId,
        sourceSha,
        pid: process.pid,
        processStartTicks,
        write: async (value) => {
            const temporary = path + '.' + process.pid + '.tmp'
            try {
                await writeFile(temporary, JSON.stringify(value), { mode: 0o600, flag: 'wx' })
                await rename(temporary, path)
            } finally {
                await unlink(temporary).catch(() => undefined)
            }
        },
    })
}
export const hostMonitoringCapture = captureFromEnvironment()
