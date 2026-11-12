import '@tanstack/react-start/server-only'
import { readFileSync, openSync, closeSync, renameSync, unlinkSync, writeFile } from 'node:fs'
import { createHostMonitoringCapture } from './host-monitoring.capture.ts'
import type { HostMonitoringCapture } from './host-monitoring.capture.ts'

function writeDescriptor(fd: number, contents: string) {
    return new Promise<void>((resolve, reject) =>
        writeFile(fd, contents, (error) => (error ? reject(error) : resolve())),
    )
}

/** Raw descriptors have one closer. Revocation does not depend on a writable directory, unlink, or a successful replacement write. */
export function createHostMonitoringCapturePublisher(
    path: string,
    io: { write?: typeof writeDescriptor; remove?: typeof unlinkSync } = {},
) {
    let publishedFd: number | undefined
    const invalidate = () => {
        const fd = publishedFd
        publishedFd = undefined
        if (fd !== undefined) closeSync(fd)
    }
    const discard = (fd: number, temporary: string) => {
        let closeFailed = false
        let closeError: unknown
        try {
            closeSync(fd)
        } catch (error) {
            closeFailed = true
            closeError = error
        }
        try {
            ;(io.remove ?? unlinkSync)(temporary)
        } catch (error) {
            if (!(error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT'))
                throw error
        }
        if (closeFailed) throw closeError
    }
    return {
        invalidate,
        async write(value: HostMonitoringCapture, isCurrent: () => boolean) {
            const temporary = path + '.' + value.epoch + '.' + value.publication.generation + '.tmp'
            // O_EXCL creates a fresh immutable candidate inode. No published inode is ever truncated.
            const fd = openSync(temporary, 'wx', 0o600)
            let granted = false
            try {
                value.publication.fd = fd
                await (io.write ?? writeDescriptor)(fd, JSON.stringify(value))
                // No await may separate this generation/terminal/expiry check from rename and lease grant.
                if (!isCurrent()) return
                renameSync(temporary, path)
                publishedFd = fd
                granted = true
            } finally {
                // Concurrent record/expiry only revokes publishedFd. Candidate I/O has settled before its sole close.
                if (!granted) discard(fd, temporary)
            }
        },
    }
}

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
        ...createHostMonitoringCapturePublisher(path),
    })
}
export const hostMonitoringCapture = captureFromEnvironment()
