import { readBackgroundCapture } from './background-capture.ts'

/** Fixed capture-only entry: no fixture context, SQL client, business module, or arbitrary command/path is accepted. */
export async function runBackgroundCaptureRead(args: readonly string[]) {
    const [runId] = args
    if (
        args.length !== 1 ||
        !runId ||
        !/^[a-f0-9]{12}$/u.test(runId) ||
        process.env.RENTNERPROXY_BACKGROUND_CAPTURE_RUN !== runId ||
        !/^[a-f0-9]{40}$/u.test(process.env.RENTNERPROXY_BACKGROUND_CAPTURE_SOURCE_SHA ?? '')
    )
        throw new Error('Invalid capture read request')
    return { capture: await readBackgroundCapture(runId) }
}

/** Client and remote deadlines are independent: Docker transport loss never proves remote drain; final container cleanup retains ownership. */
export function backgroundCaptureReadCommand(
    container: string,
    runId: string,
    remainingMs: number,
) {
    if (
        !/^[a-z0-9][a-z0-9_.-]{0,127}$/u.test(container) ||
        !/^[a-f0-9]{12}$/u.test(runId) ||
        !Number.isFinite(remainingMs) ||
        remainingMs <= 250
    )
        throw new Error('Invalid bounded capture command')
    const remoteTimeoutMs = Math.min(1000, Math.max(1, Math.floor(remainingMs - 250)))
    return {
        args: [
            'exec',
            '--workdir',
            '/opt/rentnerproxy/web',
            container,
            '/usr/bin/timeout',
            '--signal=KILL',
            (remoteTimeoutMs / 1000).toFixed(3) + 's',
            'bun',
            '--no-env-file',
            '/opt/rentnerproxy/web/background-capture-read.js',
            runId,
        ],
        clientTimeoutMs: remainingMs,
        remoteTimeoutMs,
    }
}

if (import.meta.main) {
    try {
        console.log(JSON.stringify(await runBackgroundCaptureRead(process.argv.slice(2))))
    } catch {
        console.error('Background capture read unavailable')
        process.exitCode = 1
    }
}
