// oxlint-disable no-await-in-loop -- The detached guard polls one pause until its resume request or deadline.
import { randomUUID } from 'node:crypto'
import { ReliabilityError } from '../runtime-reliability/harness.ts'

/** Allocate a unique run-scoped marker so a completed guard cannot accidentally resume a later pause. */
export function backgroundPauseMarker(runId: string) {
    return '/tmp/background-resume-' + runId + '-' + randomUUID()
}

/** Preserve cancellation as its own failure category so scenario interruption cannot be retried or reported as success. */
export function checkBackgroundCancellation(signal: AbortSignal) {
    if (signal.aborted) throw new ReliabilityError('cancelled', 'background scenario cancelled')
}

/** Own both stop and resume signals for one identity-checked process; always publish a terminal marker after the last possible resume. */
export async function backgroundPauseGuard(
    dependencies: {
        identity: () => Promise<boolean>
        signal: (signal: 'SIGSTOP' | 'SIGCONT') => void
        write: (state: string) => Promise<unknown>
        requested: () => Promise<boolean>
        now: () => number
        sleep: () => Promise<unknown>
    },
    timeoutMs = 90000,
) {
    let stopped = false
    try {
        if (!(await dependencies.identity())) return
        dependencies.signal('SIGSTOP')
        stopped = true
        await dependencies.write('armed')
        const deadline = dependencies.now() + timeoutMs
        while (dependencies.now() < deadline && !(await dependencies.requested()))
            await dependencies.sleep()
    } finally {
        // One guard owns both signals; its terminal marker follows its last possible resume.
        let state = 'stale'
        if (stopped && (await dependencies.identity())) {
            dependencies.signal('SIGCONT')
            state = 'resumed'
        }
        await dependencies.write(state)
    }
}

/** Standalone detached entry point: process identity and private paths are data arguments. */
export async function backgroundPauseProcessGuard(guard: typeof backgroundPauseGuard) {
    const [pidText, startTicks, marker] = process.argv.slice(1)
    if (
        !pidText ||
        !/^\d{1,10}$/u.test(pidText) ||
        !Number.isSafeInteger(Number(pidText)) ||
        Number(pidText) <= 1 ||
        !startTicks ||
        !/^\d{1,20}$/u.test(startTicks) ||
        !marker ||
        !/^\/tmp\/background-resume-[a-zA-Z0-9_-]{1,64}-[a-f0-9-]{36}$/u.test(marker)
    )
        throw new Error('Invalid background guard arguments')
    const pid = Number(pidText)
    await guard({
        identity: async () => {
            try {
                const stat = await Bun.file('/proc/' + pid + '/stat').text()
                return (
                    stat
                        .slice(stat.lastIndexOf(')') + 2)
                        .trim()
                        .split(/\s+/u)[19] === startTicks
                )
            } catch {
                return false
            }
        },
        signal: (signal) => process.kill(pid, signal),
        write: (state) => Bun.write(marker, state),
        requested: () => Bun.file(marker + '.resume').exists(),
        now: () => performance.now(),
        sleep: () => Bun.sleep(100),
    })
}

export const backgroundPauseDetachedScript = `await (${backgroundPauseProcessGuard.toString()})(${backgroundPauseGuard.toString()}).catch(()=>process.exit(1))`

/** Resume after every attempted arm, including cancellation and operation failure; cleanup errors never replace an earlier failure identity. */
export async function withBackgroundPause<T>(
    signal: AbortSignal,
    arm: () => Promise<unknown>,
    operation: () => Promise<T>,
    resume: () => Promise<unknown>,
) {
    checkBackgroundCancellation(signal)
    let outcome: { success: true; value: T } | { success: false; reason: unknown }
    try {
        await arm()
        checkBackgroundCancellation(signal)
        const result = await operation()
        checkBackgroundCancellation(signal)
        outcome = { success: true, value: result }
    } catch (error) {
        outcome = { success: false, reason: error }
    }
    try {
        await resume()
    } catch (error) {
        if (outcome.success) throw error
        console.error('Background pause cleanup failed after primary failure')
    }
    if (!outcome.success) throw outcome.reason
    return outcome.value
}
