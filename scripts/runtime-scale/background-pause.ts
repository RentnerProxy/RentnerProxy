// oxlint-disable no-await-in-loop -- The detached guard polls one pause until its resume request or deadline.
import { randomUUID } from 'node:crypto'
import { ReliabilityError } from '../runtime-reliability/harness.ts'

export function backgroundPauseMarker(runId: string) {
    return '/tmp/background-resume-' + runId + '-' + randomUUID()
}

export function checkBackgroundCancellation(signal: AbortSignal) {
    if (signal.aborted) throw new ReliabilityError('cancelled', 'background scenario cancelled')
}

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
