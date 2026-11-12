// oxlint-disable no-await-in-loop -- Each bounded read observes publication progress from the real web owner.
import { checkBackgroundCancellation } from './background-pause.ts'
import type { BackgroundSnapshot } from './Types/background.types.ts'

/** Require one admission and exactly one same-attempt start for every observation already inside the workload window. */
function coversObservedAttempts(
    snapshot: BackgroundSnapshot,
    expected: { runId: string; sourceSha: string },
) {
    const capture = snapshot.schedulerCapture
    if (
        !capture ||
        capture.runId !== expected.runId ||
        capture.sourceSha !== expected.sourceSha ||
        capture.overflowed ||
        capture.clockRegressed ||
        capture.expired
    )
        return false
    return snapshot.observations.every((observation) => {
        const policy = snapshot.policies.find(
            (item) =>
                item.id === observation.policyId && item.version === observation.policyVersion,
        )
        if (!policy) return false
        const events = capture.events.filter(
            (event) =>
                event.policyId === policy.id &&
                event.policyVersion === policy.version &&
                event.dueAt === policy.dueAt,
        )
        const admissions = events.filter((event) => event.kind === 'admitted')
        const starts = events.filter((event) => event.kind === 'started')
        return (
            admissions.length === 1 &&
            starts.length === 1 &&
            admissions[0]!.attemptHash === starts[0]!.attemptHash &&
            admissions[0]!.at >= policy.dueAt &&
            starts[0]!.at >= admissions[0]!.at &&
            starts[0]!.monotonicMs >= admissions[0]!.monotonicMs
        )
    })
}

/** Reread persisted capture under a bounded client wait; the caller separately bounds remote read-only execution and retains container cleanup ownership. */
export async function settleBackgroundCapture(
    initial: BackgroundSnapshot,
    expected: { runId: string; sourceSha: string },
    dependencies: {
        signal: AbortSignal
        /** Bound the client wait by remainingMs; a separate remote timeout and final container cleanup must own remote work after transport loss. */
        read: (remainingMs: number) => Promise<BackgroundSnapshot>
        now?: () => number
        sleep?: (milliseconds: number) => Promise<unknown>
    },
    timeoutMs = 2_000,
) {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0)
        throw new Error('Capture settlement deadline must be finite and positive')
    const now = dependencies.now ?? performance.now.bind(performance)
    const sleep = dependencies.sleep ?? Bun.sleep
    const started = now()
    const budgetMs = Math.max(1, Math.min(timeoutMs, 2_000))
    const deadline = started + budgetMs
    let snapshot = initial
    let reads = 0
    const result = (status: 'complete' | 'deadline' | 'unavailable' | 'not-required') => ({
        snapshot,
        status,
        reads,
        budgetMs,
        elapsedMs: Math.max(0, now() - started),
    })
    checkBackgroundCancellation(dependencies.signal)
    if (!initial.observations.length) return result('not-required')
    while (!coversObservedAttempts(snapshot, expected)) {
        checkBackgroundCancellation(dependencies.signal)
        const remainingMs = deadline - now()
        if (remainingMs <= 250) return result('deadline')
        try {
            reads++
            const observed = await dependencies.read(remainingMs)
            checkBackgroundCancellation(dependencies.signal)
            if (now() > deadline) return result('deadline')
            snapshot = { ...initial, schedulerCapture: observed.schedulerCapture }
        } catch {
            checkBackgroundCancellation(dependencies.signal)
            return result(now() >= deadline ? 'deadline' : 'unavailable')
        }
        if (!coversObservedAttempts(snapshot, expected))
            await sleep(Math.min(25, Math.max(0, deadline - now())))
    }
    checkBackgroundCancellation(dependencies.signal)
    return result('complete')
}
