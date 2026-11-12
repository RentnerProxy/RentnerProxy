import { expect, test } from 'bun:test'
import { settleBackgroundCapture } from '../../../../../../scripts/runtime-scale/background-capture-settlement.ts'
import { backgroundSnapshotSchema } from '../../../../../../scripts/runtime-scale/background.validation.ts'

const expected = { runId: '012345abcdef', sourceSha: 'a'.repeat(40) }
function snapshot(complete = false) {
    const policyId = '00000000-0000-4000-8000-000000000001'
    return backgroundSnapshotSchema.parse({ origin: 1000, managementReadMs: 1, policies: [{ id: policyId, version: policyId, index: 0, enabled: true, leased: false, dueAt: 1000 }], observations: [{ policyId, policyVersion: policyId, checkedAt: 2000, status: 'healthy', code: 'expected_status' }], schedulerCapture: { version: 1, ...expected, pid: 42, processStartTicks: '123', epoch: policyId, createdAt: 900, expiresAt: 600900, overflowed: false, expired: false, clockRegressed: false, events: (complete ? ['admitted', 'started'] : ['admitted']).map((kind, index) => ({ kind, policyId, policyVersion: policyId, attemptHash: 'b'.repeat(64), dueAt: 1000, at: 1050 + index, monotonicMs: 100 + index })) } })
}

test('rereads until delayed publication completes while preserving the original workload observation window', async () => {
    const initial = snapshot(), reads: number[] = []
    let now = 0
    const result = await settleBackgroundCapture(initial, expected, {
        signal: new AbortController().signal, now: () => now, sleep: async (ms) => { now += ms },
        read: async (remaining) => {
            reads.push(remaining)
            return reads.length < 3 ? snapshot() : { ...snapshot(true), origin: 5000, observations: [] }
        },
    }, 1000)
    expect(result).toMatchObject({ status: 'complete', reads: 3, elapsedMs: 50 })
    expect(reads).toEqual([1000, 975, 950])
    expect(result.snapshot.observations).toEqual(initial.observations)
    expect(result.snapshot.origin).toBe(initial.origin)
    expect(result.snapshot.schedulerCapture!.events).toHaveLength(2)
})

test('missing, failed or late capture stops at its bound and never qualifies late data', async () => {
    let now = 0, calls = 0
    const initial = snapshot()
    const timed = await settleBackgroundCapture(initial, expected, { signal: new AbortController().signal, now: () => now, sleep: async (ms) => { now += ms }, read: async () => { calls++; return { ...initial, schedulerCapture: null } } }, 300)
    expect(timed).toMatchObject({ status: 'deadline', reads: 2, elapsedMs: 50 })
    expect(calls).toBe(2)
    const late = await settleBackgroundCapture(initial, expected, { signal: new AbortController().signal, now: () => now, read: async (remaining) => { now += remaining + 1; return snapshot(true) } }, 300)
    expect(late.status).toBe('deadline')
    expect(late.snapshot).toBe(initial)
    const failed = await settleBackgroundCapture(initial, expected, { signal: new AbortController().signal, read: async () => { throw new Error('private capture details') } })
    expect(failed).toMatchObject({ status: 'unavailable', reads: 1 })
    expect(JSON.stringify(failed)).not.toContain('private capture details')
})

test('abort after a bounded actual read preserves cancellation rather than success or retry', async () => {
    const controller = new AbortController()
    let reads = 0
    await expect(settleBackgroundCapture(snapshot(), expected, { signal: controller.signal, read: async () => { reads++; controller.abort(); return snapshot(true) } })).rejects.toMatchObject({ category: 'cancelled' })
    expect(reads).toBe(1)
})
