import { expect, test } from 'bun:test'
import {
    createHostMonitoringCapture,
    hostMonitoringCaptureSchema,
} from '@/server/HostMonitoring/host-monitoring.capture.ts'
import type { HostMonitoringClaim } from '@/server/HostMonitoring/Types/host-monitoring.repository.types.ts'
import type { HostMonitoringCapture } from '@/server/HostMonitoring/host-monitoring.capture.ts'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

const claim = {
    policy: {
        id: '00000000-0000-4000-8000-000000000001',
        version: '00000000-0000-4000-8000-000000000002',
        nextCheckAt: new Date(1000),
    },
    leaseToken: 'secret-lease-must-never-leave-observer',
} as HostMonitoringClaim
const identity = {
    runId: '012345abcdef',
    sourceSha: 'a'.repeat(40),
    pid: 42,
    processStartTicks: '123',
}

test('capture records actual observation kinds with hashed correlation and bounded sanitized output', async () => {
    const writes: HostMonitoringCapture[] = []
    let at = 1000
    const capture = createHostMonitoringCapture({
        ...identity,
        now: () => at,
        monotonic: () => at - 900,
        write: async (value) => {
            writes.push(value)
        },
    })
    capture.record('admitted', claim)
    at = 1007
    capture.record('started', claim)
    await capture.flush()
    const value = writes[0]!
    expect(value.events.map((event) => event.kind)).toEqual(['admitted', 'started'])
    expect(value.events[0]!.attemptHash).toBe(value.events[1]!.attemptHash)
    expect(value.events[1]!.monotonicMs - value.events[0]!.monotonicMs).toBe(7)
    expect(JSON.stringify(value)).not.toContain(claim.leaseToken)
    expect(hostMonitoringCaptureSchema.safeParse({ ...value, token: 'private' }).success).toBe(
        false,
    )
})

test('capture overflow, clock regression and expiry remain explicit instead of inventing complete timing', async () => {
    let value: HostMonitoringCapture | undefined
    let at = 1000
    const capture = createHostMonitoringCapture({
        ...identity,
        capacity: 2,
        now: () => at,
        monotonic: () => at,
        write: async (snapshot) => {
            value = snapshot
        },
    })
    capture.record('admitted', claim)
    at = 999
    capture.record('cancelled', claim)
    capture.record('started', claim)
    await capture.flush()
    expect(value).toMatchObject({ overflowed: true, clockRegressed: true })
    expect(value!.events).toHaveLength(2)
    const expiring = createHostMonitoringCapture({
        ...identity,
        lifetimeMs: 10,
        now: () => at,
        write: async (snapshot) => {
            value = snapshot
        },
    })
    at += 11
    expiring.record('admitted', claim)
    await expiring.flush()
    expect(value).toMatchObject({ expired: true, events: [] })
})

test('single-flight capture retains late cancellation through actual writer settlement and tolerates unavailable storage', async () => {
    const entered = Promise.withResolvers<void>(),
        release = Promise.withResolvers<void>()
    const writes: HostMonitoringCapture[] = []
    let active = 0,
        maximum = 0
    const capture = createHostMonitoringCapture({
        ...identity,
        write: async (value) => {
            active++
            maximum = Math.max(maximum, active)
            writes.push(value)
            entered.resolve()
            await release.promise
            active--
        },
    })
    capture.record('admitted', claim)
    const draining = capture.flush()
    await entered.promise
    capture.record('cancelled', claim)
    release.resolve()
    await draining
    expect(maximum).toBe(1)
    expect(writes.at(-1)!.events.map((event) => event.kind)).toEqual(['admitted', 'cancelled'])
    const unavailable = createHostMonitoringCapture({
        ...identity,
        write: async () => {
            throw new Error('private filesystem path')
        },
    })
    unavailable.record('admitted', claim)
    await expect(unavailable.flush()).resolves.toBeUndefined()
    expect(() => unavailable.record('started', claim)).not.toThrow()
})

test('capture lifetime revokes the published lease even without another record', async () => {
    let held = false
    const capture = createHostMonitoringCapture({ ...identity, lifetimeMs: 10, now: () => 1000,
        invalidate: () => { held = false },
        write: async (_value, isCurrent) => { held = isCurrent() },
    })
    capture.record('admitted', claim)
    await capture.flush()
    expect(held).toBe(true)
    await Bun.sleep(30)
    expect(held).toBe(false)
    await capture.flush()
    expect(held).toBe(false)
})

test('throwing record revocation terminally disables telemetry without escaping or retrying close', async () => {
    let invalidations = 0, writes = 0
    const capture = createHostMonitoringCapture({ ...identity, invalidate: () => { invalidations++; throw new Error('injected close error') }, write: async () => { writes++ } })
    expect(() => capture.record('admitted', claim)).not.toThrow()
    await expect(capture.flush()).resolves.toBeUndefined()
    expect(() => capture.record('started', claim)).not.toThrow()
    expect(invalidations).toBe(1)
    expect(writes).toBe(0)
})

test('write failure publishes terminal state before a throwing revocation and never rejects the observer flush', async () => {
    let invalidations = 0, writes = 0
    const guards: (boolean | undefined)[] = []
    let current: (() => boolean) | undefined
    const capture = createHostMonitoringCapture({ ...identity,
        invalidate: () => { if (++invalidations > 1) { guards.push(current?.()); throw new Error('injected close error') } },
        write: async (_value, isCurrent) => { writes++; current = isCurrent; throw new Error('injected write failure') },
    })
    capture.record('admitted', claim)
    await expect(capture.flush()).resolves.toBeUndefined()
    capture.record('started', claim)
    await capture.flush()
    expect(invalidations).toBe(2)
    expect(writes).toBe(1)
    expect(guards).toEqual([false])
})

test('expiry marks terminal state before a throwing revocation without an uncaught timer error or another close', async () => {
    let invalidations = 0
    const guards: (boolean | undefined)[] = []
    let current: (() => boolean) | undefined
    const capture = createHostMonitoringCapture({ ...identity, lifetimeMs: 10, now: () => 1000,
        invalidate: () => { if (++invalidations > 1) { guards.push(current?.()); throw new Error('injected close error') } },
        write: async (_value, isCurrent) => { current = isCurrent },
    })
    capture.record('admitted', claim)
    await capture.flush()
    await Bun.sleep(30)
    capture.record('started', claim)
    await expect(capture.flush()).resolves.toBeUndefined()
    expect(invalidations).toBe(2)
    expect(guards).toEqual([false])
})

test('production adapter observes returned claims before starts and flushes after actual worker stop', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        const events = []
        let dependencies, stopped = false
        const claimed = Promise.withResolvers()
        mock.module('./server/HostMonitoring/host-monitoring.capture.server.ts', () => ({ hostMonitoringCapture: {
            record: kind => events.push(kind),
            flush: async () => { if (!stopped) throw new Error('flushed before stop'); events.push('flushed') },
        } }))
        mock.module('./server/Controller/domain-diagnostics.server.ts', () => ({ inspectControllerDomain: async () => {} }))
        mock.module('./server/HostMonitoring/host-monitoring.probe.ts', () => ({ createHostMonitoringProbe: () => async () => {} }))
        mock.module('./server/HostMonitoring/host-monitoring.worker.ts', () => ({ createHostMonitoringWorker: value => {
            dependencies = value
            return { start() {}, runOnce: async () => {}, stop: async () => { stopped = true } }
        } }))
        mock.module('./server/HostMonitoring/host-monitoring.repository.ts', () => ({
            claimDueHostMonitoringPolicies: () => claimed.promise,
            completeHostMonitoringClaims: async () => [],
            readHostMonitoringClaimContexts: async () => [],
            releaseHostMonitoringClaims: async claims => claims.map(() => true),
            pruneHostMonitoringHistory: async () => {}, readNextHostMonitoringAdmissionAt: async () => null,
        }))
        mock.module('./server/WebSockets/applicationChanges.ts', () => ({ publishApplicationChange() {}, subscribeToApplicationChanges: () => () => {} }))
        const { hostMonitoringWorker } = await import('./server/HostMonitoring/host-monitoring.worker.server.ts')
        const pending = dependencies.claim(1)
        await Bun.sleep(0)
        if (events.length) throw new Error('claim admission fabricated before return')
        claimed.resolve([{ policy: { id: 'fixture' } }])
        const claims = await pending
        await dependencies.runClaim(claims[0], new AbortController().signal)
        const cancelled = new AbortController(); cancelled.abort()
        await dependencies.runClaim(claims[0], cancelled.signal)
        await dependencies.releaseCancelled(claims[0])
        await hostMonitoringWorker.stop()
        console.log(JSON.stringify(events))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    expect(JSON.parse(output)).toEqual(['admitted', 'started', 'cancelled', 'cancelled', 'flushed'])
})

test('normal and malformed opt-in never enable filesystem capture', async () => {
    const output = await runBunScript(
        `
        process.env.RENTNERPROXY_BACKGROUND_CAPTURE_RUN = '../unsafe'
        process.env.RENTNERPROXY_BACKGROUND_CAPTURE_SOURCE_SHA = 'a'.repeat(40)
        const { hostMonitoringCapture } = await import('./server/HostMonitoring/host-monitoring.capture.server.ts')
        console.log(JSON.stringify({ disabled: hostMonitoringCapture === undefined }))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    expect(JSON.parse(output)).toEqual({ disabled: true })
})

test.skipIf(process.platform !== 'linux')(
    'Linux opt-in writer publishes private atomic snapshots and identifies its actual process',
    async () => {
        const output = await runBunScript(
            `
        import { readFile, stat, unlink } from 'node:fs/promises'
        import { randomBytes } from 'node:crypto'
        const runId = randomBytes(6).toString('hex')
        process.env.RENTNERPROXY_BACKGROUND_CAPTURE_RUN = runId
        process.env.RENTNERPROXY_BACKGROUND_CAPTURE_SOURCE_SHA = 'a'.repeat(40)
        const path = '/tmp/rentnerproxy-background-capture-' + runId + '.json'
        try {
            const { hostMonitoringCapture } = await import('./server/HostMonitoring/host-monitoring.capture.server.ts')
            if (!hostMonitoringCapture) throw new Error('opt-in missing')
            const claim = { policy: { id: '00000000-0000-4000-8000-000000000001', version: '00000000-0000-4000-8000-000000000002', nextCheckAt: new Date(Date.now() - 1) }, leaseToken: 'secret-test-lease' }
            hostMonitoringCapture.record('admitted', claim)
            hostMonitoringCapture.record('started', claim)
            await hostMonitoringCapture.flush()
            const text = await readFile(path, 'utf8'), value = JSON.parse(text)
            const metadata = await stat(path)
            console.log(JSON.stringify({ ownPid: value.pid === process.pid, privateMode: (metadata.mode & 0o777) === 0o600, count: value.events.length, redacted: !text.includes(claim.leaseToken), startTicks: /^\\d+$/.test(value.processStartTicks) }))
        } finally { await unlink(path).catch(() => undefined) }
    `,
            { cwd: new URL('../../..', import.meta.url) },
        )
        expect(JSON.parse(output)).toEqual({
            ownPid: true,
            privateMode: true,
            count: 2,
            redacted: true,
            startTicks: true,
        })
    },
)
