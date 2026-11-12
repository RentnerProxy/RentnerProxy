import { expect, test } from 'bun:test'
import { contentionReceiverLifetimeMs } from '../../../../../../scripts/runtime-scale/background-contention-run.ts'
import { parseContentionReceiverLifetime } from '../../../../../../scripts/runtime-scale/background-contention-receiver.ts'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test('receiver lifetime covers the maximum measurement window plus bounded setup, recovery and drain', () => {
    const lifetime = contentionReceiverLifetimeMs(300)
    expect(lifetime).toBeGreaterThan(300_000 + 90_000 + 60_000 + 150_000 + 90_000)
    expect(lifetime).toBeLessThanOrEqual(1_200_000)
    expect(contentionReceiverLifetimeMs(30)).toBeLessThan(lifetime)
    expect(parseContentionReceiverLifetime(String(lifetime))).toBe(lifetime)
    for (const value of [undefined, '', '0', '-1', '1e3', 'NaN', '1200001'])
        expect(() => parseContentionReceiverLifetime(value)).toThrow(
            'Invalid contention receiver lifetime',
        )
    for (const value of [0, -1, 301, NaN, 1.5])
        expect(() => contentionReceiverLifetimeMs(value)).toThrow('Invalid contention duration')
})

test('the supplied abandoned-receiver deadline closes every owned listener once and explicit stop cancels its timer', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        let closed = 0, discoveryStopped = 0, controlStopped = 0
        const timers = [], cancelled = []
        globalThis.setTimeout = (callback, delay) => { const timer = { callback, delay, unref() {} }; timers.push(timer); return timer }
        globalThis.clearTimeout = timer => { cancelled.push(timer.delay) }
        mock.module('./scripts/runtime-scale/background-receivers.ts', () => ({ startBackgroundNotificationReceivers: async () => ({ close: async () => { closed++ } }) }))
        mock.module('./scripts/runtime-scale/background-discovery.ts', () => ({ startBackgroundDockerEngine: () => ({ stop: () => { discoveryStopped++ } }) }))
        Bun.serve = () => ({ stop: () => { controlStopped++ } })
        const { startContentionReceivers } = await import('./scripts/runtime-scale/background-contention-receiver.ts')
        const receivers = await startContentionReceivers(1, undefined, 1_200_000)
        const before = { delay: timers[0].delay, closed, discoveryStopped, controlStopped }
        timers[0].callback()
        await receivers.stop()
        await receivers.stop()
        const abandoned = { closed, discoveryStopped, controlStopped, cancelled: [...cancelled] }
        const stopped = await startContentionReceivers(1, undefined, 930_000)
        await stopped.stop()
        console.log(JSON.stringify({ before, abandoned, explicit: { closed, discoveryStopped, controlStopped, cancelled } }))
    `,
        { cwd: new URL('../../../../../..', import.meta.url) },
    )
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
        before: { delay: 1_200_000, closed: 0, discoveryStopped: 0, controlStopped: 0 },
        abandoned: { closed: 1, discoveryStopped: 1, controlStopped: 1, cancelled: [1_200_000] },
        explicit: {
            closed: 2,
            discoveryStopped: 2,
            controlStopped: 2,
            cancelled: [1_200_000, 930_000],
        },
    })
})
