import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test('an early event-loop timer still records a fresh zero-delay sample and stops cleanly', async () => {
    const output = await runBunScript(
        `
        const { workerObservability } = await import('./server/Foundation/worker-observability.ts')
        const { startEventLoopObservation } = await import('./server/Foundation/event-loop-observation.ts')
        workerObservability.reset()
        let tick, delay, time = 1000, cleared = 0
        performance.now = () => time
        globalThis.setTimeout = (callback, milliseconds) => {
            tick = callback; delay = milliseconds
            return { unref() {} }
        }
        globalThis.clearTimeout = () => { cleared++ }
        startEventLoopObservation()
        time += delay - 0.75
        tick()
        process.emit('rentnerproxy:shutdown')
        const sample = workerObservability.snapshot()
        console.log(JSON.stringify({
            count: sample.eventLoopLag.count, maximumMs: sample.eventLoopLag.maximumMs,
            sampled: sample.eventLoopLag.sampledAt !== null, cleared,
            inventedSuccess: sample.workers.some(worker => worker.lastSuccessAt !== null),
        }))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
        count: 1,
        maximumMs: 0,
        sampled: true,
        cleared: 1,
        inventedSuccess: false,
    })
})
