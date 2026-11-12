import { afterEach, expect, spyOn, test } from 'bun:test'
import { runDetachedTask } from '@/lib/Async/runDetachedTask.ts'

afterEach(() => {
    console.error = originalConsoleError
})
const originalConsoleError = console.error

test('starts work during the event and reports a rejected operation once', async () => {
    const failure = new Error('operation failed')
    const reported = Promise.withResolvers<unknown>()
    let started = false
    runDetachedTask(() => {
        started = true
        return Promise.reject(failure)
    }, reported.resolve)
    expect(started).toBe(true)
    expect(await reported.promise).toBe(failure)
})

test('owns synchronous action failures and accepts successful synchronous actions', () => {
    const failures: unknown[] = []
    const failure = new Error('synchronous failure')
    runDetachedTask(
        () => 42,
        (error) => failures.push(error),
    )
    runDetachedTask(
        () => {
            throw failure
        },
        (error) => failures.push(error),
    )
    expect(failures).toEqual([failure])
})

test.each(['throw', 'reject'] as const)(
    'contains a reporter that fails by %s without exposing its error',
    async (mode) => {
        const logged = Promise.withResolvers<void>()
        const log = spyOn(console, 'error').mockImplementation(() => logged.resolve())
        runDetachedTask(
            () => Promise.reject(new Error('private operation details')),
            () => {
                const failure = new Error('private reporter details')
                if (mode === 'throw') throw failure
                return Promise.reject(failure)
            },
        )
        await logged.promise
        expect(log).toHaveBeenCalledTimes(1)
        expect(log).toHaveBeenCalledWith('[async] Error reporting failed')
    },
)

test('keeps the terminal handler safe when both the reporter and console fail', async () => {
    const logged = Promise.withResolvers<void>()
    spyOn(console, 'error').mockImplementation(() => {
        logged.resolve()
        throw new Error('log sink unavailable')
    })
    runDetachedTask(
        () => Promise.reject(new Error('operation failed')),
        () => Promise.reject(new Error('reporter failed')),
    )
    await logged.promise
    await Promise.resolve()
})
