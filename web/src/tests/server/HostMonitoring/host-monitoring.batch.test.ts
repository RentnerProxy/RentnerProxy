import { describe, expect, test } from 'bun:test'
import { createHostMonitoringBatcher } from '@/server/HostMonitoring/host-monitoring.batch.ts'

function fulfilled<T>(value: T): PromiseFulfilledResult<T> {
    return { status: 'fulfilled', value }
}

describe('host monitoring storage batching', () => {
    test('caps admission at 64, runs sequential batches of 16 and settles only after durable completion', async () => {
        const committed = Promise.withResolvers<void>()
        const batches: (readonly number[])[] = []
        let active = 0
        let maxActive = 0
        let delivered = 0
        const enqueue = createHostMonitoringBatcher<number, number>({
            size: 16,
            delayMs: 1,
            run: async (inputs) => {
                batches.push(inputs)
                active++
                maxActive = Math.max(active, maxActive)
                await committed.promise
                active--
                return inputs.map((value) => fulfilled(value * 2))
            },
        })
        const results = Array.from({ length: 64 }, (_, value) =>
            enqueue(value).then((output) => {
                delivered++
                return output
            }),
        )
        const completed = Promise.all(results)
        await expect(enqueue(64)).rejects.toThrow('admission is full')
        await Bun.sleep(0)
        expect(batches).toEqual([Array.from({ length: 16 }, (_, value) => value)])
        expect(delivered).toBe(0)
        committed.resolve()
        expect(await completed).toEqual(Array.from({ length: 64 }, (_, value) => value * 2))
        expect(batches.map((batch) => batch.length)).toEqual([16, 16, 16, 16])
        expect(maxActive).toBe(1)
        expect(await enqueue(65)).toBe(130)
    })

    test('flushes a partial batch and queued tail after a slow full batch', async () => {
        const entered = Promise.withResolvers<void>()
        const committed = Promise.withResolvers<void>()
        const batches: (readonly string[])[] = []
        const enqueue = createHostMonitoringBatcher<string, string>({
            size: 2,
            delayMs: 1,
            run: async (inputs) => {
                batches.push(inputs)
                if (batches.length === 1) {
                    entered.resolve()
                    await committed.promise
                }
                return inputs.map(fulfilled)
            },
        })
        const first = enqueue('one')
        const second = enqueue('two')
        const tail = enqueue('three')
        const completed = Promise.all([first, second, tail])
        await entered.promise
        expect(batches).toEqual([['one', 'two']])
        committed.resolve()
        expect(await completed).toEqual(['one', 'two', 'three'])
        expect(batches).toEqual([['one', 'two'], ['three']])
    })

    test('rejects a failed root batch, isolates per-item errors and continues flushing', async () => {
        const rootError = new Error('root commit failed')
        const itemError = new Error('one fenced write failed')
        let calls = 0
        const enqueue = createHostMonitoringBatcher<number, number>({
            size: 2,
            delayMs: 1,
            run: async (inputs) => {
                calls++
                if (calls === 1) throw rootError
                return inputs.map((value) =>
                    value === 2 ? { status: 'rejected', reason: itemError } : fulfilled(value),
                )
            },
        })
        // Attach rejection handlers before storage settles any individual request.
        const results = await Promise.allSettled(
            Array.from({ length: 5 }, (_, value) => enqueue(value)),
        )
        expect(results).toEqual([
            { status: 'rejected', reason: rootError },
            { status: 'rejected', reason: rootError },
            { status: 'rejected', reason: itemError },
            fulfilled(3),
            fulfilled(4),
        ])
        expect(calls).toBe(3)
    })

    test.each([0, 2])(
        'rejects malformed batch results of length %i without dropping later work',
        async (length) => {
            let calls = 0
            const enqueue = createHostMonitoringBatcher<string, string>({
                size: 1,
                delayMs: 1,
                run: async (inputs) => {
                    calls++
                    return calls === 1
                        ? Array.from({ length }, () => fulfilled('unexpected'))
                        : inputs.map(fulfilled)
                },
            })
            const results = await Promise.allSettled([enqueue('invalid'), enqueue('valid')])
            expect(results[0]).toMatchObject({
                status: 'rejected',
                reason: { message: 'Invalid monitoring batch result.' },
            })
            expect(results[1]).toEqual(fulfilled('valid'))
        },
    )
})
