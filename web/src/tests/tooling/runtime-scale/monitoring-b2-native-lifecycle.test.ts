import { expect, test } from 'bun:test'
import { joinMonitoringB2NativeOwner } from '../../../../../scripts/runtime-scale/monitoring-b2-fixture-storage.ts'

test('replacement joins the failed owner and both streams before spawning exactly one replacement', async () => {
    const exited = Promise.withResolvers<number>()
    const streams = Promise.withResolvers<void>()
    const signals: NodeJS.Signals[] = []
    let replacements = 0
    const settled = Promise.all([exited.promise, streams.promise]).then(([exit]) => exit)
    const replacing = joinMonitoringB2NativeOwner(
        { kill: (signal) => signals.push(signal) },
        settled,
        'replacement',
    ).then(() => {
        replacements++
    })
    await Promise.resolve()
    expect(replacements).toBe(0)
    exited.resolve(9)
    await Promise.resolve()
    expect(replacements).toBe(0)
    streams.resolve()
    await replacing
    expect(replacements).toBe(1)
    expect(signals).toEqual(['SIGINT'])
})

test('a failed stream join cannot prove owner settlement or permit replacement', async () => {
    let replacements = 0
    const replacing = joinMonitoringB2NativeOwner(
        { kill: (_signal: NodeJS.Signals) => {} },
        Promise.reject(new Error('unsettled native stream')),
        'replacement',
    ).then(() => {
        replacements++
    })
    await expect(replacing).rejects.toThrow('unsettled native stream')
    expect(replacements).toBe(0)
})

test('a failed owner cannot acknowledge maintenance even after an earlier failed drain', async () => {
    const owner = { kill: (_signal: NodeJS.Signals) => {} }
    await expect(joinMonitoringB2NativeOwner(owner, Promise.resolve(9), 'maintenance')).rejects.toThrow(
        'B2 native worker failed closed',
    )
    await expect(joinMonitoringB2NativeOwner(owner, Promise.resolve(9), 'maintenance')).rejects.toThrow(
        'B2 native worker failed closed',
    )
})

test('a timed-out owner never overlaps replacement and must finish its forced-kill join', async () => {
    const settled = Promise.withResolvers<number>()
    const killed = Promise.withResolvers<void>()
    const signals: NodeJS.Signals[] = []
    let replacements = 0
    let finished = false
    const replacing = joinMonitoringB2NativeOwner(
        {
            kill(signal) {
                signals.push(signal)
                if (signal === 'SIGKILL') killed.resolve()
            },
        },
        settled.promise,
        'replacement',
        5,
    ).then(
        () => {
            replacements++
            finished = true
        },
        (error: unknown) => {
            finished = true
            return error
        },
    )
    await killed.promise
    expect(signals).toEqual(['SIGINT', 'SIGKILL'])
    expect(replacements).toBe(0)
    expect(finished).toBe(false)
    settled.resolve(137)
    expect(await replacing).toMatchObject({ message: 'B2 native drain exceeded deadline' })
    expect(replacements).toBe(0)
})
