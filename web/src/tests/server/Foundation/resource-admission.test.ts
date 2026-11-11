// oxlint-disable no-await-in-loop -- The tests exercise FIFO acquisition and release order explicitly.
import { describe, expect, test } from 'bun:test'
import { BACKGROUND_DATABASE_POLICY } from '@/config/background-resources.config.ts'
import {
    createResourceAdmission,
    ResourceAdmissionError,
} from '@/server/Foundation/resource-admission.ts'

function clock() {
    let now = 0
    const timers = new Map<() => void, number>()
    return {
        now: () => now,
        schedule(callback: () => void, delay: number) {
            timers.set(callback, now + delay)
            return () => {
                timers.delete(callback)
            }
        },
        advance(milliseconds: number, fire = true) {
            now += milliseconds
            if (fire) for (const [callback, due] of timers) if (due <= now) callback()
        },
        timers,
    }
}

describe('resource admission', () => {
    test('bounds running/pending/timers and admits oldest waiters before new work', async () => {
        const time = clock()
        const admission = createResourceAdmission({ running: 1, pending: 2, waitMs: 100 }, time)
        const release = await admission.acquire()
        const first = admission.acquire()
        const second = admission.acquire()
        await expect(admission.acquire()).rejects.toMatchObject({ reason: 'busy' })
        expect(admission.snapshot()).toMatchObject({ running: 1, pending: 2, busy: 1 })
        expect(time.timers.size).toBe(2)
        time.advance(50)
        expect(admission.snapshot().oldestPendingAgeMs).toBe(50)
        release()
        release()
        const releaseFirst = await first
        expect(admission.snapshot()).toMatchObject({ running: 1, pending: 1 })
        const third = admission.acquire()
        releaseFirst()
        const releaseSecond = await second
        expect(admission.snapshot().pending).toBe(1)
        releaseSecond()
        ;(await third)()
        expect(admission.snapshot().running).toBe(0)
        expect(time.timers.size).toBe(0)
    })

    test('deadline and abort remove exactly their bounded waiter and never start work', async () => {
        const time = clock()
        const admission = createResourceAdmission({ running: 1, pending: 2, waitMs: 100 }, time)
        const release = await admission.acquire()
        const controller = new AbortController()
        let executed = 0
        const cancelled = admission
            .run(async () => {
                executed++
            }, controller.signal)
            .catch((error: ResourceAdmissionError) => error.reason)
        const expired = admission
            .run(async () => {
                executed++
            })
            .catch((error: ResourceAdmissionError) => error.reason)
        controller.abort()
        time.advance(100)
        expect(await cancelled).toBe('cancelled')
        expect(await expired).toBe('deadline')
        expect(executed).toBe(0)
        expect(admission.snapshot()).toMatchObject({ pending: 0, cancelled: 1, deadline: 1 })
        expect(time.timers.size).toBe(0)
        release()
        await expect(
            admission.run(async () => {
                executed++
                return 7
            }),
        ).resolves.toBe(7)
        expect(executed).toBe(1)
    })

    test('checks absolute deadlines when a delayed event loop has not fired the timeout callback', async () => {
        const time = clock()
        const admission = createResourceAdmission({ running: 1, pending: 1, waitMs: 100 }, time)
        const release = await admission.acquire()
        const pending = admission.acquire().catch((error: ResourceAdmissionError) => error.reason)
        time.advance(101, false)
        release()
        expect(await pending).toBe('deadline')
        expect(time.timers.size).toBe(0)
        expect(admission.snapshot().running).toBe(0)
    })

    test('releases on failed operation and cancellation racing immediate admission', async () => {
        const admission = createResourceAdmission({ running: 1, pending: 0, waitMs: 100 })
        await expect(
            admission.run(async () => {
                throw new Error('operation failed')
            }),
        ).rejects.toThrow('operation failed')
        const controller = new AbortController()
        let ran = false
        const result = admission.run(async () => {
            ran = true
        }, controller.signal)
        controller.abort()
        await expect(result).rejects.toBeInstanceOf(ResourceAdmissionError)
        expect(ran).toBe(false)
        expect(admission.snapshot().running).toBe(0)
    })

    test('incident and discovery storms cannot consume critical or interactive partitions', async () => {
        const classes = BACKGROUND_DATABASE_POLICY.classes
        const admissions = Object.fromEntries(
            Object.entries(classes).map(([key, policy]) => [key, createResourceAdmission(policy)]),
        )
        const releases: Array<() => void> = []
        for (const key of ['monitoring', 'notifications', 'discovery', 'housekeeping'] as const) {
            for (let index = 0; index < classes[key].running; index++)
                releases.push(await admissions[key]!.acquire())
            const pending = Array.from({ length: classes[key].pending }, () =>
                admissions[key]!.acquire(),
            )
            await expect(admissions[key]!.acquire()).rejects.toMatchObject({ reason: 'busy' })
            // Drain rather than leaving timers/tasks behind in the test.
            while (releases.length) releases.shift()!()
            for (const item of pending) (await item)()
        }
        const normal = await Promise.all(
            Object.entries(classes)
                .filter(([key]) => key !== 'critical')
                .flatMap(([key, policy]) =>
                    Array.from({ length: policy.running }, () => admissions[key]!.acquire()),
                ),
        )
        const critical = await Promise.all(
            Array.from({ length: classes.critical.running }, () => admissions.critical!.acquire()),
        )
        expect(normal.length + critical.length).toBe(BACKGROUND_DATABASE_POLICY.ceiling)
        expect(BACKGROUND_DATABASE_POLICY.interactiveHeadroom).toBe(8)
        for (const release of [...normal, ...critical]) release()
    })
})
