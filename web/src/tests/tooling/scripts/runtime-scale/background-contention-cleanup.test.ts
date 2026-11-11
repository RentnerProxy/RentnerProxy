// oxlint-disable no-await-in-loop -- Each failure-identity case owns its own workload and cleanup outcome.
import { expect, test } from 'bun:test'
import { withContentionCleanup } from '../../../../../../scripts/runtime-scale/background-contention-run.ts'

test('cleanup cannot replace the original workload failure, including falsy thrown values', async () => {
    for (const primary of [
        new Error('synthetic workload failure'),
        undefined,
        null,
        false,
        0,
        '',
    ]) {
        let stringified = false
        const stopFailure = {
            toString() {
                stringified = true
                return 'untrusted stop details'
            },
        }
        const categories: string[] = []
        let stops = 0
        const outcome = await withContentionCleanup(
            async () => {
                throw primary
            },
            async () => {
                stops++
                throw stopFailure
            },
            (category) => {
                categories.push(category)
            },
        ).then(
            (value) => ({ status: 'fulfilled' as const, value }),
            (reason: unknown) => ({ status: 'rejected' as const, reason }),
        )
        expect(outcome.status).toBe('rejected')
        if (outcome.status === 'rejected') expect(outcome.reason).toBe(primary)
        expect(stops).toBe(1)
        expect(categories).toEqual(['contention-stop-failed'])
        expect(JSON.stringify(categories)).not.toContain('untrusted stop details')
        expect(stringified).toBe(false)
    }
})

test('a cleanup-only failure still rejects with its original identity, including null/undefined', async () => {
    for (const stopFailure of [new Error('synthetic stop failure'), null, undefined]) {
        const categories: string[] = []
        const outcome = await withContentionCleanup(
            async () => 'completed',
            async () => {
                throw stopFailure
            },
            (category) => {
                categories.push(category)
            },
        ).then(
            (value) => ({ status: 'fulfilled' as const, value }),
            (reason: unknown) => ({ status: 'rejected' as const, reason }),
        )
        expect(outcome.status).toBe('rejected')
        if (outcome.status === 'rejected') expect(outcome.reason).toBe(stopFailure)
        expect(categories).toEqual(['contention-stop-failed'])
    }
})

test('successful cleanup preserves workload results and a diagnostic failure cannot mask failures', async () => {
    const order: string[] = []
    const value = {}
    expect(
        await withContentionCleanup(
            async () => {
                order.push('workload')
                return value
            },
            async () => {
                order.push('stop')
            },
            () => {
                throw new Error('unexpected diagnostic')
            },
        ),
    ).toBe(value)
    expect(order).toEqual(['workload', 'stop'])
    const primary = new Error('synthetic workload failure')
    const stopFailure = new Error('synthetic stop failure')
    const outcome = await withContentionCleanup(
        async () => {
            throw primary
        },
        async () => {
            throw stopFailure
        },
        () => {
            throw new Error('synthetic diagnostic failure')
        },
    ).then(
        () => undefined,
        (reason: unknown) => reason,
    )
    expect(outcome).toBe(primary)
})
