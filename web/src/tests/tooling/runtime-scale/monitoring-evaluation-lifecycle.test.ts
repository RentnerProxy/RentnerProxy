// oxlint-disable no-await-in-loop -- Each fault case owns an independent set of deferred probes or cleanup operations.
import { expect, test } from 'bun:test'
import {
    cleanupMonitoringEvaluation,
    runMonitoringEvaluationProbes,
} from '../../../../../scripts/runtime-scale/monitoring-evaluation-lifecycle.ts'

test('a failed probe stops new admissions and every admitted sibling settles before cleanup', async () => {
    const first = Promise.withResolvers<void>()
    const siblings = Promise.withResolvers<void>()
    const started: number[] = []
    const cleanupActive: number[] = []
    const failure = new Error('synthetic first probe failure')
    let active = 0
    const run = async () => {
        try {
            await runMonitoringEvaluationProbes(130, async (id) => {
                started.push(id)
                active++
                try {
                    await (id === 1 ? first.promise : siblings.promise)
                } finally {
                    active--
                }
            })
        } finally {
            await cleanupMonitoringEvaluation([
                () => {
                    cleanupActive.push(active)
                },
            ])
        }
    }
    const completed = run().then(
        () => undefined,
        (error) => error as unknown,
    )
    expect(started).toHaveLength(64)
    first.reject(failure)
    await new Promise<void>((resolve) => setImmediate(resolve))
    expect(cleanupActive).toEqual([])
    expect(active).toBe(63)
    siblings.resolve()
    expect(await completed).toBe(failure)
    expect(started).toHaveLength(64)
    expect(cleanupActive).toEqual([0])
})

test('successful bounded workers cover every policy and preserve falsy first failures', async () => {
    const ids: number[] = []
    let active = 0,
        peak = 0
    await runMonitoringEvaluationProbes(130, async (id) => {
        active++
        peak = Math.max(peak, active)
        await Promise.resolve()
        ids.push(id)
        active--
    })
    expect(new Set(ids).size).toBe(130)
    expect(peak).toBe(64)
    for (const failure of [undefined, null]) {
        const outcome = await runMonitoringEvaluationProbes(2, async (id) => {
            if (id === 1) throw failure
            await Promise.resolve()
            throw new Error('synthetic later failure')
        }).then(
            () => ({ fulfilled: true }),
            (error) => ({ fulfilled: false, error: error as unknown }),
        )
        expect(outcome).toEqual({ fulfilled: false, error: failure })
    }
})

test('each cleanup failure still permits worker/schema/role/database/storage cleanup and is retained', async () => {
    const steps = ['upstream', 'store', 'worker', 'schema', 'role', 'database', 'temporary-storage']
    for (const failed of steps) {
        const visited: string[] = []
        const failure = new Error('synthetic ' + failed + ' cleanup failure')
        const outcome = await cleanupMonitoringEvaluation(
            steps.map((step) => async () => {
                visited.push(step)
                if (step === failed) throw failure
            }),
        ).then(
            () => undefined,
            (error) => error as unknown,
        )
        expect(visited).toEqual(steps)
        expect(outcome).toBeInstanceOf(AggregateError)
        expect((outcome as AggregateError).errors).toEqual([failure])
    }
})

test('cleanup aggregation preserves the execution failure plus every independent cleanup error', async () => {
    const execution = new Error('synthetic execution failure')
    const store = new Error('synthetic store-close failure')
    const schema = new Error('synthetic schema-drop failure')
    const visited: string[] = []
    const outcome = await cleanupMonitoringEvaluation(
        [
            () => {
                visited.push('store')
                throw store
            },
            async () => {
                visited.push('worker')
                await Promise.resolve()
            },
            () => {
                visited.push('schema')
                throw schema
            },
            () => {
                visited.push('role')
            },
            () => {
                visited.push('database')
            },
            () => {
                visited.push('temporary-storage')
            },
        ],
        [execution],
    ).then(
        () => undefined,
        (error) => error as unknown,
    )
    expect(visited).toEqual(['store', 'worker', 'schema', 'role', 'database', 'temporary-storage'])
    expect(outcome).toBeInstanceOf(AggregateError)
    expect((outcome as AggregateError).errors).toEqual([execution, store, schema])
})
