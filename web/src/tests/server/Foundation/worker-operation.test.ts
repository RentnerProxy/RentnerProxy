import { describe, expect, test } from 'bun:test'

import { workerObservability } from '@/server/Foundation/worker-observability.ts'
import { observeWorkerOperation } from '@/server/Foundation/worker-operation.ts'

describe('observed worker execution', () => {
    test('a combined deadline abort records a dependency failure while shutdown stays cancelled', async () => {
        workerObservability.reset()
        const shutdown = new AbortController()
        const timeout = AbortSignal.abort(new DOMException('Fixture deadline', 'TimeoutError'))
        const deadlineSignal = AbortSignal.any([shutdown.signal, timeout])
        await expect(
            observeWorkerOperation(
                'notifications',
                async () => {
                    throw deadlineSignal.reason
                },
                deadlineSignal,
                undefined,
                'collection',
            ),
        ).rejects.toBe(timeout.reason)
        shutdown.abort()
        await expect(
            observeWorkerOperation(
                'notifications',
                async () => {
                    throw shutdown.signal.reason
                },
                shutdown.signal,
                undefined,
                'collection',
            ),
        ).rejects.toBe(shutdown.signal.reason)
        expect(
            workerObservability.snapshot().workers.find((item) => item.id === 'notifications'),
        ).toMatchObject({
            state: 'degraded',
            degradedLanes: ['collection'],
            lastSuccessAt: null,
            counters: { failure: 1, cancelled: 1, success: 0 },
        })
    })

    test('records actual work, preserves error identity, cancellation and maintenance state', async () => {
        workerObservability.reset()
        const pending = Promise.withResolvers<boolean>()
        const operation = observeWorkerOperation('host_monitoring', () => pending.promise)
        expect(
            workerObservability.snapshot().workers.find((worker) => worker.id === 'host_monitoring')
                ?.running,
        ).toBe(1)
        workerObservability.state('host_monitoring', 'maintenance_paused')
        pending.resolve(true)
        await operation
        const snapshot = workerObservability
            .snapshot()
            .workers.find((worker) => worker.id === 'host_monitoring')!
        expect(snapshot.state).toBe('maintenance_paused')
        expect(snapshot.counters.success).toBe(1)
        const error = new Error('private payload stays outside diagnostics')
        await expect(
            observeWorkerOperation('notifications', async () => {
                throw error
            }),
        ).rejects.toBe(error)
        const abort = new AbortController()
        abort.abort()
        await expect(
            observeWorkerOperation(
                'backup_outcomes',
                async () => {
                    throw abort.signal.reason
                },
                abort.signal,
            ),
        ).rejects.toBe(abort.signal.reason)
        expect(
            workerObservability.snapshot().workers.find((worker) => worker.id === 'backup_outcomes')
                ?.counters.cancelled,
        ).toBe(1)
        expect(JSON.stringify(workerObservability.snapshot())).not.toContain(error.message)
        await observeWorkerOperation(
            'notifications',
            async () => ({ success: true }),
            abort.signal,
            (result) => (result.success ? 'success' : 'failure'),
        )
        expect(
            workerObservability.snapshot().workers.find((worker) => worker.id === 'notifications')
                ?.counters.success,
        ).toBe(1)
    })
})
