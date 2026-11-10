import { describe, expect, test } from 'bun:test'

import { workerObservability } from '@/server/Foundation/worker-observability.ts'
import { observeWorkerOperation } from '@/server/Foundation/worker-operation.ts'

describe('observed worker execution', () => {
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
