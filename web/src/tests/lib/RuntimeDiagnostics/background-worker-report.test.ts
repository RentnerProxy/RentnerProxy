import { describe, expect, test } from 'bun:test'

import { createWorkerObservability } from '@/server/Foundation/worker-observability.ts'
import {
    createRuntimeSupportReport,
    serializeRuntimeSupportReport,
} from '@/lib/RuntimeDiagnostics/runtimeSupportReport.ts'
import {
    readBunWorkerReport,
    readControllerWorkerReport,
} from '@/lib/RuntimeDiagnostics/backgroundWorkerReport.ts'
import { supportReportFixture } from './runtime-support-report.fixture.ts'

describe('background worker report allowlist', () => {
    test('reports a stalled operation without advancing its progress when snapshots are read', () => {
        let now = 1_000
        const metrics = createWorkerObservability({ now: () => now })
        metrics.begin('host_monitoring')
        now = 60_000
        const report = readBunWorkerReport(metrics.snapshot())
        const worker = report?.workers.find((item) => item.id === 'host_monitoring')
        expect(worker?.state).toBe('running')
        expect(worker?.running).toBe(1)
        expect(worker?.lastSuccessAt).toBeNull()
        expect(worker?.lastStartedAt).toBe(1_000)
    })

    test('drops hostile nested fields and serialization hooks from both process snapshots', () => {
        const metrics = createWorkerObservability({ now: () => 1_000 }).snapshot()
        const marker = 'PRIVATE-NOTIFICATION-TOKEN'
        const input = {
            ...metrics,
            toJSON: () => ({ marker }),
            workers: metrics.workers.map((worker) =>
                Object.assign({}, worker, {
                    domain: marker,
                    counters: { ...worker.counters, [marker]: 900 },
                    duration: { ...worker.duration, rawError: marker },
                    toJSON: () => ({ marker }),
                }),
            ),
        }
        const fixture = supportReportFixture()
        const controller = fixture.controllerWorkers as Record<string, unknown>
        const report = createRuntimeSupportReport(
            {
                ...fixture,
                bunWorkers: input,
                controllerWorkers: {
                    ...controller,
                    rawError: { nested: marker },
                    toJSON: () => marker,
                },
            },
            new Date(),
        )
        expect(report.backgroundWorkers.bun.state).toBe('available')
        expect(report.backgroundWorkers.controller.state).toBe('available')
        expect(serializeRuntimeSupportReport(report)).not.toContain(marker)
    })

    test('rejects duplicate, missing and excessive worker cardinality', () => {
        const metrics = createWorkerObservability().snapshot()
        expect(readBunWorkerReport({ ...metrics, workers: metrics.workers.slice(1) })).toBeNull()
        expect(
            readBunWorkerReport({ ...metrics, workers: Array(9).fill(metrics.workers[0]) }),
        ).toBeNull()
        expect(
            readBunWorkerReport({ ...metrics, workers: Array(10_000).fill(metrics.workers[0]) }),
        ).toBeNull()
        const fixture = supportReportFixture()
        const native = fixture.controllerWorkers as { workers: unknown[] }
        expect(
            readControllerWorkerReport({ ...native, workers: native.workers.slice(1) }),
        ).toBeNull()
        expect(
            readControllerWorkerReport({ ...native, workers: Array(5).fill(native.workers[0]) }),
        ).toBeNull()
    })

    test.each([NaN, Infinity, -1, 0.5, Number.MAX_SAFE_INTEGER + 1, 'secret'])(
        'rejects invalid counters %s',
        (value) => {
            const metrics = createWorkerObservability().snapshot()
            metrics.workers[0]!.counters.success = value as number
            expect(readBunWorkerReport(metrics)).toBeNull()
        },
    )

    test('reports absent native data as partial while retaining independently observed Bun status', () => {
        const report = createRuntimeSupportReport(
            { ...supportReportFixture(), controllerWorkers: null },
            new Date(),
        )
        expect(report.completeness).toBe('partial')
        expect(report.unavailableSections).toContain('controller_workers')
        expect(report.backgroundWorkers.controller).toEqual({ state: 'unavailable', data: null })
        expect(report.backgroundWorkers.bun.state).toBe('available')
    })

    test('bounds report size at maximum counters and explicitly preserves missing evidence', () => {
        const metrics = createWorkerObservability().snapshot()
        for (const worker of metrics.workers) {
            for (const key of Object.keys(worker.counters) as Array<keyof typeof worker.counters>)
                worker.counters[key] = Number.MAX_SAFE_INTEGER
            worker.duration.buckets.fill(Number.MAX_SAFE_INTEGER)
            worker.duration.count = Number.MAX_SAFE_INTEGER
            worker.duration.sumMs = Number.MAX_SAFE_INTEGER
            worker.overflowed = true
        }
        const report = createRuntimeSupportReport(
            { ...supportReportFixture(), bunWorkers: metrics },
            new Date(),
        )
        const json = serializeRuntimeSupportReport(report)
        expect(new TextEncoder().encode(json).byteLength).toBeLessThan(64 * 1_024)
        expect(report.backgroundWorkers.bun.data?.workers[0]?.backlog).toBeNull()
        expect(report.backgroundWorkers.bun.data?.workers[0]?.lastSuccessAt).toBeNull()
        expect(report.backgroundWorkers.bun.data?.workers[0]?.overflowed).toBe(true)
    })
})
