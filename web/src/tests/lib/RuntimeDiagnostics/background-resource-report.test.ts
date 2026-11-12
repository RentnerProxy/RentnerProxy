import { expect, test } from 'bun:test'
import { readBackgroundResourceReport } from '@/lib/RuntimeDiagnostics/backgroundResourceReport.ts'
import {
    createRuntimeSupportReport,
    serializeRuntimeSupportReport,
} from '@/lib/RuntimeDiagnostics/runtimeSupportReport.ts'
import { supportReportFixture } from './runtime-support-report.fixture.ts'

test('resource report retains bounded rejection/age evidence while dropping dynamic keys and secrets', () => {
    const input = supportReportFixture()
    const sample = input.backgroundResources as {
        sampledAt: number
        database: Record<string, Record<string, number>>
    }
    sample.database.monitoring = {
        ...sample.database.monitoring!,
        running: 2,
        pending: 8,
        oldestPendingAgeMs: 123,
        busy: 50,
        deadline: 2,
    }
    const marker = 'PRIVATE-DATABASE-URL'
    const hostile = {
        ...sample,
        url: marker,
        toJSON: () => marker,
        database: {
            ...sample.database,
            [marker]: { busy: 100 },
            monitoring: { ...sample.database.monitoring, token: marker, toJSON: () => marker },
        },
    }
    const report = createRuntimeSupportReport(
        { ...input, backgroundResources: hostile },
        new Date(),
    )
    expect(report.backgroundWorkers.resources.data?.database.monitoring).toMatchObject({
        running: 2,
        pending: 8,
        oldestPendingAgeMs: 123,
        busy: 50,
        deadline: 2,
    })
    expect(serializeRuntimeSupportReport(report)).not.toContain(marker)
})

test('absent and invalid resource samples remain explicitly unavailable', () => {
    const input = supportReportFixture()
    const report = createRuntimeSupportReport({ ...input, backgroundResources: null }, new Date())
    expect(report.backgroundWorkers.resources).toEqual({ state: 'unavailable', data: null })
    expect(report.unavailableSections).toContain('background_resources')
    expect(report.completeness).toBe('partial')
    const sample = input.backgroundResources as {
        database: Record<string, { running: number; pending: number }>
    }
    sample.database.monitoring!.running = 3
    expect(readBackgroundResourceReport(sample)).toBeNull()
    expect(readBackgroundResourceReport({})).toBeNull()
})
