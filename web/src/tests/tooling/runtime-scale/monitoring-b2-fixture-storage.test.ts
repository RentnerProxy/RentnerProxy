import { expect, test } from 'bun:test'
import { monitoringB2FixtureDiagnostic } from '../../../../../scripts/runtime-scale/monitoring-b2-fixture-storage.ts'

test('fixture diagnostics retain exact bounded code/errno evidence and never parse private error messages', () => {
    expect(
        monitoringB2FixtureDiagnostic({
            code: 'ERR_POSTGRES_SERVER_ERROR',
            cause: { errno: '40P01' },
        }),
    ).toBe('40P01')
    expect(monitoringB2FixtureDiagnostic({ cause: { code: '55P03' } })).toBe('55P03')
    expect(
        monitoringB2FixtureDiagnostic({
            code: 'ERR_POSTGRES_SERVER_ERROR',
            message: '42501 private connection text',
        }),
    ).toBe('unclassified')
    expect(monitoringB2FixtureDiagnostic({ message: '40P01' })).toBe('unclassified')
    const cycle: { cause?: unknown } = {}
    cycle.cause = cycle
    expect(monitoringB2FixtureDiagnostic(cycle)).toBe('unclassified')
    expect(
        monitoringB2FixtureDiagnostic({
            cause: { cause: { cause: { cause: { errno: '42501' } } } },
        }),
    ).toBe('unclassified')
})
