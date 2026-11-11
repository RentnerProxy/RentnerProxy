import { expect, test } from 'bun:test'
import { randomBytes, randomUUID } from 'node:crypto'
import { monitoringB2FixtureDiagnostic } from '../../../../../scripts/runtime-scale/monitoring-b2-fixture-storage.ts'
import { validateMonitoringB2Role } from '../../../../../scripts/runtime-scale/monitoring-b2-fixture-storage.ts'

test('disposable role bootstrap accepts generated credentials and rejects malformed SQL inputs without disclosure', () => {
    const role = 'monitor_b2_role_' + randomUUID().replaceAll('-', '')
    const password = randomBytes(32).toString('hex')
    expect(() => validateMonitoringB2Role('create', role, password)).not.toThrow()
    expect(() => validateMonitoringB2Role('drop', role)).not.toThrow()
    for (const invalid of [undefined, '', 'a'.repeat(32), 'g'.repeat(64), "x'; SELECT 1; --"]) {
        let failure: unknown
        try {
            validateMonitoringB2Role('create', role, invalid)
        } catch (error) {
            failure = error
        }
        expect(monitoringB2FixtureDiagnostic(failure)).toBe('role-identity-invalid')
    }
    expect(() => validateMonitoringB2Role('drop', "role'; SELECT 1; --")).toThrow()
})

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
