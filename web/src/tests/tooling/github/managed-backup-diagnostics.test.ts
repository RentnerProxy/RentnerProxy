import assert from 'node:assert/strict'
import { expect, test } from 'bun:test'
import {
    isManagedBackupSmokeDiagnostic,
    managedBackupFailureDiagnostic,
    managedBackupSupervisorDiagnostic,
} from '../../../../../.github/scripts/ci/managed-backup-diagnostics.ts'
import { smokeProgress } from '../../../../../.github/scripts/ci/production-smoke-ci.ts'

test('managed backup failures retain fixed phase, category and approved source without raw operands', () => {
    const error = new assert.AssertionError({
        message: 'private-value',
        actual: 'secret-token',
        expected: 'private-key',
    })
    error.stack =
        'AssertionError: private-value\n at failure (C:\\private\\.github\\scripts\\ci\\managed-backups-smoke.ts:762:13)\n at /private/path.ts:22:5'
    expect(managedBackupFailureDiagnostic('interrupt-exit', error)).toBe(
        'Managed backup failure stage=interrupt-exit category=assertion at .github/scripts/ci/managed-backups-smoke.ts:762:13',
    )
    expect(
        managedBackupFailureDiagnostic(
            'interrupt-probe',
            new Error('Managed backup smoke operation failed: private-value'),
        ),
    ).toBe('Managed backup failure stage=interrupt-probe category=command')
    expect(
        managedBackupFailureDiagnostic(
            'interrupt-stop',
            new Error('Managed backup smoke deadline exceeded: private-value'),
        ),
    ).toBe('Managed backup failure stage=interrupt-stop category=timeout')
    expect(managedBackupFailureDiagnostic('setup', new Error('private-value'))).toBe(
        'Managed backup failure stage=setup category=other',
    )
})

test('managed backup diagnostic boundary rejects unknown phases, unapproved sources and extra payloads', () => {
    for (const line of [
        'Managed backup stage=interrupt-probe',
        'Managed backup failure stage=interrupt-probe category=command',
        'Managed backup failure stage=interrupt-probe category=other at .github/scripts/ci/fixtures/managed-restore-probe.ts:50:8',
        'Managed backup process exitCode=137 timedOut=false stopped=true',
        'Managed backup process exitCode=null timedOut=true stopped=false',
    ])
        expect(isManagedBackupSmokeDiagnostic(line)).toBe(true)
    for (const line of [
        'Managed backup stage=private-value',
        'Managed backup failure stage=interrupt-probe category=secret',
        'Managed backup failure stage=interrupt-probe category=other at /private/file.ts:50:8',
        'Managed backup failure stage=interrupt-probe category=other at .github/scripts/ci/private.ts:50:8',
        'Managed backup failure stage=interrupt-probe category=other at .github/scripts/ci/managed-backups-smoke.ts:12345678:1',
        'Managed backup process exitCode=256 timedOut=false stopped=true',
        'Managed backup process exitCode=-1 timedOut=false stopped=true',
        'Managed backup process exitCode=1 timedOut=false stopped=true private-value',
        'Managed backup stage=interrupt-probe\n',
        'Managed backup stage=interrupt-probe\r',
        'Managed backup stage=interrupt-probe\n::warning::private-value',
    ])
        expect(isManagedBackupSmokeDiagnostic(line)).toBe(false)
})

test('backup CI forwards safe checkpoints while preserving the actual failure through later cleanup checkpoints', () => {
    const progress = smokeProgress('backups')
    const failure =
        'Managed backup failure stage=interrupt-probe category=command at .github/scripts/ci/managed-backups-smoke.ts:762:13'
    expect(progress.consume('Managed backup stage=interrupt-probe')).toBe(
        'Managed backup stage=interrupt-probe',
    )
    expect(
        progress.consume('Managed backup process exitCode=137 timedOut=false stopped=true'),
    ).toBe('Managed backup process exitCode=137 timedOut=false stopped=true')
    expect(progress.consume(failure)).toBe(failure)
    expect(progress.consume('Managed backup stage=cleanup')).toBe('Managed backup stage=cleanup')
    expect(progress.consume('Managed backups integration failed: private-value')).toBeUndefined()
    expect(progress.result(1).diagnostic).toBe(failure)
    expect(smokeProgress('production').consume(failure)).toBeUndefined()
})

test('failed interrupt captures distinguish appliance OOM and exact supervisor reasons without forwarding raw logs', () => {
    const progress = smokeProgress('backups')
    const state = 'Managed backup appliance running=false exitCode=137 oomKilled=true'
    expect(progress.consume(state)).toBe(state)
    const line = managedBackupSupervisorDiagnostic(
        'private-token\n[rentnerproxy] fatal: controller recovery progress stopped\nprivate-key',
    )
    expect(line).toBe('Managed backup supervisor fatal=controller-progress')
    expect(progress.consume(line)).toBe(line)
    expect(managedBackupSupervisorDiagnostic('[rentnerproxy] fatal: private-token')).toBe(
        'Managed backup supervisor fatal=unknown',
    )
    expect(
        managedBackupSupervisorDiagnostic(
            '[rentnerproxy] fatal: controller recovery progress stopped private-key',
        ),
    ).toBe('Managed backup supervisor fatal=unknown')
    expect(
        managedBackupSupervisorDiagnostic(
            '[rentnerproxy] fatal: timed out waiting for controller recovery progress within 60 seconds',
        ),
    ).toBe('Managed backup supervisor fatal=controller-progress-startup-timeout')
    for (const invalid of [
        state + ' private-key',
        state.replace('137', '256'),
        state.replace('true', 'private-value'),
        'Managed backup supervisor fatal=private-value',
    ])
        expect(progress.consume(invalid)).toBeUndefined()
})
