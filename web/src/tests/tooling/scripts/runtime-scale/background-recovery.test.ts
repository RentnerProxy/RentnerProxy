import { expect, test } from 'bun:test'
import { assertCertificateReplayEvidence } from '../../../../../../scripts/runtime-scale/background.ts'
import { exerciseExtendedBackgroundRecovery } from '../../../../../../scripts/runtime-scale/background-extended.ts'
import { parseBackgroundOptions } from '../../../../../../scripts/runtime-scale/background.ts'
import type { createHarness } from '../../../../../../scripts/runtime-reliability/harness.ts'

test('certificate replay requires a nonempty one-to-one audit effect for every actual receipt', () => {
    const evidence = {
        cursor: 'fixture:2',
        receiptIds: ['a', 'b'],
        auditIds: ['audit-b', 'audit-a'],
        auditEventIds: ['b', 'a'],
    }
    expect(() => assertCertificateReplayEvidence(evidence)).not.toThrow()
    for (const invalid of [
        { ...evidence, receiptIds: [], auditIds: [], auditEventIds: [] },
        { ...evidence, auditIds: [], auditEventIds: [] },
        { ...evidence, auditIds: ['audit-a'], auditEventIds: ['a'] },
        { ...evidence, auditEventIds: ['a', 'a'] },
        { ...evidence, auditEventIds: ['a', 'foreign-event'] },
        { ...evidence, cursor: undefined },
    ])
        expect(() => assertCertificateReplayEvidence(invalid)).toThrow()
})

test('extended diagnostics identify a fixed stage without replacing the original failure identity', async () => {
    const primary = new Error('private exception must remain private')
    const stages: string[] = []
    const harness = {
        context: {
            domain: 'fixture.invalid',
            container: 'private-fixture',
            docker: () => Promise.reject(primary),
        },
    } as unknown as Awaited<ReturnType<typeof createHarness>>
    await expect(
        exerciseExtendedBackgroundRecovery(
            harness,
            parseBackgroundOptions([]),
            (operation) => operation(),
            () => {},
            (stage) => {
                stages.push(stage)
            },
        ),
    ).rejects.toBe(primary)
    expect(stages).toEqual(['certificate-prepare'])
})
