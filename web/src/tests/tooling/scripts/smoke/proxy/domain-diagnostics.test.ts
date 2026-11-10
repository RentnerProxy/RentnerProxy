import { describe, expect, test } from 'bun:test'
import { runDomainDiagnosticsSmoke } from '../../../../../../../scripts/smoke/proxy/domain-diagnostics.ts'
import {
    diagnosticRevision,
    domainDiagnosticsFixture,
    domainDiagnosticsInput,
} from '@/tests/lib/RuntimeDiagnostics/domain-diagnostics.fixture.ts'

function runtimeStatus() {
    return {
        available: true,
        running: true,
        activeRevision: diagnosticRevision,
        lastApplyAt: '2026-11-10T10:00:00Z',
        recovery: {
            workerRunning: true,
            circuitOpen: false,
            restartsInWindow: 1,
            faults: 0,
            consecutiveProbeFailures: 0,
        },
    }
}

async function inspectStatuses(before: unknown, after: unknown): Promise<void> {
    let statusReads = 0
    let diagnoses = 0
    await runDomainDiagnosticsSmoke({
        controller: async (path) => {
            if (path === '/internal/v1/proxy/status') {
                return Response.json(statusReads++ === 0 ? before : after)
            }
            expect(path).toBe('/internal/v1/diagnostics/domain')
            const result = domainDiagnosticsFixture()
            if (diagnoses++ > 0) {
                result.stages.dns = {
                    ...result.stages.dns,
                    status: 'unknown',
                    code: 'skipped',
                    addresses: [],
                }
            }
            return Response.json(result, { headers: { 'cache-control': 'no-store' } })
        },
        snapshot: { revision: diagnosticRevision },
        host: { id: domainDiagnosticsInput.hostId, domains: [domainDiagnosticsInput.domain] },
        upstreamCode: 'upstream_responded',
        tlsCode: 'tls_verified',
    })
    expect(statusReads).toBe(2)
    expect(diagnoses).toBe(2)
}

describe('domain diagnostics smoke status comparison', () => {
    test('allows a previous restart to age out without changing configuration', async () => {
        const before = runtimeStatus()
        const after = runtimeStatus()
        after.recovery.restartsInWindow = 0
        await inspectStatuses(before, after)
    })

    test.each([-1, 0.5, '1'])(
        'still rejects an invalid rolling restart count: %s',
        async (restartsInWindow) => {
            const before = runtimeStatus()
            await expect(
                inspectStatuses(before, {
                    ...before,
                    recovery: { ...before.recovery, restartsInWindow },
                }),
            ).rejects.toThrow()
        },
    )

    test('still rejects a configuration revision change', async () => {
        const before = runtimeStatus()
        await expect(
            inspectStatuses(before, { ...before, activeRevision: 'sha256:' + 'b'.repeat(64) }),
        ).rejects.toThrow('diagnosis must not apply or reload configuration')
    })

    test('still rejects a changed apply timestamp', async () => {
        const before = runtimeStatus()
        await expect(
            inspectStatuses(before, { ...before, lastApplyAt: '2026-11-10T10:00:01Z' }),
        ).rejects.toThrow('diagnosis must not apply or reload configuration')
    })

    test('still rejects a newly recorded runtime fault', async () => {
        const before = runtimeStatus()
        await expect(
            inspectStatuses(before, {
                ...before,
                recovery: { ...before.recovery, faults: before.recovery.faults + 1 },
            }),
        ).rejects.toThrow('diagnosis must not apply or reload configuration')
    })
})
