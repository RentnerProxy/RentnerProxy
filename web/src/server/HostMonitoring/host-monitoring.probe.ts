import {
    classifyHostMonitoringOutcome,
    unknownHostMonitoringOutcome,
} from '@/lib/HostMonitoring/state.ts'
import { domainDiagnosticsResultSchema } from '@/lib/RuntimeDiagnostics/domainDiagnostics.ts'
import {
    DomainDiagnosticsError,
    assertDiagnosticsNotAborted,
} from '@/server/RuntimeDiagnostics/domain-diagnostics.errors.ts'
import type { HostMonitoringClaim } from './Types/host-monitoring.repository.types.ts'
import type { HostMonitoringProbeDependencies } from './Types/host-monitoring-probe.types.ts'
import type { HostMonitoringOutcome } from '@/lib/HostMonitoring/Types/host-monitoring.types.ts'

/** Runs an upstream-only probe behind fresh context fences and releases every claim without a committed completion. */
export function createHostMonitoringProbe(dependencies: HostMonitoringProbeDependencies) {
    return async (claim: HostMonitoringClaim, signal: AbortSignal): Promise<void> => {
        let completed = false
        try {
            assertDiagnosticsNotAborted(signal)
            const context = await dependencies.readContext(claim)
            assertDiagnosticsNotAborted(signal)
            if (!context?.enabled) return
            const { policy } = claim
            let outcome: HostMonitoringOutcome
            try {
                const result = await dependencies.inspect(
                    {
                        hostType: 'proxy',
                        hostId: policy.proxyHostId,
                        domain: policy.domain,
                        expectedRevision: context.revision,
                    },
                    signal,
                    { upstreamOnly: true, timeoutMs: policy.timeoutMs },
                )
                assertDiagnosticsNotAborted(signal)
                const parsed = domainDiagnosticsResultSchema.safeParse(result)
                outcome =
                    !parsed.success ||
                    parsed.data.hostType !== 'proxy' ||
                    parsed.data.hostId !== policy.proxyHostId ||
                    parsed.data.domain !== policy.domain ||
                    parsed.data.expectedRevision !== context.revision
                        ? unknownHostMonitoringOutcome('invalid_response')
                        : classifyHostMonitoringOutcome(parsed.data, policy.expectedStatus)
            } catch (error) {
                assertDiagnosticsNotAborted(signal)
                outcome = unknownHostMonitoringOutcome(
                    error instanceof DomainDiagnosticsError && error.code === 'busy'
                        ? 'busy'
                        : 'runtime_unavailable',
                )
            }
            assertDiagnosticsNotAborted(signal)
            completed = await dependencies.complete(claim, outcome, context, signal)
        } finally {
            // Committed completion already clears its lease. Every unfinished
            // attempt still releases through the durable token/version fence.
            if (!completed) await dependencies.release(claim)
        }
    }
}
