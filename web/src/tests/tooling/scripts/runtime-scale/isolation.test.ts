import { describe, expect, test } from 'bun:test'
import { isolationContract } from '../../../../../../scripts/runtime-scale/isolation.config.ts'
import {
    assessIsolation,
    assertIsolationRole,
    isolationProbeIndex,
    isolationDiagnosticResult,
    isolationDistribution,
    meetsIsolationSlo,
    parseIsolationOptions,
} from '../../../../../../scripts/runtime-scale/isolation.ts'
import { domainDiagnosticsResultSchema } from '@/lib/RuntimeDiagnostics/domainDiagnostics.ts'
import { classifyHostMonitoringOutcome } from '@/lib/HostMonitoring/state.ts'
import type {
    IsolationMeasurement,
    IsolationLoad,
} from '../../../../../../scripts/runtime-scale/Types/isolation.types.ts'

function measurement(
    mode: IsolationMeasurement['mode'],
    repetition: number,
    p99 = 10,
    load: IsolationLoad = 100,
): IsolationMeasurement {
    return {
        mode,
        load,
        repetition,
        elapsedMs: 8000,
        ssr: { count: 240, p95: p99, p99, max: p99 },
        ssrFailures: 0,
        rejectedTraffic: 0,
        telemetryFailures: 0,
        foreground: {
            pid: 1,
            uid: 20002,
            rssBytes: 100,
            cpuMicroseconds: 100,
            fds: 20,
            lag: { count: 300, p95: 1, p99: 2, max: 3 },
            failures: 0,
            committed: load,
            commitLagMaxMs: 1000,
        },
        background: null,
        bunRssBytes: 100,
        idleBunRssBytes: 100,
        idleCpuPercentagePoints: 0,
        totalMemoryBytes: 1000,
        totalCpuPercentagePoints: 20,
        postgresConnections: 16,
        pids: 40,
        observations: load,
        healthyObservations: load,
        network: { probes: load, accepted: load, invalid: 0, snapshots: 16 },
        duplicates: 0,
        dueToCommitMaxMs: 1000,
        startupMs: 100,
        stopMs: 10,
    }
}
const repeats = (baseline = 10, candidate = 8) =>
    isolationContract.loads.flatMap((load) =>
        Array.from({ length: 3 }, (_, index) => [
            measurement('in-process', index + 1, baseline, load),
            measurement('separate-bun', index + 1, candidate, load),
        ]).flat(),
    )

describe('bounded process isolation evaluation', () => {
    test('derives a real HEAD request from fixture domains and classifies the expected healthy outcome', async () => {
        const domain = 'isolation-123.example.test'
        let accepted = 0
        const server = Bun.serve({
            hostname: '127.0.0.1',
            port: 0,
            fetch(request) {
                if (request.method === 'HEAD' && new URL(request.url).pathname === '/probe/123') {
                    accepted++
                    return new Response(null, { status: 200 })
                }
                return new Response(null, { status: 404 })
            },
        })
        try {
            const response = await fetch(
                'http://127.0.0.1:' + server.port + '/probe/' + isolationProbeIndex(domain),
                { method: 'HEAD', signal: AbortSignal.timeout(1000) },
            )
            expect(response.status).toBe(200)
            expect(accepted).toBe(1)
            const outcome = classifyHostMonitoringOutcome(
                isolationDiagnosticResult(
                    {
                        hostType: 'proxy',
                        hostId: '00000000-0000-4000-8000-000000000001',
                        domain,
                        expectedRevision: 'sha256:' + 'a'.repeat(64),
                    },
                    10.75,
                ),
                200,
            )
            expect(outcome).toMatchObject({ status: 'healthy', code: 'expected_status' })
            const valid = isolationDiagnosticResult(
                {
                    hostType: 'proxy',
                    hostId: '00000000-0000-4000-8000-000000000001',
                    domain,
                    expectedRevision: 'sha256:' + 'a'.repeat(64),
                },
                10.75,
            )
            expect(domainDiagnosticsResultSchema.safeParse(valid).success).toBe(true)
            expect(Number.isInteger(valid.stages.upstream.durationMs)).toBe(true)
            expect(() =>
                isolationDiagnosticResult(
                    {
                        hostType: 'proxy',
                        hostId: '00000000-0000-4000-8000-000000000001',
                        domain,
                        expectedRevision: 'sha256:' + 'a'.repeat(64),
                    },
                    NaN,
                ),
            ).toThrow()
            expect(isolationProbeIndex('isolation-0.example.test')).toBe(0)
            expect(isolationProbeIndex('isolation-999.example.test')).toBe(999)
            for (const invalid of [
                'isolation-NaN.example.test',
                'isolation-1000.example.test',
                'isolation-012.example.test',
                'foreign.example.test',
            ])
                expect(() => isolationProbeIndex(invalid)).toThrow()
        } finally {
            await server.stop(true)
        }
    })
    test('rejects role entry before runtime imports unless the exact unprivileged fixture identity is selected', () => {
        const url = 'postgresql://isolation_worker@127.0.0.1:55439/rentnerproxy_upstream_test'
        expect(() => assertIsolationRole('1', 'linux', 20004, url, 'background')).not.toThrow()
        for (const args of [
            [undefined, 'linux', 20004, url],
            ['1', 'linux', 0, url],
            ['1', 'win32', 20004, url],
            [
                '1',
                'linux',
                20004,
                'postgresql://postgres@127.0.0.1:55439/rentnerproxy_upstream_test',
            ],
            ['1', 'linux', 20004, url.replace(':55439', ':5432')],
            ['1', 'linux', 20004, url.replace('127.0.0.1', 'database')],
        ] as const)
            expect(() =>
                assertIsolationRole(args[0], args[1], args[2], args[3], 'background'),
            ).toThrow()
    })

    test('declares immutable numeric gates and refuses budget or repetition overrides', () => {
        expect(isolationContract.envelope).toEqual({
            memoryBytes: 1073741824,
            cpuCores: 2,
            pids: 512,
            postgresConnections: 100,
        })
        expect(parseIsolationOptions([])).toEqual({
            loads: [100, 500, 1000],
            repetitions: 3,
            durationSeconds: 8,
            faults: true,
            faultsOnly: false,
        })
        for (const args of [
            ['--memory', '2g'],
            ['--repetitions', '1'],
            ['--duration-seconds', '1800'],
            ['--loads', '100,100'],
            ['--loads', '200'],
            ['--faults', 'yes'],
            ['--loads', '100', '--loads', '500'],
        ])
            expect(() => parseIsolationOptions(args)).toThrow()
    })
    test('reports unavailable evidence honestly and rejects invalid measurements', () => {
        expect(isolationDistribution([])).toEqual({ count: 0, p95: null, p99: null, max: null })
        expect(isolationDistribution([5, 1, 3, 2, 4])).toMatchObject({
            count: 5,
            p95: 5,
            p99: 5,
            max: 5,
        })
        for (const invalid of [[NaN], [Infinity], [-1]])
            expect(() => isolationDistribution(invalid)).toThrow()
        expect(
            assessIsolation([], false).results.every((item) => item.decision === 'insufficient'),
        ).toBe(true)
    })
    test('cannot accept unknown outcomes or unexecuted HTTP work as a valid matched dataset', () => {
        const values = repeats()
        values[0]!.healthyObservations = 0
        values[0]!.network.accepted = 0
        expect(assessIsolation(values, false).recommendation).toBe('insufficient-evidence')
        expect(assessIsolation(values, false).results[0]?.decision).toBe('insufficient')
    })

    test('zero or partial matched counts cannot turn missing work into a topology decision', () => {
        for (const count of [0, 1, 99]) {
            const values = repeats()
            values[0]!.observations = count
            values[0]!.healthyObservations = count
            values[0]!.network.accepted = count
            const result = assessIsolation(values, false)
            expect(result.recommendation).toBe('insufficient-evidence')
            expect(result.results[0]?.decision).toBe('insufficient')
        }
    })

    test('keeps the simpler baseline when it already meets SLO, regardless of a faster candidate', () => {
        const result = assessIsolation(repeats(100, 10), true)
        expect(result.recommendation).toBe('keep-in-process')
        expect(result.results[0]).toMatchObject({
            baselineMeetsSlo: true,
            material: true,
            decision: 'keep-in-process',
        })
        expect(result.productionMigrationAuthorized).toBe(false)
    })
    test('rejects benefit hidden inside repeated-run noise', () => {
        const values = repeats(600, 400)
        values.find((item) => item.mode === 'in-process' && item.repetition === 3)!.ssr.p99 = 1000
        expect(assessIsolation(values, true).results[0]).toMatchObject({
            material: false,
            decision: 'keep-in-process',
        })
    })
    test('requires both safety proof and unchanged resource headroom for an experimental candidate', () => {
        const values = repeats(600, 100)
        expect(assessIsolation(values, false).results[0]?.decision).toBe('keep-in-process')
        expect(assessIsolation(values, true).results[0]?.decision).toBe(
            'candidate-requires-separate-authorization',
        )
        for (const item of values.filter((sample) => sample.mode === 'separate-bun'))
            item.bunRssBytes += isolationContract.thresholds.extraRssBytes + 1
        expect(assessIsolation(values, true).results[0]).toMatchObject({
            fits: false,
            decision: 'keep-in-process',
        })
    })
    test('cannot turn dropped requests, missing telemetry or partial progress into a passing capacity claim', () => {
        for (const change of [
            { rejectedTraffic: 1 },
            { telemetryFailures: 1 },
            { observations: 99 },
            { healthyObservations: 0 },
            { duplicates: 1 },
            { ssrFailures: 1 },
            { stopMs: 15001 },
            { postgresConnections: 101 },
            { pids: 513 },
            { totalMemoryBytes: 1073741825 },
        ])
            expect(meetsIsolationSlo({ ...measurement('in-process', 1), ...change })).toBe(false)
    })
})
