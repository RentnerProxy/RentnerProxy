import { describe, expect, test } from 'bun:test'
import {
    assertIsolatedMonitoringDatabase,
    monitoringDistribution,
    monitoringLatency,
    parseMonitoringOptions,
    simulatedMonitoringResult,
} from '../../../../../scripts/runtime-scale/monitoring.ts'
import { domainDiagnosticsResultSchema } from '@/lib/RuntimeDiagnostics/domainDiagnostics.ts'
import { assertNativeMonitoringFixture } from '../../../../../scripts/runtime-scale/monitoring-native.ts'

describe('bounded opt-in monitoring capacity', () => {
    test('native execution requires the exact opt-in loopback fixture and synthetic token', () => {
        const url = 'http://127.0.0.1:55441'
        const token = 'synthetic-monitoring-scale-token-392'
        expect(() => assertNativeMonitoringFixture('1', url, token)).not.toThrow()
        for (const [flag, controllerUrl, fixtureToken] of [
            [undefined, url, token],
            ['0', url, token],
            ['1', 'http://localhost:55441', token],
            ['1', 'http://127.0.0.1:8080', token],
            ['1', 'http://example.test:55441', token],
            ['1', `${url}/unexpected-path`, token],
            ['1', url, undefined],
            ['1', url, 'another-token'],
        ])
            expect(() => assertNativeMonitoringFixture(flag, controllerUrl, fixtureToken)).toThrow()
        expect(parseMonitoringOptions(['--runtime', 'native'])).toEqual({
            hosts: 100,
            periods: 2,
            profile: 'mixed',
            runtime: 'native',
        })
        expect(() => parseMonitoringOptions(['--runtime', 'external'])).toThrow()
    })
    test('accepts only exact workloads, unchanged fixed interval profiles and bounded periods', () => {
        expect(parseMonitoringOptions([])).toEqual({ hosts: 100, periods: 2, profile: 'mixed' })
        expect(
            parseMonitoringOptions(['--hosts', '1000', '--periods', '3', '--profile', 'timeout']),
        ).toEqual({ hosts: 1000, periods: 3, profile: 'timeout' })
        for (const args of [
            ['--hosts', '999'],
            ['--periods', '4'],
            ['--interval', '120'],
            ['--profile', 'fast'],
            ['--hosts', '100', '--hosts', '500'],
            ['--hosts'],
        ])
            expect(() => parseMonitoringOptions(args)).toThrow()
    })
    test('rejects ordinary, remote and unflagged databases before importing database modules', () => {
        expect(() =>
            assertIsolatedMonitoringDatabase(
                '1',
                'postgres://test:test@127.0.0.1:55439/rentnerproxy_upstream_test',
            ),
        ).not.toThrow()
        for (const [flag, url] of [
            [undefined, 'postgres://localhost/rentnerproxy_upstream_test'],
            ['1', 'postgres://127.0.0.1/rentnerproxy'],
            ['1', 'postgres://example.test/rentnerproxy_upstream_test'],
            ['1', 'https://127.0.0.1/rentnerproxy_upstream_test'],
            ['1', undefined],
        ])
            expect(() => assertIsolatedMonitoringDatabase(flag, url)).toThrow()
    })
    test('reports nearest-rank tails and empty samples honestly', () => {
        expect(monitoringDistribution([])).toEqual({
            count: 0,
            p95: null,
            p99: null,
            max: null,
            total: 0,
        })
        expect(
            monitoringDistribution(Array.from({ length: 100 }, (_, index) => index + 1)),
        ).toEqual({ count: 100, p95: 95, p99: 99, max: 100, total: 5050 })
    })
    test('controlled probe fixtures classify valid healthy and three-second timeout diagnostics', () => {
        const input = {
            hostType: 'proxy' as const,
            hostId: '0198d98a-0000-7000-8000-000000000002',
            domain: 'capacity.example.test',
            expectedRevision: 'sha256:' + 'a'.repeat(64),
        }
        expect(
            Array.from({ length: 4 }, (_, index) => monitoringLatency('mixed', index).durationMs),
        ).toEqual([20, 150, 1000, 3000])
        expect(monitoringLatency('slow', 0)).toEqual({ durationMs: 2400, timeout: false })
        expect(monitoringLatency('timeout', 0)).toEqual({ durationMs: 3000, timeout: true })
        expect(monitoringLatency('timeout', 0, 10000)).toEqual({ durationMs: 10000, timeout: true })
        expect(monitoringLatency('mixed', 3, 10000)).toEqual({ durationMs: 10000, timeout: true })
        expect(parseMonitoringOptions(['--timeout-ms', '10000', '--scheduler', 'legacy'])).toEqual({
            hosts: 100,
            periods: 2,
            profile: 'mixed',
            timeoutMs: 10000,
            scheduler: 'legacy',
        })
        expect(() => parseMonitoringOptions(['--timeout-ms', '10001'])).toThrow()
        expect(() => parseMonitoringOptions(['--scheduler', 'unbounded'])).toThrow()
        for (const timeout of [false, true])
            expect(
                domainDiagnosticsResultSchema.safeParse(
                    simulatedMonitoringResult(input, 3000, timeout),
                ).success,
            ).toBe(true)
    })
})
