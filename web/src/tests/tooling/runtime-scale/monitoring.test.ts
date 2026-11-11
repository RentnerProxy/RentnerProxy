import { describe, expect, test } from 'bun:test'
import {
    assertIsolatedMonitoringDatabase,
    formatMonitoringFailure,
    MonitoringInputError,
    monitoringDistribution,
    monitoringLatency,
    parseMonitoringOptions,
    simulatedMonitoringResult,
    isMonitoringFixtureOutcome,
} from '../../../../../scripts/runtime-scale/monitoring.ts'
import { domainDiagnosticsResultSchema } from '@/lib/RuntimeDiagnostics/domainDiagnostics.ts'
import { assertNativeMonitoringFixture } from '../../../../../scripts/runtime-scale/monitoring-native.ts'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test.each(['success', 'delete failure'] as const)(
    'a failed stop checkpoint retains actual writers before all owned cleanup stages: %s',
    async (mode) => {
        const output = await runBunScript(
            `
            import assert from 'node:assert/strict'
            import { mock } from 'bun:test'
            process.env.RENTNERPROXY_DATABASE_INTEGRATION = '1'
            process.env.DATABASE_URL = 'postgresql://fixture@127.0.0.1/rentnerproxy_upstream_test'
            mock.module('@tanstack/react-start/server-only', () => ({}))
            const { WorkerDrainTimeoutError } = await import('./server/Foundation/Types/worker-lifecycle.types.ts')
            const entered = Promise.withResolvers(), finish = Promise.withResolvers()
            const events = [], failure = new Error('private fixture deletion failure')
            let writing = false, completed = false
            const query = () => {
                const result = Promise.resolve([])
                let chain
                chain = new Proxy({}, { get: (_, key) => key === 'then' ? result.then.bind(result) : () => chain })
                return chain
            }
            const db = {
                select: query, insert: query, update: query,
                execute: async () => [{ due: 0, active: 0, lock_waiters: 0, lock_wait_ms: 0 }],
                transaction: async operation => operation(db),
                delete: () => {
                    assert.equal(writing, false)
                    events.push('delete')
                    if ('${mode}' === 'delete failure') throw failure
                    return query()
                },
            }
            mock.module('./db/index.ts', () => ({ db, closeDatabaseForRestoreMaintenance: async () => { events.push('database-close') } }))
            mock.module('./server/HostMonitoring/host-monitoring.repository.ts', () => ({
                claimDueHostMonitoringPolicies: async () => [], readHostMonitoringClaimContexts: async () => [],
                completeHostMonitoringClaims: async () => [], releaseHostMonitoringClaim: async () => true,
                pruneHostMonitoringHistory: async () => {},
            }))
            mock.module('./server/ProxyRuntime/proxy-runtime-data.ts', () => ({ readProxyRuntimeSnapshot: async () => ({}) }))
            mock.module('../../scripts/runtime-scale/monitoring-native.ts', () => ({ createNativeMonitoringFixture: async () => ({
                forwardHost: '127.0.0.1', forwardPort: 9, apply: async () => {}, traffic: async () => {},
                inspect: async () => { throw new Error('Unexpected native inspect') }, stop: async () => { events.push('native-stop') },
            }) }))
            const actualDrain = finish.promise.then(() => { writing = false; events.push('writers-settled') })
            mock.module('./server/HostMonitoring/host-monitoring.worker.ts', () => ({ createHostMonitoringWorker: () => ({
                start() { writing = true; process.emit('SIGTERM') },
                stop() { events.push('stop'); entered.resolve(); return Promise.reject(new WorkerDrainTimeoutError(actualDrain)) },
            }) }))
            const { runMonitoringCapacity } = await import('../../scripts/runtime-scale/monitoring.ts')
            const pending = runMonitoringCapacity({ hosts: 100, periods: 2, profile: 'mixed', runtime: 'native' })
                .then(value => ({ value }), error => ({ error })).then(result => { completed = true; return result })
            await entered.promise
            await Bun.sleep(0)
            assert.equal(completed, false)
            assert.deepEqual(events, ['stop'])
            finish.resolve()
            const result = await pending
            assert.deepEqual(events, ['stop', 'writers-settled', 'delete', 'native-stop', 'database-close'])
            if ('${mode}' === 'delete failure') assert.equal(result.error, failure)
            else {
                assert.equal(result.value.passed, false)
                assert.ok(result.value.failures.includes('Monitoring worker stop failed'))
                assert.ok(result.value.failures.includes('Monitoring capacity interrupted by SIGTERM'))
            }
            console.log(JSON.stringify({ completed, events, clean: '${mode}' === 'success' }))
        `,
            { cwd: new URL('../../..', import.meta.url) },
        )
        expect(JSON.parse(output.trim().split('\n').at(-1)!)).toMatchObject({
            completed: true,
            clean: mode === 'success',
            events: ['stop', 'writers-settled', 'delete', 'native-stop', 'database-close'],
        })
    },
)

describe('bounded opt-in monitoring capacity', () => {
    test('prints only fixed branded guard diagnostics and hides arbitrary runtime errors', () => {
        const secret = 'DO_NOT_DISCLOSE_postgresql://private:credential@database.invalid/private'
        expect(formatMonitoringFailure(new MonitoringInputError('nonempty'))).toBe(
            'Refusing nonempty monitoring/host database',
        )
        const branded = new MonitoringInputError('bounds')
        branded.message = secret
        expect(formatMonitoringFailure(branded)).toBe('Monitoring workload outside fixed bounds')
        for (const error of [
            new Error(secret),
            new TypeError(secret),
            secret,
            { name: 'MonitoringInputError', code: 'bounds', message: secret },
            null,
        ]) {
            expect(formatMonitoringFailure(error)).not.toContain(secret)
            expect(formatMonitoringFailure(error)).toBe(
                'Monitoring capacity failed; inspect isolated database/runtime availability. No credentials logged.',
            )
        }
        for (const operation of [
            () => parseMonitoringOptions(['--hosts', '999']),
            () => assertIsolatedMonitoringDatabase('1', 'postgresql://127.0.0.1/ordinary'),
            () => assertNativeMonitoringFixture(undefined, undefined, undefined),
        ]) {
            try {
                operation()
                throw new Error('Expected a branded guard error')
            } catch (error) {
                expect(error).toBeInstanceOf(MonitoringInputError)
                expect(formatMonitoringFailure(error)).not.toContain('capacity failed')
            }
        }
    })
    test('rejects mismatched availability codes instead of accepting a false fixture success', () => {
        expect(isMonitoringFixtureOutcome('healthy', 'expected_status')).toBe(true)
        expect(isMonitoringFixtureOutcome('unhealthy', 'upstream_timeout')).toBe(true)
        expect(isMonitoringFixtureOutcome('unhealthy', 'expected_status')).toBe(false)
        expect(isMonitoringFixtureOutcome('healthy', 'upstream_timeout')).toBe(false)
        expect(isMonitoringFixtureOutcome('unknown', 'expected_status')).toBe(false)
        expect(isMonitoringFixtureOutcome('unhealthy', 'upstream_unreachable')).toBe(false)
    })
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
