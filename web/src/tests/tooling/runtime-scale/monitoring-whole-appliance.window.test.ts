import { describe, test, expect } from 'bun:test'
import type { SQL } from 'bun'
import {
    scheduleMonitoringApplianceWindow,
    collectMonitoringApplianceWindow,
    collectMonitoringApplianceWindowTraffic,
} from '../../../../../scripts/runtime-scale/monitoring-whole-appliance.window.ts'
import { summarizeMonitoringApplianceResources } from '../../../../../scripts/runtime-scale/monitoring-whole-appliance.metrics.ts'
import {
    createMonitoringB2WindowCap,
    monitoringB2WindowPolicyHash,
} from '../../../../../scripts/runtime-scale/monitoring-b2-window-cap.ts'
import type { MonitoringApplianceResource } from '../../../../../scripts/runtime-scale/Types/monitoring-whole-appliance.types.ts'

/** Advance the same wall and monotonic clock without real-duration waits. */
function clockFixture() {
    let now = 0
    return {
        now: () => now,
        monotonic: () => now,
        sleep: (ms: number) => {
            now += ms
            return Promise.resolve()
        },
        advance: (ms: number) => {
            now += ms
        },
    }
}
/** Retain the full reference envelope while varying only measured sample alignment. */
function resources(): MonitoringApplianceResource[] {
    return Array.from({ length: 121 }, (_, index) => ({
        atMs: 1000000 + index * 1000,
        monotonicMs: 100 + index * 1000,
        scheduledAtMs: 1000000 + index * 1000,
        cpuMicroseconds: 1000000 + index * 10000,
        rssBytes: 400 * 1024 ** 2,
        workingSetBytes: 420 * 1024 ** 2,
        memoryBytes: 450 * 1024 ** 2,
        pids: 35,
        fds: 300,
        postgresConnections: 12,
        memoryLimitBytes: 1024 ** 3,
        pidLimit: 512,
        cpuQuotaCores: 2,
        postgresConnectionLimit: 100,
        oomKills: 0,
        roles: {
            web: { processes: 1, rssBytes: 100, fds: 20 },
            controller: { processes: 1, rssBytes: 100, fds: 20 },
            caddy: { processes: 1, rssBytes: 100, fds: 20 },
        },
        database: {
            commits: index,
            rollbacks: 0,
            blocksRead: 10,
            blocksHit: 10 + index,
            inserted: index * 2,
            updated: index * 3,
            deleted: index,
        },
    }))
}

describe('absolute appliance measurement window', () => {
    test('does not dispatch visibility after a final five-second request tail reaches the end', async () => {
        const timer = clockFixture(),
            clock = {
                originMs: 1000,
                endMs: 121000,
                originMonotonicMs: 1000,
                endMonotonicMs: 121000,
            }
        let visibility = 0,
            lateVisibility = 0,
            tailRequests = 0
        const result = await collectMonitoringApplianceWindowTraffic({
            clock,
            timer,
            visibility: () => {
                visibility++
                if (timer.now() >= clock.endMs) lateVisibility++
                return Promise.resolve()
            },
            request: (kind) => {
                if (timer.now() >= 116000) {
                    tailRequests++
                    if (kind === 'ssr') timer.advance(5000)
                }
                return Promise.resolve({
                    status: 200,
                    validBody: true,
                    elapsedMs: timer.now() >= clock.endMs ? 5000 : 1,
                })
            },
        })
        expect(tailRequests).toBe(2)
        expect(timer.now()).toBe(clock.endMs)
        expect(visibility).toBeGreaterThan(0)
        expect(lateVisibility).toBe(0)
        expect(result.failures).toEqual([])
    })
    test('uses fixed origin/end targets even when each resource read takes time', async () => {
        const timer = clockFixture(),
            targets: number[] = [],
            actual: number[] = []
        const clock = {
            originMs: 1000,
            endMs: 121000,
            originMonotonicMs: 1000,
            endMonotonicMs: 121000,
        }
        const armed = await scheduleMonitoringApplianceWindow(
            clock,
            1000,
            async (target) => {
                targets.push(target)
                actual.push(timer.now())
                timer.advance(5)
            },
            true,
            timer,
        )
        expect(armed.armedAtMs).toBe(0)
        expect(targets.length).toBe(121)
        expect(targets[0]).toBe(1000)
        expect(targets.at(-1)).toBe(121000)
        expect(actual).toEqual(targets)
        expect(timer.now()).toBe(121005)
    })
    test('rejects a late observer before sampling instead of starting a new relative120s interval', async () => {
        const timer = clockFixture()
        timer.advance(2173)
        let samples = 0
        await expect(
            scheduleMonitoringApplianceWindow(
                { originMs: 1000, endMs: 121000, originMonotonicMs: 1000, endMonotonicMs: 121000 },
                1000,
                async () => {
                    samples++
                },
                true,
                timer,
            ),
        ).rejects.toThrow('not armed')
        expect(samples).toBe(0)
    })
    test('passes one shared clock to both collectors and observes final unfiltered data after both settle', async () => {
        const original = { heads: [1, 2, 3] },
            received: unknown[] = []
        let release!: () => void,
            resourceDone = false,
            reportCalls = 0
        const result = collectMonitoringApplianceWindow({
            start: () => Promise.resolve({ originMs: 1000 }),
            now: () => 0,
            monotonic: () => 0,
            resources: (clock) => {
                received.push(clock)
                return new Promise<void>((resolve) => {
                    release = () => {
                        resourceDone = true
                        resolve()
                    }
                })
            },
            traffic: (clock) => {
                received.push(clock)
                return Promise.resolve('traffic')
            },
            report: () => {
                reportCalls++
                expect(resourceDone).toBe(true)
                return Promise.resolve(original)
            },
        })
        await Promise.resolve()
        await Promise.resolve()
        expect(reportCalls).toBe(0)
        release()
        const captured = await result
        expect(received[0]).toBe(received[1])
        expect(captured.report).toBe(original)
        expect(captured.clock.endMs - captured.clock.originMs).toBe(120000)
        await expect(
            collectMonitoringApplianceWindow({
                start: () => Promise.resolve({ originMs: 1000 }),
                now: () => 1000,
                monotonic: () => 0,
                resources: () => Promise.resolve(),
                traffic: () => Promise.resolve(),
                report: () => Promise.resolve(),
            }),
        ).rejects.toThrow('missed its origin')
    })
    test('rejects the observed2173ms shift despite otherwise passing original resource gates', () => {
        const samples = resources(),
            expected = {
                clock: {
                    originMs: 1000000,
                    endMs: 1120000,
                    originMonotonicMs: 100,
                    endMonotonicMs: 120100,
                },
                arming: { armedAtMs: 999000, armedMonotonicMs: 0 },
            }
        expect(summarizeMonitoringApplianceResources(samples, expected).failures).toEqual([])
        for (const sample of samples) {
            sample.atMs += 2173
            sample.monotonicMs += 2173
        }
        expect(summarizeMonitoringApplianceResources(samples).failures).toEqual([])
        expect(summarizeMonitoringApplianceResources(samples, expected).failures).toContain(
            'resource-window-origin-or-end-misaligned',
        )
    })
    test('keeps original traffic quantiles instead of rejecting one request merely for exceeding250ms', async () => {
        const timer = clockFixture(),
            clock = {
                originMs: 1000,
                endMs: 121000,
                originMonotonicMs: 1000,
                endMonotonicMs: 121000,
            }
        let requests = 0
        const result = await collectMonitoringApplianceWindowTraffic({
            clock,
            timer,
            visibility: () => Promise.resolve(),
            request: (kind) => {
                const slow = kind === 'ssr' && requests++ === 0
                if (slow) timer.advance(300)
                return Promise.resolve({ status: 200, validBody: true, elapsedMs: slow ? 300 : 1 })
            },
        })
        expect(result.failures).toEqual([])
        expect(result.rows.length).toBeGreaterThan(900)
        expect(result.rows.some((row) => row.elapsedMs === 300)).toBe(true)
    })
    test('leaves ordinary safety preparation uncapped without even opening a database', async () => {
        let writes = 0
        const database = {
            begin: () => {
                writes++
                throw new Error('unexpected DDL')
            },
        } as unknown as SQL
        expect(
            await createMonitoringB2WindowCap(database, {
                runId: 'not-sealed',
                windowId: 'safety',
                design: 'A',
                policyIds: [],
            }),
        ).toBeUndefined()
        expect(writes).toBe(0)
        const ids = Array.from({ length: 1000 }, () => Bun.randomUUIDv7())
        expect(monitoringB2WindowPolicyHash(ids)).toBe(
            monitoringB2WindowPolicyHash(ids.toReversed()),
        )
        expect(() => monitoringB2WindowPolicyHash([...ids.slice(1), ids[1]!])).toThrow()
    })
})
