import { collectMonitoringApplianceResources } from './monitoring-whole-appliance.metrics.ts'
// oxlint-disable no-await-in-loop -- Absolute cadence targets must settle in order without relative deadline resets.
import type {
    MonitoringApplianceWindowClock,
    MonitoringApplianceWindowTimer,
    MonitoringApplianceWindowArming,
    MonitoringApplianceTraffic,
    MonitoringApplianceWindowDependencies,
    MonitoringApplianceWindowTrafficDependencies,
} from './Types/monitoring-whole-appliance.types.ts'

/** Schedule against a shared absolute origin/end; reject late arming or missed sample starts. */
export async function scheduleMonitoringApplianceWindow(
    clock: MonitoringApplianceWindowClock,
    intervalMs: number,
    sample: (scheduledAtMs: number) => Promise<void>,
    includeEnd = true,
    timer: MonitoringApplianceWindowTimer = {
        now: Date.now,
        monotonic: () => performance.now(),
        sleep: Bun.sleep,
    },
): Promise<MonitoringApplianceWindowArming> {
    const armedAtMs = timer.now(),
        armedMonotonicMs = timer.monotonic()
    if (
        clock.endMs - clock.originMs !== 120000 ||
        Math.abs(clock.endMonotonicMs - clock.originMonotonicMs - 120000) > 0.001 ||
        !Number.isFinite(intervalMs) ||
        intervalMs <= 0 ||
        120000 % intervalMs !== 0 ||
        armedAtMs >= clock.originMs ||
        armedMonotonicMs >= clock.originMonotonicMs
    )
        throw new Error('B2 observer was not armed before the common origin')
    const count = 120000 / intervalMs
    for (let index = 0; index < count + (includeEnd ? 1 : 0); index++) {
        const target = clock.originMonotonicMs + index * intervalMs
        while (timer.monotonic() < target) await timer.sleep(target - timer.monotonic())
        if (timer.monotonic() - target >= intervalMs)
            throw new Error('B2 observer missed an absolute cadence start')
        await sample(clock.originMs + index * intervalMs)
        if (timer.monotonic() - target >= intervalMs)
            throw new Error('B2 observer exceeded its sample interval')
    }
    return { armedAtMs, armedMonotonicMs }
}

/** Own /start and both local collectors so host Docker latency cannot shift either measured interval. */
export async function collectMonitoringApplianceWindow<Resource, Traffic, Report>(
    dependencies: MonitoringApplianceWindowDependencies<Resource, Traffic, Report>,
) {
    const start = await dependencies.start()
    const wall = dependencies.now(),
        monotonic = dependencies.monotonic()
    if (
        !Number.isSafeInteger(start.originMs) ||
        start.originMs <= wall ||
        start.originMs - wall > 5000
    )
        throw new Error('B2 local start handshake missed its origin')
    const clock = {
        originMs: start.originMs,
        endMs: start.originMs + 120000,
        originMonotonicMs: monotonic + start.originMs - wall,
        endMonotonicMs: monotonic + start.originMs - wall + 120000,
    }
    const [resources, traffic] = await Promise.allSettled([
        dependencies.resources(clock),
        dependencies.traffic(clock),
    ])
    if (resources.status !== 'fulfilled' || traffic.status !== 'fulfilled')
        throw new Error('B2 absolute window collector failed')
    // Final live data is observed only after both collectors settle. The private
    // two-cadence scheduling cap, not row filtering, excludes a third due batch.
    const report = await dependencies.report()
    return { start, clock, resources: resources.value, traffic: traffic.value, report }
}

/** Preserve the original traffic cadence/quantiles while bounding dispatch by the common end. */
export async function collectMonitoringApplianceWindowTraffic(
    dependencies: MonitoringApplianceWindowTrafficDependencies,
) {
    const { clock, timer } = dependencies,
        rows: MonitoringApplianceTraffic[] = [],
        failures: string[] = []
    const armedAtMs = timer.now(),
        armedMonotonicMs = timer.monotonic()
    if (armedAtMs >= clock.originMs || armedMonotonicMs >= clock.originMonotonicMs)
        throw new Error('B2 traffic observer was not armed before origin')
    while (timer.monotonic() < clock.originMonotonicMs)
        await timer.sleep(clock.originMonotonicMs - timer.monotonic())
    let nextVisibility = clock.originMonotonicMs
    while (timer.monotonic() < clock.endMonotonicMs) {
        const began = timer.monotonic()
        await Promise.all(
            (['ssr', 'proxy'] as const).map(async (kind) => {
                try {
                    const value = await dependencies.request(kind)
                    rows.push({ kind, atMs: timer.now(), ...value })
                } catch {
                    rows.push({
                        kind,
                        atMs: timer.now(),
                        status: 0,
                        elapsedMs: 5000,
                        validBody: false,
                    })
                    failures.push('traffic-observation-unavailable')
                }
            }),
        )
        if (timer.monotonic() >= clock.endMonotonicMs) break
        if (timer.monotonic() >= nextVisibility) {
            try {
                await dependencies.visibility()
            } catch {
                failures.push('progress-observation-unavailable')
            }
            nextVisibility += 1000
        }
        const remaining = clock.endMonotonicMs - timer.monotonic()
        if (remaining <= 0) break
        await timer.sleep(Math.min(remaining, Math.max(1, 250 - (timer.monotonic() - began))))
    }
    return { rows, failures, arming: { armedAtMs, armedMonotonicMs } }
}

/** Serialize one private local start/capture process without exposing its control capability. */
export function monitoringApplianceWindowProbe(readCapability: () => Promise<string>) {
    return `const capability=await (${readCapability.toString()})();
const request=async(path,body,timeoutMs=30000)=>{const response=await fetch('http://127.0.0.1:9020'+path,{method:body===undefined?'GET':'POST',headers:{authorization:'Bearer '+capability,'content-type':'application/json'},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(timeoutMs)});if(!response.ok)throw new Error('B2 local window request failed');return await response.json()};
const schedule=(${scheduleMonitoringApplianceWindow.toString()});
const result=await (${collectMonitoringApplianceWindow.toString()})({
 now:Date.now,monotonic:()=>performance.now(),start:()=>request('/start',{}),report:()=>request('/report',undefined,5000),
 resources:async clock=>{const samples=[];const arming=await (${collectMonitoringApplianceResources.toString()})(120000,1000,value=>samples.push(value),clock,schedule);return {samples,arming}},
 traffic:clock=>(${collectMonitoringApplianceWindowTraffic.toString()})({clock,request:kind=>request(kind==='ssr'?'/management':'/traffic',undefined,5000),visibility:()=>request('/visibility',undefined,5000),timer:{now:Date.now,monotonic:()=>performance.now(),sleep:Bun.sleep}})
});process.stdout.write(JSON.stringify({start:result.start,clock:result.clock,resources:result.resources.samples,resourceArming:result.resources.arming,traffic:result.traffic.rows,trafficArming:result.traffic.arming,report:result.report,failures:result.traffic.failures}));`
}
