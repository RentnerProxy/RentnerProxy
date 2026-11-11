import { workerObservability } from './worker-observability.ts'
import { backgroundWorkerMetricsConfig } from '@/config/background-workers.config.ts'

let timer: ReturnType<typeof setTimeout> | null = null
let stopped = false

/** Start one unrefed sampler per process; clamp early callbacks to zero delay and permanently stop sampling on shutdown. */
export function startEventLoopObservation(): void {
    if (timer || stopped) return
    /** Schedule relative to the current monotonic clock so each observation measures this timer deadline without catch-up bursts. */
    const schedule = () => {
        const due = performance.now() + backgroundWorkerMetricsConfig.eventLoopSampleMs
        timer = setTimeout(() => {
            timer = null
            workerObservability.eventLoopDelay(Math.max(0, performance.now() - due))
            if (!stopped) schedule()
        }, backgroundWorkerMetricsConfig.eventLoopSampleMs)
        timer.unref()
    }
    process.once('rentnerproxy:shutdown', () => {
        stopped = true
        if (timer) clearTimeout(timer)
        timer = null
    })
    schedule()
}
