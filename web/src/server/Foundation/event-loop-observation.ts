import { workerObservability } from './worker-observability.ts'
import { backgroundWorkerMetricsConfig } from '@/config/background-workers.config.ts'

let timer: ReturnType<typeof setTimeout> | null = null
let stopped = false

export function startEventLoopObservation(): void {
    if (timer || stopped) return
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
