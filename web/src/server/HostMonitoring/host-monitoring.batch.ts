import { HOST_MONITORING_LIMITS } from '@/config/host-monitoring.config.ts'
import type {
    HostMonitoringBatchEntry,
    HostMonitoringBatchOptions,
} from './Types/host-monitoring-batch.types.ts'

export function createHostMonitoringBatcher<Input, Output>(
    options: HostMonitoringBatchOptions<Input, Output>,
) {
    const pending: HostMonitoringBatchEntry<Input, Output>[] = []
    let timer: ReturnType<typeof setTimeout> | undefined
    let running = false
    let admitted = 0

    function schedule() {
        if (running || timer || pending.length === 0) return
        timer = setTimeout(() => {
            timer = undefined
            void flush()
        }, options.delayMs)
    }

    async function flush() {
        if (running || pending.length === 0) return
        if (timer) clearTimeout(timer)
        timer = undefined
        running = true
        const entries = pending.splice(0, options.size)
        try {
            const results = await options.run(entries.map((entry) => entry.input))
            if (results.length !== entries.length)
                throw new Error('Invalid monitoring batch result.')
            for (const [index, entry] of entries.entries()) {
                const result = results[index]!
                if (result.status === 'fulfilled') entry.resolve(result.value)
                else entry.reject(result.reason)
            }
        } catch (error) {
            for (const entry of entries) entry.reject(error)
        } finally {
            admitted -= entries.length
            running = false
            if (pending.length >= options.size) void flush()
            else schedule()
        }
    }

    return (input: Input): Promise<Output> => {
        if (admitted >= HOST_MONITORING_LIMITS.workerConcurrency)
            return Promise.reject(new Error('Monitoring batch admission is full.'))
        admitted++
        const result = new Promise<Output>((resolve, reject) => {
            pending.push({ input, resolve, reject })
        })
        if (pending.length >= options.size) void flush()
        else schedule()
        return result
    }
}
