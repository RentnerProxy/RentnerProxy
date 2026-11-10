import { drainWorker } from '@/server/Foundation/worker-lifecycle.server.ts'
import type { BackupAuditWorkerDependencies } from './Types/backup-outcomes.types.ts'

export function createBackupAuditWorker(dependencies: BackupAuditWorkerDependencies) {
    let running: Promise<void> | null = null
    let controller: AbortController | null = null
    let timer: ReturnType<typeof setTimeout> | null = null
    let started = false
    let stopped = false
    let unavailable = false
    function schedule() {
        if (!started || stopped || timer) return
        timer = setTimeout(() => {
            timer = null
            void runOnce().finally(schedule)
        }, 15_000)
        timer.unref()
    }
    function runOnce(): Promise<void> {
        if (stopped) return Promise.resolve()
        if (running) return running
        controller = new AbortController()
        const signal = controller.signal
        running = Promise.resolve()
            .then(() => dependencies.ingest(signal))
            .then(() => {
                unavailable = false
            })
            .catch(() => {
                if (signal.aborted) return
                if (!unavailable) dependencies.unavailable()
                unavailable = true
            })
            .finally(() => {
                running = null
                controller = null
            })
        return running
    }
    return {
        runOnce,
        start() {
            if (started || stopped) return
            started = true
            stopped = false
            void runOnce().finally(schedule)
        },
        async stop() {
            started = false
            stopped = true
            if (timer) clearTimeout(timer)
            timer = null
            controller?.abort()
            if (!running) return
            await drainWorker(running, 5_000)
        },
    }
}
