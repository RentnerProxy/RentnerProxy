import { runDetachedTask } from '@/lib/Async/runDetachedTask.ts'
import {
    createCsrfMiddleware,
    createMiddleware,
    createServerOnlyFn,
    createStart,
} from '@tanstack/react-start'
import { getRequestProtocol, setResponseHeaders } from '@tanstack/react-start/server'

import { getTrustProxyHeaders, validateProductionEnvironment } from '@/server/env.server.ts'
import { startRealtimeValkey } from '@/server/WebSockets/realtimeValkey.service.ts'
import { startWorkerLifecycle } from '@/server/Foundation/worker-lifecycle.server.ts'
import { observeWorkerOperation } from '@/server/Foundation/worker-operation.ts'
import {
    applyAdminUiSecurityHeaders,
    createCspNonce,
    getAdminUiSecurityHeaders,
} from '@/server/security-headers.ts'

const validateProductionEnvironmentAtStartup = createServerOnlyFn(() => {
    if (process.env.NODE_ENV === 'production') validateProductionEnvironment()
})

if (typeof window === 'undefined') validateProductionEnvironmentAtStartup()

const startRealtimeEvents = createServerOnlyFn(startRealtimeValkey)
if (typeof window === 'undefined') startRealtimeEvents()

const startProxyRuntimeLifecycle = createServerOnlyFn(() =>
    startWorkerLifecycle(async () => {
        const { reconcileProxyConfigurationService } =
            await import('@/server/ProxyRuntime/proxy-runtime.service.ts')
        return reconcileProxyConfigurationService
    }, 'proxy_reconciliation'),
)

if (typeof window === 'undefined')
    runDetachedTask(
        () => startProxyRuntimeLifecycle(),
        () => console.error('[startup] Background operation failed'),
    )

const startDockerDiscoveryLifecycle = createServerOnlyFn(() =>
    startWorkerLifecycle(async () => {
        const { dockerDiscoveryWorker } =
            await import('@/server/Admin/DockerDiscovery/discovery-worker.ts')
        return {
            // Reconciliation is admitted work, not startup: its SQL/controller budget exceeds startup's deadline.
            start: () => {
                runDetachedTask(
                    () => dockerDiscoveryWorker.start(),
                    () => console.error('[startup] Background operation failed'),
                )
            },
            // The worker retains its initial flight, so maintenance still drains every admitted write.
            stop: () => dockerDiscoveryWorker.stop(),
        }
    }, 'docker_discovery'),
)
if (typeof window === 'undefined')
    runDetachedTask(
        () => startDockerDiscoveryLifecycle(),
        () => console.error('[startup] Background operation failed'),
    )

const startCrowdSecRuntimeLifecycle = createServerOnlyFn(() =>
    startWorkerLifecycle(async () => {
        const { reconcileCrowdSecConfiguration } =
            await import('@/server/Admin/CrowdSec/crowdsec.service.ts')
        return reconcileCrowdSecConfiguration
    }, 'crowdsec_reconciliation'),
)

if (typeof window === 'undefined')
    runDetachedTask(
        () => startCrowdSecRuntimeLifecycle(),
        () => console.error('[startup] Background operation failed'),
    )

const startCertificateEventsLifecycle = createServerOnlyFn(() =>
    startWorkerLifecycle(async () => {
        const { startCertificateEventsSynchronization, stopCertificateEventsSynchronization } =
            await import('@/server/Admin/CertificateManagement/certificate-events.worker.ts')
        return {
            start: startCertificateEventsSynchronization,
            stop: stopCertificateEventsSynchronization,
        }
    }, 'certificate_events'),
)

if (typeof window === 'undefined')
    runDetachedTask(
        () => startCertificateEventsLifecycle(),
        () => console.error('[startup] Background operation failed'),
    )

const startCertificateJobsLifecycle = createServerOnlyFn(() =>
    startWorkerLifecycle(async () => {
        const { startCertificateJobWorker, stopCertificateJobWorker } =
            await import('@/server/Admin/ProxyHostManagement/certificate-jobs.worker.server.ts')
        return { start: startCertificateJobWorker, stop: stopCertificateJobWorker }
    }, 'certificate_jobs'),
)

if (typeof window === 'undefined')
    runDetachedTask(
        () => startCertificateJobsLifecycle(),
        () => console.error('[startup] Background operation failed'),
    )

const startBackupOutcomesLifecycle = createServerOnlyFn(() =>
    startWorkerLifecycle(async () => {
        const { backupOutcomesWorker } =
            await import('@/server/Admin/Backups/backup-outcomes.server.ts')
        return backupOutcomesWorker
    }, 'backup_outcomes'),
)
if (typeof window === 'undefined')
    runDetachedTask(
        () => startBackupOutcomesLifecycle(),
        () => console.error('[startup] Background operation failed'),
    )

const startNotificationLifecycle = createServerOnlyFn(() =>
    startWorkerLifecycle(async () => {
        const { notificationWorker } =
            await import('@/server/Notifications/notification-worker.server.ts')
        return notificationWorker
    }, 'notifications'),
)
if (typeof window === 'undefined')
    runDetachedTask(
        () => startNotificationLifecycle(),
        () => console.error('[startup] Background operation failed'),
    )

const startHostMonitoringLifecycle = createServerOnlyFn(() =>
    startWorkerLifecycle(async () => {
        const { hostMonitoringWorker } =
            await import('@/server/HostMonitoring/host-monitoring.worker.server.ts')
        return hostMonitoringWorker
    }, 'host_monitoring'),
)
if (typeof window === 'undefined')
    runDetachedTask(
        () => startHostMonitoringLifecycle(),
        () => console.error('[startup] Background operation failed'),
    )

const startNpmImportTempCleanup = createServerOnlyFn(async () => {
    await startWorkerLifecycle(async () => {
        const { pruneStaleNpmImports } = await import('@/server/Admin/NpmImport/npm-temp.ts')
        let timer: ReturnType<typeof setInterval> | null = null
        let running: Promise<void> | null = null
        let stopped = false
        let unavailable = false
        const prune = () => {
            if (stopped) return
            running ??= observeWorkerOperation('npm_import_cleanup', pruneStaleNpmImports)
                .then(() => {
                    unavailable = false
                })
                .catch(() => {
                    if (!unavailable) console.warn('[npm-import] temporary cleanup unavailable')
                    unavailable = true
                })
                .then(() => undefined)
                .finally(() => {
                    running = null
                })
        }
        return {
            start() {
                if (stopped || timer) return
                prune()
                timer = setInterval(prune, 60 * 60 * 1_000)
                timer.unref()
            },
            async stop() {
                stopped = true
                if (timer !== null) clearInterval(timer)
                timer = null
                await running
            },
        }
    }, 'npm_import_cleanup')
})
if (typeof window === 'undefined')
    runDetachedTask(
        () => startNpmImportTempCleanup(),
        () => console.error('[startup] Background operation failed'),
    )

const securityHeadersMiddleware = createMiddleware().server(async ({ next }) => {
    const nonce = createCspNonce()
    const securityHeaders = getAdminUiSecurityHeaders(
        getRequestProtocol({ xForwardedProto: getTrustProxyHeaders() }),
        nonce,
    )
    setResponseHeaders(new Headers(securityHeaders))

    const result = await next({ context: { cspNonce: nonce } })
    return {
        ...result,
        response: applyAdminUiSecurityHeaders(result.response, securityHeaders),
    }
})

const csrfMiddleware = createCsrfMiddleware({
    filter: (context) => context.handlerType === 'serverFn',
})

export const startInstance = createStart(() => ({
    requestMiddleware: [securityHeadersMiddleware, csrfMiddleware],
}))
