export const backgroundWorkerIds = [
    'proxy_reconciliation',
    'crowdsec_reconciliation',
    'certificate_events',
    'certificate_jobs',
    'backup_outcomes',
    'notifications',
    'host_monitoring',
    'docker_discovery',
    'npm_import_cleanup',
] as const

export const backgroundWorkerStates = [
    'unknown',
    'starting',
    'idle',
    'running',
    'retrying',
    'degraded',
    'maintenance_paused',
    'stopping',
    'stopped',
    'failed',
] as const

export const controllerWorkerIds = [
    'caddy_recovery',
    'acme_renewal',
    'acme_orders',
    'ddns',
    'docker_observation',
] as const

export const backgroundWorkerMetricsConfig = {
    lanes: ['execution', 'admission', 'persistence', 'retention', 'collection', 'delivery'],
    maximumInteger: 9_007_199_254_740_991,
    durationBoundsMs: [10, 100, 1_000, 10_000, 60_000],
    logIntervalMs: 60_000,
    eventLoopSampleMs: 5_000,
} as const
