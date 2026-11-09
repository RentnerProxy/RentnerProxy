export interface MonitoringScaleOptions {
    hosts: 100 | 500 | 1000
    periods: 2 | 3
    profile: 'mixed' | 'slow' | 'timeout'
    runtime?: 'native'
    timeoutMs?: 10000
    scheduler?: 'legacy'
}

export interface MonitoringDistribution {
    count: number
    p95: number | null
    p99: number | null
    max: number | null
    total: number
}
