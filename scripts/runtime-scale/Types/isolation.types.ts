import type { isolationContract } from '../isolation.config.ts'

export type IsolationMode = (typeof isolationContract.modes)[number]
export type IsolationLoad = (typeof isolationContract.loads)[number]
export interface IsolationOptions {
    loads: IsolationLoad[]
    repetitions: 3
    durationSeconds: number
    faults: boolean
    faultsOnly: boolean
}
export interface IsolationDistribution {
    count: number
    p95: number | null
    p99: number | null
    max: number | null
}
export interface IsolationRoleMetrics {
    pid: number
    uid: number
    rssBytes: number
    cpuMicroseconds: number
    fds: number
    lag: IsolationDistribution
    failures: number
    committed: number
    commitLagMaxMs: number | null
}
export interface IsolationMeasurement {
    mode: IsolationMode
    load: IsolationLoad
    repetition: number
    elapsedMs: number
    ssr: IsolationDistribution
    ssrFailures: number
    rejectedTraffic: number
    telemetryFailures: number
    foreground: IsolationRoleMetrics
    background: IsolationRoleMetrics | null
    bunRssBytes: number
    idleBunRssBytes: number
    idleCpuPercentagePoints: number
    totalMemoryBytes: number
    totalCpuPercentagePoints: number
    postgresConnections: number
    pids: number
    observations: number
    healthyObservations: number
    network: { probes: number; accepted: number; invalid: number; snapshots: number }
    duplicates: number
    dueToCommitMaxMs: number | null
    startupMs: number
    stopMs: number
}

export interface IsolationChild {
    process: Bun.Subprocess<'ignore', 'pipe', 'pipe'>
    ready: { port: number; pid: number; uid: number }
    startupMs: number
}
