export type ReliabilityOptions = {
    source: 'current'
    durationSeconds: number
    iterations: number
    concurrency: number
    captureResources: boolean
    seed: number
    image?: string
    reportPath?: string
    internalNetwork?: boolean
    backgroundContention?: true
    backgroundTelemetry?: true
}

export type ResourceSample = {
    elapsedSeconds: number
    phase: 'quiescent'
    /** Cgroup working set: memory.current minus inactive_file; never process RSS. */
    memoryBytes: number
    cgroupMemoryCurrentBytes?: number | null
    /** Per serving owner VmRSS from /proc/PID/status; absent/null means unknown. Shared pages may overlap. */
    webRssBytes?: number | null
    controllerRssBytes?: number | null
    caddyRssBytes?: number | null
    cpuPercent: number
    pids: number
    postgresConnections: number
    webFds: number
    controllerFds: number
    caddyFds: number
    restartCount: number
    oomKilled: boolean
}

export type ReliabilityCheck = {
    name: 'health' | 'proxy' | 'revision' | 'restart' | 'reload' | 'rollback' | 'resources'
    passed: boolean
}

export type ReliabilityFailure = {
    stage: 'setup' | 'warmup' | 'cycle' | 'resources' | 'cleanup'
    category: 'assertion' | 'timeout' | 'command' | 'telemetry' | 'unexpected' | 'cancelled'
}

export type ReliabilityReportInput = {
    targetSha: string
    imageIdentity: string
    options: ReliabilityOptions
    completedIterations: number
    observedDurationSeconds: number
    checks: ReliabilityCheck[]
    samples: ResourceSample[]
    failure?: ReliabilityFailure
}

import type { resourceLimits, metrics } from '../control.config.ts'

export type ResourceMetric = (typeof metrics)[number]

export type ResourceAnalysis = {
    memoryMeasurements: ReturnType<typeof import('../resource-probe.ts').memoryResourceEvidence>
    sampleCount: number
    trendEvidence: 'insufficient' | 'comparable'
    passed: boolean
    violations: (
        | 'oom'
        | 'restart'
        | 'memory-limit'
        | 'pid-limit'
        | 'connection-limit'
        | 'memory-growth'
        | 'connection-growth'
        | 'fd-growth'
        | 'telemetry-missing'
    )[]
    maxima: Record<ResourceMetric, number>
    headroom: typeof resourceLimits
    deltas: Partial<Record<ResourceMetric, number>>
}
