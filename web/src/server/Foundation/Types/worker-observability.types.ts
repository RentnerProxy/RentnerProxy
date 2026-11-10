import type {
    backgroundWorkerIds,
    backgroundWorkerStates,
    backgroundWorkerMetricsConfig,
} from '@/config/background-workers.config.ts'

export type BackgroundWorkerId = (typeof backgroundWorkerIds)[number]
export type BackgroundWorkerState = (typeof backgroundWorkerStates)[number]
export type BackgroundWorkerLane = (typeof backgroundWorkerMetricsConfig.lanes)[number]
export type WorkerOutcome = 'success' | 'failure' | 'cancelled' | 'deferred'
export type WorkerCountEvent =
    'retry' | 'exhausted' | 'admissionRejected' | 'leaseRecovered' | 'drainTimeout'

export interface WorkerSample {
    backlog?: number | null
    oldestPendingAt?: number | null
    nextDueAt?: number | null
    concurrency?: number | null
}

export interface WorkerSnapshot {
    id: BackgroundWorkerId
    state: BackgroundWorkerState
    degradedLanes: BackgroundWorkerLane[]
    stateChangedAt: number | null
    lastStartedAt: number | null
    lastFinishedAt: number | null
    lastSuccessAt: number | null
    lastFailureAt: number | null
    running: number
    counters: Record<'started' | WorkerOutcome | WorkerCountEvent, number>
    duration: { boundsMs: number[]; buckets: number[]; count: number; sumMs: number }
    backlog: number | null
    sampledAt: number | null
    oldestPendingAt: number | null
    nextDueAt: number | null
    concurrency: number | null
    overflowed: boolean
}

export interface WorkerStateTransition {
    workerId: BackgroundWorkerId
    previousState: BackgroundWorkerState
    state: BackgroundWorkerState
}

export interface WorkerObservabilityOptions {
    now?: () => number
    log?: (event: WorkerStateTransition) => void
}

export interface WorkerObservabilitySnapshot {
    epoch: { startedAt: number; sequence: number }
    observedAt: number
    eventLoopLag: {
        sampledAt: number | null
        count: number
        maximumMs: number | null
        buckets: number[]
        overflowed: boolean
    }
    workers: WorkerSnapshot[]
}

export interface WorkerObservability {
    eventLoopDelay: (milliseconds: number) => void
    count: (id: BackgroundWorkerId, event: WorkerCountEvent) => void
    state: (id: BackgroundWorkerId, state: BackgroundWorkerState) => void
    begin: (id: BackgroundWorkerId, lane?: BackgroundWorkerLane) => (outcome: WorkerOutcome) => void
    sample: (id: BackgroundWorkerId, sample: WorkerSample) => void
    snapshot: () => WorkerObservabilitySnapshot
    reset: () => void
}
