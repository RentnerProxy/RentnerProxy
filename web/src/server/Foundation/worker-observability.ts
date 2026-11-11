import {
    backgroundWorkerIds,
    backgroundWorkerMetricsConfig,
    backgroundWorkerStates,
} from '@/config/background-workers.config.ts'
import type {
    BackgroundWorkerState,
    BackgroundWorkerId,
    WorkerCountEvent,
    WorkerObservability,
    WorkerObservabilityOptions,
    WorkerOutcome,
    WorkerSnapshot,
} from './Types/worker-observability.types.ts'

const outcomes = new Set<WorkerOutcome>(['success', 'failure', 'cancelled', 'deferred'])
const countEvents = new Set<WorkerCountEvent>([
    'retry',
    'exhausted',
    'admissionRejected',
    'leaseRecovered',
    'drainTimeout',
])
const protectedStates = new Set<BackgroundWorkerState>([
    'maintenance_paused',
    'stopping',
    'stopped',
    'failed',
])
const { maximumInteger, durationBoundsMs } = backgroundWorkerMetricsConfig

function boundedNumber(value: unknown): number | null {
    if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) return null
    return Math.min(maximumInteger, Math.floor(value))
}

function makeWorkers() {
    return backgroundWorkerIds.map((id): WorkerSnapshot => ({
        id,
        state: 'unknown',
        degradedLanes: [],
        stateChangedAt: null,
        lastStartedAt: null,
        lastFinishedAt: null,
        lastSuccessAt: null,
        lastFailureAt: null,
        running: 0,
        counters: {
            started: 0,
            success: 0,
            failure: 0,
            cancelled: 0,
            deferred: 0,
            retry: 0,
            exhausted: 0,
            admissionRejected: 0,
            leaseRecovered: 0,
            drainTimeout: 0,
        },
        duration: {
            boundsMs: [...durationBoundsMs],
            buckets: Array<number>(durationBoundsMs.length + 1).fill(0),
            count: 0,
            sumMs: 0,
        },
        backlog: null,
        sampledAt: null,
        oldestPendingAt: null,
        nextDueAt: null,
        concurrency: null,
        overflowed: false,
    }))
}

function add(worker: WorkerSnapshot, value: number, increment = 1) {
    if (increment > maximumInteger - value) {
        worker.overflowed = true
        return maximumInteger
    }
    return value + increment
}

export function createWorkerObservability(
    options: WorkerObservabilityOptions = {},
): WorkerObservability {
    const now = () => boundedNumber((options.now ?? Date.now)()) ?? 0
    let generation = {}
    let epoch = { startedAt: now(), sequence: 0 }
    let workers = makeWorkers()
    let eventLoopLag = {
        sampledAt: null as number | null,
        count: 0,
        maximumMs: null as number | null,
        buckets: Array<number>(durationBoundsMs.length + 1).fill(0),
        overflowed: false,
    }
    const find = (id: unknown) => workers.find((worker) => worker.id === id)
    const transition = (worker: WorkerSnapshot, state: BackgroundWorkerState) => {
        if (!backgroundWorkerStates.includes(state) || worker.state === state) return
        const previousState = worker.state
        worker.state = state
        worker.stateChangedAt = now()
        try {
            options.log?.({ workerId: worker.id, previousState, state })
        } catch {
            // Diagnostic sinks must never interrupt worker progress or shutdown.
        }
    }

    return {
        eventLoopDelay(milliseconds) {
            const delay = boundedNumber(milliseconds)
            if (delay === null) return
            eventLoopLag.sampledAt = now()
            eventLoopLag.maximumMs = Math.max(eventLoopLag.maximumMs ?? 0, delay)
            const index = durationBoundsMs.findIndex((bound) => delay <= bound)
            const bucket = index < 0 ? durationBoundsMs.length : index
            if (
                eventLoopLag.count === maximumInteger ||
                eventLoopLag.buckets[bucket] === maximumInteger
            )
                eventLoopLag.overflowed = true
            eventLoopLag.count = Math.min(maximumInteger, eventLoopLag.count + 1)
            eventLoopLag.buckets[bucket] = Math.min(
                maximumInteger,
                (eventLoopLag.buckets[bucket] ?? 0) + 1,
            )
        },
        count(id, event) {
            const worker = find(id)
            if (worker && countEvents.has(event))
                worker.counters[event] = add(worker, worker.counters[event])
        },
        state(id, state) {
            const worker = find(id)
            if (worker) transition(worker, state)
        },
        begin(id, lane = 'execution') {
            const worker = find(id)
            if (!worker || !backgroundWorkerMetricsConfig.lanes.includes(lane)) return () => {}
            const startedAt = now()
            const startedGeneration = generation
            worker.lastStartedAt = startedAt
            worker.running = add(worker, worker.running)
            worker.counters.started = add(worker, worker.counters.started)
            if (!protectedStates.has(worker.state) && worker.degradedLanes.length === 0)
                transition(worker, 'running')
            let finished = false
            return (outcome) => {
                if (finished || startedGeneration !== generation || !outcomes.has(outcome)) return
                finished = true
                const finishedAt = now()
                worker.running = Math.max(0, worker.running - 1)
                worker.lastFinishedAt = finishedAt
                worker.counters[outcome] = add(worker, worker.counters[outcome])
                if (outcome === 'success') worker.lastSuccessAt = finishedAt
                if (outcome === 'failure') worker.lastFailureAt = finishedAt
                if (outcome === 'failure' && !worker.degradedLanes.includes(lane))
                    worker.degradedLanes.push(lane)
                if (outcome === 'success' || outcome === 'deferred')
                    worker.degradedLanes = worker.degradedLanes.filter((item) => item !== lane)
                const elapsed = Math.max(0, finishedAt - startedAt)
                const boundedBucket = durationBoundsMs.findIndex((bound) => elapsed <= bound)
                const bucket = boundedBucket < 0 ? durationBoundsMs.length : boundedBucket
                worker.duration.buckets[bucket] = add(worker, worker.duration.buckets[bucket] ?? 0)
                worker.duration.count = add(worker, worker.duration.count)
                worker.duration.sumMs = add(worker, worker.duration.sumMs, elapsed)
                if (protectedStates.has(worker.state)) return
                if (worker.degradedLanes.length > 0) transition(worker, 'degraded')
                else transition(worker, worker.running > 0 ? 'running' : 'idle')
            }
        },
        sample(id, sample) {
            const worker = find(id)
            if (!worker || !sample || typeof sample !== 'object') return
            worker.sampledAt = now()
            for (const key of ['backlog', 'oldestPendingAt', 'nextDueAt', 'concurrency'] as const) {
                if (Object.hasOwn(sample, key)) worker[key] = boundedNumber(sample[key])
            }
        },
        snapshot() {
            return {
                epoch: { ...epoch },
                observedAt: now(),
                eventLoopLag: { ...eventLoopLag, buckets: [...eventLoopLag.buckets] },
                workers: workers.map((worker) =>
                    Object.assign({}, worker, {
                        degradedLanes: [...worker.degradedLanes],
                        counters: { ...worker.counters },
                        duration: {
                            ...worker.duration,
                            boundsMs: [...worker.duration.boundsMs],
                            buckets: [...worker.duration.buckets],
                        },
                    }),
                ),
            }
        },
        reset() {
            generation = {}
            epoch = { startedAt: now(), sequence: Math.min(maximumInteger, epoch.sequence + 1) }
            workers = makeWorkers()
            eventLoopLag = {
                sampledAt: null,
                count: 0,
                maximumMs: null,
                buckets: Array<number>(durationBoundsMs.length + 1).fill(0),
                overflowed: false,
            }
        },
    }
}

const loggedAt = new Map<BackgroundWorkerId, number>()

export const workerObservability = createWorkerObservability({
    log(event) {
        if (event.state === 'running' || event.state === 'starting') return
        if (event.state === 'idle' && !['degraded', 'retrying'].includes(event.previousState))
            return
        const now = Date.now()
        if (
            now - (loggedAt.get(event.workerId) ?? -Infinity) <
            backgroundWorkerMetricsConfig.logIntervalMs
        )
            return
        loggedAt.set(event.workerId, now)
        console.info('[worker-observability]', event)
    },
})
