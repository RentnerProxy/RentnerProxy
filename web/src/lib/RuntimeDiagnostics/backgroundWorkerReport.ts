import { z } from 'zod'

import {
    backgroundWorkerIds,
    backgroundWorkerStates,
    controllerWorkerIds,
    backgroundWorkerMetricsConfig,
} from '@/config/background-workers.config.ts'
import { isRecord } from '@/lib/Records/isRecord.ts'

const integer = z.number().int().min(0).max(Number.MAX_SAFE_INTEGER)
const instant = integer.nullable()
const buckets = z.tuple([integer, integer, integer, integer, integer, integer])
const epoch = z.object({ startedAt: integer, sequence: integer })
const counters = z.object({
    started: integer,
    success: integer,
    failure: integer,
    cancelled: integer,
    deferred: integer,
    retry: integer,
    exhausted: integer,
    admissionRejected: integer,
    leaseRecovered: integer,
    drainTimeout: integer,
})
const bunWorker = z.object({
    id: z.enum(backgroundWorkerIds),
    state: z.enum(backgroundWorkerStates),
    degradedLanes: z
        .array(z.enum(backgroundWorkerMetricsConfig.lanes))
        .max(backgroundWorkerMetricsConfig.lanes.length),
    stateChangedAt: instant,
    lastStartedAt: instant,
    lastFinishedAt: instant,
    lastSuccessAt: instant,
    lastFailureAt: instant,
    running: integer,
    counters,
    duration: z.object({
        boundsMs: z.tuple([
            z.literal(10),
            z.literal(100),
            z.literal(1_000),
            z.literal(10_000),
            z.literal(60_000),
        ]),
        buckets,
        count: integer,
        sumMs: integer,
    }),
    backlog: integer.nullable(),
    sampledAt: instant,
    oldestPendingAt: instant,
    nextDueAt: instant,
    concurrency: integer.nullable(),
    overflowed: z.boolean(),
})

const controllerWorker = z.object({
    id: z.enum(controllerWorkerIds),
    owner: z.literal('controller'),
    state: z.enum([
        'unknown',
        'starting',
        'idle',
        'running',
        'degraded',
        'stopping',
        'stopped',
        'failed',
    ]),
    lastAttempt: instant,
    lastSuccess: instant,
    lastFinished: instant,
    runningCount: integer,
    attempts: integer,
    successes: integer,
    failures: integer,
    cancellations: integer,
    durationBuckets: buckets,
    overflowed: z.boolean(),
})

const bunSnapshot = z.object({
    epoch,
    observedAt: integer,
    eventLoopLag: z.object({
        sampledAt: instant,
        count: integer,
        maximumMs: instant,
        buckets,
        overflowed: z.boolean(),
    }),
    workers: z.array(bunWorker).length(backgroundWorkerIds.length),
})
const controllerSnapshot = z.object({
    epoch,
    observedAt: integer,
    workers: z.array(controllerWorker).length(controllerWorkerIds.length),
})

function boundedSnapshot<T>(
    input: unknown,
    ids: readonly string[],
    schema: z.ZodType<T>,
): T | null {
    if (!isRecord(input) || !Array.isArray(input.workers) || input.workers.length !== ids.length)
        return null
    try {
        const result = schema.safeParse(input)
        if (!result.success) return null
        const seen = new Set<string>()
        for (const entry of input.workers) {
            if (!isRecord(entry) || typeof entry.id !== 'string' || !ids.includes(entry.id))
                return null
            seen.add(entry.id)
        }
        return seen.size === ids.length ? result.data : null
    } catch {
        return null
    }
}

export function readBunWorkerReport(input: unknown) {
    return boundedSnapshot(input, backgroundWorkerIds, bunSnapshot)
}

export function readControllerWorkerReport(input: unknown) {
    return boundedSnapshot(input, controllerWorkerIds, controllerSnapshot)
}

export type BunWorkerReport = z.infer<typeof bunSnapshot>
export type ControllerWorkerReport = z.infer<typeof controllerSnapshot>
