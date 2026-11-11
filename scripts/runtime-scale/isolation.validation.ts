import { z } from 'zod'
const nonnegative = z.number().finite().nonnegative()
export const isolationReadySchema = z.object({
    ready: z.literal(true),
    port: z.number().int().min(1).max(65535),
    pid: z.number().int().positive(),
    uid: z.number().int().positive(),
})
const distribution = z.object({
    count: nonnegative,
    p95: nonnegative.nullable(),
    p99: nonnegative.nullable(),
    max: nonnegative.nullable(),
})
export const isolationRoleMetricsSchema = z.object({
    pid: nonnegative,
    uid: nonnegative,
    rssBytes: nonnegative,
    cpuMicroseconds: nonnegative,
    fds: nonnegative,
    lag: distribution,
    failures: nonnegative,
    committed: nonnegative,
    commitLagMaxMs: nonnegative.nullable(),
})
