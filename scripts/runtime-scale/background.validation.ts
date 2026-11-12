import { z } from 'zod'
import { hostMonitoringCaptureSchema } from '../../web/src/server/HostMonitoring/host-monitoring.capture.ts'

export const backgroundExtendedRecoverySchema = z.object({
    status: z.literal('met'),
    elapsedMs: z.number().finite().nonnegative().max(600_000),
    interruptedCertificateJobs: z.literal(true),
    activeRestore: z.object({
        jobId: z.uuid(),
        finalStage: z.literal('applied'),
        strandedMonitoringLeases: z.literal(0),
        noLateMonitoringWrites: z.literal(true),
    }),
    audit: z.object({
        status: z.literal('met'),
        finalCursor: z.string().min(1),
        receiptIds: z.array(z.string().min(1)).min(1).max(10_000)
            .refine(ids => new Set(ids).size === ids.length),
        auditedEventCount: z.number().int().positive(),
        duplicateAuditEffects: z.literal(0),
        monitoringLeases: z.literal(0),
    }),
})

export const backgroundSnapshotSchema = z.strictObject({
    origin: z.number().finite().nonnegative(),
    policies: z
        .array(
            z.strictObject({
                id: z.uuid(),
                index: z.number().int().min(0).max(999),
                dueAt: z.number().finite().nonnegative(),
                enabled: z.boolean(),
                leased: z.boolean(),
                version: z.uuid(),
            }),
        )
        .max(1000),
    observations: z
        .array(
            z.strictObject({
                policyId: z.uuid(),
                policyVersion: z.uuid(),
                checkedAt: z.number().finite().nonnegative(),
                status: z.enum(['healthy', 'unhealthy', 'unknown']),
                code: z.enum([
                    'expected_status',
                    'unexpected_status',
                    'upstream_unreachable',
                    'upstream_timeout',
                    'upstream_dns_failed',
                    'tls_failed',
                    'configuration_stale',
                    'configuration_changed',
                    'host_not_active',
                    'runtime_unavailable',
                    'busy',
                    'invalid_response',
                ]),
            }),
        )
        .max(10000),
    managementReadMs: z.number().finite().nonnegative(),
    schedulerCapture: hostMonitoringCaptureSchema.nullable().optional(),
})

export const backgroundReceiptsSchema = z.strictObject({
    overflowed: z.boolean(),
    receipts: z
        .array(
            z.strictObject({
                index: z.number().int().min(0).max(999),
                latency: z.enum(['healthy', 'slow', 'timeout', 'failure']),
                startedAt: z.number().finite().nonnegative(),
                finishedAt: z.number().finite().nonnegative().nullable(),
            }),
        )
        .max(10000),
})
