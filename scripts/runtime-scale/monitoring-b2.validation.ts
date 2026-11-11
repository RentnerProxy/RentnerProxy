import { z } from 'zod'

const hash = z.string().regex(/^sha256:[a-f0-9]{64}$/u)
export const monitoringB2OriginalAdmissionSchema = z.strictObject({
    original_claim: z.unknown(),
    admitted_ms: z.string().regex(/^\d+$/u).nullable(),
})
export const monitoringB2ClaimSchema = z.strictObject({
    storeId: z.uuid(),
    controllerEpoch: z.uuid(),
    generation: z.number().int().positive(),
    policyId: z.uuid(),
    policyVersion: z.uuid(),
    leaseToken: z.uuid(),
    leaseExpiresAtMs: z.number().int().positive(),
    dueAtMs: z.number().int().nonnegative(),
    hostId: z.uuid(),
    domain: z.string().min(1).max(253),
    timeoutMs: z.number().int().min(1000).max(10000),
    before: z.strictObject({
        enabled: z.literal(true),
        proof: hash,
        revision: hash,
        availability: z.strictObject({ proof: hash, revision: hash }),
    }),
})

/** Compare lower-trust raw evidence to the immutable claim and first admission owned by PostgreSQL. */
export function monitoringB2AdmissionRejection(
    claim: z.infer<typeof monitoringB2ClaimSchema>,
    original: undefined | { original_claim: unknown; admitted_ms: string | number | null },
    row: {
        started_ms: number
        admitted_ms: number | null
        durable_ms: number | null
        completed_ms: number | null
    },
) {
    if (!original) return 'original_claim_unavailable'
    const parsed = monitoringB2ClaimSchema.safeParse(original.original_claim)
    if (!parsed.success || JSON.stringify(parsed.data) !== JSON.stringify(claim))
        return 'original_claim_mismatch'
    if (original.admitted_ms === null) return 'admission_not_recorded'
    const admitted = Number(original.admitted_ms)
    if (!Number.isSafeInteger(admitted) || row.admitted_ms !== admitted)
        return 'admission_timestamp_mismatch'
    if (
        ![row.started_ms, row.admitted_ms, row.durable_ms, row.completed_ms].every(
            Number.isSafeInteger,
        ) ||
        row.started_ms < claim.dueAtMs ||
        row.started_ms > admitted ||
        row.durable_ms! < admitted ||
        row.completed_ms! < row.durable_ms!
    )
        return 'invalid_start_timestamps'
    return null
}
