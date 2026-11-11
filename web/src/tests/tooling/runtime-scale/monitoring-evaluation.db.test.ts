import { describe, expect, test } from 'bun:test'
import { assertIsolatedMonitoringDatabase } from '../../../../../scripts/runtime-scale/monitoring.ts'
import { runMonitoringAuthoritySlice } from '../../../../../scripts/runtime-scale/monitoring-evaluation-db.ts'

let isolated = false
try {
    assertIsolatedMonitoringDatabase(
        process.env.RENTNERPROXY_DATABASE_INTEGRATION,
        process.env.DATABASE_URL,
    )
    isolated = true
} catch {
    /* Ordinary checks never connect to a database. */
}

describe('isolated restricted monitoring authority slice', () => {
    ;(isolated ? test : test.skip)(
        'fresh policy reads deny committed revocation and isolate secrets/writes across three paired slices',
        async () => {
            const result = await runMonitoringAuthoritySlice()
            expect(result.samples).toHaveLength(6)
            expect(result.denied).toHaveLength(4)
            expect(result.revocation).toMatchObject({
                cachedStillHasPolicy: true,
                freshDeniesPolicy: true,
            })
            expect(result.epochChanged).toBe(true)
            expect(result.duplicateReplayIdempotent).toBe(true)
            expect(result.conflictingReplayRejected).toBe(true)
            expect(result.remainingOutboxRecords).toBe(0)
            expect(result.migrationProof).toBe(false)
            expect(result.recommendation).toBe('keep-hybrid')
        },
        30000,
    )
})
