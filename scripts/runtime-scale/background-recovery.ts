import assert from 'node:assert/strict'
import { and, eq, isNull, sql } from 'drizzle-orm'
import {
    auditEvents,
    certificateEventCursor,
    certificateEventReceipts,
    certificateJobs,
    hostCheckPolicies,
} from '../../web/src/db/schema.ts'
import { synchronizeCertificateEventsOnce } from '../../web/src/server/Admin/CertificateManagement/certificate-events.worker.ts'
import type { FixtureContext } from '../runtime-reliability/Types/fixture.types.ts'
import { executeFixture } from '../runtime-reliability/fixture.ts'

/** Prepare/resume the same durable queued job or replay real certificate events, asserting exact cursor and audit idempotence. */
export async function backgroundRecoveryFixture(context: FixtureContext) {
    const database = context.database
    if (context.command.phase === 'background-job-prepare') {
        const result = await executeFixture({
            ...context,
            command: { ...context.command, phase: 'binding-request' },
        })
        assert.equal(typeof result.jobId, 'string')
        const held = await database
            .update(certificateJobs)
            .set({ nextAttemptAt: new Date(Date.now() + 3600000) })
            .where(
                and(
                    eq(certificateJobs.id, result.jobId as string),
                    eq(certificateJobs.stage, 'preparing'),
                    isNull(certificateJobs.leaseToken),
                ),
            )
            .returning({ id: certificateJobs.id, stage: certificateJobs.stage })
        assert.equal(
            held.length,
            1,
            'The suspended Bun owner leaves an actual queued certificate job for maintenance restore',
        )
        return { jobId: held[0]!.id, stage: held[0]!.stage, queuedForRestore: true }
    }
    if (context.command.phase === 'background-job-resume') {
        assert.ok(context.state.jobId)
        const [job] = await database
            .select()
            .from(certificateJobs)
            .where(eq(certificateJobs.id, context.state.jobId))
        assert.ok(
            job && job.stage === 'preparing' && !job.leaseToken,
            'Restore retains the same queued job without inventing a lease',
        )
        await database
            .update(certificateJobs)
            .set({ nextAttemptAt: new Date() })
            .where(eq(certificateJobs.id, job.id))
        return { jobId: job.id, stage: job.stage, leaseCleared: true }
    }
    assert.equal(context.command.phase, 'background-audit-replay')
    await synchronizeCertificateEventsOnce()
    /** Read sorted durable cursor, receipt, and audit identities so replay assertions compare effects rather than insertion order. */
    const read = async () => {
        const [cursor] = await database
            .select()
            .from(certificateEventCursor)
            .where(eq(certificateEventCursor.id, 1))
        const receipts = await database
            .select({ id: certificateEventReceipts.eventId })
            .from(certificateEventReceipts)
        const audit = await database
            .select({
                id: auditEvents.id,
                eventId: sql<string>`${auditEvents.metadata}->>'eventId'`,
            })
            .from(auditEvents)
            .where(
                and(
                    eq(auditEvents.resource, 'certificate'),
                    sql`${auditEvents.metadata}->>'eventId' is not null`,
                ),
            )
        return {
            cursor: cursor?.cursor,
            receiptIds: receipts.map((item) => item.id).toSorted(),
            auditIds: audit.map((item) => item.id).toSorted(),
            auditEventIds: audit.map((item) => item.eventId).toSorted(),
        }
    }
    const before = await read()
    assert.ok(
        before.cursor && before.receiptIds.length > 0,
        'Actual controller events are required to establish replay evidence',
    )
    assert.equal(
        new Set(before.auditEventIds).size,
        before.auditEventIds.length,
        'Every certificate event has at most one audit effect',
    )
    await database
        .update(certificateEventCursor)
        .set({ cursor: before.cursor.split(':')[0] + ':0' })
        .where(eq(certificateEventCursor.id, 1))
    await synchronizeCertificateEventsOnce()
    const after = await read()
    assert.deepEqual(
        after,
        before,
        'Replaying actual controller outcomes restores the final durable cursor without duplicate receipts/audit effects',
    )
    const leases = await database
        .select({ id: hostCheckPolicies.id })
        .from(hostCheckPolicies)
        .where(sql`${hostCheckPolicies.leaseToken} is not null`)
    return {
        status: 'met',
        finalCursor: after.cursor,
        receiptIds: after.receiptIds,
        auditedEventCount: after.auditEventIds.length,
        duplicateAuditEffects: 0,
        monitoringLeases: leases.length,
    }
}
