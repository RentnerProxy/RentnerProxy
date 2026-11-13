import '@tanstack/react-start/server-only'

import { sql } from 'drizzle-orm'

import { auditEvents } from '@/db/schema.ts'
import type {
    AuditEventInput,
    AuditMetadata,
} from '@/lib/Admin/AuditLogs/Types/audit-events.types.ts'
import { auditEventInputSchema } from '@/features/Admin/AuditLogs/validation.ts'
import { getAuthDatabase } from '@/server/Auth/Core/database.server.ts'
import type { AuthTransaction } from '@/server/Auth/Core/Types/database.types.ts'
import { AuthDomainError } from '@/server/Auth/Core/errors.server.ts'
import { markAuditedApplicationMutation } from '@/server/WebSockets/committedChanges.ts'
import {
    lockAuditRetentionInTransaction,
    retainAuditEventsInTransaction,
} from './audit-retention.ts'

export { AUDIT_RETENTION_LOCK_ID } from './audit-retention.ts'
const AUDIT_METADATA_MAX_BYTES = 4_096

/** Validate the audit allowlist and encoded size before any row is written. */
function parseEventInput(input: AuditEventInput): AuditEventInput {
    let parsed: ReturnType<typeof auditEventInputSchema.parse>
    try {
        parsed = auditEventInputSchema.parse(input)
    } catch {
        throw new AuthDomainError('invalid_input', 'Audit event is invalid.')
    }
    const metadata = parsed.metadata ?? {}
    if (Buffer.byteLength(JSON.stringify(metadata), 'utf8') > AUDIT_METADATA_MAX_BYTES) {
        throw new AuthDomainError('invalid_input', 'Audit metadata is too large.')
    }
    return {
        ...parsed,
        metadata: Object.fromEntries(
            Object.entries(metadata).filter(([, value]) => value !== undefined),
        ),
    }
}

/** Bind the validated object once, retaining SQL-queryable JSONB and the empty-object default. */
function metadataValue(metadata: AuditMetadata | undefined) {
    // Bun SQL serializes the bound object; bypass Drizzle's additional JSON string encoding.
    return sql`${metadata ?? {}}`
}

/** Keep the event, retention and committed-change marker in the caller's atomic mutation. */
export async function appendAuditEventInTransactionService(
    transaction: AuthTransaction,
    input: AuditEventInput,
): Promise<void> {
    const parsed = parseEventInput(input)

    await lockAuditRetentionInTransaction(transaction)
    await transaction.insert(auditEvents).values({
        actorUserId: parsed.actorUserId,
        actorKind: parsed.actorKind,
        action: parsed.action,
        resource: parsed.resource,
        targetId: parsed.targetId,
        result: parsed.result,
        metadata: metadataValue(parsed.metadata),
    })
    await retainAuditEventsInTransaction(transaction)
    markAuditedApplicationMutation(transaction, parsed)
}

/** Validate at most 500 events before one atomic insert and retention pass. */
export async function appendAuditEventsInTransactionService(
    transaction: AuthTransaction,
    inputs: readonly AuditEventInput[],
): Promise<void> {
    if (inputs.length === 0) return
    if (inputs.length > 500) throw new AuthDomainError('invalid_input', 'Audit batch is too large.')
    const events = inputs.map(parseEventInput)
    await lockAuditRetentionInTransaction(transaction)
    await transaction.insert(auditEvents).values(
        events.map((event) => ({
            actorUserId: event.actorUserId,
            actorKind: event.actorKind,
            action: event.action,
            resource: event.resource,
            targetId: event.targetId,
            result: event.result,
            metadata: metadataValue(event.metadata),
        })),
    )
    await retainAuditEventsInTransaction(transaction)
    for (const event of events) markAuditedApplicationMutation(transaction, event)
}

export async function recordAuditEventBestEffortService(input: AuditEventInput): Promise<void> {
    try {
        await getAuthDatabase().transaction(async (transaction) => {
            await appendAuditEventInTransactionService(transaction, input)
        })
    } catch {}
}
