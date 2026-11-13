import { sql } from 'drizzle-orm'
import { bigint, boolean, check } from 'drizzle-orm/pg-core'

import { rentnerProxySchema } from './base.ts'

/** Exact cardinality maintained by statement triggers, including raw SQL and COPY writes. */
export const auditRetentionState = rentnerProxySchema.table(
    'audit_retention_state',
    {
        singleton: boolean('singleton').primaryKey().default(true),
        rowCount: bigint('row_count', { mode: 'bigint' }).notNull(),
    },
    (table) => [
        check('audit_retention_state_singleton_check', sql`${table.singleton}`),
        check('audit_retention_state_row_count_check', sql`${table.rowCount} >= 0`),
    ],
)
