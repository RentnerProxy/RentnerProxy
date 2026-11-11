import { describe, expect, test } from 'bun:test'
import { eq } from 'drizzle-orm'
import { sql } from 'drizzle-orm'
import { DATABASE_CONNECTION_BUDGET } from '@/config/database.config.ts'

import { db } from '@/db/index.ts'
import { systemSettings, users } from '@/db/schema.ts'
import { getDatabaseUrl } from '@/server/env.server.ts'
import { checkDatabaseHealth } from '@/server/Foundation/database-health.server.ts'

const integrationTest =
    process.env.RENTNERPROXY_DATABASE_INTEGRATION === '1' && getDatabaseUrl() ? test : test.skip

describe('PostgreSQL integration', () => {
    integrationTest(
        'bounds the shared pool while eight background transactions leave interactive capacity',
        async () => {
            let release!: () => void
            const gate = new Promise<void>((resolve) => {
                release = resolve
            })
            const pids = new Set<number>()
            let admitted = 0
            const hold = () =>
                db.transaction(async (transaction) => {
                    await transaction.execute(sql`set local statement_timeout = '2000ms'`)
                    await transaction.execute(sql`set local lock_timeout = '2000ms'`)
                    await transaction.execute(sql`set local transaction_timeout = '3000ms'`)
                    const [row] = await transaction.execute<{ pid: number }>(
                        sql`select pg_backend_pid() pid`,
                    )
                    pids.add(row!.pid)
                    admitted++
                    await gate
                })
            const background = Array.from(
                { length: DATABASE_CONNECTION_BUDGET.backgroundTransactions },
                hold,
            )
            const waitFor = async (count: number) => {
                const deadline = Date.now() + 2000
                for (;;) {
                    if (admitted >= count) return
                    if (Date.now() > deadline)
                        throw new Error(
                            'Pool admission did not complete within its fixture deadline.',
                        )
                    // oxlint-disable-next-line no-await-in-loop -- Polls owned test transaction admission, never application work.
                    await Bun.sleep(1)
                }
            }
            let saturation: Promise<unknown>[] = []
            try {
                await waitFor(DATABASE_CONNECTION_BUDGET.backgroundTransactions)
                const management = await Promise.all(
                    Array.from({ length: DATABASE_CONNECTION_BUDGET.interactiveHeadroom }, () =>
                        db.execute(sql`select 1 as ready`),
                    ),
                )
                expect(management.every((rows) => rows[0]?.ready === 1)).toBe(true)
                saturation = Array.from(
                    { length: DATABASE_CONNECTION_BUDGET.interactiveHeadroom },
                    hold,
                )
                await waitFor(DATABASE_CONNECTION_BUDGET.maxConnections)
                let overflowAdmitted = false
                const overflow = db.execute(sql`select 1 as ready`).then(() => {
                    overflowAdmitted = true
                })
                await Bun.sleep(25)
                expect(overflowAdmitted).toBe(false)
                expect(pids.size).toBe(DATABASE_CONNECTION_BUDGET.maxConnections)
                release()
                await overflow
                expect(overflowAdmitted).toBe(true)
            } finally {
                release()
                await Promise.all([...background, ...saturation])
            }
        },
    )

    integrationTest(
        'uses PostgreSQL 18, the application schema, and the updated_at trigger',
        async () => {
            const key = `integration.${Date.now()}.${Math.random().toString(16).slice(2)}`
            let inserted = false

            try {
                expect(await checkDatabaseHealth()).toEqual({ state: 'connected' })

                const versionRows =
                    await db.$client`SELECT current_setting('server_version_num')::integer AS version`
                expect(versionRows[0]?.version).toBeGreaterThanOrEqual(180_000)

                const namespaceRows = await db.$client`
        SELECT
          to_regclass('public.system_settings') IS NULL AS public_table_missing,
          to_regclass('rentnerproxy.system_settings') IS NOT NULL AS application_table_exists,
          (
            SELECT namespace.nspname
            FROM pg_proc AS procedure
            INNER JOIN pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
            WHERE procedure.proname = 'rentnerproxy_set_system_settings_updated_at'
          ) AS function_schema
      `
                expect(namespaceRows[0]).toMatchObject({
                    public_table_missing: true,
                    application_table_exists: true,
                    function_schema: 'rentnerproxy',
                })

                const createdRows = await db
                    .insert(systemSettings)
                    .values({ key, value: { enabled: true } })
                    .returning()
                inserted = true
                const created = createdRows[0]

                expect(created).toBeDefined()

                if (!created) {
                    throw new Error('system setting was not returned after insert')
                }

                expect(created.id[14]).toBe('7')
                expect(created.createdAt).toBeInstanceOf(Date)
                expect(created.updatedAt).toBeInstanceOf(Date)

                await Bun.sleep(10)

                const updatedRows = await db
                    .update(systemSettings)
                    .set({ value: { enabled: false } })
                    .where(eq(systemSettings.key, key))
                    .returning({ updatedAt: systemSettings.updatedAt })
                const updated = updatedRows[0]

                expect(updated).toBeDefined()

                if (!updated) {
                    throw new Error('system setting was not returned after update')
                }

                expect(updated.updatedAt.getTime()).toBeGreaterThan(created.updatedAt.getTime())
            } finally {
                if (inserted) {
                    await db.delete(systemSettings).where(eq(systemSettings.key, key))
                }
            }
        },
    )

    integrationTest('round-trips profile image WebP bytes through the users schema', async () => {
        const email = `profile-image-${Date.now()}-${Math.random().toString(16).slice(2)}@integration.invalid`
        const expectedBytes = new Uint8Array([82, 73, 70, 70, 1, 2, 3, 4])
        let userId: string | null = null

        try {
            const insertedRows = await db
                .insert(users)
                .values({
                    displayName: 'Profile image integration',
                    email,
                    profileImageVersion: 7,
                    profileImageWebp: expectedBytes,
                })
                .returning({ id: users.id })
            userId = insertedRows.at(0)?.id ?? null

            expect(userId).not.toBeNull()

            const rows = await db
                .select({
                    profileImageVersion: users.profileImageVersion,
                    profileImageWebp: users.profileImageWebp,
                })
                .from(users)
                .where(eq(users.email, email))
            const stored = rows.at(0)

            expect(stored?.profileImageVersion).toBe(7)
            expect([...new Uint8Array(stored?.profileImageWebp ?? [])]).toEqual([...expectedBytes])
        } finally {
            if (userId) {
                await db.delete(users).where(eq(users.id, userId))
            }
        }
    })
})
