import { describe, expect, test } from 'bun:test'
import { eq } from 'drizzle-orm'
import { sql } from 'drizzle-orm'
import { DATABASE_CONNECTION_BUDGET } from '@/config/database.config.ts'
import { BACKGROUND_DATABASE_POLICY } from '@/config/background-resources.config.ts'
import {
    getBackgroundDatabase,
    getBackgroundDatabaseResourceSnapshot,
} from '@/server/Foundation/background-database.server.ts'
import type { BackgroundDatabaseClass } from '@/server/Foundation/Types/resource-admission.types.ts'

import { db } from '@/db/index.ts'
import {
    admitDatabaseTransaction,
    withBackgroundDatabaseContext,
} from '@/db/background-admission.ts'
import { ResourceAdmissionError } from '@/server/Foundation/resource-admission.ts'
import { observeWorkerOperation } from '@/server/Foundation/worker-operation.ts'
import { systemSettings, users } from '@/db/schema.ts'
import { getDatabaseUrl } from '@/server/env.server.ts'
import { checkDatabaseHealth } from '@/server/Foundation/database-health.server.ts'

const integrationTest =
    process.env.RENTNERPROXY_DATABASE_INTEGRATION === '1' && getDatabaseUrl() ? test : test.skip

const backgroundSlots = Object.entries(BACKGROUND_DATABASE_POLICY.classes).flatMap(
    ([resource, policy]) =>
        Array.from({ length: policy.running }, () => resource as BackgroundDatabaseClass),
)

async function waitForAdmission(ready: () => boolean): Promise<void> {
    const deadline = Date.now() + 2000
    while (!ready()) {
        if (Date.now() > deadline) throw new Error('Fixture admission deadline exceeded')
        // oxlint-disable-next-line no-await-in-loop -- Observe owned admission within a fixed fixture deadline.
        await Bun.sleep(1)
    }
}

describe('PostgreSQL integration', () => {
    test('a cancelled running background operation retains its permit until actual settlement', async () => {
        const controller = new AbortController()
        let release!: () => void
        const gate = new Promise<void>((resolve) => {
            release = resolve
        })
        let running = 0
        let queuedRan = false
        const held = backgroundSlots.map((resource) =>
            withBackgroundDatabaseContext(
                () =>
                    admitDatabaseTransaction(async () => {
                        running++
                        await gate
                    }),
                controller.signal,
                resource,
            ),
        )
        let queued: Promise<unknown> | undefined
        try {
            await waitForAdmission(
                () => running === DATABASE_CONNECTION_BUDGET.backgroundTransactions,
            )
            queued = withBackgroundDatabaseContext(() =>
                admitDatabaseTransaction(async () => {
                    queuedRan = true
                }),
            )
            controller.abort()
            await Bun.sleep(10)
            expect(queuedRan).toBe(false)
            expect(await admitDatabaseTransaction(async () => 'foreground')).toBe('foreground')
            await expect(
                withBackgroundDatabaseContext(
                    () =>
                        withBackgroundDatabaseContext(() =>
                            admitDatabaseTransaction(async () => 'must not execute'),
                        ),
                    controller.signal,
                ),
            ).rejects.toMatchObject({ reason: 'cancelled' })
            release()
            await queued
            expect(queuedRan).toBe(true)
        } finally {
            release()
            await Promise.allSettled([...held, ...(queued ? [queued] : [])])
        }
    })

    integrationTest(
        'class reservations admit eight background transactions once and leave all eight interactive slots available',
        async () => {
            let release!: () => void
            const gate = new Promise<void>((resolve) => {
                release = resolve
            })
            let admitted = 0
            let interactive = 0
            let overflowAdmitted = false
            const backgroundPids = new Set<number>()
            const interactivePids = new Set<number>()
            const hold = (resource: BackgroundDatabaseClass) =>
                getBackgroundDatabase(resource).transaction(async (transaction) => {
                    const [row] = await transaction.execute<{ pid: number; timeout: string }>(
                        sql`select pg_backend_pid() pid, current_setting('statement_timeout') as timeout`,
                    )
                    expect(row!.timeout).toBe(
                        `${BACKGROUND_DATABASE_POLICY.classes[resource].transactionMs / 1000}s`,
                    )
                    backgroundPids.add(row!.pid)
                    admitted++
                    await gate
                })
            const background = backgroundSlots.map((resource) =>
                observeWorkerOperation(
                    resource === 'critical' ? 'certificate_jobs' : 'notifications',
                    () => hold(resource),
                ),
            )
            let overflow: Promise<unknown> | undefined
            let management: Promise<unknown>[] = []
            try {
                await waitForAdmission(
                    () => admitted === DATABASE_CONNECTION_BUDGET.backgroundTransactions,
                )
                overflow = observeWorkerOperation('notifications', () =>
                    db.transaction(async () => {
                        overflowAdmitted = true
                    }),
                )
                management = Array.from(
                    { length: DATABASE_CONNECTION_BUDGET.interactiveHeadroom },
                    () =>
                        db.transaction(
                            async (transaction) => {
                                const [row] = await transaction.execute<{
                                    pid: number
                                    readOnly: string
                                }>(
                                    sql`select pg_backend_pid() pid, current_setting('transaction_read_only') as "readOnly"`,
                                )
                                expect(row!.readOnly).toBe('on')
                                await transaction.transaction(async (nested) => {
                                    await nested.execute(sql`select 1`)
                                })
                                interactivePids.add(row!.pid)
                                interactive++
                                await gate
                            },
                            { accessMode: 'read only' },
                        ),
                )
                await waitForAdmission(
                    () => interactive === DATABASE_CONNECTION_BUDGET.interactiveHeadroom,
                )
                await Bun.sleep(25)
                expect(overflowAdmitted).toBe(false)
                const usage = getBackgroundDatabaseResourceSnapshot()
                for (const [resource, policy] of Object.entries(BACKGROUND_DATABASE_POLICY.classes))
                    expect(usage[resource]!.running).toBe(policy.running)
                expect(usage.critical!.pending).toBe(1)
                expect(backgroundPids.size).toBe(DATABASE_CONNECTION_BUDGET.backgroundTransactions)
                expect(interactivePids.size).toBe(DATABASE_CONNECTION_BUDGET.interactiveHeadroom)
                expect(new Set([...backgroundPids, ...interactivePids]).size).toBe(
                    DATABASE_CONNECTION_BUDGET.maxConnections,
                )
                release()
                await overflow
                expect(overflowAdmitted).toBe(true)
            } finally {
                release()
                await Promise.allSettled([
                    ...background,
                    ...management,
                    ...(overflow ? [overflow] : []),
                ])
            }
        },
    )

    integrationTest(
        'queued cancellation never opens SQL and every failed transaction returns its background permit',
        async () => {
            let release!: () => void
            const gate = new Promise<void>((resolve) => {
                release = resolve
            })
            let admitted = 0
            const held = backgroundSlots.map((resource) =>
                getBackgroundDatabase(resource).transaction(async () => {
                    admitted++
                    await gate
                }),
            )
            let cancelled: Promise<unknown> | undefined
            try {
                const until = Date.now() + 2000
                while (admitted < DATABASE_CONNECTION_BUDGET.backgroundTransactions) {
                    if (Date.now() > until) throw new Error('Fixture admission deadline exceeded')
                    // oxlint-disable-next-line no-await-in-loop -- Wait only for the owned transaction fixture to saturate.
                    await Bun.sleep(1)
                }
                const controller = new AbortController()
                let invoked = false
                cancelled = withBackgroundDatabaseContext(
                    () =>
                        db.transaction(async () => {
                            invoked = true
                        }),
                    controller.signal,
                )
                const rejected = cancelled.then(
                    () => undefined,
                    (error: unknown) => error,
                )
                controller.abort()
                expect(await rejected).toMatchObject({ reason: 'cancelled' })
                expect(invoked).toBe(false)
                await expect(
                    withBackgroundDatabaseContext(
                        () =>
                            db.transaction(async () => {
                                invoked = true
                            }),
                        controller.signal,
                    ),
                ).rejects.toBeInstanceOf(ResourceAdmissionError)
                expect(invoked).toBe(false)
            } finally {
                release()
                await Promise.allSettled([...held, ...(cancelled ? [cancelled] : [])])
            }
            const failure = new Error('fixture transaction failure')
            const outcomes = await Promise.allSettled(
                backgroundSlots.map((resource) =>
                    getBackgroundDatabase(resource).transaction(async (transaction) => {
                        await transaction.execute(sql`select 1`)
                        throw failure
                    }),
                ),
            )
            expect(
                outcomes.every(
                    (result) => result.status === 'rejected' && result.reason === failure,
                ),
            ).toBe(true)
            const recovered = await Promise.all(
                backgroundSlots.map((resource) =>
                    getBackgroundDatabase(resource).transaction((transaction) =>
                        transaction.transaction(async (nested) => {
                            const [row] = await nested.execute<{ ready: number }>(
                                sql`select 1 as ready`,
                            )
                            return row!.ready
                        }),
                    ),
                ),
            )
            expect(recovered).toEqual(
                Array(DATABASE_CONNECTION_BUDGET.backgroundTransactions).fill(1),
            )
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
