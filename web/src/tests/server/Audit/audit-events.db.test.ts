import { afterEach, describe, expect, test } from 'bun:test'

const integrationEnabled =
    process.env.RENTNERPROXY_DATABASE_INTEGRATION === '1' && Boolean(process.env.DATABASE_URL)
const integrationTest = integrationEnabled ? test : test.skip

type DatabaseTestContext = Awaited<ReturnType<typeof loadDatabaseTestContext>>

async function waitForLock(condition: () => Promise<boolean>) {
    const deadline = Date.now() + 3_000
    // oxlint-disable-next-line no-await-in-loop -- Poll until the expected lock exists before opening the next participant.
    while (!(await condition())) {
        if (Date.now() >= deadline) throw new Error('Expected PostgreSQL lock wait did not appear')
        // oxlint-disable-next-line no-await-in-loop -- A small delay avoids busy polling the owned database.
        await Bun.sleep(5)
    }
}

async function loadDatabaseTestContext() {
    const { createServerEntry } = await import('@tanstack/react-start/server-entry')
    const { SESSION_COOKIE_NAME } = await import('@/config/auth.config.ts')
    const { db } = await import('@/db/index.ts')
    const { auditEvents, roles, userRoles, users } = await import('@/db/schema.ts')
    const { ensureAuthorizationRegistryInTransaction } =
        await import('@/server/Auth/Access/registry.service.ts')
    const { createSessionService } = await import('@/server/Auth/Access/sessions.service.ts')
    const { getAuthDatabase } = await import('@/server/Auth/Core/database.server.ts')
    const { listAuditEventsService } = await import('@/server/Audit/audit-reader.service.ts')
    const { AUDIT_RETENTION_LOCK_ID } = await import('@/server/Audit/audit-retention.ts')
    const { appendAuditEventInTransactionService, appendAuditEventsInTransactionService } =
        await import('@/server/Audit/audit.service.ts')
    const { count, eq, inArray, sql } = await import('drizzle-orm')

    async function withUser<T>(userId: string, operation: () => Promise<T>): Promise<T> {
        const session = await createSessionService(userId)
        let result: T | undefined
        let failure: unknown
        const handler = createServerEntry({
            fetch: async () => {
                try {
                    result = await operation()
                } catch (error) {
                    failure = error
                }
                return new Response(null, { status: failure ? 500 : 204 })
            },
        }).fetch
        const request = new Request('http://localhost/')
        request.headers.set('cookie', `${SESSION_COOKIE_NAME}=${session.token}`)
        await handler(request, {})
        if (failure) throw failure
        return result as T
    }

    async function createUser(role: string) {
        const id = crypto.randomUUID()
        await getAuthDatabase().transaction(async (transaction) => {
            await transaction.insert(users).values({
                id,
                displayName: `Audit ${role}`,
                email: `audit-${id}@integration.invalid`,
                emailVerifiedAt: new Date(),
                status: 'active',
            })
            const roleRow = (
                await transaction.select({ id: roles.id }).from(roles).where(eq(roles.key, role))
            ).at(0)
            if (!roleRow) throw new Error(`Missing ${role} role.`)
            await transaction.insert(userRoles).values({ roleId: roleRow.id, userId: id })
        })
        return id
    }

    async function deleteUser(id: string) {
        await db.delete(users).where(eq(users.id, id))
    }

    return {
        appendAuditEventInTransactionService,
        appendAuditEventsInTransactionService,
        AUDIT_RETENTION_LOCK_ID,
        auditEvents,
        db,
        deleteUser,
        count,
        eq,
        ensureAuthorizationRegistryInTransaction,
        getAuthDatabase,
        inArray,
        listAuditEventsService,
        sql,
        withUser,
        createUser,
    }
}

describe('audit event PostgreSQL persistence', () => {
    let context: DatabaseTestContext | undefined

    afterEach(() => {
        context = undefined
    })

    integrationTest(
        'single and batch writes store SQL-queryable JSONB objects with exact event identities',
        async () => {
            const current = await loadDatabaseTestContext()
            const {
                auditEvents,
                db,
                eq,
                inArray,
                sql,
                appendAuditEventInTransactionService,
                appendAuditEventsInTransactionService,
            } = current
            const inputs = Array.from({ length: 3 }, () => ({
                actorUserId: null,
                actorKind: 'system' as const,
                action: 'started' as const,
                resource: 'certificate' as const,
                targetId: crypto.randomUUID(),
                result: 'success' as const,
                metadata: {
                    operationId: crypto.randomUUID(),
                    eventId: crypto.randomUUID(),
                    certificateStage: 'creating_order' as const,
                    occurredAt: new Date().toISOString(),
                },
            }))
            const emptyTarget = crypto.randomUUID()
            const targets = [...inputs.map((input) => input.targetId), emptyTarget]
            try {
                await db.transaction(async (transaction) => {
                    await appendAuditEventInTransactionService(transaction, inputs[0]!)
                    await appendAuditEventsInTransactionService(transaction, [
                        inputs[1]!,
                        inputs[2]!,
                        {
                            actorUserId: null,
                            actorKind: 'system',
                            action: 'started',
                            resource: 'certificate',
                            targetId: emptyTarget,
                            result: 'success',
                        },
                    ])
                })
                expect(
                    await db
                        .select({
                            object: sql<boolean>`jsonb_typeof(${auditEvents.metadata}) = 'object'`,
                        })
                        .from(auditEvents)
                        .where(inArray(auditEvents.targetId, targets)),
                ).toEqual(Array.from({ length: 4 }, () => ({ object: true })))
                const matches = await Promise.all(
                    inputs.map((input) =>
                        db
                            .select({
                                exact: sql<boolean>`${auditEvents.metadata}->>'eventId' = ${input.metadata.eventId}`,
                                object: sql<boolean>`jsonb_typeof(${auditEvents.metadata}) = 'object'`,
                            })
                            .from(auditEvents)
                            .where(eq(auditEvents.targetId, input.targetId)),
                    ),
                )
                expect(matches).toEqual(
                    Array.from({ length: 3 }, () => [{ exact: true, object: true }]),
                )
                expect(
                    await db
                        .select({ empty: sql<boolean>`${auditEvents.metadata} = '{}'::jsonb` })
                        .from(auditEvents)
                        .where(eq(auditEvents.targetId, emptyTarget)),
                ).toEqual([{ empty: true }])
            } finally {
                await db.delete(auditEvents).where(inArray(auditEvents.targetId, targets))
            }
        },
    )

    integrationTest(
        'single and batch JSONB audit writes roll back atomically with their enclosing mutation',
        async () => {
            const current = await loadDatabaseTestContext()
            const {
                auditEvents,
                db,
                inArray,
                sql,
                appendAuditEventInTransactionService,
                appendAuditEventsInTransactionService,
            } = current
            const inputs = Array.from({ length: 3 }, () => ({
                actorUserId: null,
                actorKind: 'system' as const,
                action: 'started' as const,
                resource: 'certificate' as const,
                targetId: crypto.randomUUID(),
                result: 'success' as const,
                metadata: { eventId: crypto.randomUUID() },
            }))
            const targets = inputs.map((input) => input.targetId)
            const rollback = new Error('rollback private JSONB audit fixture')
            await expect(
                db.transaction(async (transaction) => {
                    await appendAuditEventInTransactionService(transaction, inputs[0]!)
                    await appendAuditEventsInTransactionService(transaction, inputs.slice(1))
                    expect(
                        await transaction
                            .select({
                                object: sql<boolean>`jsonb_typeof(${auditEvents.metadata}) = 'object'`,
                            })
                            .from(auditEvents)
                            .where(inArray(auditEvents.targetId, targets)),
                    ).toEqual(Array.from({ length: 3 }, () => ({ object: true })))
                    throw rollback
                }),
            ).rejects.toBe(rollback)
            expect(
                await db
                    .select({ id: auditEvents.id })
                    .from(auditEvents)
                    .where(inArray(auditEvents.targetId, targets)),
            ).toHaveLength(0)
        },
    )

    integrationTest(
        'appends, rolls back with its mutation, and preserves historical actors',
        async () => {
            context = await loadDatabaseTestContext()
            const {
                appendAuditEventInTransactionService,
                auditEvents,
                db,
                deleteUser,
                eq,
                getAuthDatabase,
            } = context
            await getAuthDatabase().transaction((transaction) =>
                context!.ensureAuthorizationRegistryInTransaction(transaction),
            )
            const ownerId = await context.createUser('owner')
            const historicalActorId = await context.createUser('viewer')
            const targetId = crypto.randomUUID()
            try {
                await db.transaction(async (transaction) => {
                    await appendAuditEventInTransactionService(transaction, {
                        actorUserId: historicalActorId,
                        actorKind: 'user',
                        action: 'login',
                        resource: 'session',
                        targetId,
                        result: 'success',
                    })
                })
                await deleteUser(historicalActorId)
                const historical = await context.withUser(ownerId, () =>
                    context!.listAuditEventsService({
                        actorUserId: historicalActorId,
                        action: 'login',
                        resource: 'session',
                        limit: 1,
                    }),
                )
                expect(historical.events).toHaveLength(1)
                expect(historical.events[0]).toMatchObject({
                    actorUserId: historicalActorId,
                    actorDisplayName: null,
                    targetId,
                })

                const rolledBackTargetId = crypto.randomUUID()
                await expect(
                    db.transaction(async (transaction) => {
                        await appendAuditEventInTransactionService(transaction, {
                            actorUserId: null,
                            actorKind: 'system',
                            action: 'apply',
                            resource: 'proxy-runtime',
                            targetId: rolledBackTargetId,
                            result: 'failure',
                            metadata: { runtimeStatus: 'failed' },
                        })
                        throw new Error('mutation failed')
                    }),
                ).rejects.toThrow('mutation failed')
                expect(
                    await db
                        .select({ id: auditEvents.id })
                        .from(auditEvents)
                        .where(eq(auditEvents.targetId, rolledBackTargetId)),
                ).toEqual([])
            } finally {
                await db.delete(auditEvents).where(eq(auditEvents.targetId, targetId))
                await deleteUser(ownerId)
                await deleteUser(historicalActorId)
            }
        },
    )

    integrationTest(
        'enforces retention and the exact 100000-row cap in one transaction',
        async () => {
            context = await loadDatabaseTestContext()
            const { appendAuditEventInTransactionService, auditEvents, count, db, eq, sql } =
                context
            const oldTargetId = crypto.randomUUID()
            const targetId = crypto.randomUUID()
            await expect(
                db.transaction(async (transaction) => {
                    await transaction.execute(sql`
                    insert into "rentnerproxy"."audit_events"
                        ("id", "created_at", "actor_user_id", "actor_kind", "action", "resource", "target_id", "result", "metadata")
                    select uuidv7(), now() - interval '91 days', null, 'system', 'apply', 'proxy-runtime', ${oldTargetId}::uuid, 'failure', '{"runtimeStatus":"failed"}'::jsonb
                `)
                    await appendAuditEventInTransactionService(transaction, {
                        actorUserId: null,
                        actorKind: 'system',
                        action: 'apply',
                        resource: 'proxy-runtime',
                        targetId,
                        result: 'success',
                    })
                    const oldRows = await transaction
                        .select({ value: count() })
                        .from(auditEvents)
                        .where(eq(auditEvents.targetId, oldTargetId))
                    expect(oldRows[0]?.value).toBe(0)

                    await transaction.execute(sql`
                    insert into "rentnerproxy"."audit_events"
                        ("id", "created_at", "actor_user_id", "actor_kind", "action", "resource", "target_id", "result", "metadata")
                    select uuidv7(), now(), null, 'system', 'apply', 'proxy-runtime', ${targetId}::uuid, 'success', '{}'::jsonb
                    from generate_series(1, 100001)
                `)
                    await appendAuditEventInTransactionService(transaction, {
                        actorUserId: null,
                        actorKind: 'system',
                        action: 'apply',
                        resource: 'proxy-runtime',
                        targetId,
                        result: 'success',
                    })
                    const rows = await transaction.select({ value: count() }).from(auditEvents)
                    expect(rows[0]?.value).toBe(100_000)
                    throw new Error('rollback retention fixture')
                }),
            ).rejects.toThrow('rollback retention fixture')
            const remaining = await db
                .select({ value: count() })
                .from(auditEvents)
                .where(eq(auditEvents.targetId, targetId))
            expect(remaining[0]?.value).toBe(0)
        },
    )

    integrationTest(
        'keeps exact cardinality through raw SQL, conflicts, no-ops, savepoints and TRUNCATE rollback',
        async () => {
            const current = await loadDatabaseTestContext()
            const { db, sql } = current
            const targetId = crypto.randomUUID()
            const rollback = new Error('rollback cardinality fixture')
            await expect(
                db.transaction(async (transaction) => {
                    const cardinality = async () => {
                        const [row] = await transaction.execute<{
                            actual: string
                            recorded: string
                            version: string
                        }>(sql`
                        select count(*)::text as actual,
                            (select row_count::text from rentnerproxy.audit_retention_state where singleton) as recorded,
                            (select ctid::text from rentnerproxy.audit_retention_state where singleton) as version
                        from rentnerproxy.audit_events
                    `)
                        expect(row?.recorded).toBe(row?.actual)
                        return row!
                    }
                    const original = await cardinality()
                    const [inserted] = await transaction.execute<{ id: string }>(sql`
                    insert into rentnerproxy.audit_events (actor_kind, action, resource, target_id, result)
                    values ('system', 'apply', 'proxy-runtime', ${targetId}::uuid, 'success') returning id
                `)
                    const afterInsert = await cardinality()
                    expect(BigInt(afterInsert.actual)).toBe(BigInt(original.actual) + 1n)
                    await transaction.execute(sql`
                    insert into rentnerproxy.audit_events (id, actor_kind, action, resource, result)
                    values (${inserted!.id}::uuid, 'system', 'apply', 'proxy-runtime', 'success') on conflict do nothing
                `)
                    const beforeNoop = await cardinality()
                    expect(beforeNoop.version).toBe(afterInsert.version)
                    await transaction.execute(
                        sql`delete from rentnerproxy.audit_events where false`,
                    )
                    await transaction.execute(
                        sql`update rentnerproxy.audit_events set created_at = now() where id = ${inserted!.id}::uuid`,
                    )
                    expect((await cardinality()).version).toBe(beforeNoop.version)
                    await expect(
                        transaction.transaction(async (nested) => {
                            await nested.execute(
                                sql`delete from rentnerproxy.audit_events where id = ${inserted!.id}::uuid`,
                            )
                            const [count] = await nested.execute<{ recorded: string }>(
                                sql`select row_count::text as recorded from rentnerproxy.audit_retention_state where singleton`,
                            )
                            expect(count?.recorded).toBe(original.recorded)
                            throw new Error('rollback raw delete')
                        }),
                    ).rejects.toThrow('rollback raw delete')
                    expect((await cardinality()).actual).toBe(beforeNoop.actual)
                    await expect(
                        transaction.transaction(async (nested) => {
                            await nested.execute(sql`truncate rentnerproxy.audit_events`)
                            const [count] = await nested.execute<{ recorded: string }>(
                                sql`select row_count::text as recorded from rentnerproxy.audit_retention_state where singleton`,
                            )
                            expect(count?.recorded).toBe('0')
                            throw new Error('rollback truncate')
                        }),
                    ).rejects.toThrow('rollback truncate')
                    expect((await cardinality()).actual).toBe(beforeNoop.actual)
                    await transaction.execute(
                        sql`delete from rentnerproxy.audit_events where id = ${inserted!.id}::uuid`,
                    )
                    expect((await cardinality()).actual).toBe(original.actual)
                    throw rollback
                }),
            ).rejects.toBe(rollback)
        },
    )

    integrationTest(
        'drains multiple bounded oldest-ID batches and preserves the exact cap for tied-time rows',
        async () => {
            const current = await loadDatabaseTestContext()
            const { db, sql } = current
            const targetId = crypto.randomUUID()
            const rollback = new Error('rollback multi-batch cap fixture')
            await expect(
                db.transaction(async (transaction) => {
                    await transaction.execute(sql`
                    insert into rentnerproxy.audit_events (created_at, actor_kind, action, resource, target_id, result)
                    select now() - interval '1 day', 'system', 'apply', 'proxy-runtime', ${targetId}::uuid, 'success'
                    from generate_series(1, 100513)
                `)
                    const [oldest] = await transaction.execute<{ id: string }>(sql`
                    select id from rentnerproxy.audit_events order by created_at,id limit 1
                `)
                    await current.appendAuditEventInTransactionService(transaction, {
                        actorUserId: null,
                        actorKind: 'system',
                        action: 'apply',
                        resource: 'proxy-runtime',
                        targetId,
                        result: 'success',
                    })
                    const [counts] = await transaction.execute<{
                        actual: string
                        recorded: string
                        oldestRemains: boolean
                    }>(sql`
                    select count(*)::text as actual,
                        (select row_count::text from rentnerproxy.audit_retention_state where singleton) as recorded,
                        exists(select 1 from rentnerproxy.audit_events where id = ${oldest!.id}::uuid) as "oldestRemains"
                    from rentnerproxy.audit_events
                `)
                    expect(counts?.actual).toBe('100000')
                    expect(counts?.recorded).toBe('100000')
                    expect(counts?.oldestRemains).toBe(false)
                    throw rollback
                }),
            ).rejects.toBe(rollback)
        },
        10_000,
    )

    integrationTest(
        'serializes raw INSERT, UPDATE and DELETE before concurrent audited appends',
        async () => {
            const current = await loadDatabaseTestContext()
            const { db, getAuthDatabase, sql } = current
            const targetId = crypto.randomUUID()
            try {
                for (const operation of ['insert', 'update', 'delete'] as const) {
                    // oxlint-disable-next-line no-await-in-loop -- Each independent raw statement case must finish before the next fixture.
                    await db.execute(sql`
                        insert into rentnerproxy.audit_events (actor_kind, action, resource, target_id, result)
                        values ('system', 'apply', 'proxy-runtime', ${targetId}::uuid, 'success')
                    `)
                    let releaseRaw!: () => void
                    const gate = new Promise<void>((resolve) => {
                        releaseRaw = resolve
                    })
                    let rawReady!: (pid: number) => void
                    const started = new Promise<number>((resolve) => {
                        rawReady = resolve
                    })
                    const write = (
                        transaction: Parameters<
                            typeof current.appendAuditEventInTransactionService
                        >[0],
                    ) =>
                        current.appendAuditEventInTransactionService(transaction, {
                            actorUserId: null,
                            actorKind: 'system',
                            action: 'apply',
                            resource: 'proxy-runtime',
                            targetId,
                            result: 'success',
                        })
                    const raw = db.transaction(async (transaction) => {
                        if (operation === 'insert')
                            await transaction.execute(sql`
                            insert into rentnerproxy.audit_events (actor_kind, action, resource, target_id, result)
                            values ('system', 'apply', 'proxy-runtime', ${targetId}::uuid, 'success')
                        `)
                        else if (operation === 'update')
                            await transaction.execute(sql`
                            update rentnerproxy.audit_events set created_at = now() where target_id = ${targetId}::uuid
                        `)
                        else
                            await transaction.execute(
                                sql`delete from rentnerproxy.audit_events where target_id = ${targetId}::uuid`,
                            )
                        const [pid] = await transaction.execute<{ pid: number }>(
                            sql`select pg_backend_pid() as pid`,
                        )
                        rawReady(pid!.pid)
                        await gate
                        // If raw writes locked rows/counter before the audit lock, this reentrant append
                        // could deadlock against the application transaction waiting on those writes.
                        await write(transaction)
                    })
                    let writer: Promise<void> | undefined
                    try {
                        // oxlint-disable-next-line no-await-in-loop -- The raw writer must own its statement lock first.
                        const rawPid = await started
                        // oxlint-disable-next-line no-await-in-loop -- Assert the statement trigger's actual held advisory lock.
                        const [rawLock] = await db.execute<{ held: boolean }>(sql`
                            select exists(select 1 from pg_locks where pid = ${rawPid}
                                and locktype = 'advisory' and objid = ${current.AUDIT_RETENTION_LOCK_ID}::oid and granted) as held
                        `)
                        expect(rawLock?.held).toBe(true)
                        let writerPid = 0
                        writer = getAuthDatabase().transaction(async (transaction) => {
                            const [pid] = await transaction.execute<{ pid: number }>(
                                sql`select pg_backend_pid() as pid`,
                            )
                            writerPid = pid!.pid
                            await write(transaction)
                        })
                        // oxlint-disable-next-line no-await-in-loop -- Observe the application blocked before allowing the raw transaction to continue.
                        await waitForLock(async () => {
                            const [lock] = await db.execute<{ waiting: boolean }>(sql`
                                select exists(select 1 from pg_locks where pid = ${writerPid}
                                    and locktype = 'advisory' and not granted) as waiting
                            `)
                            return lock!.waiting
                        })
                        releaseRaw()
                        // oxlint-disable-next-line no-await-in-loop -- Commit both participants before checking the resulting cardinality.
                        await Promise.all([raw, writer])
                        // oxlint-disable-next-line no-await-in-loop -- Verify each statement's committed count independently.
                        const [counts] = await db.execute<{ actual: string; recorded: string }>(sql`
                            select count(*)::text as actual,
                                (select row_count::text from rentnerproxy.audit_retention_state where singleton) as recorded
                            from rentnerproxy.audit_events
                        `)
                        expect(counts?.recorded).toBe(counts?.actual)
                    } finally {
                        releaseRaw()
                        // oxlint-disable-next-line no-await-in-loop -- Finish both owned transactions even if a lock assertion fails.
                        await Promise.allSettled(
                            [raw, writer].filter((value) => value !== undefined),
                        )
                    }
                }
            } finally {
                await db.execute(
                    sql`delete from rentnerproxy.audit_events where target_id = ${targetId}::uuid`,
                )
            }
        },
        15_000,
    )

    integrationTest(
        'acquires the table lock before the advisory lock so concurrent TRUNCATE cannot invert them',
        async () => {
            const current = await loadDatabaseTestContext()
            const { db, getAuthDatabase, sql } = current
            const targetId = crypto.randomUUID()
            let releaseBlocker!: () => void
            const blockerGate = new Promise<void>((resolve) => {
                releaseBlocker = resolve
            })
            let blockerReady!: () => void
            const blockerStarted = new Promise<void>((resolve) => {
                blockerReady = resolve
            })
            const blocker = db.transaction(async (transaction) => {
                await transaction.execute(
                    sql`select pg_advisory_xact_lock(${current.AUDIT_RETENTION_LOCK_ID})`,
                )
                blockerReady()
                await blockerGate
            })
            let writer: Promise<void> | undefined
            let truncation: Promise<void> | undefined
            const rollback = new Error('rollback concurrent truncate')
            try {
                await blockerStarted
                let writerPid = 0
                writer = getAuthDatabase().transaction(async (transaction) => {
                    const [pid] = await transaction.execute<{ pid: number }>(
                        sql`select pg_backend_pid() as pid`,
                    )
                    writerPid = pid!.pid
                    await current.appendAuditEventInTransactionService(transaction, {
                        actorUserId: null,
                        actorKind: 'system',
                        action: 'apply',
                        resource: 'proxy-runtime',
                        targetId,
                        result: 'success',
                    })
                })
                await waitForLock(async () => {
                    const [lock] = await db.execute<{ held: boolean }>(sql`
                        select exists(select 1 from pg_locks where pid = ${writerPid}
                            and locktype = 'advisory' and not granted) as held
                    `)
                    return lock!.held
                })
                let truncatePid = 0
                truncation = db
                    .transaction(async (transaction) => {
                        const [pid] = await transaction.execute<{ pid: number }>(
                            sql`select pg_backend_pid() as pid`,
                        )
                        truncatePid = pid!.pid
                        await transaction.execute(sql`truncate rentnerproxy.audit_events`)
                        const [state] = await transaction.execute<{ recorded: string }>(
                            sql`select row_count::text as recorded from rentnerproxy.audit_retention_state where singleton`,
                        )
                        expect(state?.recorded).toBe('0')
                        throw rollback
                    })
                    .catch((error: unknown) => {
                        if (error !== rollback) throw error
                    })
                await waitForLock(async () => {
                    const [lock] = await db.execute<{ waiting: boolean }>(sql`
                        select exists(select 1 from pg_locks where pid = ${truncatePid}
                            and relation = 'rentnerproxy.audit_events'::regclass
                            and mode = 'AccessExclusiveLock' and not granted) as waiting
                    `)
                    return lock!.waiting
                })
                releaseBlocker()
                await Promise.all([blocker, writer, truncation])
                const [counts] = await db.execute<{ actual: string; recorded: string }>(sql`
                    select count(*)::text as actual,
                        (select row_count::text from rentnerproxy.audit_retention_state where singleton) as recorded
                    from rentnerproxy.audit_events
                `)
                expect(counts?.recorded).toBe(counts?.actual)
            } finally {
                releaseBlocker()
                await Promise.allSettled(
                    [blocker, writer, truncation].filter((value) => value !== undefined),
                )
                await db.execute(
                    sql`delete from rentnerproxy.audit_events where target_id = ${targetId}::uuid`,
                )
            }
        },
        10_000,
    )

    integrationTest(
        'managed concurrent mutations reconcile raw fixtures exactly once before commit',
        async () => {
            const current = await loadDatabaseTestContext()
            const { auditEvents, count, db, getAuthDatabase, inArray, sql } = current
            const seedTarget = crypto.randomUUID()
            const targets = [crypto.randomUUID(), crypto.randomUUID()]
            try {
                await db.execute(sql`
                    insert into "rentnerproxy"."audit_events"
                        ("created_at", "actor_kind", "action", "resource", "target_id", "result")
                    select now() - interval '1 day', 'system', 'apply', 'proxy-runtime', ${seedTarget}::uuid, 'success'
                    from generate_series(1, 100001)
                `)
                await Promise.all(
                    targets.map((targetId) =>
                        getAuthDatabase().transaction(async (transaction) => {
                            for (let index = 0; index < 20; index++) {
                                // oxlint-disable-next-line no-await-in-loop -- Mirrors per-object audit writes within one import.
                                await current.appendAuditEventInTransactionService(transaction, {
                                    actorUserId: null,
                                    actorKind: 'system',
                                    action: 'apply',
                                    resource: 'proxy-runtime',
                                    targetId,
                                    result: 'success',
                                })
                            }
                        }),
                    ),
                )
                expect((await db.select({ value: count() }).from(auditEvents))[0]?.value).toBe(
                    100_000,
                )
                expect(
                    (
                        await db
                            .select({ value: count() })
                            .from(auditEvents)
                            .where(inArray(auditEvents.targetId, targets))
                    )[0]?.value,
                ).toBe(40)
            } finally {
                await db
                    .delete(auditEvents)
                    .where(inArray(auditEvents.targetId, [seedTarget, ...targets]))
            }
        },
        15_000,
    )

    integrationTest(
        'managed savepoints discard rolled-back audit events and preserve atomic root rollback',
        async () => {
            const current = await loadDatabaseTestContext()
            const { auditEvents, db, getAuthDatabase, inArray } = current
            const targets = Array.from({ length: 3 }, () => crypto.randomUUID())
            const write = (
                transaction: Parameters<typeof current.appendAuditEventInTransactionService>[0],
                targetId: string,
            ) =>
                current.appendAuditEventInTransactionService(transaction, {
                    actorUserId: null,
                    actorKind: 'system',
                    action: 'apply',
                    resource: 'proxy-runtime',
                    targetId,
                    result: 'success',
                })
            try {
                await getAuthDatabase().transaction(async (outer) => {
                    await expect(
                        outer.transaction(async (inner) => {
                            await write(inner, targets[0]!)
                            throw new Error('rollback savepoint')
                        }),
                    ).rejects.toThrow('rollback savepoint')
                    await outer.transaction((inner) => write(inner, targets[1]!))
                    await write(outer, targets[2]!)
                })
                const committed = await db
                    .select({ targetId: auditEvents.targetId })
                    .from(auditEvents)
                    .where(inArray(auditEvents.targetId, targets))
                expect(committed.map((row) => row.targetId).toSorted()).toEqual(
                    targets.slice(1).toSorted(),
                )
                await db.delete(auditEvents).where(inArray(auditEvents.targetId, targets))
                await expect(
                    getAuthDatabase().transaction(async (outer) => {
                        await outer.transaction((inner) => write(inner, targets[0]!))
                        await write(outer, targets[1]!)
                        throw new Error('rollback root')
                    }),
                ).rejects.toThrow('rollback root')
                expect(
                    await db
                        .select({ id: auditEvents.id })
                        .from(auditEvents)
                        .where(inArray(auditEvents.targetId, targets)),
                ).toHaveLength(0)
            } finally {
                await db.delete(auditEvents).where(inArray(auditEvents.targetId, targets))
            }
        },
    )

    integrationTest(
        'reads hide expired rows without deletion or waiting for the audit writer lock',
        async () => {
            const current = await loadDatabaseTestContext()
            const { auditEvents, db, eq, getAuthDatabase, sql } = current
            await getAuthDatabase().transaction((transaction) =>
                current.ensureAuthorizationRegistryInTransaction(transaction),
            )
            const ownerId = await current.createUser('owner')
            const targetId = crypto.randomUUID()
            try {
                // Session setup must finish before the synthetic writer starts holding its lock.
                await current.withUser(ownerId, async () => {
                    await db.execute(sql`
                        insert into "rentnerproxy"."audit_events"
                            ("created_at", "actor_user_id", "actor_kind", "action", "resource", "target_id", "result")
                        values (now() - interval '91 days', ${ownerId}::uuid, 'user', 'update', 'user', ${targetId}::uuid, 'success')
                    `)
                    await db.transaction(async (writer) => {
                        await writer.execute(
                            sql`select pg_advisory_xact_lock(${current.AUDIT_RETENTION_LOCK_ID})`,
                        )
                        let timeout: ReturnType<typeof setTimeout> | undefined
                        try {
                            const result = await Promise.race([
                                current.listAuditEventsService({
                                    actorUserId: ownerId,
                                    action: 'update',
                                    resource: 'user',
                                    from: new Date(
                                        Date.now() - 365 * 24 * 60 * 60 * 1_000,
                                    ).toISOString(),
                                }),
                                new Promise<never>((_resolve, reject) => {
                                    timeout = setTimeout(
                                        () =>
                                            reject(
                                                new Error('audit reader waited for writer lock'),
                                            ),
                                        2_000,
                                    )
                                }),
                            ])
                            expect(result.events).toHaveLength(0)
                            expect(result.hasMore).toBe(false)
                        } finally {
                            clearTimeout(timeout)
                        }
                    })
                    expect(
                        await db
                            .select({ id: auditEvents.id })
                            .from(auditEvents)
                            .where(eq(auditEvents.targetId, targetId)),
                    ).toHaveLength(1)
                })
            } finally {
                await db.delete(auditEvents).where(eq(auditEvents.targetId, targetId))
                await current.deleteUser(ownerId)
            }
        },
        10_000,
    )

    integrationTest(
        'uses the id tie-breaker for same-millisecond keyset pages and filters',
        async () => {
            context = await loadDatabaseTestContext()
            const {
                auditEvents,
                db,
                ensureAuthorizationRegistryInTransaction,
                getAuthDatabase,
                inArray,
                listAuditEventsService,
                sql,
                withUser,
            } = context
            await getAuthDatabase().transaction((transaction) =>
                ensureAuthorizationRegistryInTransaction(transaction),
            )
            const ownerId = await context.createUser('owner')
            const firstId = '0198d98a-0000-7000-8000-000000000101'
            const secondId = '0198d98a-0000-7000-8000-000000000102'
            const timestamp = new Date().toISOString()
            try {
                await db.execute(sql`
                insert into "rentnerproxy"."audit_events"
                    ("id", "created_at", "actor_user_id", "actor_kind", "action", "resource", "target_id", "result", "metadata")
                values
                    (${firstId}::uuid, ${timestamp}::timestamptz, ${ownerId}::uuid, 'user', 'update', 'user', null, 'success', '{}'::jsonb),
                    (${secondId}::uuid, ${timestamp}::timestamptz, ${ownerId}::uuid, 'user', 'update', 'user', null, 'success', '{}'::jsonb)
            `)
                const query = {
                    actorUserId: ownerId,
                    action: 'update' as const,
                    resource: 'user' as const,
                    result: 'success' as const,
                    from: new Date(Date.now() - 5_000).toISOString(),
                    to: new Date(Date.now() + 5_000).toISOString(),
                    limit: 1,
                }
                const firstPage = await withUser(ownerId, () => listAuditEventsService(query))
                expect(firstPage.events.map((event) => event.id)).toEqual([secondId])
                expect(firstPage.hasMore).toBe(true)
                const secondPage = await withUser(ownerId, () =>
                    listAuditEventsService({ ...query, cursor: firstPage.nextCursor ?? undefined }),
                )
                expect(secondPage.events.map((event) => event.id)).toEqual([firstId])
            } finally {
                await db.delete(auditEvents).where(inArray(auditEvents.id, [firstId, secondId]))
                await context.deleteUser(ownerId)
            }
        },
    )

    integrationTest('denies the viewer before the audit query is opened', async () => {
        context = await loadDatabaseTestContext()
        const {
            ensureAuthorizationRegistryInTransaction,
            getAuthDatabase,
            listAuditEventsService,
        } = context
        await getAuthDatabase().transaction((transaction) =>
            ensureAuthorizationRegistryInTransaction(transaction),
        )
        const viewerId = await context.createUser('viewer')
        try {
            await expect(
                context.withUser(viewerId, () => listAuditEventsService({})),
            ).rejects.toMatchObject({
                code: 'permission_denied',
            })
        } finally {
            await context.deleteUser(viewerId)
        }
    })
})
