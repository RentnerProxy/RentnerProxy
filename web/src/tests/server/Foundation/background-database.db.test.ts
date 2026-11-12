import { expect, test } from 'bun:test'
import { sql } from 'drizzle-orm'
import { getDatabaseUrl } from '@/server/env.server.ts'
import { getAuthDatabase } from '@/server/Auth/Core/database.server.ts'
import {
    getBackgroundDatabase,
    getBackgroundDatabaseResourceSnapshot,
} from '@/server/Foundation/background-database.server.ts'
import { BACKGROUND_DATABASE_POLICY } from '@/config/background-resources.config.ts'
import type { BackgroundDatabaseClass } from '@/server/Foundation/Types/resource-admission.types.ts'

const url = getDatabaseUrl()
const isolated =
    process.env.RENTNERPROXY_DATABASE_INTEGRATION === '1' &&
    url !== null &&
    new URL(url).hostname === '127.0.0.1' &&
    new URL(url).pathname === '/rentnerproxy_upstream_test'

;(isolated ? test : test.skip)(
    'real SQL reservations protect critical and foreground transactions during a background storm',
    async () => {
        const release = Promise.withResolvers<void>()
        const ready = Promise.withResolvers<void>()
        let entered = 0
        const ordinary: BackgroundDatabaseClass[] = [
            'monitoring',
            'notifications',
            'discovery',
            'housekeeping',
        ]
        const running = ordinary.flatMap((resource) =>
            Array.from({ length: BACKGROUND_DATABASE_POLICY.classes[resource].running }, () =>
                getBackgroundDatabase(resource).transaction(async (transaction) => {
                    await transaction.execute(sql`select 1`)
                    if (++entered === 6) ready.resolve()
                    await release.promise
                }),
            ),
        )
        try {
            await Promise.race([ready.promise, Promise.all(running)])
            const overflow = Array.from({ length: 16 }, () =>
                getBackgroundDatabase('monitoring')
                    .transaction(async () => true)
                    .catch((error: { reason: string }) => error.reason),
            )
            const before = performance.now()
            const progress = await Promise.all([
                getBackgroundDatabase('critical').transaction(async (transaction) => {
                    await transaction.execute(sql`select 1`)
                    return 'critical'
                }),
                getAuthDatabase().transaction(async (transaction) => {
                    await transaction.execute(sql`select 1`)
                    return 'interactive'
                }),
            ])
            expect(progress).toEqual(['critical', 'interactive'])
            expect(performance.now() - before).toBeLessThan(500)
            const snapshot = getBackgroundDatabaseResourceSnapshot()
            expect(snapshot.monitoring).toMatchObject({ running: 2, pending: 8, busy: 8 })
            expect(snapshot.critical!.running).toBe(0)
            release.resolve()
            await Promise.all(running)
            const results = await Promise.all(overflow)
            expect(results.filter((result) => result === 'busy')).toHaveLength(8)
            expect(results.filter((result) => result === true)).toHaveLength(8)
            expect(
                Object.values(getBackgroundDatabaseResourceSnapshot()).every(
                    (resource) => resource.running === 0 && resource.pending === 0,
                ),
            ).toBe(true)
        } finally {
            release.resolve()
            await Promise.allSettled(running)
        }
    },
)
