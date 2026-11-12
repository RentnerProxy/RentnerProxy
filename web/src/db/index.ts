import { drizzle } from 'drizzle-orm/bun-sql'
import { SQL } from 'bun'
import { sql } from 'drizzle-orm'
import { DATABASE_CONNECTION_BUDGET } from '../config/database.config.ts'
import { BACKGROUND_DATABASE_POLICY } from '../config/background-resources.config.ts'
import { validateDatabaseEnvironment } from '../server/env.server.ts'
import { admitDatabaseTransaction } from './background-admission.ts'
import * as schema from './schema.ts'

const { DATABASE_URL } = validateDatabaseEnvironment()
const developmentGlobal = globalThis as typeof globalThis & {
    rentnerproxyDatabaseClient?: { url: string; maxConnections: number; client: SQL }
}
const cached =
    process.env.NODE_ENV === 'production' ? undefined : developmentGlobal.rentnerproxyDatabaseClient
const cacheMatches =
    cached?.url === DATABASE_URL &&
    cached.maxConnections === DATABASE_CONNECTION_BUDGET.maxConnections
if (cached && !cacheMatches) void cached.client.close().catch(() => undefined)
const client = cacheMatches
    ? cached.client
    : new SQL(DATABASE_URL, { max: DATABASE_CONNECTION_BUDGET.maxConnections })
if (process.env.NODE_ENV !== 'production') {
    developmentGlobal.rentnerproxyDatabaseClient = {
        url: DATABASE_URL,
        maxConnections: DATABASE_CONNECTION_BUDGET.maxConnections,
        client,
    }
}
const database = drizzle(client, { schema })
export const db = new Proxy(database, {
    get(target, property, receiver): unknown {
        if (property !== 'transaction') return Reflect.get(target, property, receiver)
        return <T>(
            operation: Parameters<typeof database.transaction<T>>[0],
            options?: Parameters<typeof database.transaction<T>>[1],
        ): Promise<T> =>
            admitDatabaseTransaction((resource) =>
                target.transaction(async (transaction) => {
                    if (resource) {
                        const timeout = `${BACKGROUND_DATABASE_POLICY.classes[resource].transactionMs}ms`
                        await transaction.execute(
                            sql`select set_config('statement_timeout', ${timeout}, true), set_config('lock_timeout', ${timeout}, true), set_config('transaction_timeout', ${timeout}, true)`,
                        )
                    }
                    return operation(transaction)
                }, options),
            )
    },
})

export async function closeDatabaseForRestoreMaintenance(): Promise<void> {
    await client.close()
}
