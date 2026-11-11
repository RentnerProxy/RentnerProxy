import { drizzle } from 'drizzle-orm/bun-sql'
import { SQL } from 'bun'
import { DATABASE_CONNECTION_BUDGET } from '../config/database.config.ts'
import { validateDatabaseEnvironment } from '../server/env.server.ts'
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
export const db = drizzle(client, { schema })

export async function closeDatabaseForRestoreMaintenance(): Promise<void> {
    await client.close()
}
