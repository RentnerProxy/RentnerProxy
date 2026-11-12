import { SQL } from 'bun'
import { randomUUID } from 'node:crypto'
import { lstat, readFile } from 'node:fs/promises'
import { z } from 'zod'

const ownerKey = 'monitoring_b2_experiment_owner_v1'
const modeSchema = z.strictObject({
    runId: z.string().regex(/^[a-f0-9]{12}$/u),
    design: z.enum(['A', 'B2']),
})
const ownerSchema = modeSchema.extend({
    applianceStartTicks: z.string().regex(/^\d+$/u),
    namespace: z
        .string()
        .regex(/^monitor_b2_[a-f0-9]{32}$/u)
        .optional(),
    storeId: z.uuid().optional(),
    controllerEpoch: z.uuid().optional(),
    restoredPaused: z.boolean(),
    profile: z.enum(['mixed', 'all-3s-timeout']).optional(),
    hosts: z
        .array(
            z.strictObject({
                policyId: z.uuid(),
                hostId: z.uuid(),
                domain: z.string().regex(/^b2-[0-9]{1,3}-[a-f0-9-]{36}\.fixture\.test$/u),
                index: z.number().int().min(0).max(999),
            }),
        )
        .length(1000)
        .optional(),
})

/** Read and validate the fixture ownership descriptor without returning database or controller credentials. */
export async function readMonitoringB2Owner(database: SQL) {
    const [row] = await database.unsafe<{ value: unknown }[]>(
        'SELECT value FROM rentnerproxy.system_settings WHERE key=$1',
        [ownerKey],
    )
    return row ? ownerSchema.parse(row.value) : undefined
}

/** Bind persisted ownership to this container init process, rejecting an unavailable process identity. */
async function applianceStartTicks() {
    const stat = await readFile('/proc/1/stat', 'utf8')
    const ticks = stat
        .slice(stat.lastIndexOf(')') + 2)
        .trim()
        .split(/\s+/u)[19]
    if (!ticks || !/^\d+$/u.test(ticks)) throw new Error('B2 appliance identity unavailable')
    return ticks
}

/** Descriptor contains only ownership/fence identities, never a DSN or a worker/controller credential. */
export async function persistMonitoringB2Owner(
    database: SQL,
    input: Omit<z.infer<typeof ownerSchema>, 'applianceStartTicks' | 'restoredPaused'>,
) {
    const value = ownerSchema.parse({
        ...input,
        applianceStartTicks: await applianceStartTicks(),
        restoredPaused: false,
    })
    await database.unsafe(
        'INSERT INTO rentnerproxy.system_settings(key,value) VALUES($1,$2::text::jsonb) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
        [ownerKey, JSON.stringify(value)],
    )
}

/** Root-sealed prototype startup preserves B2 ownership across fresh-volume restore before loading application lifecycles. */
export async function monitoringB2BootMode() {
    let local: z.infer<typeof modeSchema> | undefined
    try {
        const stat = await lstat('/tmp/monitoring-b2/mode.json')
        if (!stat.isFile() || stat.uid !== 0 || (stat.mode & 0o022) !== 0 || stat.size > 4096)
            throw new Error('B2 local mode ownership invalid')
        local = modeSchema.parse(JSON.parse(await readFile('/tmp/monitoring-b2/mode.json', 'utf8')))
    } catch (error) {
        if (!(error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT'))
            throw error
    }
    let sealed = false
    try {
        const stat = await lstat('/opt/rentnerproxy/web/monitoring-b2-build-seal.json')
        if (!stat.isFile() || stat.uid !== 0 || (stat.mode & 0o022) !== 0 || stat.size > 4096)
            throw new Error('B2 prototype build seal ownership invalid')
        z.strictObject({
            prototype: z.literal(true),
            baseRevision: z.string().regex(/^[a-f0-9]{40}$/u),
            baseImageIdentity: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
            experimentSha256: z.string().regex(/^[a-f0-9]{64}$/u),
            nativeBinarySha256: z.string().regex(/^[a-f0-9]{64}$/u),
        }).parse(
            JSON.parse(
                await readFile('/opt/rentnerproxy/web/monitoring-b2-build-seal.json', 'utf8'),
            ),
        )
        sealed = true
    } catch (error) {
        if (!(error && typeof error === 'object' && 'code' in error && error.code === 'ENOENT'))
            throw error
    }
    if (!sealed) {
        if (!local) throw new Error('B2 explicit fixture mode required')
        return { ...local, attachReplay: local.design === 'B2', restoredPaused: false }
    }
    if (
        process.platform !== 'linux' ||
        process.env.DATABASE_URL_FILE !== '/run/rentnerproxy/database-url/value'
    )
        throw new Error('B2 prototype startup requires its appliance')
    const url = (await readFile('/run/rentnerproxy/database-url/value', 'utf8')).trim(),
        parsed = new URL(url)
    if (
        parsed.hostname !== '127.0.0.1' ||
        parsed.pathname !== '/rentnerproxy' ||
        !['postgres:', 'postgresql:'].includes(parsed.protocol)
    )
        throw new Error('B2 prototype database guard failed')
    const database = new SQL(url, {
        max: 1,
        connectionTimeout: 3,
        connection: { statement_timeout: 2000, lock_timeout: 2000 },
    })
    try {
        const [row] = await database.unsafe<{ value: unknown }[]>(
            'SELECT value FROM rentnerproxy.system_settings WHERE key=$1',
            [ownerKey],
        )
        let owner = row ? ownerSchema.parse(row.value) : undefined
        if (owner?.design === 'B2') {
            if (!owner.namespace || !owner.storeId || !owner.controllerEpoch)
                throw new Error('B2 persisted owner is incomplete')
            const ticks = await applianceStartTicks()
            if (owner.applianceStartTicks !== ticks) {
                const namespace = owner.namespace
                owner = {
                    ...owner,
                    applianceStartTicks: ticks,
                    storeId: randomUUID(),
                    controllerEpoch: randomUUID(),
                    restoredPaused: true,
                }
                const next = owner
                await database.begin(async (transaction) => {
                    const fenced = await transaction.unsafe(
                        `UPDATE ${namespace}.identity SET paused=true,resync_required=true,generation=generation+1,store_id=$1,epoch=$2 WHERE id=1 RETURNING id`,
                        [next.storeId!, next.controllerEpoch!],
                    )
                    if (fenced.length !== 1)
                        throw new Error('B2 restored authority identity missing')
                    await transaction.unsafe(
                        'UPDATE rentnerproxy.system_settings SET value=$1::text::jsonb WHERE key=$2',
                        [JSON.stringify(next), ownerKey],
                    )
                })
            }
            return {
                runId: owner.runId,
                design: 'B2' as const,
                attachReplay: !owner.restoredPaused && local?.design === 'B2',
                restoredPaused: owner.restoredPaused,
            }
        }
        return {
            ...(local ?? owner ?? { runId: '000000000000', design: 'A' as const }),
            attachReplay: local?.design === 'B2',
            restoredPaused: false,
        }
    } finally {
        await database.close()
    }
}
