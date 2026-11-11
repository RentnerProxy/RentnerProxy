import '@tanstack/react-start/server-only'

import { randomUUID } from 'node:crypto'
import { open, rename, unlink } from 'node:fs/promises'
import { sql } from 'drizzle-orm'
import { z } from 'zod'

import { PERMISSIONS, SYSTEM_ROLES } from '@/config/permissions.config.ts'
import type { ManagedBackupMaintenance } from '@/lib/Backups/Types/backups.types.ts'
import { createRestoreMaintenanceFence } from './restore-maintenance-fence.ts'
import { stopWorkersForRestoreMaintenance } from '@/server/Foundation/worker-lifecycle.server.ts'
import { readRestoreMaintenanceObservation } from './restore-maintenance-status.server.ts'
import { advanceRestoreAdmission, isDestructiveRestore } from './restore-admission.ts'

const directory = '/run/rentnerproxy/maintenance'
let lastMaintenance: ManagedBackupMaintenance | null = null
let restartingBootstrap = false
let destructivelyQuiesced = false
let waiting = new Set<string>()
let fenceOperationId: string | null = null

async function readViewerSessions() {
    const { db } = await import('@/db/index.ts')
    const rows = await db.execute(sql`
        SELECT s.token_hash AS "tokenHash", s.expires_at AS "expiresAt"
        FROM rentnerproxy.sessions s
        JOIN rentnerproxy.users u ON u.id = s.user_id
        WHERE s.expires_at > now() AND u.status = 'active'
          -- Provider trust cannot be re-evaluated after the database is quiesced.
          AND (to_jsonb(s)->>'oidc_proof') IS NULL
          AND (to_jsonb(u)->>'oidc_provider_id') IS NULL
          AND EXISTS (
            SELECT 1 FROM rentnerproxy.user_roles ur
            JOIN rentnerproxy.roles r ON r.id = ur.role_id
            LEFT JOIN rentnerproxy.role_permissions rp ON rp.role_id = r.id
            LEFT JOIN rentnerproxy.permissions p ON p.id = rp.permission_id
            WHERE ur.user_id = s.user_id AND (r.key = ${SYSTEM_ROLES.OWNER} OR p.key = ${PERMISSIONS.APP_ACCESS})
        )
        LIMIT 10001
    `)
    return z
        .array(
            z.object({
                tokenHash: z.string().regex(/^[a-f0-9]{64}$/u),
                expiresAt: z.coerce.date(),
            }),
        )
        .max(10_000)
        .parse(rows)
}

async function quiesce() {
    await stopWorkersForRestoreMaintenance()
    const sessions = await readViewerSessions()
    const { closeDatabaseForRestoreMaintenance } = await import('@/db/index.ts')
    // Stop detached or timed-out work before acknowledging destructive replacement.
    await closeDatabaseForRestoreMaintenance()
    return sessions
}

async function acknowledge(id: string): Promise<void> {
    if (restartingBootstrap) return
    const temporary = directory + '/.web-quiesced-' + randomUUID() + '.json'
    try {
        const file = await open(temporary, 'wx', 0o600)
        try {
            await file.writeFile(JSON.stringify({ id, quiescedAt: Date.now() }))
            await file.sync()
        } finally {
            await file.close()
        }
        await rename(temporary, directory + '/web-quiesced.json')
    } finally {
        await unlink(temporary).catch(() => undefined)
    }
}

const fence = createRestoreMaintenanceFence({ prepare: readViewerSessions, quiesce, acknowledge })
let polling: Promise<void> | null = null
let timer: ReturnType<typeof setInterval> | null = null

async function pollMaintenance(): Promise<void> {
    const observation = await readRestoreMaintenanceObservation()
    waiting = advanceRestoreAdmission(waiting, observation)
    const status = observation.maintenance
    if (
        !fence.isFenced() &&
        status?.operation === 'restore' &&
        status.state !== 'succeeded' &&
        status.phase === 'restarting'
    )
        restartingBootstrap = true
    if (restartingBootstrap) {
        if (status?.operation === 'restore' && status.id === lastMaintenance?.id)
            lastMaintenance = status
        if (waiting.size === 0 && status?.state === 'succeeded' && status.phase === 'complete') {
            fence.release()
            fenceOperationId = null
            restartingBootstrap = false
            return
        }
        // This new process already uses the replacement DB. Prepare read-only viewers
        // while root establishes readiness; never stop its workers or close that DB.
        if (status?.operation === 'restore') {
            lastMaintenance = status
            prepareFence(status.id, false)
        }
        return
    }
    const destructive =
        status?.operation === 'restore' && waiting.has(status.id) && isDestructiveRestore(status)
            ? status
            : observation.restoreResults.findLast(
                  (receipt) => waiting.has(receipt.id) && isDestructiveRestore(receipt),
              )
    if (destructive) {
        if (status?.id === destructive.id) lastMaintenance = status
        else if (lastMaintenance?.id === destructive.id)
            lastMaintenance = { ...lastMaintenance, ...destructive }
        else if (!destructivelyQuiesced) lastMaintenance = null
        // ID-only historical receipts never borrow actor/artifact details from a later delete.
        if (!destructivelyQuiesced) {
            destructivelyQuiesced = true
            prepareFence(destructive.id, true)
        }
        return
    }
    if (destructivelyQuiesced) return
    if (waiting.size === 0) {
        fence.release()
        fenceOperationId = null
        lastMaintenance = null
        return
    }
    if (status?.operation === 'restore' && waiting.has(status.id)) {
        lastMaintenance = status
        prepareFence(status.id, false)
    }
}

function prepareFence(id: string, destructive: boolean): void {
    // A safe terminal receipt can be overwritten by the next queued operation.
    // Retarget its temporary preparation; the fence itself never releases destructive entry.
    if (fenceOperationId !== null && fenceOperationId !== id) fence.release()
    fenceOperationId = id
    const preparing = destructive ? fence.enter(id) : fence.prepare(id)
    void preparing.catch(() => {
        console.warn('[backups] Restore maintenance could not quiesce; replacement remains blocked')
    })
}

function poll(): Promise<void> {
    polling ??= pollMaintenance().finally(() => {
        polling = null
    })
    return polling
}

export function startRestoreMaintenanceWatcher(): void {
    if (timer !== null) return
    tickMaintenance()
    timer = setInterval(tickMaintenance, 250)
    timer.unref()
    process.once('rentnerproxy:shutdown', () => {
        if (timer !== null) clearInterval(timer)
        timer = null
    })
}

function tickMaintenance(): void {
    void poll().catch(() => {
        console.warn('[backups] Restore maintenance fence unavailable')
    })
}

export async function getActiveRestoreMaintenance(): Promise<ManagedBackupMaintenance | null> {
    await poll()
    return fence.isFenced() ? lastMaintenance : null
}

export function authorizeMaintenanceSnapshot(request: Request): boolean {
    return fence.authorize(request)
}

export function isRestoreMaintenanceFenced(): boolean {
    return fence.isFenced()
}

export async function withRestoreMaintenanceFence(
    request: Request,
    next: () => Promise<Response>,
): Promise<Response> {
    // Observe a newly published root operation before admitting another request.
    try {
        await poll()
    } catch {
        return new Response(null, { status: 503, headers: { 'Cache-Control': 'no-store' } })
    }
    if (
        restartingBootstrap &&
        request.method === 'GET' &&
        ['/health/ready', '/health/live'].includes(new URL(request.url).pathname)
    )
        return next()
    return fence.fetch(request, next)
}
