import { Database } from 'bun:sqlite'
import type { MonitoringEvaluationResult } from './Types/monitoring-evaluation.types.ts'
import { monitoringEvaluationContract } from './monitoring-evaluation.ts'

/**
 * Executable persistence experiment only. SQLite represents a native bounded store;
 * this selects neither a Rust store dependency nor a production persisted format.
 */
export function createMonitoringEvaluationOutbox(path: string, storeId: string) {
    const database = new Database(path, { create: true, strict: true })
    database.exec(`
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = FULL;
        CREATE TABLE IF NOT EXISTS identity (id INTEGER PRIMARY KEY CHECK(id = 1), store_id TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 0, gaps INTEGER NOT NULL DEFAULT 0, paused INTEGER NOT NULL DEFAULT 1, resync_required INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS outbox (sequence INTEGER PRIMARY KEY AUTOINCREMENT, attempt_id TEXT UNIQUE NOT NULL, payload TEXT NOT NULL, bytes INTEGER NOT NULL, checked_at INTEGER NOT NULL);
    `)
    database.query('INSERT OR IGNORE INTO identity (id, store_id) VALUES (1, ?)').run(storeId)
    const identity = () =>
        database
            .query<
                {
                    store_id: string
                    generation: number
                    gaps: number
                    paused: number
                    resync_required: number
                },
                []
            >('SELECT * FROM identity WHERE id = 1')
            .get()!
    if (identity().store_id !== storeId) {
        database.close()
        throw new Error('Monitoring experiment store identity mismatch')
    }
    const stats = () =>
        database
            .query<{ records: number; bytes: number; oldest: number | null }, []>(
                'SELECT count(*) AS records, coalesce(sum(bytes), 0) AS bytes, min(checked_at) AS oldest FROM outbox',
            )
            .get()!
    const insert = database.transaction((result: MonitoringEvaluationResult, nowMs: number) => {
        if (result.storeId !== identity().store_id || identity().paused) return 'paused' as const
        if (
            !Number.isSafeInteger(nowMs) ||
            !Number.isSafeInteger(result.checkedAtMs) ||
            result.checkedAtMs > nowMs ||
            nowMs - result.checkedAtMs >= monitoringEvaluationContract.retentionMs
        )
            return 'expired' as const
        const payload = JSON.stringify(result)
        const bytes = Buffer.byteLength(payload)
        if (bytes > monitoringEvaluationContract.maxRecordBytes) return 'oversize' as const
        const previous = database
            .query<{ payload: string }, [string]>('SELECT payload FROM outbox WHERE attempt_id = ?')
            .get(result.attemptId)
        if (previous)
            return previous.payload === payload ? ('duplicate' as const) : ('conflict' as const)
        const current = stats()
        if (
            current.oldest !== null &&
            nowMs - current.oldest >= monitoringEvaluationContract.retentionMs
        )
            return 'expired' as const
        if (
            current.records >= monitoringEvaluationContract.maxOutboxRecords ||
            current.bytes + bytes > monitoringEvaluationContract.maxOutboxBytes
        )
            return 'pressure' as const
        database
            .query(
                'INSERT INTO outbox (attempt_id, payload, bytes, checked_at) VALUES (?, ?, ?, ?)',
            )
            .run(result.attemptId, payload, bytes, result.checkedAtMs)
        return 'recorded' as const
    })
    return {
        /** Full resync can jump generations; deltas require the exact next generation. */
        publish(
            publicationStoreId: string,
            generation: number,
            mode: 'delta' | 'resync',
            reactivate = false,
        ) {
            const current = identity()
            if (publicationStoreId !== current.store_id) return false
            if (!Number.isSafeInteger(generation) || generation <= current.generation) return false
            if (
                mode === 'delta' &&
                (generation !== current.generation + 1 || current.resync_required)
            ) {
                database.exec('UPDATE identity SET paused = 1, resync_required = 1 WHERE id = 1')
                return false
            }
            database
                .query(
                    'UPDATE identity SET generation = ?, paused = ?, resync_required = 0 WHERE id = 1',
                )
                .run(generation, reactivate ? 0 : current.paused)
            return true
        },
        append: insert,
        /** Read a fixed replay batch; repeated reads preserve durable attempt identities. */
        read() {
            return database
                .query<{ sequence: number; attempt_id: string; payload: string }, []>(
                    'SELECT sequence, attempt_id, payload FROM outbox ORDER BY sequence LIMIT 64',
                )
                .all()
        },
        /** The caller supplies only receipts already committed by the authoritative application sink. */
        acknowledge(receipts: readonly { sequence: number; attemptId: string; storeId: string }[]) {
            if (receipts.length > 64) throw new Error('Monitoring experiment ACK batch exceeded')
            return database.transaction(() => {
                let removed = 0
                for (const receipt of receipts) {
                    if (receipt.storeId !== identity().store_id) continue
                    removed += database
                        .query('DELETE FROM outbox WHERE sequence = ? AND attempt_id = ?')
                        .run(receipt.sequence, receipt.attemptId).changes
                }
                return removed
            })()
        },
        /** Destructive reset invalidates every old stream and leaves restored automation paused. */
        reset(nextStoreId: string) {
            if (!nextStoreId || nextStoreId === identity().store_id)
                throw new Error('Monitoring experiment reset requires a new identity')
            database.transaction(() => {
                const discarded = stats().records
                database.exec('DELETE FROM outbox')
                database
                    .query(
                        'UPDATE identity SET store_id = ?, generation = 0, gaps = gaps + ?, paused = 1, resync_required = 1 WHERE id = 1',
                    )
                    .run(nextStoreId, discarded)
            })()
        },
        status: () => ({ ...identity(), ...stats() }),
        close: () => database.close(),
    }
}
