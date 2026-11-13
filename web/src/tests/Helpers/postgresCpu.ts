import type { SQL } from 'bun'
import { readFile } from 'node:fs/promises'
import type { PostgresCpuIdentity, PostgresCpuProcess } from './Types/postgres-cpu.types.ts'

/** Validate the same Linux PID generation against PostgreSQL backend_start before accepting OS CPU counters. */
export function parsePostgresCpuProcess(
    stat: string,
    schedstat: string,
    identity: PostgresCpuIdentity,
): PostgresCpuProcess {
    const fields = stat
        .slice(stat.lastIndexOf(')') + 2)
        .trim()
        .split(/\s+/u)
    const startTicks = fields.at(19)
    const counters = schedstat.trim().split(/\s+/u)
    if (
        !Number.isSafeInteger(identity.pid) ||
        identity.pid <= 0 ||
        !Number.isFinite(identity.backendStartEpoch) ||
        !Number.isFinite(identity.bootEpoch) ||
        !Number.isSafeInteger(identity.clockTicks) ||
        identity.clockTicks <= 0 ||
        !stat.startsWith(String(identity.pid) + ' (') ||
        !stat.includes(') ') ||
        fields[0] === 'Z' ||
        fields[0] === 'X' ||
        !startTicks ||
        !/^\d+$/u.test(startTicks) ||
        counters.length !== 3 ||
        counters.some((value) => !/^\d+$/u.test(value))
    )
        throw new Error('PostgreSQL CPU process identity is unavailable')
    const started = identity.bootEpoch + Number(startTicks) / identity.clockTicks
    if (!Number.isFinite(started) || Math.abs(started - identity.backendStartEpoch) > 2)
        throw new Error('PostgreSQL CPU process belongs to a different PID namespace or generation')
    const [cpuNanoseconds, runQueueNanoseconds] = counters
    if (cpuNanoseconds === undefined || runQueueNanoseconds === undefined)
        throw new Error('PostgreSQL CPU counters are incomplete')
    return { pid: identity.pid, startTicks, cpuNanoseconds, runQueueNanoseconds }
}

/** Read only the owned database's non-observer backend PIDs; the opt-in caller must share their Linux PID namespace. */
export async function readPostgresCpuSnapshot(observer: SQL): Promise<PostgresCpuProcess[]> {
    const clockTicks = Number(process.env.RENTNERPROXY_POSTGRES_CLOCK_TICKS)
    if (process.platform !== 'linux' || !Number.isSafeInteger(clockTicks) || clockTicks <= 0)
        throw new Error('Collocated Linux PostgreSQL CPU profiling needs measured CLK_TCK')
    const bootEpoch = Number(/^btime (\d+)$/mu.exec(await readFile('/proc/stat', 'utf8'))?.[1])
    if (!Number.isFinite(bootEpoch)) throw new Error('Linux boot clock is unavailable')
    const rows = await observer<{ pid: number; started: number }[]>`
        select pid, extract(epoch from backend_start)::float8 started from pg_stat_activity
        where datname=current_database() and backend_type='client backend'
            and application_name <> 'worker-profile-observer'`
    if (rows.length === 0 || rows.length > 64)
        throw new Error('PostgreSQL CPU backend bound failed')
    return Promise.all(
        rows.map(async (row) => {
            const path = '/proc/' + row.pid
            const before = await readFile(path + '/stat', 'utf8')
            const counters = await readFile(path + '/schedstat', 'utf8')
            const after = await readFile(path + '/stat', 'utf8')
            const identity = {
                pid: row.pid,
                backendStartEpoch: Number(row.started),
                bootEpoch,
                clockTicks,
            }
            const process = parsePostgresCpuProcess(before, counters, identity)
            if (
                parsePostgresCpuProcess(after, counters, identity).startTicks !== process.startTicks
            )
                throw new Error('PostgreSQL CPU PID changed while reading')
            return process
        }),
    )
}

/** CPU/runqueue deltas are operation-window backend totals, never SQL execution wall time or lock-wait attribution. */
export function postgresCpuDelta(
    before: readonly PostgresCpuProcess[],
    after: readonly PostgresCpuProcess[],
) {
    if (!before.length || !after.length) throw new Error('PostgreSQL CPU snapshots are empty')
    if (
        new Set(before.map((row) => row.pid)).size !== before.length ||
        new Set(after.map((row) => row.pid)).size !== after.length
    )
        throw new Error('PostgreSQL CPU snapshots contain duplicate PIDs')
    const previous = new Map(before.map((row) => [row.pid, row]))
    let cpu = 0n,
        runQueue = 0n,
        started = 0
    for (const row of after) {
        const old = previous.get(row.pid)
        if (old && old.startTicks !== row.startTicks)
            throw new Error('PostgreSQL CPU PID was reused')
        const deltaCpu = BigInt(row.cpuNanoseconds) - BigInt(old?.cpuNanoseconds ?? '0')
        const deltaQueue = BigInt(row.runQueueNanoseconds) - BigInt(old?.runQueueNanoseconds ?? '0')
        if (deltaCpu < 0 || deltaQueue < 0) throw new Error('PostgreSQL CPU counters regressed')
        cpu += deltaCpu
        runQueue += deltaQueue
        if (!old) started++
        previous.delete(row.pid)
    }
    if (cpu > BigInt(Number.MAX_SAFE_INTEGER) || runQueue > BigInt(Number.MAX_SAFE_INTEGER))
        throw new Error('PostgreSQL CPU counter window exceeds bound')
    return {
        status: previous.size === 0 ? ('complete' as const) : ('insufficient' as const),
        processCpuMs: Number(cpu) / 1_000_000,
        runQueueMs: Number(runQueue) / 1_000_000,
        observedBackends: after.length,
        startedBackends: started,
        exitedBackendsWithoutFinalCounter: previous.size,
    }
}
