// oxlint-disable no-await-in-loop -- Read each process identity without spawning a subprocess per PID.
/** Census native owners and all web UID processes using confirmed start ticks; tolerate only vanished processes. */
export async function readRuntimeProcesses(procRoot = '/proc') {
    const { readdir, readFile } = await import('node:fs/promises')
    const processes: {
        pid: number
        startTicks: string
        uid: number
        state: string
        role: 'web' | 'controller' | 'caddy'
    }[] = []
    for (const pid of await readdir(procRoot)) {
        if (!/^\d+$/u.test(pid)) continue
        try {
            const directory = procRoot + '/' + pid
            const [status, stat, command] = await Promise.all([
                readFile(directory + '/status', 'utf8'),
                readFile(directory + '/stat', 'utf8'),
                readFile(directory + '/cmdline', 'utf8'),
            ])
            const uid = Number(/^Uid:[\t ]+(\d+)/mu.exec(status)?.[1])
            const executable = command.split('\0')[0]?.split('/').at(-1)
            const role =
                uid === 10002
                    ? 'web'
                    : executable === 'rentnerproxy-controller'
                      ? 'controller'
                      : executable === 'caddy'
                        ? 'caddy'
                        : undefined
            if (!role) continue
            // comm may contain spaces and parentheses; field 22 follows the final ')'.
            const fields = stat
                .slice(stat.lastIndexOf(')') + 2)
                .trim()
                .split(/\s+/u)
            if (!/^\d+$/u.test(fields[19] ?? '') || !Number.isFinite(uid))
                throw new Error('runtime process identity is incomplete')
            const confirmedStat = await readFile(directory + '/stat', 'utf8')
            if (
                confirmedStat
                    .slice(confirmedStat.lastIndexOf(')') + 2)
                    .trim()
                    .split(/\s+/u)[19] !== fields[19]
            )
                throw new Error('runtime process identity changed during enumeration')
            processes.push({
                pid: Number(pid),
                startTicks: fields[19]!,
                uid,
                state: fields[0]!,
                role,
            })
        } catch (error) {
            if (
                typeof error === 'object' &&
                error !== null &&
                'code' in error &&
                (error.code === 'ENOENT' || error.code === 'ESRCH')
            )
                continue
            throw error
        }
    }
    return processes.toSorted((left, right) => left.pid - right.pid)
}

/** Serialize the same census implementation for execution inside an appliance or a synthetic proc fixture. */
export function runtimeProcessesProbe(procRoot = '/proc'): string {
    return `process.stdout.write(JSON.stringify(await (${readRuntimeProcesses.toString()})(${JSON.stringify(procRoot)})));`
}

/** Produce numeric descriptor counts without reading descriptor targets or their potentially sensitive contents. */
export function runtimeFdCountsProbe(procRoot = '/proc'): string {
    // Count directory entries only; descriptor targets and their contents may contain secrets.
    return `const {readdir}=await import('node:fs/promises');const rows=await (${readRuntimeProcesses.toString()})(${JSON.stringify(procRoot)});const counts=await Promise.all(rows.map(async row=>({role:row.role,pid:row.pid,startTicks:row.startTicks,openFds:(await readdir(${JSON.stringify(procRoot)}+'/'+row.pid+'/fd')).length})));process.stdout.write(JSON.stringify(counts));`
}
