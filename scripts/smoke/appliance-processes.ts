// oxlint-disable no-await-in-loop -- Shared appliance probes read identities without spawning a subprocess per PID.
/** Census native owners and all web UID processes using confirmed start ticks; tolerate only vanished processes. */
export async function readRuntimeProcesses(procRoot = '/proc') {
    const { readdir, readFile } = await import('node:fs/promises')
    /** Attach only a fixed read stage without replacing the original filesystem failure or its code. */
    const read = async (path: string, stage: 'status' | 'stat' | 'cmdline' | 'confirm-stat') => {
        try {
            return await readFile(path, 'utf8')
        } catch (error) {
            try {
                if (typeof error === 'object' && error !== null)
                    Object.defineProperty(error, 'runtimeCensusStage', {
                        value: stage,
                        configurable: true,
                    })
            } catch {
                /* Diagnostics cannot replace the original failure. */
            }
            throw error
        }
    }
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
                read(directory + '/status', 'status'),
                read(directory + '/stat', 'stat'),
                read(directory + '/cmdline', 'cmdline'),
            ])
            const uid = Number(/^Uid:[\t ]+(\d+)/mu.exec(status)?.[1])
            const commandArguments = command.split('\0')
            const executable = commandArguments[0]?.split('/').at(-1)
            // Health and version clients share the daemon executable but never own its serving lifecycle.
            const role =
                uid === 10002
                    ? 'web'
                    : executable === 'rentnerproxy-controller' &&
                        commandArguments[1] !== '--healthcheck'
                      ? 'controller'
                      : executable === 'caddy' && commandArguments[1] !== 'version'
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
            const confirmedStat = await read(directory + '/stat', 'confirm-stat')
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

/** Convert census failures to fixed stages/codes only; never return error messages, process arguments or paths. */
export function runtimeCensusErrorDiagnostic(error: unknown) {
    const row =
        typeof error === 'object' && error !== null
            ? (error as {
                  code?: unknown
                  cause?: { code?: unknown }
                  errno?: unknown
                  syscall?: unknown
                  path?: unknown
                  message?: unknown
                  runtimeCensusStage?: unknown
              })
            : {}
    const codes = new Set([
        'ENOENT',
        'ESRCH',
        'EIO',
        'EACCES',
        'EPERM',
        'EINVAL',
        'EBADF',
        'ENOMEM',
        'EISDIR',
        'ENOTDIR',
        'EMFILE',
        'ENFILE',
    ])
    const code = typeof row.code === 'string' && codes.has(row.code) ? row.code : 'unknown'
    const causeCode =
        typeof row.cause?.code === 'string' && codes.has(row.cause.code)
            ? row.cause.code
            : 'unknown'
    const syscall =
        typeof row.syscall === 'string' &&
        ['read', 'open', 'readFile', 'scandir'].includes(row.syscall)
            ? row.syscall
            : 'unknown'
    const path = typeof row.path === 'string' ? row.path : ''
    const stage =
        typeof row.runtimeCensusStage === 'string' &&
        ['status', 'stat', 'cmdline', 'confirm-stat'].includes(row.runtimeCensusStage)
            ? row.runtimeCensusStage
            : row.message === 'runtime process identity is incomplete'
              ? 'identity-incomplete'
              : row.message === 'runtime process identity changed during enumeration'
                ? 'identity-changed'
                : syscall === 'scandir'
                  ? 'directory'
                  : path
                    ? (['status', 'stat', 'cmdline', 'fd'].find(
                          (name) => path.endsWith('/' + name) || path.endsWith('\\' + name),
                      ) ?? 'unknown')
                    : 'unknown'
    const errno =
        typeof row.errno === 'number' &&
        Number.isSafeInteger(row.errno) &&
        Math.abs(row.errno) <= 2147483647
            ? row.errno
            : null
    return { stage, code, causeCode, syscall, errno }
}

/** Accept only an exact bounded census diagnostic record, or the caller's explicit census deadline bit. */
export function runtimeCensusFailureDiagnostic(stderr: string, timedOut = false) {
    if (timedOut)
        return {
            stage: 'command',
            code: 'timeout',
            causeCode: 'unknown',
            syscall: 'unknown',
            errno: null,
        }
    for (const line of stderr.split('\n')) {
        const match = /^RUNTIME_CENSUS_DIAGNOSTIC=(\{[^\r\n]{1,1024}\})\r?$/u.exec(line)
        if (!match) continue
        try {
            const value: unknown = JSON.parse(match[1]!)
            if (typeof value !== 'object' || value === null || Array.isArray(value)) continue
            const row = value as Record<string, unknown>
            const keys = Object.keys(row).toSorted()
            if (keys.join(',') !== 'causeCode,code,errno,stage,syscall') continue
            const codes = new Set([
                'unknown',
                'ENOENT',
                'ESRCH',
                'EIO',
                'EACCES',
                'EPERM',
                'EINVAL',
                'EBADF',
                'ENOMEM',
                'EISDIR',
                'ENOTDIR',
                'EMFILE',
                'ENFILE',
            ])
            if (
                typeof row.stage !== 'string' ||
                ![
                    'directory',
                    'status',
                    'stat',
                    'cmdline',
                    'fd',
                    'identity-incomplete',
                    'identity-changed',
                    'unknown',
                    'confirm-stat',
                ].includes(row.stage)
            )
                continue
            if (
                typeof row.code !== 'string' ||
                !codes.has(row.code) ||
                typeof row.causeCode !== 'string' ||
                !codes.has(row.causeCode)
            )
                continue
            if (
                typeof row.syscall !== 'string' ||
                !['unknown', 'read', 'open', 'readFile', 'scandir'].includes(row.syscall)
            )
                continue
            if (
                row.errno !== null &&
                (typeof row.errno !== 'number' ||
                    !Number.isSafeInteger(row.errno) ||
                    Math.abs(row.errno) > 2147483647)
            )
                continue
            return {
                stage: row.stage,
                code: row.code,
                causeCode: row.causeCode,
                syscall: row.syscall,
                errno: row.errno as number | null,
            }
        } catch {
            /* Unrecognized stderr remains private. */
        }
    }
    return null
}

/** Keep serialized probes closure-free while making every rejected census nonzero and safely diagnosable. */
function censusProbe(body: string): string {
    return `try{${body}}catch(error){process.stderr.write('RUNTIME_CENSUS_DIAGNOSTIC='+JSON.stringify((${runtimeCensusErrorDiagnostic.toString()})(error))+'\\n');process.exitCode=1;}`
}

/** Serialize the same census implementation for execution inside an appliance or a synthetic proc fixture. */
export function runtimeProcessesProbe(procRoot = '/proc'): string {
    return censusProbe(
        `process.stdout.write(JSON.stringify(await (${readRuntimeProcesses.toString()})(${JSON.stringify(procRoot)})));`,
    )
}

/** Produce numeric descriptor counts without reading descriptor targets or their potentially sensitive contents. */
export function runtimeFdCountsProbe(procRoot = '/proc'): string {
    // Count directory entries only; descriptor targets and their contents may contain secrets.
    return censusProbe(
        `const {readdir}=await import('node:fs/promises');const rows=await (${readRuntimeProcesses.toString()})(${JSON.stringify(procRoot)});const counts=await Promise.all(rows.map(async row=>({role:row.role,pid:row.pid,startTicks:row.startTicks,openFds:(await readdir(${JSON.stringify(procRoot)}+'/'+row.pid+'/fd')).length})));process.stdout.write(JSON.stringify(counts));`,
    )
}
