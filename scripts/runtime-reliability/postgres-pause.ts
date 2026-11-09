// oxlint-disable no-await-in-loop -- Validate each process identity immediately before signaling it.
import type {
    PostgresPauseDependencies,
    PostgresProcessIdentity,
} from './Types/postgres-pause.types.ts'

/** Signal only still-matching PostgreSQL identities; a recycled PID must never receive STOP or CONT. */
export async function signalPostgresIdentities(
    targets: PostgresProcessIdentity[],
    signal: 'SIGSTOP' | 'SIGCONT',
    procRoot = '/proc',
    sendSignal: (pid: number, signal: 'SIGSTOP' | 'SIGCONT') => unknown = process.kill,
): Promise<number> {
    const { readFile } = await import('node:fs/promises')
    let signaled = 0
    for (const target of targets) {
        try {
            const directory = procRoot + '/' + target.pid
            const [stat, status, command] = await Promise.all([
                readFile(directory + '/stat', 'utf8'),
                readFile(directory + '/status', 'utf8'),
                readFile(directory + '/comm', 'utf8'),
            ])
            const ticks = stat
                .slice(stat.lastIndexOf(')') + 2)
                .trim()
                .split(/\s+/u)[19]
            const uid = Number(/^Uid:[\t ]+(\d+)/mu.exec(status)?.[1])
            if (ticks !== target.startTicks || uid !== target.uid || command.trim() !== 'postgres')
                continue
            sendSignal(target.pid, signal)
            signaled += 1
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
    return signaled
}

/** Arm an independent guard before pausing and always resume before cancelling that guard. */
export async function probeWithPausedPostgres(dependencies: PostgresPauseDependencies) {
    const cancelGuard = await dependencies.guard()
    try {
        await dependencies.pause()
        const ready = await dependencies.probeReady()
        const proxyStatus = await dependencies.probeProxy()
        return { notReady: !ready, proxyStatus, resumed: true }
    } finally {
        // Leave the independent guard armed if direct resumption fails.
        await dependencies.resume()
        await cancelGuard()
    }
}

/** Run the entire brief database fault inside the appliance so host round trips cannot extend its pause. */
async function runPostgresPauseProbe(domain: string) {
    const { readdir, readFile } = await import('node:fs/promises')
    const targets: PostgresProcessIdentity[] = []
    for (const pid of await readdir('/proc')) {
        if (!/^\d+$/u.test(pid)) continue
        try {
            const root = '/proc/' + pid
            if ((await readFile(root + '/comm', 'utf8')).trim() !== 'postgres') continue
            const stat = await readFile(root + '/stat', 'utf8')
            const startTicks = stat
                .slice(stat.lastIndexOf(')') + 2)
                .trim()
                .split(/\s+/u)[19]
            const uid = Number(
                /^Uid:[\t ]+(\d+)/mu.exec(await readFile(root + '/status', 'utf8'))?.[1],
            )
            if (!startTicks || !/^\d+$/u.test(startTicks) || !Number.isSafeInteger(uid))
                throw new Error('invalid PostgreSQL identity')
            targets.push({ pid: Number(pid), startTicks, uid })
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
    if (!targets.length) throw new Error('PostgreSQL process census is empty')
    let guardDeadline = 0
    return probeWithPausedPostgres({
        /** A separately scheduled child resumes surviving identities if the probe process stalls or dies. */
        guard: async () => {
            const source = `const resume=${signalPostgresIdentities.toString()};const targets=${JSON.stringify(targets)};const deadline=Date.now()+5000;process.stdout.write('ready:'+deadline+'\\n');await Bun.sleep(Math.max(0,deadline-Date.now()));await resume(targets,'SIGCONT');`
            const child = Bun.spawn([process.execPath, '--no-env-file', '-e', source], {
                stdin: 'ignore',
                stdout: 'pipe',
                stderr: 'ignore',
            })
            const reader = child.stdout.getReader()
            let timer: ReturnType<typeof setTimeout> | undefined
            /** Read a bounded acknowledgment even when the pipe splits it across chunks. */
            const acknowledgment = async () => {
                const decoder = new TextDecoder()
                let line = ''
                while (line.length < 32 && !line.includes('\n')) {
                    const chunk = await reader.read()
                    if (chunk.done) break
                    line += decoder.decode(chunk.value, { stream: true })
                }
                return line
            }
            try {
                const result = await Promise.race([
                    acknowledgment(),
                    new Promise<never>((_resolve, reject) => {
                        timer = setTimeout(
                            () => reject(new Error('PostgreSQL resume guard did not start')),
                            5000,
                        )
                    }),
                ])
                const acknowledged = /^ready:(\d{13})\n$/u.exec(result)
                if (!acknowledged) throw new Error('PostgreSQL resume guard did not acknowledge')
                guardDeadline = Number(acknowledged[1])
            } catch (error) {
                child.kill()
                await child.exited
                throw error
            } finally {
                if (timer !== undefined) clearTimeout(timer)
                reader.releaseLock()
            }
            return async () => {
                child.kill()
                await child.exited
            }
        },
        /** Refuse an empty matching census rather than claiming an unperformed PostgreSQL fault. */
        pause: async () => {
            if (guardDeadline - Date.now() < 3500)
                throw new Error('PostgreSQL resume guard has insufficient probe headroom')
            if (
                !(await signalPostgresIdentities(targets, 'SIGSTOP', '/proc', (pid, signal) => {
                    // Proc reads can stall; refuse a late STOP even after earlier targets were paused.
                    if (guardDeadline - Date.now() < 3500)
                        throw new Error('PostgreSQL resume guard expired during pause')
                    process.kill(pid, signal)
                }))
            )
                throw new Error('PostgreSQL identities changed before pause')
        },
        /** Resume only identities still owned by the captured PostgreSQL processes. */
        resume: async () => {
            await signalPostgresIdentities(targets, 'SIGCONT')
        },
        /** Bound management probing to one second while PostgreSQL is stopped. */
        probeReady: async () => {
            try {
                const response = await fetch('http://127.0.0.1:3000/health/ready', {
                    signal: AbortSignal.timeout(1000),
                })
                await response.body?.cancel()
                return response.ok
            } catch {
                return false
            }
        },
        /** Observe the already active proxy route without involving a host port lookup. */
        probeProxy: async () => {
            try {
                const response = await fetch('http://127.0.0.1:8080/', {
                    headers: { host: domain },
                    signal: AbortSignal.timeout(2000),
                })
                await response.body?.cancel()
                return response.status
            } catch {
                return null
            }
        },
    })
}

/** Serialize the tested pause protocol and identity checks for a single root-owned container exec. */
export function postgresPauseProbe(domain: string): string {
    return `const signalPostgresIdentities=${signalPostgresIdentities.toString()};const probeWithPausedPostgres=${probeWithPausedPostgres.toString()};const run=${runPostgresPauseProbe.toString()};process.stdout.write(JSON.stringify(await run(${JSON.stringify(domain)})));`
}
