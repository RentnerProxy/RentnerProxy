// oxlint-disable no-await-in-loop -- Each process observation is bracketed by identity checks.
import { z } from 'zod'
import type { ResourceProcFilesystem } from './Types/resource-probe.types.ts'
import { readRuntimeProcesses } from '../smoke/appliance-processes.ts'

/** Read VmRSS only for the stable serving owners; missing, malformed or ambiguous observations stay null. No command lines or paths leave the probe. */
export async function readRuntimeResourceMetrics(
    procRoot = '/proc',
    filesystem?: ResourceProcFilesystem,
) {
    const { readdir, readFile } = filesystem ?? (await import('node:fs/promises'))
    const before = await readRuntimeProcesses(procRoot)
    const values = {
        webFds: null as number | null,
        controllerFds: null as number | null,
        caddyFds: null as number | null,
        webRssBytes: null as number | null,
        controllerRssBytes: null as number | null,
        caddyRssBytes: null as number | null,
    }
    for (const role of ['web', 'controller', 'caddy'] as const) {
        const owners = before.filter((row) => row.role === role)
        if (owners.length !== 1) continue
        const owner = owners[0]!
        if (owner.state === 'Z' || owner.state === 'X') continue
        try {
            const directory = procRoot + '/' + owner.pid
            const [status, command, commandName, descriptors] = await Promise.all([
                readFile(directory + '/status', 'utf8'),
                readFile(directory + '/cmdline', 'utf8'),
                readFile(directory + '/comm', 'utf8'),
                readdir(directory + '/fd'),
            ])
            const uid = role === 'web' ? '10002' : '10001'
            const identities = /^Uid:[\t ]+(\d+)[\t ]+(\d+)[\t ]+(\d+)[\t ]+(\d+)$/mu.exec(status)
            if (!identities || identities.slice(1).some((value) => value !== uid)) continue
            const args = command.split('\0').filter(Boolean)
            const expectedCommandName =
                role === 'web' ? 'bun' : role === 'controller' ? 'rentnerproxy-co' : 'caddy'
            if (commandName.trim() !== expectedCommandName) continue
            const expectedPath =
                role === 'web'
                    ? '/usr/local/bin/bun'
                    : role === 'controller'
                      ? '/usr/local/bin/rentnerproxy-controller'
                      : '/usr/bin/caddy'
            if (args[0] !== expectedPath && args[0] !== expectedPath.split('/').at(-1)) continue
            if (
                role === 'web' &&
                (args.length !== 2 || args[1] !== '/opt/rentnerproxy/web/docker/web/serve.ts')
            )
                continue
            if (role === 'controller' && args.length !== 1) continue
            if (role === 'caddy' && args[1] !== 'run') continue
            const rss = /^VmRSS:[\t ]+(\d+)[\t ]+kB$/mu.exec(status)
            const bytes = rss ? Number(rss[1]) * 1024 : NaN
            if (!Number.isSafeInteger(bytes) || bytes <= 0) continue
            const after = (await readRuntimeProcesses(procRoot)).filter((row) => row.role === role)
            if (
                after.length !== 1 ||
                after[0]!.pid !== owner.pid ||
                after[0]!.startTicks !== owner.startTicks ||
                after[0]!.state === 'Z' ||
                after[0]!.state === 'X'
            )
                continue
            const [confirmedStatus, confirmedCommand, confirmedCommandName] = await Promise.all([
                readFile(directory + '/status', 'utf8'),
                readFile(directory + '/cmdline', 'utf8'),
                readFile(directory + '/comm', 'utf8'),
            ])
            if (
                /^Uid:[\t ]+(\d+)[\t ]+(\d+)[\t ]+(\d+)[\t ]+(\d+)$/mu.exec(
                    confirmedStatus,
                )?.[0] !== identities[0] ||
                confirmedCommand !== command ||
                confirmedCommandName !== commandName
            )
                continue
            const confirmedStat = await readFile(directory + '/stat', 'utf8')
            const fields = confirmedStat
                .slice(confirmedStat.lastIndexOf(')') + 2)
                .trim()
                .split(/\s+/u)
            if (fields[19] !== owner.startTicks || fields[0] === 'Z' || fields[0] === 'X') continue
            values[`${role}Fds`] = descriptors.length
            values[`${role}RssBytes`] = bytes
        } catch {
            // Unreadable or vanished owners are unknown, never zero or a different process.
        }
    }
    return values
}

/** Execute the same identity-checked parser in the appliance; output contains a fixed numeric/null allowlist. */
export function runtimeResourceProbeSource(): string {
    return `const readRuntimeProcesses=${readRuntimeProcesses.toString()};const processResources=await (${readRuntimeResourceMetrics.toString()})(process.argv[1]??'/proc');`
}

const memoryObservationSchema = z.object({
    memoryBytes: z.number().finite().nonnegative(),
    cgroupMemoryCurrentBytes: z.number().finite().nonnegative().nullable().optional(),
    webRssBytes: z.number().int().positive().safe().nullable().optional(),
    controllerRssBytes: z.number().int().positive().safe().nullable().optional(),
    caddyRssBytes: z.number().int().positive().safe().nullable().optional(),
})

/** Strip everything except numeric memory measurements, preserving unavailable RSS as null. */
export function readMemoryObservation(value: unknown) {
    const result = memoryObservationSchema.safeParse(value)
    if (!result.success) return null
    return {
        memoryBytes: result.data.memoryBytes,
        cgroupMemoryCurrentBytes: result.data.cgroupMemoryCurrentBytes ?? null,
        webRssBytes: result.data.webRssBytes ?? null,
        controllerRssBytes: result.data.controllerRssBytes ?? null,
        caddyRssBytes: result.data.caddyRssBytes ?? null,
    }
}

/** RSS is descriptive per process; shared pages prevent comparing its sum with a cgroup memory bound. */
export const memoryMeasurementSemantics = {
    memoryBytes: 'Cgroup working set: memory.current minus inactive_file; not RSS',
    cgroupMemoryCurrentBytes: 'Cgroup memory.current including charged page cache',
    processRssBytes:
        'Per serving owner /proc/PID/status VmRSS in bytes; shared pages may overlap; never summed or used as the cgroup bound',
} as const

/** Preserve partial numeric evidence without making missing owners look measured or adding overlapping RSS values. */
export function memoryResourceEvidence(samples: unknown[]) {
    const observations = samples.map(readMemoryObservation)
    const keys = [
        'cgroupMemoryCurrentBytes',
        'webRssBytes',
        'controllerRssBytes',
        'caddyRssBytes',
    ] as const
    return {
        complete:
            observations.length > 0 &&
            observations.every((item) => item !== null && keys.every((key) => item[key] !== null)),
        maxima: Object.fromEntries(
            keys.map((key) => {
                const known = observations.flatMap((item) =>
                    item?.[key] == null ? [] : [item[key]],
                )
                return [key, known.length ? Math.max(...known) : null]
            }),
        ),
        semantics: memoryMeasurementSemantics,
    }
}
