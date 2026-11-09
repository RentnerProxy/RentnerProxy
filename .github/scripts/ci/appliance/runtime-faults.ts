// oxlint-disable no-await-in-loop -- Fault scenarios and their bounded observations must run in order.
import assert from 'node:assert/strict'
import {
    runtimeFdCountsProbe,
    runtimeProcessesProbe,
    type readRuntimeProcesses,
} from './runtime-processes.ts'
import type { ApplianceRuntimeFaultContext } from './Types/runtime-faults.types.ts'
import { samplePublishedRuntimeTraffic } from './runtime-traffic.ts'

type RuntimeProcess = Awaited<ReturnType<typeof readRuntimeProcesses>>[number]
type RuntimeStage =
    | 'baseline-container'
    | 'baseline-native'
    | 'resource-stats'
    | 'resource-fds'
    | 'resource-postgres'
    | 'fault-processes'
    | 'fault-native'
    | 'fault-signal'
    | 'fault-observation'
    | 'fault-result'
type RuntimeStatus = {
    activeRevision: string
    running: boolean
    available: boolean
    recovery: {
        workerRunning: boolean
        lastCompletedAgeSeconds: number | null
        completedTicks: number
        circuitOpen: boolean
        restartsInWindow: number
    }
}

function identity(process: RuntimeProcess): string {
    return process.pid + ':' + process.startTicks
}

function stableRoles(rows: RuntimeProcess[]): string[] {
    return rows
        .filter((row) => row.role !== 'web')
        .map((row) => row.role + ':' + identity(row))
        .toSorted()
}

export async function verifyApplianceRuntimeFaults(
    context: ApplianceRuntimeFaultContext,
): Promise<void> {
    let stage: RuntimeStage = 'baseline-container'
    const setStage = (value: RuntimeStage) => {
        stage = value
        console.log('Runtime web stage=' + stage)
    }
    try {
        await runApplianceRuntimeFaults(context, setStage)
    } catch (error) {
        console.error('Runtime web failure stage=' + stage)
        throw error
    }
}

async function runApplianceRuntimeFaults(
    context: ApplianceRuntimeFaultContext,
    setStage: (value: RuntimeStage) => void,
): Promise<void> {
    const id = context.recreatedId
    const processes = async () =>
        JSON.parse(
            await context.command(
                ['docker', 'exec', id, 'bun', '--no-env-file', '-e', runtimeProcessesProbe()],
                5_000,
            ),
        ) as RuntimeProcess[]
    const status = async (allowInitialPending = false): Promise<RuntimeStatus> => {
        const source = `const token=await Bun.file('/run/rentnerproxy/controller-token/value').text();const response=await fetch('http://127.0.0.1:8081/internal/v1/proxy/status',{headers:{authorization:'Bearer '+token},signal:AbortSignal.timeout(2000)});process.stdout.write(JSON.stringify({status:response.status,body:await response.text()}));`
        const response = JSON.parse(
            await context.command(
                [
                    'docker',
                    'exec',
                    '--user',
                    '10001:10001',
                    id,
                    'bun',
                    '--no-env-file',
                    '-e',
                    source,
                ],
                5_000,
            ),
        ) as { status: number; body: string }
        assert.equal(response.status, 200)
        const value = JSON.parse(response.body) as RuntimeStatus
        if (
            allowInitialPending &&
            typeof value.recovery.workerRunning === 'boolean' &&
            typeof value.recovery.circuitOpen === 'boolean' &&
            Number.isSafeInteger(value.recovery.completedTicks) &&
            (value.recovery.lastCompletedAgeSeconds === null ||
                Number.isSafeInteger(value.recovery.lastCompletedAgeSeconds))
        ) {
            console.log(
                'Runtime web native workerRunning=' +
                    value.recovery.workerRunning +
                    ' circuitOpen=' +
                    value.recovery.circuitOpen +
                    ' completedTicks=' +
                    value.recovery.completedTicks +
                    ' lastCompletedAgeSeconds=' +
                    value.recovery.lastCompletedAgeSeconds,
            )
        }
        assert.equal(value.recovery.circuitOpen, false)
        assert.ok(Number.isSafeInteger(value.recovery.completedTicks))
        if (!allowInitialPending) {
            assert.equal(value.recovery.workerRunning, true)
            assert.ok(
                value.recovery.lastCompletedAgeSeconds !== null &&
                    value.recovery.lastCompletedAgeSeconds <= 90,
            )
        }
        return value
    }
    setStage('baseline-container')
    const restartCount = await context.inspect(id, '{{.RestartCount}}')
    setStage('baseline-native')
    const initialStarted = performance.now()
    let original = await status(true)
    while (
        !(
            original.recovery.workerRunning &&
            original.recovery.lastCompletedAgeSeconds !== null &&
            original.recovery.lastCompletedAgeSeconds <= 90 &&
            original.recovery.completedTicks > 0
        ) &&
        performance.now() - initialStarted < 15_000
    ) {
        await Bun.sleep(500)
        original = await status(true)
    }
    assert.ok(
        performance.now() - initialStarted <= 15_000 &&
            original.recovery.workerRunning &&
            original.recovery.lastCompletedAgeSeconds !== null &&
            original.recovery.lastCompletedAgeSeconds <= 90 &&
            original.recovery.completedTicks > 0,
        'initial native recovery worker completes a tick within 15 seconds',
    )
    const resourceSample = async (label: string) => {
        setStage('resource-stats')
        const sample = await context.command(
            [
                'docker',
                'stats',
                '--no-stream',
                '--format',
                '{{.CPUPerc}}|{{.MemUsage}}|{{.PIDs}}',
                id,
            ],
            10_000,
        )
        console.log('Runtime resource sample ' + label + ' cpu|memory|pids=' + sample)
        setStage('resource-fds')
        const descriptorCounts = await context.command(
            ['docker', 'exec', id, 'bun', '--no-env-file', '-e', runtimeFdCountsProbe()],
            5_000,
        )
        console.log('Runtime descriptor counts ' + label + ' ' + descriptorCounts)
        setStage('resource-postgres')
        const databaseConnections = await context.command(
            [
                'docker',
                'exec',
                id,
                'gosu',
                'postgres',
                'psql',
                '--no-psqlrc',
                '--no-password',
                '--host=/var/run/postgresql',
                '--username=postgres',
                '--dbname=rentnerproxy',
                '--tuples-only',
                '--no-align',
                '--command',
                "SELECT count(*) FROM pg_stat_activity WHERE datname = 'rentnerproxy' AND usename = 'rentnerproxy' AND backend_type = 'client backend';",
            ],
            5_000,
        )
        assert.match(databaseConnections, /^\d+$/u)
        console.log(
            'Runtime application PostgreSQL client connections ' +
                label +
                '=' +
                databaseConnections,
        )
    }
    await resourceSample('before faults')
    for (const signal of ['TERM', 'KILL', 'STOP'] as const) {
        setStage('fault-processes')
        const beforeProcesses = await processes()
        const oldWeb = beforeProcesses.filter((row) => row.role === 'web')
        assert.equal(oldWeb.length, 1, 'steady appliance has exactly one web process')
        assert.equal(beforeProcesses.filter((row) => row.role === 'controller').length, 1)
        assert.equal(beforeProcesses.filter((row) => row.role === 'caddy').length, 1)
        setStage('fault-native')
        const beforeStatus = await status()
        const sampling = new AbortController()
        const traffic = { http: { requests: 0, errors: 0 }, https: { requests: 0, errors: 0 } }
        const sampleTraffic = async (protocol: 'http' | 'https') => {
            const counts = traffic[protocol]
            while (!sampling.signal.aborted) {
                counts.requests += 1
                if (!(await samplePublishedRuntimeTraffic(context, protocol))) counts.errors += 1
                await Bun.sleep(250)
            }
        }
        const samplers = [sampleTraffic('http'), sampleTraffic('https')]
        const started = performance.now()
        let recoveredAt: number | undefined
        let lastTicks = beforeStatus.recovery.completedTicks
        let stoppedTicksBaseline: number | undefined
        let stoppedTicksDelta = 0
        const replacementIdentities = new Set<string>()
        try {
            setStage('fault-signal')
            // Match both PID and kernel start time again before sending a signal.
            const target = oldWeb[0]!
            const source = `const stat=await Bun.file('/proc/${target.pid}/stat').text();if(stat.slice(stat.lastIndexOf(')')+2).trim().split(/\\s+/)[19]!==${JSON.stringify(target.startTicks)})throw new Error('web identity changed');process.kill(${target.pid},'SIG${signal}');`
            await context.command(
                ['docker', 'exec', id, 'bun', '--no-env-file', '-e', source],
                5_000,
            )
            setStage('fault-observation')
            while (performance.now() - started < 60_000) {
                const rows = await processes()
                const current = await status()
                assert.deepEqual(
                    stableRoles(rows),
                    stableRoles(beforeProcesses),
                    'controller and Caddy identities stay unchanged',
                )
                assert.equal(current.activeRevision, original.activeRevision)
                assert.equal(current.running, true)
                assert.equal(current.available, true)
                assert.equal(
                    current.recovery.restartsInWindow,
                    beforeStatus.recovery.restartsInWindow,
                )
                assert.ok(current.recovery.completedTicks >= lastTicks)
                lastTicks = current.recovery.completedTicks
                const web = rows.filter((row) => row.role === 'web')
                const oldSurvivors = web.filter((row) =>
                    oldWeb.some((old) => identity(old) === identity(row)),
                )
                const replacements = web.filter(
                    (row) => !oldWeb.some((old) => identity(old) === identity(row)),
                )
                for (const replacement of replacements)
                    replacementIdentities.add(identity(replacement))
                assert.ok(
                    replacementIdentities.size <= 1,
                    'one fault starts at most one replacement web process',
                )
                assert.ok(
                    !(oldSurvivors.length && replacements.length),
                    'old UID 10002 identity must be reaped before replacement',
                )
                if (
                    signal === 'STOP' &&
                    web.length === 1 &&
                    oldSurvivors.length === 1 &&
                    (oldSurvivors[0]!.state === 'T' || oldSurvivors[0]!.state === 't')
                ) {
                    // Bracket the counter read with the same stopped identity, before replacement.
                    const confirmedWeb = (await processes()).filter((row) => row.role === 'web')
                    if (
                        confirmedWeb.length === 1 &&
                        identity(confirmedWeb[0]!) === identity(oldWeb[0]!) &&
                        (confirmedWeb[0]!.state === 'T' || confirmedWeb[0]!.state === 't')
                    ) {
                        stoppedTicksBaseline ??= lastTicks
                        stoppedTicksDelta = Math.max(
                            stoppedTicksDelta,
                            lastTicks - stoppedTicksBaseline,
                        )
                    }
                }
                if (web.length === 1 && replacements.length === 1 && web[0]!.state !== 'Z') {
                    try {
                        const ready = await fetch(
                            'http://127.0.0.1:' + context.managementPort + '/health/ready',
                            { signal: AbortSignal.timeout(2_000) },
                        )
                        await ready.body?.cancel()
                        if (ready.ok) {
                            recoveredAt ??= performance.now() - started
                            if (lastTicks > beforeStatus.recovery.completedTicks) break
                        }
                    } catch {
                        /* Retry inside the same recovery deadline. */
                    }
                }
                await Bun.sleep(500)
            }
            assert.ok(
                recoveredAt !== undefined && recoveredAt <= 60_000,
                'web recovers within 60 seconds',
            )
            assert.ok(
                lastTicks > beforeStatus.recovery.completedTicks,
                'native recovery worker advances across web recovery',
            )
            if (signal === 'STOP')
                assert.ok(
                    stoppedTicksDelta > 0,
                    'native recovery worker advances while the original web process remains stopped',
                )
            await Bun.sleep(1_000)
        } finally {
            sampling.abort()
            await Promise.all(samplers)
        }
        console.log(
            'Runtime web SIG' +
                signal +
                ' recoveryMs=' +
                Math.round(recoveredAt!) +
                ' nativeCompletedTicksDelta=' +
                (lastTicks - beforeStatus.recovery.completedTicks) +
                (signal === 'STOP' ? ' nativeTicksWhileWebStopped=' + stoppedTicksDelta : '') +
                ' httpRequests=' +
                traffic.http.requests +
                ' httpErrors=' +
                traffic.http.errors +
                ' httpsRequests=' +
                traffic.https.requests +
                ' httpsErrors=' +
                traffic.https.errors,
        )
        setStage('fault-result')
        assert.equal(traffic.http.errors, 0, 'published HTTP traffic remains available')
        assert.equal(traffic.https.errors, 0, 'published HTTPS traffic remains available')
        assert.ok(traffic.http.requests > 0 && traffic.https.requests > 0)
        assert.equal(
            await context.inspect(id, '{{.RestartCount}}'),
            restartCount,
            'web fault never restarts appliance',
        )
        context.passed(
            'web SIG' + signal + ' is isolated from native runtime and published proxy traffic',
        )
    }
    await resourceSample('after faults')
}
