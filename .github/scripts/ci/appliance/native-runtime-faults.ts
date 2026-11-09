// oxlint-disable no-await-in-loop -- Each bounded fault must settle before consuming another restart token.
import assert from 'node:assert/strict'
import { runtimeProcessesProbe, type readRuntimeProcesses } from './runtime-processes.ts'
import { samplePublishedRuntimeTraffic } from './runtime-traffic.ts'
import type { ApplianceRuntimeFaultContext } from './Types/runtime-faults.types.ts'

type RuntimeProcess = Awaited<ReturnType<typeof readRuntimeProcesses>>[number]
type NativeStatus = {
    activeRevision: string | null
    running: boolean
    available: boolean
    recovery: { circuitOpen: boolean; workerRunning: boolean; restartsInWindow: number }
}

/** Distinguish a surviving native owner from a replacement even when its PID is reused. */
function identity(row: RuntimeProcess): string {
    return row.pid + ':' + row.startTicks
}

/** Reject missing or duplicate owners before using a role's process identity as evidence. */
function singleRole(rows: RuntimeProcess[], role: 'controller' | 'caddy'): RuntimeProcess {
    const matches = rows.filter((row) => row.role === role)
    assert.equal(matches.length, 1, 'exactly one native process owns ' + role)
    return matches[0]!
}

/** Verify Caddy's restart budget and controller fail-stop against restored revision and real traffic. */
export async function verifyApplianceNativeRuntimeFaults(
    context: ApplianceRuntimeFaultContext,
): Promise<void> {
    const id = context.recreatedId
    /** Bound inspection and identify appliance restarts consistently under Docker and Podman. */
    const inspectStartedAt = () =>
        context.command(['docker', 'inspect', '--format', '{{.State.StartedAt}}', id], 5_000)
    /** Capture PID/start-tick ownership without inspecting process environments or secret contents. */
    const processes = async () =>
        JSON.parse(
            await context.command(
                ['docker', 'exec', id, 'bun', '--no-env-file', '-e', runtimeProcessesProbe()],
                5_000,
            ),
        ) as RuntimeProcess[]
    /** Read authenticated runtime state with bounded requests while keeping the bearer token inside the appliance. */
    const status = async () =>
        JSON.parse(
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
                    "const token=await Bun.file('/run/rentnerproxy/controller-token/value').text();const r=await fetch('http://127.0.0.1:8081/internal/v1/proxy/status',{headers:{authorization:'Bearer '+token},signal:AbortSignal.timeout(2000)});if(r.status!==200)process.exit(1);process.stdout.write(await r.text());",
                ],
                5_000,
            ),
        ) as NativeStatus
    const original = await status()
    assert.ok(original.activeRevision)
    const baselineRestarts = original.recovery.restartsInWindow
    assert.equal(baselineRestarts, 0, 'native fault matrix starts with a fresh Caddy budget')
    const scenarios = [
        { role: 'caddy', signal: 'TERM', boundaryRestart: false, restarts: 1 },
        { role: 'caddy', signal: 'KILL', boundaryRestart: false, restarts: 2 },
        { role: 'caddy', signal: 'STOP', boundaryRestart: false, restarts: 3 },
        { role: 'caddy', signal: 'KILL', boundaryRestart: true, restarts: 0 },
        { role: 'controller', signal: 'TERM', boundaryRestart: true, restarts: 0 },
        { role: 'controller', signal: 'KILL', boundaryRestart: true, restarts: 0 },
        { role: 'controller', signal: 'STOP', boundaryRestart: true, restarts: 0 },
    ] as const
    for (const scenario of scenarios) {
        const beforeStartedAt = await inspectStartedAt()
        const before = await processes()
        const oldController = singleRole(before, 'controller')
        const oldCaddy = singleRole(before, 'caddy')
        const target = scenario.role === 'controller' ? oldController : oldCaddy
        const traffic = { http: { requests: 0, errors: 0 }, https: { requests: 0, errors: 0 } }
        const sampling = new AbortController()
        /** Measure expected interruptions through published ports until recovery or the scenario deadline. */
        const sample = async (protocol: 'http' | 'https') => {
            while (!sampling.signal.aborted) {
                traffic[protocol].requests += 1
                if (!(await samplePublishedRuntimeTraffic(context, protocol)))
                    traffic[protocol].errors += 1
                await Bun.sleep(250)
            }
        }
        const samplers = [sample('http'), sample('https')]
        const started = performance.now()
        const deadlineMs = scenario.boundaryRestart ? 120_000 : 90_000
        let recovered = false
        let recoveredAt: number | undefined
        let boundaryChanged = false
        try {
            const source = `const stat=await Bun.file('/proc/${target.pid}/stat').text();if(stat.slice(stat.lastIndexOf(')')+2).trim().split(/\\s+/)[19]!==${JSON.stringify(target.startTicks)})throw new Error('native identity changed');process.kill(${target.pid},'SIG${scenario.signal}');`
            await context.command(
                ['docker', 'exec', id, 'bun', '--no-env-file', '-e', source],
                5_000,
            )
            while (performance.now() - started < deadlineMs) {
                let observed:
                    { startedAt: string; rows: RuntimeProcess[]; native: NativeStatus } | undefined
                try {
                    const startedAt = await inspectStartedAt()
                    const rows = await processes()
                    const native = await status()
                    // Reject snapshots spanning an appliance restart.
                    if (startedAt === (await inspectStartedAt()))
                        observed = { startedAt, rows, native }
                } catch {
                    /* Container stop/start and unavailable native endpoints are expected here. */
                }
                if (observed) {
                    const { startedAt, rows, native } = observed
                    boundaryChanged ||= startedAt !== beforeStartedAt
                    const controllers = rows.filter((row) => row.role === 'controller')
                    const caddies = rows.filter((row) => row.role === 'caddy')
                    if (!scenario.boundaryRestart) {
                        assert.equal(
                            startedAt,
                            beforeStartedAt,
                            'Caddy fault stays inside the same appliance',
                        )
                        assert.equal(
                            identity(singleRole(rows, 'controller')),
                            identity(oldController),
                        )
                    } else if (startedAt === beforeStartedAt) {
                        assert.ok(
                            controllers.every((row) => identity(row) === identity(oldController)),
                            'controller cannot restart locally alongside the previous appliance',
                        )
                        assert.ok(
                            caddies.every((row) => identity(row) === identity(oldCaddy)),
                            'fail-stop cannot start a replacement Caddy inside the previous appliance',
                        )
                    }
                    const hasReplacement =
                        controllers.length === 1 &&
                        caddies.length === 1 &&
                        (scenario.boundaryRestart ? startedAt !== beforeStartedAt : true) &&
                        identity(caddies[0]!) !== identity(oldCaddy) &&
                        (!scenario.boundaryRestart ||
                            identity(controllers[0]!) !== identity(oldController))
                    if (
                        hasReplacement &&
                        native.activeRevision === original.activeRevision &&
                        native.running &&
                        native.available &&
                        native.recovery.workerRunning &&
                        !native.recovery.circuitOpen &&
                        native.recovery.restartsInWindow === scenario.restarts
                    ) {
                        try {
                            const ready = await fetch(
                                'http://127.0.0.1:' + context.managementPort + '/health/ready',
                                { signal: AbortSignal.timeout(2_000) },
                            )
                            await ready.body?.cancel()
                            const success = await Promise.all(
                                ['http', 'https'].map((protocol) =>
                                    samplePublishedRuntimeTraffic(
                                        context,
                                        protocol as 'http' | 'https',
                                    ),
                                ),
                            )
                            recovered = ready.ok && success.every(Boolean)
                            if (recovered) {
                                recoveredAt = performance.now() - started
                                break
                            }
                        } catch {
                            /* Retry inside the same bounded recovery window. */
                        }
                    }
                }
                await Bun.sleep(500)
            }
        } finally {
            sampling.abort()
            await Promise.all(samplers)
        }
        console.log(
            'Runtime native ' +
                scenario.role +
                ' SIG' +
                scenario.signal +
                ' expectedBoundaryRestart=' +
                scenario.boundaryRestart +
                ' boundaryChanged=' +
                boundaryChanged +
                ' recovered=' +
                recovered +
                ' recoveryMs=' +
                Math.round(recoveredAt ?? performance.now() - started) +
                ' httpRequests=' +
                traffic.http.requests +
                ' httpErrors=' +
                traffic.http.errors +
                ' httpsRequests=' +
                traffic.https.requests +
                ' httpsErrors=' +
                traffic.https.errors,
        )
        assert.equal(
            recovered,
            true,
            'native fault recovers the exact active revision and published traffic within deadline',
        )
        assert.ok(
            recoveredAt !== undefined && recoveredAt <= deadlineMs,
            'native recovery completes inside its deadline',
        )
        assert.equal(boundaryChanged, scenario.boundaryRestart)
        context.passed(
            scenario.role +
                ' SIG' +
                scenario.signal +
                (scenario.boundaryRestart
                    ? ' recovers through appliance fail-stop'
                    : ' recovers inside the native Caddy budget'),
        )
    }
}
