import { monitoringApplianceWindowProbe } from './monitoring-whole-appliance.window.ts'
// oxlint-disable no-await-in-loop -- Matched windows, exact-identity faults and cleanup must execute in order.
import { createHash, randomBytes } from 'node:crypto'
import { mkdir, readFile, readdir, writeFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { z } from 'zod'
import { monitoringEvaluationContract as contract } from './monitoring-evaluation.ts'
import { monitoringB2RustSourceHash } from './monitoring-b2-build.ts'
import { buildMonitoringPrototypeImage } from './monitoring-whole-appliance.image.ts'
import {
    monitoringApplianceFaultCases,
    runMonitoringApplianceFaultCase,
} from './monitoring-whole-appliance.faults.ts'
import {
    monitoringApplianceReportSchema,
    monitoringApplianceResourceSchema,
    monitoringApplianceTrafficSchema,
    summarizeMonitoringApplianceProgress,
    summarizeMonitoringApplianceResources,
    assessMonitoringAppliancePairs,
    assessMonitoringApplianceWake,
} from './monitoring-whole-appliance.metrics.ts'
import type { ReliabilityContext } from '../runtime-reliability/Types/harness.types.ts'
import type { ComponentRecoveryDependencies } from '../runtime-reliability/Types/component-recovery.types.ts'
import type {
    MonitoringApplianceOptions,
    MonitoringApplianceWindow,
    MonitoringAppliancePairInput,
    MonitoringApplianceContinuityDependencies,
    MonitoringApplianceContinuityCapture,
    MonitoringApplianceReport,
    MonitoringApplianceStoppedIdentity,
    MonitoringApplianceFaultSession,
    MonitoringApplianceFaultRecord,
    MonitoringApplianceEvaluationReport,
    MonitoringApplianceCleanupDependencies,
} from './Types/monitoring-whole-appliance.types.ts'

const experimentRoot = resolve(import.meta.dir, '../..')
const containerRoot = '/opt/rentnerproxy/web'

/** No duration, workload or envelope override can silently turn the reference experiment into a smaller test. */
export function parseMonitoringApplianceOptions(args: string[]): MonitoringApplianceOptions {
    const flags = new Map<string, string>()
    const names = [
        '--baseline-root',
        '--baseline-sha',
        '--baseline-image-id',
        '--image',
        '--native-binary',
        '--report',
        '--measurement-window',
    ]
    for (let index = 0; index < args.length; index += 2) {
        const name = args[index]!,
            value = args[index + 1]
        if (!names.includes(name) || flags.has(name) || !value || value.startsWith('--'))
            throw new Error('Invalid B2 whole-appliance option')
        flags.set(name, value)
    }
    if (
        names.some((name) => !flags.has(name)) ||
        ![
            'approved',
            'topology-approved',
            'safety-approved',
            'publication-diagnostic-approved',
        ].includes(flags.get('--measurement-window')!)
    )
        throw new Error(
            'Explicit baseline, image, binary, report and approved measurement window required',
        )
    return {
        baselineRoot: resolve(flags.get('--baseline-root')!),
        baselineSha: z
            .string()
            .regex(/^[a-f0-9]{40}$/u)
            .parse(flags.get('--baseline-sha')),
        baselineImageIdentity: z
            .string()
            .regex(/^sha256:[a-f0-9]{64}$/u)
            .parse(flags.get('--baseline-image-id')),
        image: flags.get('--image')!,
        nativeBinary: resolve(flags.get('--native-binary')!),
        reportPath: resolve(flags.get('--report')!),
        topologyOnly: flags.get('--measurement-window') === 'topology-approved',
        safetyOnly: ['safety-approved', 'publication-diagnostic-approved'].includes(
            flags.get('--measurement-window')!,
        ),
        diagnosticOnly: flags.get('--measurement-window') === 'publication-diagnostic-approved',
    }
}

/** Declare three alternating A/B2 pairs for each frozen profile without accepting a shorter workload. */
export function monitoringApplianceWindows(): MonitoringApplianceWindow[] {
    return (['mixed', 'all-3s-timeout'] as const).flatMap((profile) =>
        Array.from({ length: contract.samples }, (_, pair) =>
            (pair % 2 === 0 ? (['A', 'B2'] as const) : (['B2', 'A'] as const)).map((design) => ({
                profile,
                pair: pair + 1,
                design,
                windowId: profile + '-' + (pair + 1) + '-' + design.toLowerCase(),
            })),
        ).flat(),
    )
}

/** Hash explicit experiment and isolation sources separately from the baseline; exclude Rust build products. */
export async function monitoringApplianceOverlay(root = experimentRoot) {
    const paths = [
        'scripts/runtime-reliability/harness.ts',
        'scripts/runtime-reliability/Types/control.types.ts',
        'scripts/runtime-scale/monitoring-b2-appliance.ts',
        'scripts/runtime-scale/monitoring-b2-window-cap.ts',
        'scripts/runtime-scale/Types/monitoring-b2-window-cap.types.ts',
        'scripts/runtime-scale/monitoring-whole-appliance.window.ts',
        'scripts/runtime-scale/monitoring-b2-authority.ts',
        'scripts/runtime-scale/monitoring-b2-isolation.ts',
        'scripts/runtime-scale/monitoring-b2-replay.ts',
        'scripts/runtime-scale/monitoring-b2-bundled.ts',
        'scripts/runtime-scale/monitoring-b2-build.ts',
        'scripts/runtime-scale/monitoring-b2-boot.ts',
        'scripts/runtime-scale/monitoring-b2-publisher.ts',
        'scripts/runtime-scale/monitoring-b2-publication-case.ts',
        'scripts/runtime-scale/monitoring-whole-appliance.image.ts',
        'scripts/runtime-scale/monitoring-whole-appliance.faults.ts',
        'scripts/runtime-scale/monitoring-b2-recovery.ts',
        'scripts/runtime-scale/Types/monitoring-b2-recovery.types.ts',
        'scripts/runtime-scale/monitoring-b2-web-replay.ts',
        'scripts/runtime-scale/monitoring-b2-web.ts',
        'scripts/runtime-scale/monitoring-b2-web-lifecycle.ts',
        'scripts/runtime-scale/monitoring-b2-fixture-storage.ts',
        'scripts/runtime-scale/monitoring-b2-capabilities.ts',
        'scripts/runtime-scale/monitoring-b2-probe-broker.ts',
        'scripts/runtime-scale/monitoring-b2-capability-proof.ts',
        'scripts/runtime-scale/monitoring-b2.validation.ts',
        'scripts/runtime-scale/Types/monitoring-b2.types.ts',
        'scripts/runtime-scale/monitoring-evaluation.ts',
        'scripts/runtime-scale/Types/monitoring-evaluation.types.ts',
        'scripts/runtime-scale/monitoring-whole-appliance.ts',
        'scripts/runtime-scale/monitoring-whole-appliance.metrics.ts',
        'scripts/runtime-scale/Types/monitoring-whole-appliance.types.ts',
    ]
    /** Include ordinary nested crate inputs only, refusing links and skipping the target build directory. */
    async function add(directory: string) {
        for (const entry of (
            await readdir(join(root, directory), { withFileTypes: true })
        ).toSorted((a, b) => a.name.localeCompare(b.name))) {
            const relative = directory + '/' + entry.name
            if (entry.isDirectory()) {
                if (entry.name !== 'target') await add(relative)
            } else if (entry.isFile()) paths.push(relative)
            else throw new Error('B2 overlay must contain ordinary source files')
        }
    }
    await add('scripts/runtime-scale/monitoring-b2')
    const hash = createHash('sha256')
    for (const path of paths.toSorted()) {
        hash.update(path)
        hash.update(await readFile(join(root, path)))
    }
    return { paths: paths.toSorted(), sha256: hash.digest('hex') }
}

/** Only the isolated root observer may read the private experiment control capability. */
export async function readMonitoringApplianceControlCapability() {
    const { lstat, readFile: readCapability } = await import('node:fs/promises')
    if (process.platform !== 'linux' || process.getuid?.() !== 0)
        throw new Error('B2 control observer must be isolated root')
    const directory = await lstat('/tmp/monitoring-b2')
    const file = '/tmp/monitoring-b2/control.cap'
    const metadata = await lstat(file)
    if (
        !directory.isDirectory() ||
        directory.uid !== 0 ||
        (directory.mode & 0o022) !== 0 ||
        !metadata.isFile() ||
        metadata.uid !== 0 ||
        metadata.gid !== 0 ||
        (metadata.mode & 0o777) !== 0o600 ||
        metadata.nlink !== 1 ||
        metadata.size !== 64
    )
        throw new Error('B2 private control capability scope mismatch')
    const capability = await readCapability(file, 'utf8')
    if (!/^[a-f0-9]{64}$/u.test(capability))
        throw new Error('B2 private control capability malformed')
    return capability
}

/** Send a bounded loopback fixture request from its root observer without exporting the management key. */
export async function monitoringApplianceRequest(
    context: ReliabilityContext,
    path: string,
    body?: unknown,
    timeoutMs = 30_000,
): Promise<unknown> {
    if (!/^\/[a-z-]+$/u.test(path)) throw new Error('Invalid B2 fixture endpoint')
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > 100_000)
        throw new Error('Invalid bounded B2 request deadline')
    const source = `const capability=await (${readMonitoringApplianceControlCapability.toString()})();const input=JSON.parse(await Bun.stdin.text());const r=await fetch('http://127.0.0.1:9020'+input.path,{method:input.body===undefined?'GET':'POST',headers:{authorization:'Bearer '+capability,'content-type':'application/json'},body:input.body===undefined?undefined:JSON.stringify(input.body),signal:AbortSignal.timeout(${timeoutMs})});if(!r.ok)throw new Error('B2 fixture rejected request');process.stdout.write(await r.text());`
    return JSON.parse(
        await context.docker(
            ['exec', '-i', context.container, 'bun', '--no-env-file', '-e', source],
            {
                stdin: JSON.stringify({ path, body }),
                timeoutMs: timeoutMs + 5000,
            },
        ),
    ) as unknown
}

/** Launch only the owned private observer/control process with explicit appliance fixture configuration. */
export async function startMonitoringApplianceControl(context: ReliabilityContext) {
    await context.docker([
        'exec',
        '--detach',
        '--workdir',
        containerRoot,
        '--env',
        'RENTNERPROXY_RELIABILITY_ISOLATED=' + context.runId,
        '--env',
        'RENTNERPROXY_MONITORING_B2=1',
        '--env',
        'B2_APPLIANCE_FIXTURE=' + context.runId,
        '--env',
        'APP_ENCRYPTION_KEY_FILE=/run/rentnerproxy/app-key/value',
        '--env',
        'DATABASE_URL_FILE=/run/rentnerproxy/database-url/value',
        '--env',
        'VALKEY_URL=redis://127.0.0.1:6379',
        '--env',
        'NODE_ENV=production',
        '--env',
        'RENTNERPROXY_CONTROLLER_URL=http://127.0.0.1:8081',
        '--env',
        'RENTNERPROXY_CONTROLLER_TOKEN_FILE=/run/rentnerproxy/controller-token/value',
        context.container,
        'bun',
        '--no-env-file',
        containerRoot + '/monitoring-b2-appliance.js',
        context.runId,
    ])
}

const windowClockSchema = z.strictObject({
    originMs: z.number().int().positive(),
    endMs: z.number().int().positive(),
    originMonotonicMs: z.number().finite().nonnegative(),
    endMonotonicMs: z.number().finite().nonnegative(),
})
const windowArmingSchema = z.strictObject({
    armedAtMs: z.number().int().positive(),
    armedMonotonicMs: z.number().finite().nonnegative(),
})
const finiteWorkloadSchema = z.strictObject({
    runId: z.string().regex(/^[a-f0-9]{12}$/u),
    windowId: z.string(),
    policySetSha256: z.string().regex(/^[a-f0-9]{64}$/u),
    policies: z.literal(1000),
    installed: z.literal(true),
    originMs: z.number().int().positive(),
    endMs: z.number().int().positive(),
    deferredMs: z.number().int().positive(),
})

const trafficCaptureSchema = z.strictObject({
    start: z.looseObject({
        originMs: z.number().int().positive(),
        finiteWorkload: finiteWorkloadSchema,
        nativeQualification: z.looseObject({
            windowId: z.string(),
            native: z
                .object({ pid: z.number().int().positive(), startTicks: z.string() })
                .nullable(),
            nativeStartedAtMs: z.number().int().nonnegative(),
        }),
    }),
    clock: windowClockSchema,
    resourceArming: windowArmingSchema,
    trafficArming: windowArmingSchema,
    resources: z.array(monitoringApplianceResourceSchema),
    traffic: z.array(
        monitoringApplianceTrafficSchema.extend({
            kind: z.enum(['ssr', 'proxy']),
            atMs: z.number().int().nonnegative(),
        }),
    ),
    report: monitoringApplianceReportSchema,
    failures: z.array(z.string()),
})
const webProofSchema = z.looseObject({
    design: z.enum(['A', 'B2']),
    chunk: z.string(),
    chunkSha256: z.string().regex(/^[a-f0-9]{64}$/u),
    originalStartCalls: z.number().int(),
    suppressedStartCalls: z.number().int(),
})

/** Require the exact source, binary and Cargo inputs plus declared release/offline immutable-builder provenance. */
export function validateMonitoringApplianceNativeManifest(
    value: unknown,
    binarySha256: string,
    rustSourceSha256: string,
    cargoLockSha256: string,
    cargoManifestSha256: string,
) {
    const manifest = z
        .strictObject({
            rustSourceSha256: z.string().regex(/^[a-f0-9]{64}$/u),
            binarySha256: z.string().regex(/^[a-f0-9]{64}$/u),
            compiler: z.string().regex(/^rustc \d+\.\d+\.\d+ \([a-f0-9]+ \d{4}-\d{2}-\d{2}\)$/u),
            target: z.enum(['x86_64-unknown-linux-gnu', 'aarch64-unknown-linux-gnu']),
            builderImage: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
            builderReference: z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9._/:@-]{0,255}$/u),
            compilerVerboseSha256: z.string().regex(/^[a-f0-9]{64}$/u),
            cargoLockSha256: z.string().regex(/^[a-f0-9]{64}$/u),
            cargoManifestSha256: z.string().regex(/^[a-f0-9]{64}$/u),
            profile: z.literal('release'),
            offline: z.literal(true),
            network: z.literal('none'),
            registry: z.literal('read-only-local-cargo-cache'),
        })
        .parse(value)
    if (
        manifest.binarySha256 !== binarySha256 ||
        manifest.rustSourceSha256 !== rustSourceSha256 ||
        manifest.cargoLockSha256 !== cargoLockSha256 ||
        manifest.cargoManifestSha256 !== cargoManifestSha256
    )
        throw new Error('B2 native build source or binary manifest mismatch')
    return manifest
}

/** Collect one uninterrupted local STOP interval; never resume a reused or vanished PID. */
export async function collectMonitoringApplianceContinuity(
    dependencies: MonitoringApplianceContinuityDependencies,
    stopMs: number,
): Promise<MonitoringApplianceContinuityCapture> {
    if (!Number.isFinite(stopMs) || stopMs < 0) throw new Error('Invalid B2 STOP interval')
    const same = (value: MonitoringApplianceStoppedIdentity | null) =>
        value?.pid === dependencies.owner.pid && value.startTicks === dependencies.owner.startTicks
    const read = async () => {
        const value = await dependencies.readOwner()
        if (!value || !same(value)) throw new Error('B2 continuity web identity changed')
        return value
    }
    const before = await dependencies.report()
    await read()
    let stopped!: MonitoringApplianceStoppedIdentity
    let confirmed!: MonitoringApplianceStoppedIdentity
    let afterReport!: MonitoringApplianceStoppedIdentity
    let after!: MonitoringApplianceReport
    let stoppedMonotonicMs = 0,
        confirmedMonotonicMs = 0,
        afterReportMonotonicMs = 0
    let resumed!: MonitoringApplianceContinuityCapture['resumed']
    const resume = async (): Promise<MonitoringApplianceContinuityCapture['resumed']> => {
        const beforeResume = await read()
        // A failed STOP acknowledgement may still have delivered STOP. Always try
        // CONT for this exact owner; disappearance before delivery is an error.
        await dependencies.signal('SIGCONT')
        const deadline = dependencies.monotonic() + 3000
        let observed = await dependencies.readOwner()
        while (
            same(observed) &&
            observed &&
            ['T', 't'].includes(observed.state) &&
            dependencies.monotonic() < deadline
        ) {
            await dependencies.sleep(5)
            observed = await dependencies.readOwner()
        }
        if (same(observed) && observed && ['T', 't'].includes(observed.state))
            throw new Error('B2 web CONT was not confirmed')
        return {
            delivered: true,
            before: beforeResume,
            after: observed,
            outcome: same(observed) && observed?.state !== 'Z' ? 'resumed' : 'exited',
        }
    }
    try {
        await dependencies.signal('SIGSTOP')
        const stopDeadline = dependencies.monotonic() + 3000
        for (;;) {
            stopped = await read()
            if (['T', 't'].includes(stopped.state)) break
            if (dependencies.monotonic() >= stopDeadline)
                throw new Error('B2 web STOP was not confirmed')
            await dependencies.sleep(5)
        }
        stoppedMonotonicMs = dependencies.monotonic()
        const end = stoppedMonotonicMs + stopMs
        while (dependencies.monotonic() < end)
            await dependencies.sleep(end - dependencies.monotonic())
        confirmed = await read()
        confirmedMonotonicMs = dependencies.monotonic()
        if (!['T', 't'].includes(confirmed.state)) throw new Error('B2 web did not remain stopped')
        after = await dependencies.report()
        afterReport = await read()
        afterReportMonotonicMs = dependencies.monotonic()
        if (!['T', 't'].includes(afterReport.state))
            throw new Error('B2 stopped report lacks a halted owner')
    } finally {
        resumed = await resume()
    }
    return {
        before,
        after,
        stopped,
        confirmed,
        afterReport,
        stoppedMonotonicMs,
        confirmedMonotonicMs,
        afterReportMonotonicMs,
        resumed,
    }
}

/** Validate local evidence on the host without weakening fresh-admission or identity gates. */
export function proveMonitoringApplianceContinuity(value: unknown) {
    const identity = z.strictObject({
        pid: z.number().int().positive(),
        startTicks: z.string().regex(/^\d+$/u),
        state: z.string().min(1),
        atMs: z.number().int().nonnegative(),
    })
    const capture = z
        .strictObject({
            before: monitoringApplianceReportSchema,
            after: monitoringApplianceReportSchema,
            stopped: identity,
            confirmed: identity,
            afterReport: identity,
            stoppedMonotonicMs: z.number().finite().nonnegative(),
            confirmedMonotonicMs: z.number().finite().nonnegative(),
            afterReportMonotonicMs: z.number().finite().nonnegative(),
            resumed: z.strictObject({
                delivered: z.literal(true),
                before: identity,
                after: identity.nullable(),
                outcome: z.enum(['resumed', 'exited']),
            }),
        })
        .parse(value)
    const { before, after, stopped, confirmed, afterReport, resumed } = capture
    const failures: string[] = []
    const same = (owner: { pid: number; startTicks: string }) =>
        owner.pid === stopped.pid && owner.startTicks === stopped.startTicks
    if (
        ![confirmed, afterReport, resumed.before].every(
            (owner) => same(owner) && ['T', 't'].includes(owner.state),
        ) ||
        !['T', 't'].includes(stopped.state) ||
        confirmed.atMs - stopped.atMs < contract.confirmedBunStopMs ||
        capture.confirmedMonotonicMs - capture.stoppedMonotonicMs < contract.confirmedBunStopMs ||
        capture.afterReportMonotonicMs < capture.confirmedMonotonicMs ||
        after.endMs < confirmed.atMs ||
        after.endMs > afterReport.atMs ||
        before.endMs > stopped.atMs ||
        afterReport.atMs > resumed.before.atMs ||
        (resumed.outcome === 'resumed' &&
            (!resumed.after ||
                !same(resumed.after) ||
                ['T', 't', 'Z'].includes(resumed.after.state))) ||
        (resumed.outcome === 'exited' &&
            resumed.after &&
            same(resumed.after) &&
            resumed.after.state !== 'Z')
    )
        failures.push('bun-stop-duration-or-identity-unproven')
    if (
        before.design !== 'B2' ||
        after.design !== 'B2' ||
        before.originMs !== after.originMs ||
        before.windowId !== after.windowId ||
        !before.wakeProof ||
        !after.wakeProof ||
        before.wakeProof.runId !== after.wakeProof.runId ||
        before.wakeProof.windowId !== before.windowId ||
        after.wakeProof.windowId !== after.windowId ||
        before.wakeProof.originMs !== before.originMs ||
        after.wakeProof.originMs !== after.originMs ||
        !same(before.wakeProof.webIdentity) ||
        !same(after.wakeProof.webIdentity) ||
        !before.nativeIdentity ||
        !after.nativeIdentity ||
        before.nativeIdentity.pid !== after.nativeIdentity.pid ||
        before.nativeIdentity.startTicks !== after.nativeIdentity.startTicks ||
        before.controllerIdentity.pid !== after.controllerIdentity.pid ||
        before.controllerIdentity.startTicks !== after.controllerIdentity.startTicks
    )
        failures.push('native-controller-or-window-identity-changed')
    const starts = after.starts.filter(
        (start) =>
            start.durable &&
            start.admittedAtMs >= stopped.atMs &&
            start.durableAtMs >= stopped.atMs &&
            start.durableAtMs >= start.admittedAtMs &&
            start.atMs >= stopped.atMs &&
            start.admittedAtMs <= after.endMs &&
            start.durableAtMs <= after.endMs,
    )
    const distinctAdmissions = new Set(starts.map((start) => start.policyId)).size
    const hosts = new Map(after.policies.map((policy) => [policy.policyId, policy.hostId]))
    const actualNativeHeads = new Set(
        starts
            .filter((start) =>
                after.heads.some(
                    (head) =>
                        head.hostId === hosts.get(start.policyId) &&
                        head.atMs >= Math.max(start.admittedAtMs, start.durableAtMs) &&
                        head.atMs <= after.endMs,
                ),
            )
            .map((start) => start.policyId),
    ).size
    if (
        distinctAdmissions < contract.distinctDurableStartsAfterStop ||
        actualNativeHeads < contract.distinctDurableStartsAfterStop
    )
        failures.push('fresh-admission-and-durable-native-start-threshold')
    return {
        failures,
        confirmedBunStopMs: confirmed.atMs - stopped.atMs,
        distinctAdmissions,
        distinctDurableStartsAfterStop: distinctAdmissions,
        rawDurableAttempts: starts.length,
        distinctPoliciesWithDurableStarts: distinctAdmissions,
        actualNativeHeads,
        retainedNativeIdentity: !failures.includes('native-controller-or-window-identity-changed'),
        stopped,
        confirmed,
        afterReport,
        resumed,
        before,
        after,
    }
}

/** Serialize the pure collector into one root container process; the capability never leaves it. */
export function monitoringApplianceContinuityProbe(owner: { pid: number; startTicks: string }) {
    if (!Number.isSafeInteger(owner.pid) || owner.pid < 2 || !/^\d+$/u.test(owner.startTicks))
        throw new Error('Invalid B2 web identity')
    return `const owner=${JSON.stringify({ pid: owner.pid, startTicks: owner.startTicks })};const capability=await (${readMonitoringApplianceControlCapability.toString()})();
const readOwner=async()=>{try{const stat=await Bun.file('/proc/'+owner.pid+'/stat').text();const fields=stat.slice(stat.lastIndexOf(')')+2).trim().split(/\\s+/);return {pid:owner.pid,startTicks:fields[19],state:fields[0],atMs:Date.now()}}catch(error){if(error.code==='ENOENT'||error.code==='ESRCH')return null;throw new Error('B2 web identity read failed')}};
const report=async()=>{const response=await fetch('http://127.0.0.1:9020/report',{headers:{authorization:'Bearer '+capability},signal:AbortSignal.timeout(30000)});if(!response.ok)throw new Error('B2 continuity report unavailable');return await response.json()};
const signal=async signal=>{const value=await readOwner();if(!value||value.startTicks!==owner.startTicks)throw new Error('B2 continuity web identity changed');process.kill(owner.pid,signal)};
const result=await (${collectMonitoringApplianceContinuity.toString()})({owner,readOwner,report,signal,sleep:Bun.sleep,monotonic:()=>performance.now()},${contract.confirmedBunStopMs});process.stdout.write(JSON.stringify(result));`
}

/** Attempt every owned cleanup before persisting a verdict; a cleanup failure makes the evidence incomplete. */
export async function finalizeMonitoringApplianceReport(
    report: MonitoringApplianceEvaluationReport,
    reportPath: string,
    cleanup: MonitoringApplianceCleanupDependencies,
) {
    for (const [failure, remove] of [
        ['fixture-shutdown-unavailable', cleanup.shutdown],
        ['owned-appliance-cleanup-failed', cleanup.appliance],
        ['owned-prototype-image-cleanup-failed', cleanup.prototypeImage],
    ] as const) {
        try {
            await remove()
        } catch {
            report.failures.push(failure)
            report.completeness = 'insufficient'
            report.recommendation = 'incomplete'
        }
    }
    await mkdir(dirname(reportPath), { recursive: true })
    await writeFile(reportPath, JSON.stringify(report, null, 2) + '\n', { mode: 0o600 })
}

/** Run source-matched internal-network fixtures; incomplete topology, fault or resource proof fails closed. */
export async function runMonitoringAppliance(options: MonitoringApplianceOptions) {
    const { baselineSha, baselineImageIdentity } = options
    const imported: typeof import('../runtime-reliability/harness.ts') = await import(
        pathToFileURL(join(options.baselineRoot, 'scripts/runtime-reliability/harness.ts')).href
    )
    if (
        typeof imported.reliabilityNetworkArguments !== 'function' ||
        imported.reliabilityNetworkArguments('monitoring-isolation-check', true).join('\0') !==
            'network\0create\0--internal\0monitoring-isolation-check'
    )
        throw new Error('B2 baseline harness must support verified internal network isolation')
    const baselineHarnessSha256 = createHash('sha256')
        .update(
            await readFile(join(options.baselineRoot, 'scripts/runtime-reliability/harness.ts')),
        )
        .digest('hex')
    const recovery: typeof import('../runtime-reliability/component-recovery.ts') = await import(
        pathToFileURL(
            join(options.baselineRoot, 'scripts/runtime-reliability/component-recovery.ts'),
        ).href
    )
    const dependencies: {
        recoveryDependencies: (context: ReliabilityContext) => ComponentRecoveryDependencies
    } = await import(
        pathToFileURL(join(options.baselineRoot, 'scripts/runtime-reliability/run.ts')).href
    )
    const overlay = await monitoringApplianceOverlay()
    const nativeBytes = await readFile(options.nativeBinary)
    const nativeBinarySha256 = createHash('sha256').update(nativeBytes).digest('hex')
    const nativeBuildManifest = validateMonitoringApplianceNativeManifest(
        JSON.parse(await readFile(join(dirname(options.nativeBinary), 'manifest.json'), 'utf8')),
        nativeBinarySha256,
        await monitoringB2RustSourceHash(),
        createHash('sha256')
            .update(
                await readFile(
                    join(experimentRoot, 'scripts/runtime-scale/monitoring-b2/Cargo.lock'),
                ),
            )
            .digest('hex'),
        createHash('sha256')
            .update(
                await readFile(
                    join(experimentRoot, 'scripts/runtime-scale/monitoring-b2/Cargo.toml'),
                ),
            )
            .digest('hex'),
    )
    if (
        nativeBytes.length < 20 ||
        nativeBytes.subarray(0, 4).toString('hex') !== '7f454c46' ||
        nativeBytes[4] !== 2 ||
        nativeBytes[5] !== 1 ||
        nativeBytes.readUInt16LE(18) !==
            (nativeBuildManifest.target === 'x86_64-unknown-linux-gnu' ? 62 : 183)
    )
        throw new Error('B2 native artifact is not the declared Linux ELF target')
    const prototypeImage = 'rentnerproxy-b2-prototype:' + randomBytes(6).toString('hex')
    const harness = await imported.createHarness(
        {
            source: 'current',
            image: prototypeImage,
            durationSeconds: 120,
            iterations: 1,
            concurrency: 1,
            captureResources: true,
            seed: 402,
            internalNetwork: true,
        },
        [],
    )
    const context = harness.context
    const report: MonitoringApplianceEvaluationReport = {
        header: {
            baselineSha,
            baselineHarnessSha256,
            expectedBaseImageIdentity: baselineImageIdentity,
            image: options.image,
            experimentSha256: overlay.sha256,
            nativeBinarySha256,
            nativeBuildManifest,
            topologyOnly: options.topologyOnly,
            safetyOnly: options.safetyOnly,
            diagnosticOnly: options.diagnosticOnly,
            contract,
            rssMetric: 'sum of /proc/*/status VmRSS; shared pages may appear in multiple processes',
            memoryMetric: 'cgroup memory.current and working set reported separately',
            databaseConnectionMetric:
                'all pg_stat_activity client backends, including the one-connection observer',
            cpuTimeBasis:
                'cgroup usage_usec delta divided by the collector monotonic clock interval',
            observer:
                'identical root fixture and one combined SQL/resource/traffic/progress observer inside appliance cgroup',
            networkIsolation: 'owned-internal-docker-network',
            commitTimeSource: 'first-visible-observation-upper-bound',
            dueToStartSource:
                'actual native HEAD receipt upper bound; B2 durable admission independently recorded',
        },
        windows: [],
        faultEvidence: null,
        completeness: 'insufficient',
        recommendation: 'incomplete',
        failures: [],
    }
    const pairInputs: MonitoringAppliancePairInput[] = []
    /** Give each fault a fresh sealed appliance and preserve cleanup failures as incomplete evidence. */
    async function runSafetyCase(
        name: (typeof monitoringApplianceFaultCases)[number],
    ): Promise<MonitoringApplianceFaultRecord> {
        const fixture = await imported.createHarness(
            {
                source: 'current',
                image: prototypeImage,
                durationSeconds: 120,
                iterations: 1,
                concurrency: 1,
                captureResources: true,
                seed: 402,
                internalNetwork: true,
            },
            [],
        )
        const current = fixture.context
        const request = (path: string, body?: unknown) =>
            monitoringApplianceRequest(current, path, body)
        const runtime = () => dependencies.recoveryDependencies(current)
        const recover = async (
            role: 'web' | 'controller',
            signal: 'SIGTERM' | 'SIGKILL' = 'SIGTERM',
        ) => {
            const controls = runtime()
            await recovery.recoverReliabilityComponent(
                role,
                signal === 'SIGTERM'
                    ? controls
                    : {
                          ...controls,
                          signal: async (target) => {
                              await current.docker(
                                  [
                                      'exec',
                                      current.container,
                                      'bun',
                                      '--no-env-file',
                                      '-e',
                                      `const s=await Bun.file('/proc/${target.pid}/stat').text();if(s.slice(s.lastIndexOf(')')+2).trim().split(/\\s+/)[19]!==${JSON.stringify(target.startTicks)})throw new Error('B2 fault owner changed');process.kill(${target.pid},'SIGKILL');`,
                                  ],
                                  { timeoutMs: 5000 },
                              )
                          },
                      },
            )
        }
        const startControl = async () => {
            await startMonitoringApplianceControl(current)
            await current.waitFor(
                async () => {
                    await request('/owner-status')
                    return true
                },
                'B2 fault rootcontrol readiness',
                15000,
            )
        }
        let seed = 0
        const prepare = async () => {
            await request('/prepare', {
                design: 'B2',
                profile:
                    name === 'bun-stop' || name === 'fresh-volume-restore'
                        ? 'all-3s-timeout'
                        : 'mixed',
                policies: contract.policies,
                windowId: 'safety-' + name + '-' + ++seed,
            })
            await recover('web')
            const proof = webProofSchema.parse(await request('/web-proof'))
            if (
                proof.design !== 'B2' ||
                proof.originalStartCalls !== 0 ||
                proof.suppressedStartCalls !== 1
            )
                throw new Error('B2 safety scheduler suppression unproven')
        }
        let result: MonitoringApplianceFaultRecord
        try {
            await fixture.setup()
            if (fixture.imageIdentity !== harness.imageIdentity)
                throw new Error('B2 fault image differs from matched window image')
            await current.synced(await current.fixture('prepare'))
            await startControl()
            await prepare()
            const session: MonitoringApplianceFaultSession = {
                context: current,
                request,
                report: async () => monitoringApplianceReportSchema.parse(await request('/report')),
                start: async () => {
                    await request('/start', {})
                },
                recover,
                startControl,
                backupRestore: fixture.backupRestore,
                prepareAfterRebootstrap: prepare,
                continuity: async () => {
                    const snapshot = await runtime().snapshot(),
                        owners = snapshot?.processes.filter((process) => process.role === 'web')
                    if (!owners || owners.length !== 1)
                        throw new Error('B2 STOP requires one original web owner')
                    const owner = owners[0]!
                    const captured = JSON.parse(
                        await current.docker(
                            [
                                'exec',
                                current.container,
                                'bun',
                                '--no-env-file',
                                '-e',
                                monitoringApplianceContinuityProbe(owner),
                            ],
                            { timeoutMs: contract.confirmedBunStopMs + 75000 },
                        ),
                    )
                    const continuity = proveMonitoringApplianceContinuity(captured)
                    // Readiness must follow the real supervisor outcome, never an
                    // invented live old PID after successful CONT with pending TERM.
                    let recovered: Awaited<ReturnType<ComponentRecoveryDependencies['snapshot']>>
                    const controls = runtime()
                    await current.waitFor(
                        async () => {
                            const candidate = await controls.snapshot()
                            if (!candidate) return false
                            if (!candidate.running || candidate.startedAt !== snapshot!.startedAt)
                                throw new Error('B2 STOP changed appliance ownership')
                            for (const role of ['controller', 'caddy'] as const) {
                                const expected = snapshot!.processes.filter(
                                    (process) => process.role === role,
                                )
                                const actual = candidate.processes.filter(
                                    (process) => process.role === role,
                                )
                                if (
                                    expected.length !== 1 ||
                                    actual.length !== 1 ||
                                    actual.at(0)!.pid !== expected.at(0)!.pid ||
                                    actual.at(0)!.startTicks !== expected.at(0)!.startTicks
                                )
                                    throw new Error('B2 STOP changed native supervisor ownership')
                            }
                            const web = candidate.processes.filter(
                                (process) => process.role === 'web',
                            )
                            if (web.length > 1)
                                throw new Error('B2 STOP retained parallel web writers')
                            if (
                                web.length !== 1 ||
                                ['T', 't', 'Z'].includes(web[0]!.state) ||
                                !(await controls.ready())
                            )
                                return false
                            const confirmed = await controls.snapshot()
                            const writers = confirmed?.processes.filter(
                                (process) => process.role === 'web',
                            )
                            if (writers && writers.length > 1)
                                throw new Error('B2 STOP retained parallel web writers')
                            if (
                                !confirmed?.running ||
                                confirmed.startedAt !== candidate.startedAt ||
                                writers?.length !== 1 ||
                                writers[0]!.pid !== web[0]!.pid ||
                                writers[0]!.startTicks !== web[0]!.startTicks
                            )
                                return false
                            for (const role of ['controller', 'caddy'] as const) {
                                const expected = candidate.processes.filter(
                                    (process) => process.role === role,
                                )
                                const actual = confirmed.processes.filter(
                                    (process) => process.role === role,
                                )
                                if (
                                    actual.length !== 1 ||
                                    actual.at(0)!.pid !== expected.at(0)!.pid ||
                                    actual.at(0)!.startTicks !== expected.at(0)!.startTicks
                                )
                                    throw new Error('B2 STOP readiness changed native ownership')
                            }
                            recovered = confirmed
                            return true
                        },
                        'B2 STOP supervisor recovery and single-writer readiness',
                        60000,
                    )
                    return { ...continuity, webRecovery: recovered }
                },
            }
            result = await runMonitoringApplianceFaultCase(session, name)
        } catch {
            result = {
                name,
                state: 'incomplete',
                evidence: null,
                failures: ['fault-prototype-or-infrastructure-proof-unavailable'],
            }
        }
        try {
            await request('/shutdown', {})
        } catch {
            result.failures.push('fault-control-shutdown-unavailable')
            result.state = 'incomplete'
        }
        try {
            await fixture.cleanup()
        } catch {
            result.failures.push('fault-owned-cleanup-failed')
            result.state = 'incomplete'
        }
        return result
    }
    let stage = 'baseline-provenance'
    try {
        const experimentCommitSha = z
            .string()
            .regex(/^[a-f0-9]{40}$/u)
            .parse(await context.command(['git', '-C', experimentRoot, 'rev-parse', 'HEAD']))
        const experimentTreeSha = z
            .string()
            .regex(/^[a-f0-9]{40}$/u)
            .parse(await context.command(['git', '-C', experimentRoot, 'rev-parse', 'HEAD^{tree}']))
        await context.command([
            'git',
            '-C',
            experimentRoot,
            'ls-files',
            '--error-unmatch',
            '--',
            ...overlay.paths,
        ])
        const dirtyOverlay = await context.command([
            'git',
            '-C',
            experimentRoot,
            'status',
            '--porcelain=v1',
            '--untracked-files=all',
            '--',
            ...overlay.paths,
        ])
        if (dirtyOverlay) throw new Error('B2 overlay requires its committed source snapshot')
        report.header.experimentCommitSha = experimentCommitSha
        report.header.experimentTreeSha = experimentTreeSha
        report.header.overlaySourceClean = true
        const head = await context.command(['git', '-C', options.baselineRoot, 'rev-parse', 'HEAD'])
        const dirty = await context.command([
            'git',
            '-C',
            options.baselineRoot,
            'status',
            '--porcelain',
            '--untracked-files=no',
        ])
        if (head !== baselineSha || dirty)
            throw new Error('B2 requires the clean pinned baseline worktree')
        stage = 'prototype-image-build'
        report.header.prototypeImage = await buildMonitoringPrototypeImage(
            context,
            options,
            overlay,
            nativeBinarySha256,
            prototypeImage,
        )
        stage = 'harness-setup'
        await harness.setup()
        if (harness.targetSha !== baselineSha) throw new Error('B2 harness baseline mismatch')
        report.header.overlayImageIdentity = harness.imageIdentity
        const imagePlatform = z
            .enum(['linux/amd64', 'linux/arm64'])
            .parse(
                await context.docker([
                    'image',
                    'inspect',
                    '--format',
                    '{{.Os}}/{{.Architecture}}',
                    options.image,
                ]),
            )
        if (
            imagePlatform !==
            (nativeBuildManifest.target === 'x86_64-unknown-linux-gnu'
                ? 'linux/amd64'
                : 'linux/arm64')
        )
            throw new Error('B2 native binary target differs from appliance image platform')
        report.header.imagePlatform = imagePlatform
        stage = 'original-fixture-prepare'
        await context.synced(await context.fixture('prepare'))
        stage = 'control-start'
        await startMonitoringApplianceControl(context)
        stage = 'control-readiness'
        await context.waitFor(
            async () => {
                await monitoringApplianceRequest(context, '/report')
                return true
            },
            'B2 fixture ready',
            15000,
        )
        const topology = []
        let originalChunk: string | undefined
        let originalChangeChunk: string | undefined
        for (const design of ['A', 'B2'] as const) {
            stage = 'topology:' + design
            await monitoringApplianceRequest(context, '/prepare', {
                design,
                profile: 'mixed',
                policies: contract.policies,
                windowId: 'topology-' + design.toLowerCase(),
            })
            await recovery.recoverReliabilityComponent(
                'web',
                dependencies.recoveryDependencies(context),
            )
            const proof = webProofSchema.parse(
                await monitoringApplianceRequest(context, '/web-proof'),
            )
            const management = monitoringApplianceTrafficSchema.parse(
                await monitoringApplianceRequest(context, '/management'),
            )
            const traffic = monitoringApplianceTrafficSchema.parse(
                await monitoringApplianceRequest(context, '/traffic'),
            )
            if (
                proof.design !== design ||
                proof.originalStartCalls !== (design === 'A' ? 1 : 0) ||
                proof.suppressedStartCalls !== (design === 'B2' ? 1 : 0) ||
                (originalChunk && originalChunk !== proof.chunkSha256) ||
                management.status !== 200 ||
                !management.validBody ||
                traffic.status !== 200 ||
                !traffic.validBody
            )
                throw new Error('B2 common-image topology qualification failed')
            originalChunk = proof.chunkSha256
            const entry: Record<string, unknown> = {
                design,
                proof,
                management,
                traffic,
                capacityEvidence: false,
            }
            topology.push(entry)
            report.header.topology = topology
            if (options.diagnosticOnly) {
                stage = 'common-wake:' + design
                const start = z
                    .object({ originMs: z.number().int().positive() })
                    .parse(await monitoringApplianceRequest(context, '/start', {}))
                const snapshot = await dependencies.recoveryDependencies(context).snapshot()
                const owners = snapshot?.processes.filter((process) => process.role === 'web')
                if (owners?.length !== 1) throw new Error('Common wake requires one live web owner')
                let observed = monitoringApplianceReportSchema.parse(
                    await monitoringApplianceRequest(context, '/report'),
                )
                await context.waitFor(
                    async () => {
                        observed = monitoringApplianceReportSchema.parse(
                            await monitoringApplianceRequest(context, '/report'),
                        )
                        return (
                            new Set(
                                observed.heads
                                    .filter((requestHead) => requestHead.atMs >= start.originMs)
                                    .map((requestHead) => requestHead.hostId),
                            ).size >= 16
                        )
                    },
                    'common wake real native HEAD evidence',
                    30000,
                )
                if (
                    observed.originMs !== start.originMs ||
                    observed.windowId !== 'topology-' + design.toLowerCase() ||
                    assessMonitoringApplianceWake(observed, {
                        runId: context.runId,
                        webIdentity: owners[0]!,
                    }).length ||
                    (originalChangeChunk &&
                        originalChangeChunk !== observed.wakeProof!.originalChangeChunkSha256)
                )
                    throw new Error('Common original web wake unproven')
                originalChangeChunk = observed.wakeProof!.originalChangeChunkSha256
                entry.wakeProof = observed.wakeProof
                entry.actualDistinctNativeHosts = new Set(
                    observed.heads.map((requestHead) => requestHead.hostId),
                ).size
                entry.firstHeadLagMs = Math.min(
                    ...observed.heads.map((requestHead) => requestHead.atMs - start.originMs),
                )
            }
            stage = 'topology:' + design + ':stop'
            await monitoringApplianceRequest(context, '/stop', {})
            stage = 'topology:' + design + ':drain'
            await context.waitFor(
                async () =>
                    monitoringApplianceReportSchema.parse(
                        await monitoringApplianceRequest(context, '/report'),
                    ).active === 0,
                'native HEAD drain before fixture reseed',
                10000,
            )
        }
        report.header.topology = topology
        if (!options.topologyOnly) {
            const cases: MonitoringApplianceFaultRecord[] = []
            for (const name of options.diagnosticOnly
                ? (['automatic-publication'] as const)
                : monitoringApplianceFaultCases) {
                stage = 'safety:' + name
                const result = await runSafetyCase(name)
                cases.push(result)
                console.log('B2 safety ' + name + ': ' + result.state)
                if (result.state !== 'passed') break
            }
            const passed =
                cases.length === monitoringApplianceFaultCases.length &&
                cases.every((entry) => entry.state === 'passed')
            report.faultEvidence = {
                cases,
                passed,
                diagnosticOnly: options.diagnosticOnly,
                diagnosticPassed:
                    options.diagnosticOnly && cases.length === 1 && cases[0]!.state === 'passed',
                imageIdentity: harness.imageIdentity,
                experimentSha256: overlay.sha256,
                nativeBinarySha256,
            }
            if (options.diagnosticOnly)
                report.failures.push(
                    ...cases.flatMap((entry) =>
                        entry.failures.map((failure) => entry.name + ':' + failure),
                    ),
                )
            if (!passed && !options.diagnosticOnly)
                throw new Error(
                    'B2 safety matrix incomplete; matched measurements are not admitted',
                )
        }
        for (const window of options.topologyOnly || options.safetyOnly
            ? []
            : monitoringApplianceWindows()) {
            stage = window.windowId + ':prepare'
            await monitoringApplianceRequest(context, '/prepare', {
                ...window,
                pair: undefined,
                policies: contract.policies,
                finiteWindow: true,
            })
            stage = window.windowId + ':web-recovery'
            await recovery.recoverReliabilityComponent(
                'web',
                dependencies.recoveryDependencies(context),
            )
            stage = window.windowId + ':web-proof'
            const proof = webProofSchema.parse(
                await monitoringApplianceRequest(context, '/web-proof'),
            )
            if (
                proof.design !== window.design ||
                proof.originalStartCalls !== (window.design === 'A' ? 1 : 0) ||
                proof.suppressedStartCalls !== (window.design === 'B2' ? 1 : 0) ||
                (originalChunk !== undefined && originalChunk !== proof.chunkSha256)
            )
                throw new Error('B2 scheduler topology/source proof failed')
            originalChunk = proof.chunkSha256
            stage = window.windowId + ':qualify'
            const qualifiedOwner = await monitoringApplianceRequest(context, '/qualify-window', {})
            stage = window.windowId + ':capture'
            const captured = await context.docker(
                [
                    'exec',
                    context.container,
                    'bun',
                    '--no-env-file',
                    '-e',
                    monitoringApplianceWindowProbe(readMonitoringApplianceControlCapability),
                ],
                { timeoutMs: contract.sampleMs + 15000 },
            )
            stage = window.windowId + ':validate'
            const capture = trafficCaptureSchema.parse(JSON.parse(captured))
            const resources = capture.resources
            if (
                capture.report.design !== window.design ||
                capture.report.profile !== window.profile ||
                capture.report.originMs !== capture.start.originMs ||
                capture.clock.originMs !== capture.start.originMs ||
                capture.clock.endMs !== capture.start.originMs + contract.sampleMs
            )
                throw new Error('B2 window identity mismatch')
            const resourceSummary = summarizeMonitoringApplianceResources(resources, {
                clock: capture.clock,
                arming: capture.resourceArming,
            })
            const progress = summarizeMonitoringApplianceProgress(capture.report, capture.traffic)
            const actualCap = finiteWorkloadSchema.parse(capture.report.finiteWorkload)
            const expectedHash = createHash('sha256')
                .update(
                    capture.report.policies
                        .map((policy) => policy.policyId)
                        .toSorted()
                        .join('\n'),
                )
                .digest('hex')
            for (const installed of [actualCap, capture.start.finiteWorkload])
                if (
                    installed.runId !== context.runId ||
                    installed.windowId !== window.windowId ||
                    installed.policySetSha256 !== expectedHash ||
                    installed.originMs !== capture.start.originMs ||
                    installed.endMs !== capture.clock.endMs ||
                    installed.deferredMs !== capture.clock.endMs + 60000
                )
                    throw new Error('B2 finite two-cadence workload scope unproven')
            if (
                capture.trafficArming.armedAtMs >= capture.clock.originMs ||
                capture.trafficArming.armedMonotonicMs >= capture.clock.originMonotonicMs
            )
                throw new Error('B2 traffic observer armed after origin')
            const nativeOwner = capture.start.nativeQualification.native
            if (
                capture.start.nativeQualification.windowId !== window.windowId ||
                (window.design === 'B2' &&
                    (!nativeOwner ||
                        nativeOwner.pid !== capture.report.nativeIdentity?.pid ||
                        nativeOwner.startTicks !== capture.report.nativeIdentity.startTicks)) ||
                (window.design === 'A' && nativeOwner !== null)
            )
                throw new Error('B2 qualified window owner changed')

            const snapshot = await dependencies.recoveryDependencies(context).snapshot()
            const owners = snapshot?.processes.filter((process) => process.role === 'web')
            const failures = [
                ...resourceSummary.failures,
                ...progress.failures,
                ...capture.failures,
                ...(owners?.length === 1
                    ? assessMonitoringApplianceWake(capture.report, {
                          runId: context.runId,
                          webIdentity: owners[0]!,
                      })
                    : ['common-wake-web-owner-unproven']),
            ]
            if (
                capture.report.windowId !== window.windowId ||
                (originalChangeChunk &&
                    originalChangeChunk !== capture.report.wakeProof?.originalChangeChunkSha256)
            )
                failures.push('common-wake-window-or-source-mismatch')
            originalChangeChunk ??= capture.report.wakeProof?.originalChangeChunkSha256
            if (
                resources.some(
                    (sample) =>
                        (sample.roles['monitoring-b2']?.processes ?? 0) !==
                        (window.design === 'B2' ? 1 : 0),
                )
            )
                failures.push('native-scheduler-owner-count')
            report.windows.push({
                ...window,
                webProof: proof,
                qualifiedOwner,
                captureTiming: {
                    clock: capture.clock,
                    resourceArming: capture.resourceArming,
                    trafficArming: capture.trafficArming,
                    start: capture.start,
                },
                resources,
                traffic: capture.traffic,
                progressReport: capture.report,
                resourceSummary,
                progress,
                failures,
            })
            pairInputs.push({
                ...window,
                cpuPercent: resourceSummary.cpuPercent,
                rssMiB: resourceSummary.rssMiB,
                peakDatabaseConnections: resourceSummary.peakDatabaseConnections,
                failures,
            })
            report.failures.push(...failures.map((failure) => window.windowId + ':' + failure))
            stage = window.windowId + ':stop'
            await monitoringApplianceRequest(context, '/stop', {})
            await context.waitFor(
                async () =>
                    monitoringApplianceReportSchema.parse(
                        await monitoringApplianceRequest(context, '/report'),
                    ).active === 0,
                'native HEAD drain before matched window reseed',
                10000,
            )
            console.log(
                'B2 window ' +
                    window.windowId +
                    ': ' +
                    (failures.length ? 'numeric gates failed' : 'numeric gates passed'),
            )
        }
        if (!options.topologyOnly && !options.safetyOnly) {
            const pairSummary = assessMonitoringAppliancePairs(pairInputs)
            report.header.pairedComparison = pairSummary
            report.failures.push(...pairSummary.failures)
            const observationsComplete =
                report.windows.length === 12 &&
                report.windows.every((entry) => {
                    const progress = z
                        .object({
                            coveredSlots: z.number(),
                            unexpectedOutcomes: z.number(),
                            trafficErrors: z.number(),
                        })
                        .parse(entry.progress)
                    return (
                        progress.coveredSlots === contract.policies * 2 &&
                        progress.unexpectedOutcomes === 0 &&
                        progress.trafficErrors === 0
                    )
                })
            report.completeness = observationsComplete ? 'complete' : 'insufficient'
            report.recommendation = observationsComplete
                ? report.failures.length
                    ? 'keep-hybrid'
                    : 'eligible-for-separate-design-review'
                : 'incomplete'
        }
    } catch (error) {
        report.failures.push(stage + ':experiment-or-observation-failed; no raw driver data logged')
        if (error instanceof z.ZodError)
            report.header.schemaFailure = error.issues.map((issue) => ({
                code: issue.code,
                path: issue.path,
            }))
        try {
            const observation = z
                .object({
                    active: z.number().int(),
                    heads: z.array(z.unknown()),
                    observations: z.array(z.unknown()),
                    originMs: z.number().int(),
                    windowId: z.string().regex(/^[a-z0-9-]{1,40}$/u),
                    stopped: z.boolean(),
                })
                .parse(await monitoringApplianceRequest(context, '/report'))
            report.header.failureObservation = {
                active: observation.active,
                heads: observation.heads.length,
                observations: observation.observations.length,
                originMs: observation.originMs,
                windowId: observation.windowId,
                stopped: observation.stopped,
            }
        } catch {
            report.header.failureObservation = 'unavailable'
        }
        try {
            report.header.failureDiagnostic = z
                .object({
                    phase: z.string().regex(/^[a-z0-9_-]{1,80}$/u),
                    diagnostic: z.string().regex(/^[a-zA-Z0-9_-]{1,80}$/u),
                    web: z
                        .object({
                            phase: z.string().regex(/^[a-z0-9_-]{1,80}$/u),
                            diagnostic: z.string().regex(/^[a-zA-Z0-9_-]{1,80}$/u),
                        })
                        .nullable()
                        .optional(),
                })
                .parse(await monitoringApplianceRequest(context, '/diagnostics'))
        } catch {
            report.header.failureDiagnostic = 'unavailable'
        }
    } finally {
        await finalizeMonitoringApplianceReport(report, options.reportPath, {
            shutdown: () => monitoringApplianceRequest(context, '/shutdown', {}),
            appliance: () => harness.cleanup(),
            prototypeImage: () => context.docker(['image', 'rm', prototypeImage]),
        })
    }
    return report
}

if (import.meta.main) {
    const report = await runMonitoringAppliance(
        parseMonitoringApplianceOptions(process.argv.slice(2)),
    )
    console.log(
        'B2 evaluation: ' + report.completeness + '; recommendation=' + report.recommendation,
    )
    if (report.failures.length) process.exitCode = 1
}
