// oxlint-disable no-await-in-loop -- Fixture transitions, draining and native activation are ordered proof boundaries.
import { SQL } from 'bun'
import { Database } from 'bun:sqlite'
import { chmod, chown, lstat, mkdir, readFile, readdir, rename, writeFile } from 'node:fs/promises'
import { z } from 'zod'
import { createMonitoringB2Authority } from './monitoring-b2-authority.ts'
import { monitoringB2DatabaseUrl } from './monitoring-b2-isolation.ts'
import { monitoringLatency } from './monitoring.ts'
import { persistMonitoringB2Owner, readMonitoringB2Owner } from './monitoring-b2-boot.ts'
import type { MonitoringB2RecoveryResult } from './Types/monitoring-b2-recovery.types.ts'
import type { MonitoringB2PublicationResult } from './monitoring-b2-publication-case.ts'
import {
    monitoringB2ClaimSchema,
    monitoringB2OriginalAdmissionSchema,
    monitoringB2AdmissionRejection,
} from './monitoring-b2.validation.ts'
import {
    monitoringB2PolicyTransaction,
    monitoringB2FixtureDiagnostic,
    validateMonitoringB2Role,
    openMonitoringB2OutboxForDrain,
} from './monitoring-b2-fixture-storage.ts'
import { monitoringB2ControlKey, monitoringB2WebKey } from './monitoring-b2-capabilities.ts'
import { randomBytes } from 'node:crypto'
import {
    admitMonitoringB2Probe,
    startMonitoringB2ProbeBroker,
} from './monitoring-b2-probe-broker.ts'
import { runMonitoringB2CapabilityProof } from './monitoring-b2-capability-proof.ts'

const root = '/tmp/monitoring-b2'
const prepareSchema = z.strictObject({
    design: z.enum(['A', 'B2']),
    profile: z.enum(['mixed', 'all-3s-timeout']),
    policies: z.literal(1000),
    windowId: z.string().regex(/^[a-z0-9-]{1,40}$/u),
})
/** Read a PID/start-tick pair; an absent process cannot establish a live owner. */
async function processIdentity(pid: number | undefined) {
    if (!pid) return null
    try {
        const value = await readFile('/proc/' + pid + '/stat', 'utf8')
        const tail = value.slice(value.lastIndexOf(')') + 2).split(' ')
        return { pid, startTicks: tail[19]! }
    } catch {
        return null
    }
}
/** Identify exactly one expected controller Unix role and executable, checking PID reuse around the read. */
async function controllerIdentity() {
    const entries = (await readdir('/proc')).filter((name) => /^\d+$/u.test(name)).slice(0, 1024)
    const matches: { pid: number; startTicks: string }[] = []
    for (const entry of entries)
        try {
            const status = await readFile('/proc/' + entry + '/status', 'utf8')
            if (!/^Uid:\s+10001\s+10001\s+10001\s+10001\s*$/mu.test(status)) continue
            const before = await processIdentity(Number(entry))
            const [comm, command] = await Promise.all([
                readFile('/proc/' + entry + '/comm', 'utf8'),
                readFile('/proc/' + entry + '/cmdline', 'utf8'),
            ])
            const args = command.split('\0').filter(Boolean)
            const after = await processIdentity(Number(entry))
            if (
                before &&
                after?.startTicks === before.startTicks &&
                comm.trim() === 'rentnerproxy-co' &&
                args.length === 1 &&
                args[0] === '/usr/local/bin/rentnerproxy-controller'
            )
                matches.push(before)
        } catch (error) {
            if (!(
                error &&
                typeof error === 'object' &&
                'code' in error &&
                ['ENOENT', 'ESRCH'].includes(String(error.code))
            ))
                throw error
        }
    return matches.length === 1 ? matches[0]! : null
}

/** Count every process of the exclusive native UID, including an orphan outside the current child handle. */
async function nativeIdentities() {
    const identities: { pid: number; startTicks: string }[] = []
    for (const entry of (await readdir('/proc'))
        .filter((name) => /^\d+$/u.test(name))
        .slice(0, 1024)) {
        try {
            const status = await readFile('/proc/' + entry + '/status', 'utf8')
            // UID 20004 is exclusively assigned to this fixture's native owner; count all of it,
            // including an orphan, rather than relying on only the current parent's child handle.
            if (!/^Uid:\s+20004\s+20004\s+20004\s+20004\s*$/mu.test(status)) continue
            const identity = await processIdentity(Number(entry))
            if (identity) identities.push(identity)
        } catch (error) {
            if (!(
                error &&
                typeof error === 'object' &&
                'code' in error &&
                ['ENOENT', 'ESRCH'].includes(String(error.code))
            ))
                throw error
        }
    }
    return identities
}

async function jsonFile(path: string, value: unknown) {
    await writeFile(path + '.next', JSON.stringify(value), { mode: 0o644 })
    await rename(path + '.next', path)
}

/** Only the existing root-owned #397 fixture seal can bootstrap this private disposable control plane. */
async function bootstrap(runId: string) {
    if (
        process.platform !== 'linux' ||
        process.getuid?.() !== 0 ||
        !/^[a-f0-9]{12}$/u.test(runId) ||
        process.env.RENTNERPROXY_RELIABILITY_ISOLATED !== runId
    )
        throw new Error('B2 bootstrap requires the owned Linux reliability fixture')
    const metadata = await lstat('/tmp/rentnerproxy-reliability-fixture.json')
    if (
        !metadata.isFile() ||
        metadata.uid !== 0 ||
        (metadata.mode & 0o077) !== 0 ||
        metadata.size > 4096
    )
        throw new Error('B2 bootstrap seal ownership mismatch')
    const state = z
        .object({
            runId: z.literal(runId),
            session: z.string().min(1).max(256),
            proxyId: z.uuid(),
            policyHostId: z.uuid(),
            redirectId: z.uuid(),
        })
        .parse(JSON.parse(await readFile('/tmp/rentnerproxy-reliability-fixture.json', 'utf8')))
    await mkdir(root, { mode: 0o755, recursive: true })
    await chmod(root, 0o755)
    await mkdir(root + '/state', { mode: 0o2770, recursive: true })
    await chown(root + '/state', 0, 20004)
    await chmod(root + '/state', 0o2770)
    await jsonFile(root + '/mode.json', { runId, design: 'A' })
    return state
}

/** Manage only generated disposable SQL roles through the local postgres owner, exposing fixed SQLSTATE only. */
async function workerRole(action: 'create' | 'drop', role: string, password?: string) {
    validateMonitoringB2Role(action, role, password)
    const statement =
        action === 'create'
            ? `CREATE ROLE ${role} LOGIN PASSWORD '${password}' CONNECTION LIMIT 2 NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;`
            : `DROP ROLE IF EXISTS ${role};`
    const child = Bun.spawn(
        [
            'gosu',
            'postgres',
            'psql',
            '--no-psqlrc',
            '--no-password',
            '--host=/var/run/postgresql',
            '--username=postgres',
            '--dbname=rentnerproxy',
            '--set=ON_ERROR_STOP=1',
            '--set=VERBOSITY=sqlstate',
        ],
        {
            stdin: new TextEncoder().encode(
                "SET log_statement='none'; SET log_min_error_statement='panic'; " + statement,
            ),
            stdout: 'pipe',
            stderr: 'pipe',
        },
    )
    const [exit, , error] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
    ])
    if (exit !== 0) {
        const code = error.match(/ERROR:\s*([A-Z0-9]{5})\b/u)?.[1]
        throw Object.assign(new Error('B2 isolated worker role action failed'), {
            code: code ?? 'role-bootstrap-failed',
        })
    }
}
/** One common fixture process supplies deterministic native HEAD targets and observation-only sampling to both designs. */
export async function runMonitoringB2Appliance(runId: string) {
    const seal = await bootstrap(runId)
    const controlKey = await monitoringB2ControlKey(),
        webKey = await monitoringB2WebKey(),
        probeKey = randomBytes(32).toString('hex')
    process.env.RENTNERPROXY_MONITORING_B2 = '1'
    process.env.B2_APPLIANCE_FIXTURE = runId
    const databaseUrl = await monitoringB2DatabaseUrl()
    const sqlClient = new SQL(databaseUrl, { max: 1, connectionTimeout: 3 })
    const [
        { getAuthDatabase },
        { hostCheckPolicies, hostCheckObservations, proxyHosts, redirectHosts, hostDomains },
        { eq, inArray, sql },
        { readProxyRuntimeSnapshot },
        { applyProxyRuntimeConfiguration },
        { getControllerToken },
        { lockProxyRuntimeSettings },
    ] = await Promise.all([
        import('../../web/src/server/Auth/Core/database.server.ts'),
        import('../../web/src/db/schema.ts'),
        import('drizzle-orm'),
        import('../../web/src/server/ProxyRuntime/proxy-runtime-data.ts'),
        import('../../web/src/server/Controller/proxy.server.ts'),
        import('../../web/src/server/env.server.ts'),
        import('../../web/src/server/ProxyRuntime/proxy-runtime-settings.ts'),
    ])
    const database = getAuthDatabase()
    const baselineIds = [seal.proxyId, seal.policyHostId]
    const [baselineRedirectRow] = await database
        .select()
        .from(redirectHosts)
        .where(eq(redirectHosts.id, seal.redirectId))
    if (!baselineRedirectRow) throw new Error('B2 expected the owned baseline redirect')
    const baselineRedirect = baselineRedirectRow
    const baselineHosts = await database
        .select()
        .from(proxyHosts)
        .where(inArray(proxyHosts.id, baselineIds))
    const baselineDomains = await database
        .select()
        .from(hostDomains)
        .where(inArray(hostDomains.proxyHostId, baselineIds))
    if (baselineHosts.length !== 2)
        throw new Error('B2 expected the two owned baseline traffic hosts')
    type Window = {
        design: 'A' | 'B2'
        profile: 'mixed' | 'all-3s-timeout'
        windowId: string
        hosts: { hostId: string; policyId: string; domain: string; index: number }[]
        origin: number
        stopped: boolean
        nativeStartAttempted: boolean
    }
    let current: Window | undefined,
        authority: Awaited<ReturnType<typeof createMonitoringB2Authority>> | undefined
    let native: ReturnType<typeof Bun.spawn> | undefined,
        nativeExit: Promise<number> | undefined,
        nativeSummary: unknown = null
    let heads: { hostId: string; atMs: number; durationMs: number }[] = [],
        active = 0,
        maxActive = 0
    let mutation: Promise<unknown> = Promise.resolve()
    let quiesced = false
    let quiescing = false
    let fastProbes = false
    let stoppedController: Awaited<ReturnType<typeof processIdentity>> = null
    let controllerResume: ReturnType<typeof setTimeout> | undefined
    let phase = 'ready',
        diagnostic = 'none'
    const visible = new Map<string, number>()
    let recovery:
        | {
              running: boolean
              result: MonitoringB2RecoveryResult | MonitoringB2PublicationResult | null
          }
        | undefined
    let wakeProof: unknown = null
    const aliases = new Map<string, string>()
    const nativeUpstream = Bun.serve({
        hostname: '127.0.0.1',
        port: 9021,
        idleTimeout: 10,
        async fetch(request) {
            const domain = request.headers.get('host') ?? ''
            const host = current?.hosts.find(
                (item) => item.domain === domain || item.hostId === aliases.get(domain),
            )
            if (!host) return new Response(null, { status: 404 })
            if (request.method === 'GET' && new URL(request.url).pathname === '/traffic')
                return new Response('monitoring-b2-traffic')
            if (request.method === 'GET' && new URL(request.url).pathname === '/')
                return Response.json({ backend: 'a' })
            if (request.method !== 'HEAD') return new Response(null, { status: 404 })
            if (active >= 64 || heads.length >= 4096) return new Response(null, { status: 503 })
            const atMs = Date.now()
            active++
            maxActive = Math.max(maxActive, active)
            const receipt = { hostId: host.hostId, atMs, durationMs: 0 }
            heads.push(receipt)
            const latency = monitoringLatency(
                current!.profile === 'mixed' ? 'mixed' : 'timeout',
                host.index,
                3000,
            )
            try {
                await new Promise<void>((resolve) => {
                    const finish = () => {
                        clearTimeout(timer)
                        request.signal.removeEventListener('abort', finish)
                        resolve()
                    }
                    const timer = setTimeout(
                        finish,
                        fastProbes ? 2 : latency.durationMs + (latency.timeout ? 500 : 0),
                    )
                    request.signal.addEventListener('abort', finish, { once: true })
                    if (request.signal.aborted) finish()
                })
                return new Response(null, { status: 200 })
            } finally {
                receipt.durationMs = Date.now() - atMs
                active--
            }
        },
    })
    const probeBroker = startMonitoringB2ProbeBroker({
        probeKey,
        admit: async (request, attempt) =>
            authority && current?.design === 'B2'
                ? admitMonitoringB2Probe(sqlClient, authority.namespace, request, attempt)
                : null,
        forward: async (request, signal) => {
            const controllerToken = getControllerToken()
            if (!controllerToken) throw new Error('B2 root probe transport unavailable')
            return fetch('http://127.0.0.1:8081/internal/v1/diagnostics/domain', {
                method: 'POST',
                headers: {
                    authorization: 'Bearer ' + controllerToken,
                    'content-type': 'application/json',
                },
                body: JSON.stringify(request),
                redirect: 'error',
                signal: AbortSignal.any([
                    signal,
                    AbortSignal.timeout(Math.min(12000, request.timeoutMs + 2000)),
                ]),
            })
        },
    })
    /** Request native drain and join its child; a forced kill or nonzero exit cannot prove a clean drain. */
    async function stopNative() {
        if (!native || !nativeExit) return
        native.kill('SIGINT')
        const exit = await Promise.race([nativeExit, Bun.sleep(15000).then(() => null)])
        if (exit === null) {
            native.kill('SIGKILL')
            await nativeExit
            throw new Error('B2 native drain exceeded deadline')
        }
        native = undefined
        nativeExit = undefined
        if (exit !== 0) throw new Error('B2 native worker failed closed')
    }
    /** Suspend/resume only the identified fixture controller, with a bounded automatic resume fallback. */
    async function controllerAvailable(enabled: boolean) {
        if (enabled) {
            clearTimeout(controllerResume)
            if (stoppedController) {
                const live = await processIdentity(stoppedController.pid)
                if (live?.startTicks !== stoppedController.startTicks)
                    throw new Error('B2 controller identity changed during outage')
                process.kill(live.pid, 'SIGCONT')
                stoppedController = null
            }
            return
        }
        if (stoppedController) throw new Error('B2 controller outage already active')
        const identity = await controllerIdentity()
        if (!identity) throw new Error('B2 controller identity unavailable')
        stoppedController = identity
        process.kill(identity.pid, 'SIGSTOP')
        // An abandoned fault driver cannot leave the owned appliance controller suspended.
        controllerResume = setTimeout(() => {
            void controllerAvailable(true).catch(() => {
                diagnostic = 'controller-outage-identity-changed'
            })
        }, 5000)
    }
    /** Start one isolated native owner with restricted SQL/probe capabilities after checking for orphan writers. */
    async function restartNative() {
        await stopNative()
        if (!authority || current?.design !== 'B2' || quiesced)
            throw new Error('B2 live owned authority required for native restart')
        if ((await nativeIdentities()).length !== 0)
            throw new Error('B2 untracked native writer prevents restart')
        current.nativeStartAttempted = true
        native = Bun.spawn(['gosu', 'monitoring-b2', '/opt/rentnerproxy/web/monitoring-b2'], {
            env: {
                PATH: process.env.PATH ?? '/usr/local/bin:/usr/bin:/bin',
                RENTNERPROXY_MONITORING_B2: '1',
                B2_APPLIANCE_FIXTURE: runId,
                B2_DATABASE_URL: authority!.workerUrl,
                B2_NAMESPACE: authority!.namespace,
                B2_CONTROLLER_URL: 'http://127.0.0.1:9024',
                B2_CONTROLLER_TOKEN: probeKey,
                B2_OUTBOX: root + '/state/' + current!.windowId + '.sqlite',
                B2_STORE_ID: authority!.storeId,
                B2_CONTROLLER_EPOCH: authority!.controllerEpoch,
                B2_DURATION_SECONDS: '180',
            },
            stdout: 'pipe',
            stderr: 'pipe',
        })
        const child = native
        nativeExit = Promise.all([
            child.exited,
            new Response(child.stdout as ReadableStream<Uint8Array>).text(),
            new Response(child.stderr as ReadableStream<Uint8Array>).text(),
        ]).then(([exit, output, nativeDiagnostic]) => {
            if (output.trim())
                try {
                    nativeSummary = JSON.parse(output)
                } catch {
                    nativeSummary = { invalidSummary: true }
                }
            if (nativeDiagnostic) nativeSummary = { failedClosed: true }
            return exit
        })
    }
    /** Stop native work, briefly allow existing replay, then disable this window's policies. */
    async function stop() {
        await controllerAvailable(true)
        await stopNative()
        if ((await nativeIdentities()).length !== 0)
            throw new Error('B2 untracked native writer prevents quiescence')
        if (!current) return
        const window = current
        if (current.design === 'B2') {
            const path = root + '/state/' + current.windowId + '.sqlite'
            const outbox = await openMonitoringB2OutboxForDrain(path, current.nativeStartAttempted)
            if (outbox) {
                try {
                    const deadline = Date.now() + 15000
                    while (
                        Date.now() < deadline &&
                        outbox
                            .query<{ count: number }, []>('SELECT count(*) count FROM b2_outbox')
                            .get()!.count > 0
                    )
                        await Bun.sleep(50)
                } finally {
                    outbox.close()
                }
            }
        }
        await monitoringB2PolicyTransaction(database, async (transaction) => {
            await transaction
                .update(hostCheckPolicies)
                .set({ enabled: false })
                .where(
                    inArray(
                        hostCheckPolicies.proxyHostId,
                        window.hosts.map((item) => item.hostId),
                    ),
                )
        })
        current.stopped = true
    }
    /** Latch authority closed and require native, broker, HEAD and replay drain proofs before maintenance ACK. */
    async function quiesce() {
        if (quiesced) return { quiesced: true, nativeStopped: true, activeNativeHeads: 0 }
        quiescing = true
        probeBroker.closeAdmission()
        const owner = await readMonitoringB2Owner(sqlClient)
        const namespace =
            authority?.namespace ?? (owner?.design === 'B2' ? owner.namespace : undefined)
        if (owner?.design === 'B2' && !namespace)
            throw new Error('B2 maintenance owner namespace missing')
        if (namespace) {
            const fenced = await sqlClient.unsafe(
                `UPDATE ${namespace}.identity SET paused=true,resync_required=true,generation=generation+1 WHERE id=1 RETURNING id`,
            )
            if (fenced.length !== 1) throw new Error('B2 maintenance authority identity missing')
        }
        await controllerAvailable(true)
        await stopNative()
        if ((await nativeIdentities()).length !== 0)
            throw new Error('B2 untracked native writer prevents quiescence')
        const deadline = Date.now() + 4000
        // oxlint-disable-next-line no-unmodified-loop-condition -- Receiver callbacks settle this counter concurrently with drain observation.
        while (active !== 0 && Date.now() < deadline) await Bun.sleep(10)
        if (active !== 0) throw new Error('B2 native HTTP drain not proven')
        await probeBroker.quiesce()
        if (probeBroker.active() !== 0 || !probeBroker.admissionsClosed())
            throw new Error('B2 scoped probe broker drain not proven')
        if (owner?.design === 'B2' && !owner.restoredPaused) {
            try {
                const response = await fetch('http://127.0.0.1:9022/drain', {
                    method: 'POST',
                    headers: { authorization: 'Bearer ' + webKey },
                    signal: AbortSignal.timeout(5000),
                })
                if (
                    !response.ok ||
                    ((await response.json()) as { drained?: boolean }).drained !== true
                )
                    throw new Error('B2 replay drain not acknowledged')
            } catch {
                // Canonical Web maintenance closes its control listener only after writing the same drain proof.
            }
            const proofPath = root + '/state/replay-drained.json',
                metadata = await lstat(proofPath)
            if (
                !metadata.isFile() ||
                metadata.uid !== 10002 ||
                (metadata.mode & 0o022) !== 0 ||
                metadata.size > 2048
            )
                throw new Error('B2 replay drain receipt owner invalid')
            const proof = z
                .object({
                    drained: z.literal(true),
                    pid: z.number().int().positive(),
                    startTicks: z.string().regex(/^\d+$/u),
                    storeId: z.uuid(),
                    controllerEpoch: z.uuid(),
                })
                .parse(JSON.parse(await readFile(proofPath, 'utf8')))
            const web = await processIdentity(proof.pid)
            if (
                !web ||
                web.startTicks !== proof.startTicks ||
                proof.storeId !== owner.storeId ||
                proof.controllerEpoch !== owner.controllerEpoch
            )
                throw new Error('B2 replay drain receipt identity mismatch')
        }
        await Promise.race([
            Promise.all([database.$client.close(), sqlClient.close()]),
            Bun.sleep(5000).then(() => {
                throw new Error('B2 maintenance database drain timed out')
            }),
        ])
        quiesced = true
        return {
            quiesced: true,
            nativeStopped: true,
            activeNativeHeads: 0,
            brokerActive: 0,
            brokerAdmissionsClosed: true,
            brokerForwardTasksSettled: true,
            quiescedAtMs: Date.now(),
        }
    }
    /** Release the prior owned window and authority before restoring baseline fixture hosts for another design. */
    async function clear() {
        await stop()
        if (authority) {
            await authority.cleanup()
            authority = undefined
        }
        if (current) {
            await database.delete(hostCheckPolicies).where(
                inArray(
                    hostCheckPolicies.proxyHostId,
                    current.hosts.map((item) => item.hostId),
                ),
            )
            await database.delete(hostDomains).where(
                inArray(
                    hostDomains.domain,
                    current.hosts.map((item) => item.domain),
                ),
            )
            await database.delete(proxyHosts).where(
                inArray(
                    proxyHosts.id,
                    current.hosts
                        .filter((item) => !baselineIds.includes(item.hostId))
                        .map((item) => item.hostId),
                ),
            )
            for (const baseline of baselineHosts)
                await database
                    .update(proxyHosts)
                    .set({
                        forwardScheme: baseline.forwardScheme,
                        forwardHost: baseline.forwardHost,
                        forwardPort: baseline.forwardPort,
                        upstreamPool: baseline.upstreamPool ? sql`${baseline.upstreamPool}` : null,
                    })
                    .where(eq(proxyHosts.id, baseline.id))
            current = undefined
            aliases.clear()
            await database
                .update(redirectHosts)
                .set({ enabled: baselineRedirect.enabled })
                .where(eq(redirectHosts.id, baselineRedirect.id))
        }
    }
    /** Build the exact owned 1000-policy window and optional native authority; start owns the later due origin. */
    async function prepare(input: unknown) {
        phase = 'prepare-validate'
        const options = prepareSchema.parse(input)
        phase = 'prepare-clear'
        await clear()
        const entries = Array.from({ length: options.policies }, (_, index) => {
            const hostId =
                index >= options.policies - 2
                    ? baselineIds[index - (options.policies - 2)]!
                    : Bun.randomUUIDv7()
            return {
                hostId,
                policyId: Bun.randomUUIDv7(),
                domain: `b2-${index}-${hostId}.fixture.test`,
                index,
            }
        })
        current = {
            ...options,
            hosts: entries,
            origin: 0,
            stopped: false,
            nativeStartAttempted: false,
        }
        for (const domain of baselineDomains)
            if (domain.proxyHostId) aliases.set(domain.domain, domain.proxyHostId)
        visible.clear()
        heads = []
        active = 0
        maxActive = 0
        nativeSummary = null
        wakeProof = null
        fastProbes = false
        phase = 'prepare-seed'
        const snapshot = await database.transaction(async (transaction) => {
            await lockProxyRuntimeSettings(transaction)
            await transaction
                .update(redirectHosts)
                .set({ enabled: false })
                .where(eq(redirectHosts.id, baselineRedirect.id))
            await transaction.insert(proxyHosts).values(
                entries
                    .filter((item) => !baselineIds.includes(item.hostId))
                    .map((item) => ({
                        id: item.hostId,
                        forwardScheme: 'http' as const,
                        forwardHost: '127.0.0.1',
                        forwardPort: 9021,
                    })),
            )
            await transaction
                .update(proxyHosts)
                .set({
                    forwardScheme: 'http',
                    forwardHost: '127.0.0.1',
                    forwardPort: 9021,
                    upstreamPool: null,
                })
                .where(inArray(proxyHosts.id, baselineIds))
            await transaction
                .insert(hostDomains)
                .values(entries.map((item) => ({ proxyHostId: item.hostId, domain: item.domain })))
            await transaction.insert(hostCheckPolicies).values(
                entries.map((item) => ({
                    id: item.policyId,
                    proxyHostId: item.hostId,
                    domain: item.domain,
                    enabled: false,
                    intervalSeconds: 60,
                    timeoutMs: 3000,
                    nextCheckAt: new Date(Date.now() + 3600000),
                })),
            )
            const validated = await readProxyRuntimeSnapshot(transaction)
            if (validated.proxyHosts.length !== 1000 || validated.redirectHosts.length !== 0)
                throw new Error('B2 exact enabled native host coverage failed')
            return validated
        })
        const [coverage] = await database.execute<{ hosts: number; policies: number }>(
            sql`SELECT (SELECT count(*)::integer FROM rentnerproxy.proxy_hosts) hosts,(SELECT count(*)::integer FROM rentnerproxy.host_check_policies) policies`,
        )
        if (coverage?.hosts !== 1000 || coverage.policies !== 1000)
            throw new Error('B2 exact 1000-host/policy fixture coverage failed')
        phase = 'prepare-native-activation'
        if (!(await applyProxyRuntimeConfiguration(snapshot)))
            throw new Error('B2 configured native snapshot failed activation')
        await jsonFile(root + '/mode.json', { runId, design: options.design })
        if (options.design === 'B2') {
            phase = 'prepare-authority'
            authority = await createMonitoringB2Authority(sqlClient, {
                create: (role, password) => workerRole('create', role, password),
                drop: (role) => workerRole('drop', role),
            })
            const path = root + '/state/' + options.windowId + '.sqlite'
            await jsonFile(root + '/runtime.json', {
                namespace: authority.namespace,
                storeId: authority.storeId,
                controllerEpoch: authority.controllerEpoch,
                path,
            })
            await restartNative()
            phase = 'prepare-native-store'
            const deadline = Date.now() + 10000
            while (Date.now() < deadline) {
                try {
                    await lstat(path)
                    break
                } catch {
                    await Bun.sleep(25)
                }
            }
            await lstat(path)
        }
        await persistMonitoringB2Owner(sqlClient, {
            runId,
            design: options.design,
            profile: options.profile,
            hosts: entries,
            ...(authority
                ? {
                      namespace: authority.namespace,
                      storeId: authority.storeId,
                      controllerEpoch: authority.controllerEpoch,
                  }
                : {}),
        })
        phase = 'prepared'
        diagnostic = 'none'
        return {
            prepared: true,
            design: options.design,
            profile: options.profile,
            policies: entries.length,
        }
    }
    /** Rebind only a restored paused fixture with rotated identities; old streams must remain inadmissible. */
    async function rebootstrap(input: unknown) {
        const expected = z
            .strictObject({ expectedOldStoreId: z.uuid(), expectedOldEpoch: z.uuid() })
            .parse(input)
        if (current || authority || native)
            throw new Error('B2 restore rebootstrap requires a fresh control process')
        const owner = await readMonitoringB2Owner(sqlClient)
        if (
            owner?.design !== 'B2' ||
            !owner.restoredPaused ||
            !owner.namespace ||
            !owner.storeId ||
            !owner.controllerEpoch
        )
            throw new Error('B2 restored paused owner required')
        if (
            owner.storeId === expected.expectedOldStoreId ||
            owner.controllerEpoch === expected.expectedOldEpoch
        )
            throw new Error('B2 restore did not rotate writer identity')
        const namespace = owner.namespace
        const [identity] = await sqlClient.unsafe<
            { paused: boolean; resync_required: boolean; store_id: string; epoch: string }[]
        >(`SELECT paused,resync_required,store_id,epoch FROM ${namespace}.identity WHERE id=1`)
        if (
            !identity?.paused ||
            !identity.resync_required ||
            identity.store_id !== owner.storeId ||
            identity.epoch !== owner.controllerEpoch
        )
            throw new Error('B2 restored admission pause not proven')
        const oldClaims = await sqlClient.unsafe(`SELECT * FROM ${namespace}.claim(16,$1,$2)`, [
            expected.expectedOldStoreId,
            expected.expectedOldEpoch,
        ])
        if (oldClaims.length !== 0)
            throw new Error('B2 old restored writer still has claim authority')
        if (
            !owner.hosts ||
            new Set(owner.hosts.map((host) => host.hostId)).size !== 1000 ||
            new Set(owner.hosts.map((host) => host.policyId)).size !== 1000
        )
            throw new Error('B2 restored exact owner selectors missing')
        const entries = await sqlClient.unsafe<
            { policy_id: string; host_id: string; domain: string }[]
        >(
            `SELECT p.id policy_id,p.proxy_host_id host_id,p.domain FROM rentnerproxy.host_check_policies p JOIN rentnerproxy.proxy_hosts host ON host.id=p.proxy_host_id WHERE p.id=ANY($1::uuid[]) AND host.forward_host='127.0.0.1' AND host.forward_port=9021`,
            [owner.hosts.map((host) => host.policyId)],
        )
        if (
            entries.length !== 1000 ||
            new Set(entries.map((entry) => entry.host_id)).size !== 1000 ||
            baselineIds.some((id) => !entries.some((entry) => entry.host_id === id)) ||
            entries.some(
                (entry) =>
                    !owner.hosts!.some(
                        (host) =>
                            host.policyId === entry.policy_id &&
                            host.hostId === entry.host_id &&
                            host.domain === entry.domain,
                    ) ||
                    !new RegExp('^b2-[0-9]{1,3}-' + entry.host_id + '\\.fixture\\.test$', 'u').test(
                        entry.domain,
                    ),
            )
        )
            throw new Error('B2 restored exact owned host set invalid')
        // Dropping the exact root-sealed experiment schema removes only its dependent disposable triggers.
        await sqlClient.unsafe(`DROP SCHEMA ${namespace} CASCADE`)
        await workerRole('drop', namespace.replace('monitor_b2_', 'monitor_b2_role_'))
        await database.transaction(async (transaction) => {
            await transaction.delete(hostCheckPolicies).where(
                inArray(
                    hostCheckPolicies.id,
                    entries.map((entry) => entry.policy_id),
                ),
            )
            await transaction.delete(hostDomains).where(
                inArray(
                    hostDomains.domain,
                    entries.map((entry) => entry.domain),
                ),
            )
            await transaction.delete(proxyHosts).where(
                inArray(
                    proxyHosts.id,
                    entries
                        .filter((entry) => !baselineIds.includes(entry.host_id))
                        .map((entry) => entry.host_id),
                ),
            )
            await transaction.execute(
                sql`DELETE FROM rentnerproxy.system_settings WHERE key='monitoring_b2_experiment_owner_v1'`,
            )
        })
        return {
            rebootstrap: true,
            restoredPaused: true,
            oldWriterClaimRows: oldClaims.length,
            removedPolicies: entries.length,
            remainingBaselineHosts: baselineIds.length,
            nativeIdentity: null,
        }
    }
    /** Publish one common future due origin and require the original web owner's wake acknowledgement. */
    async function start() {
        if (!current || current.origin !== 0) throw new Error('B2 prepared window required')
        const window = current
        current.origin = Date.now() + 1000
        await monitoringB2PolicyTransaction(database, async (transaction) => {
            await transaction
                .update(hostCheckPolicies)
                .set({ enabled: true, nextCheckAt: new Date(window.origin) })
                .where(
                    inArray(
                        hostCheckPolicies.proxyHostId,
                        window.hosts.map((item) => item.hostId),
                    ),
                )
        })
        if (authority) await authority.publish(true)
        const response = await fetch('http://127.0.0.1:9023/wake', {
            method: 'POST',
            headers: {
                authorization: 'Bearer ' + webKey,
                'content-type': 'application/json',
            },
            body: JSON.stringify({
                runId,
                design: current.design,
                windowId: current.windowId,
                originMs: current.origin,
            }),
            signal: AbortSignal.timeout(Math.max(1, current.origin - Date.now())),
            redirect: 'error',
        })
        if (!response.ok) throw new Error('B2 original web change wake failed')
        const proof = z
            .strictObject({
                design: z.literal(current.design),
                runId: z.literal(runId),
                windowId: z.literal(current.windowId),
                originMs: z.literal(current.origin),
                publishedAtMs: z.number().int(),
                webIdentity: z.strictObject({
                    pid: z.number().int().positive(),
                    startTicks: z.string().regex(/^\d+$/u),
                }),
                originalChangeChunkSha256: z.string().regex(/^[a-f0-9]{64}$/u),
                wakeCount: z.number().int().positive(),
            })
            .parse(await response.json())
        const web = await processIdentity(proof.webIdentity.pid)
        if (
            web?.startTicks !== proof.webIdentity.startTicks ||
            proof.publishedAtMs >= current.origin ||
            Date.now() >= current.origin
        )
            throw new Error('B2 original web change wake identity or deadline unproven')
        wakeProof = proof
        return { originMs: current.origin, design: current.design, policies: current.hosts.length }
    }
    /** Commit one owned host edit through original settings locks and apply its real controller revision. */
    async function configureHost(enabled: boolean) {
        if (current?.design !== 'B2' || current.origin === 0)
            throw new Error('B2 active owned configuration case required')
        const target = current.hosts[0]!
        const snapshot = await monitoringB2PolicyTransaction(database, async (transaction) => {
            await transaction
                .update(proxyHosts)
                .set({ enabled })
                .where(eq(proxyHosts.id, target.hostId))
            return readProxyRuntimeSnapshot(transaction)
        })
        if (!(await applyProxyRuntimeConfiguration(snapshot)))
            throw new Error('B2 mutated configured snapshot failed activation')
        if (enabled) {
            const scheduled = await sqlClient.unsafe(
                'UPDATE rentnerproxy.host_check_policies SET next_check_at=clock_timestamp() WHERE id=$1 AND lease_token IS NULL RETURNING id',
                [target.policyId],
            )
            if (scheduled.length !== 1)
                throw new Error('B2 configuration case cannot preempt an active lease')
        }
        return { hostId: target.hostId, changedAtMs: Date.now(), revision: snapshot.revision }
    }
    /** Sample actual fixture progress and first-visible observation upper bounds without returning capabilities. */
    async function report() {
        if (!current) return { prepared: false }
        const ids = current.hosts.map((item) => item.policyId)
        const observations = await database
            .select({
                id: hostCheckObservations.id,
                policyId: hostCheckObservations.policyId,
                checkedAt: hostCheckObservations.checkedAt,
                status: hostCheckObservations.status,
                code: hostCheckObservations.code,
            })
            .from(hostCheckObservations)
            .where(inArray(hostCheckObservations.policyId, ids))
        const [connections] = await database.execute<{ count: number }>(
            sql`SELECT count(*)::integer count FROM pg_stat_activity WHERE datname=current_database()`,
        )
        const firstVisible = Date.now()
        for (const observation of observations)
            if (!visible.has(observation.id)) visible.set(observation.id, firstVisible)
        let outbox: {
                records: number
                bytes: number
                oldestAtMs: number | null
                rows: unknown[]
                status: unknown
            } = { records: 0, bytes: 0, oldestAtMs: null, rows: [], status: null },
            replay: unknown = null,
            receipts: unknown = null,
            admissionStore: unknown = null
        let starts: {
            policyId: string
            dueAtMs: number
            atMs: number
            admittedAtMs: number
            durableAtMs: number
            attemptId: string
            durable: boolean
        }[] = []
        if (current.design === 'B2') {
            const originalSchema = monitoringB2OriginalAdmissionSchema.extend({ attempt: z.uuid() })
            const originalRows = authority
                ? await sqlClient.unsafe<
                      { attempt: string; original_claim: unknown; admitted_ms: string | null }[]
                  >(
                      `SELECT lease_token::text attempt,original_claim,admitted_ms::text FROM ${authority.namespace}.admissions`,
                  )
                : []
            const originals = new Map(
                originalRows.map((row) => {
                    const original = originalSchema.parse(row)
                    return [original.attempt, original] as const
                }),
            )
            const store = new Database(root + '/state/' + current.windowId + '.sqlite', {
                readonly: true,
                strict: true,
            })
            try {
                const rows = store
                    .query<
                        {
                            attempt_id: string
                            claim: string
                            started_ms: number
                            admitted_ms: number | null
                            durable_ms: number | null
                            completed_ms: number | null
                        },
                        []
                    >(
                        'SELECT attempt_id,claim,started_ms,admitted_ms,durable_ms,completed_ms FROM b2_outbox ORDER BY sequence',
                    )
                    .all()
                const stats = store
                    .query<{ records: number; bytes: number; oldestAtMs: number | null }, []>(
                        'SELECT count(*) records,coalesce(sum(length(cast(claim AS blob))+coalesce(length(cast(result AS blob)),0)),0) bytes,min(started_ms) oldestAtMs FROM b2_outbox',
                    )
                    .get()!
                outbox = {
                    ...stats,
                    rows: rows.map((row) => ({ ...row, claim: undefined })),
                    status: store.query('SELECT * FROM b2_status').get(),
                }
                starts = rows.flatMap((row) => {
                    const claim = monitoringB2ClaimSchema.parse(JSON.parse(row.claim))
                    return monitoringB2AdmissionRejection(claim, originals.get(row.attempt_id), {
                        ...row,
                        completed_ms: row.completed_ms ?? row.durable_ms,
                    }) !== null
                        ? []
                        : [
                              {
                                  policyId: claim.policyId,
                                  dueAtMs: claim.dueAtMs,
                                  atMs: row.admitted_ms!,
                                  admittedAtMs: row.admitted_ms!,
                                  durableAtMs: row.durable_ms!,
                                  attemptId: row.attempt_id,
                                  durable: true,
                              },
                          ]
                })
            } finally {
                store.close()
            }
            try {
                replay = JSON.parse(await readFile(root + '/state/replay-proof.json', 'utf8'))
            } catch {
                /* Web STOP has no recent replay report. */
            }
            if (authority) {
                const [admissions] = await sqlClient.unsafe<
                    { records: number; admitted: number; oldest: string | null }[]
                >(
                    `SELECT count(*)::integer records,count(admitted_ms)::integer admitted,floor(extract(epoch FROM min(created_at))*1000)::bigint::text oldest FROM ${authority.namespace}.admissions`,
                )
                if (!admissions) throw new Error('B2 original admission storage evidence missing')
                admissionStore = {
                    records: admissions.records,
                    admitted: admissions.admitted,
                    oldestAtMs: admissions.oldest === null ? null : Number(admissions.oldest),
                    maxRecords: 4096,
                    retentionMs: 120000,
                    pressure: admissions.records >= 4096,
                }
                receipts = await sqlClient.unsafe(
                    `SELECT disposition,count(*)::integer count FROM ${authority.namespace}.receipts GROUP BY disposition`,
                )
                const committed = await sqlClient.unsafe<
                    {
                        policyId: string
                        dueAtMs: string
                        atMs: string
                        admittedAtMs: string
                        durableAtMs: string
                        attemptId: string
                    }[]
                >(
                    `SELECT receipt.policy_id::text "policyId",receipt.due_ms::text "dueAtMs",receipt.admitted_ms::text "atMs",receipt.admitted_ms::text "admittedAtMs",receipt.durable_ms::text "durableAtMs",receipt.attempt_id::text "attemptId" FROM ${authority.namespace}.receipts receipt WHERE receipt.disposition='candidate' AND receipt.admitted_ms IS NOT NULL AND receipt.durable_ms IS NOT NULL AND EXISTS(SELECT 1 FROM rentnerproxy.host_check_observations observation WHERE observation.id=receipt.observation_id AND observation.policy_id=receipt.policy_id AND observation.policy_version=receipt.policy_version)`,
                )
                const unique = new Map(starts.map((item) => [item.attemptId, item]))
                for (const item of committed)
                    unique.set(item.attemptId, {
                        ...item,
                        dueAtMs: Number(item.dueAtMs),
                        atMs: Number(item.atMs),
                        admittedAtMs: Number(item.admittedAtMs),
                        durableAtMs: Number(item.durableAtMs),
                        durable: true,
                    })
                starts = [...unique.values()]
            }
        } else {
            const ordinals = new Map<string, number>()
            const byHost = new Map(current.hosts.map((host) => [host.hostId, host]))
            starts = heads.map((head) => {
                const ordinal = ordinals.get(head.hostId) ?? 0
                ordinals.set(head.hostId, ordinal + 1)
                return {
                    policyId: byHost.get(head.hostId)!.policyId,
                    dueAtMs: current!.origin + ordinal * 60000,
                    atMs: head.atMs,
                    admittedAtMs: head.atMs,
                    durableAtMs: head.atMs,
                    attemptId: 'a-' + head.hostId + '-' + ordinal,
                    durable: false,
                }
            })
        }
        return {
            prepared: true,
            design: current.design,
            profile: current.profile,
            windowId: current.windowId,
            originMs: current.origin,
            endMs: Date.now(),
            policies: current.hosts.map((item) => ({
                policyId: item.policyId,
                hostId: item.hostId,
                dueAtMs: current!.origin,
                expectedStatus:
                    current!.profile === 'all-3s-timeout' || item.index % 4 === 3
                        ? 'unhealthy'
                        : 'healthy',
                expectedCode:
                    current!.profile === 'all-3s-timeout' || item.index % 4 === 3
                        ? 'upstream_timeout'
                        : 'expected_status',
            })),
            heads,
            starts,
            maxActive,
            active,
            observations: observations.map((row) => ({
                id: row.id,
                policyId: row.policyId,
                status: row.status,
                code: row.code,
                checkedAtMs: row.checkedAt.getTime(),
                committedAtMs: visible.get(row.id)!,
                commitTimeSource: 'first-visible-observation-upper-bound',
            })),
            outbox,
            backlog: outbox.records,
            replay,
            receipts,
            admissionStore,
            nativePid: native?.pid ?? null,
            nativeIdentity: await processIdentity(native?.pid),
            controllerIdentity: await controllerIdentity(),
            nativeSummary,
            wakeProof,
            postgresConnections: connections?.count ?? null,
            stopped: current.stopped,
        }
    }
    const control = Bun.serve({
        hostname: '127.0.0.1',
        port: 9020,
        idleTimeout: 30,
        async fetch(request) {
            const path = new URL(request.url).pathname
            const credential = request.headers.get('authorization')
            if (
                credential !== 'Bearer ' + controlKey &&
                !(
                    request.method === 'POST' &&
                    path === '/quiesce' &&
                    credential === 'Bearer ' + webKey
                )
            )
                return new Response(null, { status: 403 })
            try {
                if (request.method === 'GET' && path === '/diagnostics') {
                    let web: unknown = null
                    try {
                        web = JSON.parse(await readFile(root + '/state/web-failure.json', 'utf8'))
                    } catch {
                        /* No wrapper failure has been recorded. */
                    }
                    return Response.json({ phase, diagnostic, web })
                }
                if (request.method === 'GET' && path === '/report')
                    return Response.json(await report())
                if (request.method === 'GET' && path === '/capability-proof')
                    return Response.json(await runMonitoringB2CapabilityProof(probeKey))
                if (request.method === 'GET' && path === '/recovery')
                    return Response.json(recovery ?? { running: false, result: null })
                if (request.method === 'GET' && path === '/owner-status') {
                    const owner = await readMonitoringB2Owner(sqlClient)
                    const [counts] =
                        owner?.namespace && owner.hosts
                            ? await sqlClient.unsafe<
                                  {
                                      policies: number
                                      observations: number
                                      incidents: number
                                      receipts: number
                                      pending: number
                                  }[]
                              >(
                                  `SELECT (SELECT count(*)::integer FROM rentnerproxy.host_check_policies WHERE id=ANY($1::uuid[])) policies,(SELECT count(*)::integer FROM rentnerproxy.host_check_observations WHERE policy_id=ANY($1::uuid[])) observations,(SELECT count(*)::integer FROM rentnerproxy.host_check_incidents WHERE policy_id=ANY($1::uuid[])) incidents,(SELECT count(*)::integer FROM ${owner.namespace}.receipts) receipts,(SELECT count(*)::integer FROM ${owner.namespace}.replay_pending) pending`,
                                  [owner.hosts.map((host) => host.policyId)],
                              )
                            : []
                    let replayAttached = false
                    try {
                        const response = await fetch('http://127.0.0.1:9022/status', {
                            headers: { authorization: 'Bearer ' + webKey },
                            signal: AbortSignal.timeout(1000),
                        })
                        replayAttached = response.ok
                    } catch {
                        /* Paused restored startup deliberately has no replay listener. */
                    }
                    const natives = await nativeIdentities()
                    return Response.json({
                        owner: owner ? { ...owner, hosts: undefined } : null,
                        nativeIdentity: natives.length === 1 ? natives[0] : null,
                        nativeCount: natives.length,
                        counts: counts ?? null,
                        replayAttached,
                    })
                }
                if (request.method === 'GET' && path === '/visibility') {
                    const observations = current
                        ? await database
                              .select({ id: hostCheckObservations.id })
                              .from(hostCheckObservations)
                              .where(
                                  inArray(
                                      hostCheckObservations.policyId,
                                      current.hosts.map((item) => item.policyId),
                                  ),
                              )
                        : []
                    const atMs = Date.now()
                    for (const observation of observations)
                        if (!visible.has(observation.id)) visible.set(observation.id, atMs)
                    return Response.json({ count: observations.length, atMs })
                }
                if (request.method === 'GET' && path === '/web-proof')
                    return Response.json(
                        JSON.parse(await readFile(root + '/state/web-proof.json', 'utf8')),
                    )
                if (request.method === 'GET' && path === '/management') {
                    const began = performance.now()
                    const response = await fetch('http://127.0.0.1:3000/proxy-hosts', {
                        headers: { cookie: 'rentnerproxy_session=' + seal.session },
                        signal: AbortSignal.timeout(3000),
                        redirect: 'manual',
                    })
                    let bytes = 0,
                        prefix = ''
                    if (response.body)
                        for await (const chunk of response.body) {
                            bytes += chunk.byteLength
                            if (prefix.length < 4096)
                                prefix += new TextDecoder().decode(
                                    chunk.subarray(0, 4096 - prefix.length),
                                )
                            if (bytes > 4 * 1024 * 1024) throw new Error('B2 SSR body bound')
                        }
                    return Response.json({
                        status: response.status,
                        elapsedMs: performance.now() - began,
                        validBody:
                            response.status === 200 &&
                            response.headers.get('content-type')?.includes('text/html') === true &&
                            prefix.includes('<html'),
                    })
                }
                if (request.method === 'GET' && path === '/traffic') {
                    const began = performance.now()
                    const response = await fetch('http://127.0.0.1:8080/traffic', {
                        headers: { host: current?.hosts[0]?.domain ?? '' },
                        signal: AbortSignal.timeout(2000),
                        redirect: 'error',
                    })
                    return Response.json({
                        status: response.status,
                        elapsedMs: performance.now() - began,
                        validBody: (await response.text()) === 'monitoring-b2-traffic',
                    })
                }
                if (request.method === 'POST') {
                    if (Number(request.headers.get('content-length') ?? 0) > 4096)
                        return new Response(null, { status: 413 })
                    const input = await request.json()
                    const operation = async () => {
                        if (recovery?.running)
                            throw new Error('B2 recovery job owns fixture mutations')
                        if (path === '/quiesce') return quiesce()
                        if (path === '/shutdown' && quiesced) {
                            setTimeout(() => {
                                control.stop(true)
                                nativeUpstream.stop(true)
                                probeBroker.stop(true)
                            }, 20)
                            return { stopped: true, alreadyQuiesced: true }
                        }
                        if (quiesced) throw new Error('B2 maintenance is permanently quiesced')
                        if (quiescing)
                            throw new Error('B2 maintenance remains fail-closed pending quiescence')
                        if (path === '/prepare') return prepare(input)
                        if (path === '/rebootstrap') return rebootstrap(input)
                        if (path === '/start') return start()
                        if (path === '/native-restart') {
                            await restartNative()
                            return {
                                restarted: true,
                                nativeIdentity: await processIdentity(native?.pid),
                            }
                        }
                        if (path === '/recovery') {
                            const options = z
                                .strictObject({
                                    scenario: z.enum([
                                        'replay',
                                        'fences',
                                        'lease-expiry',
                                        'pressure',
                                        'gap',
                                        'automatic-publication',
                                    ]),
                                })
                                .parse(input)
                            if (recovery?.running) throw new Error('B2 recovery already active')
                            const job = {
                                running: true,
                                result: null as
                                    | MonitoringB2RecoveryResult
                                    | MonitoringB2PublicationResult
                                    | null,
                            }
                            recovery = job
                            const recoveryOperation =
                                options.scenario === 'automatic-publication'
                                    ? (await import('./monitoring-b2-publication-case.ts'))
                                          .runMonitoringB2PublicationCase
                                    : (
                                          context: import('./Types/monitoring-b2.types.ts').MonitoringB2RecoveryContext,
                                      ) =>
                                          import('./monitoring-b2-recovery.ts').then(
                                              ({ runMonitoringB2Recovery }) =>
                                                  runMonitoringB2Recovery(
                                                      context,
                                                      options.scenario as import('./Types/monitoring-b2-recovery.types.ts').MonitoringB2RecoveryScenario,
                                                  ),
                                          )
                            void recoveryOperation({
                                database: sqlClient,
                                state: () =>
                                    current ? { ...current, originMs: current.origin } : undefined,
                                authority: () => authority,
                                outboxPath: () => root + '/state/' + current!.windowId + '.sqlite',
                                heads: () => heads,
                                start,
                                stopNative,
                                restartNative,
                                controllerAvailable,
                                configureHost,
                                fastProbes: (enabled) => {
                                    fastProbes = enabled
                                },
                                quiesce,
                            })
                                .then(async (result) => {
                                    if (options.scenario === 'fences') {
                                        const capability =
                                            await runMonitoringB2CapabilityProof(probeKey)
                                        Object.assign(result.evidence, capability.evidence)
                                        result.failures.push(...capability.failures)
                                        result.passed = result.passed && capability.passed
                                    }
                                    job.result = result
                                })
                                .catch(() => {
                                    job.result = {
                                        scenario: options.scenario,
                                        passed: false,
                                        evidence: {},
                                        failures: ['fixture-recovery-failed-closed'],
                                    }
                                })
                                .finally(() => {
                                    job.running = false
                                })
                            return { started: true, scenario: options.scenario }
                        }
                        if (path === '/stop') {
                            await stop()
                            return report()
                        }
                        if (path === '/shutdown') {
                            await clear()
                            setTimeout(() => {
                                control.stop(true)
                                nativeUpstream.stop(true)
                                probeBroker.stop(true)
                                void database.$client.close()
                                void sqlClient.close()
                            }, 20)
                            return { stopped: true }
                        }
                        throw new Error('Unsupported B2 control action')
                    }
                    const result = mutation.then(operation, operation)
                    mutation = result.then(
                        () => undefined,
                        () => undefined,
                    )
                    return Response.json(await result)
                }
                return new Response(null, { status: 404 })
            } catch (error) {
                diagnostic =
                    error instanceof z.ZodError
                        ? 'invalid-fixture-input'
                        : monitoringB2FixtureDiagnostic(error)
                return Response.json(
                    { error: 'B2 isolated fixture action failed', phase, diagnostic },
                    { status: 500 },
                )
            }
        },
    })
    return { control, nativeUpstream, probeBroker }
}

if (import.meta.main) await runMonitoringB2Appliance(process.argv[2] ?? '')
