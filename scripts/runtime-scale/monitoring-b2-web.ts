// oxlint-disable no-await-in-loop -- Startup must prove the intercepted original lifecycle completed before reporting topology.
import { readdir, readFile, stat, writeFile } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { pathToFileURL } from 'node:url'
import { dirname, resolve } from 'node:path'
import { monitoringB2BootMode } from './monitoring-b2-boot.ts'
import { monitoringB2OriginalImport } from './monitoring-b2-bundled.ts'
import { createMonitoringB2AttachmentFence } from './monitoring-b2-web-lifecycle.ts'
import { monitoringB2WebKey } from './monitoring-b2-capabilities.ts'

/** Preloads the same built worker for both designs, suppressing only B2's scheduler before original application startup. */
export async function prepareMonitoringB2Web(design: 'A' | 'B2', assets: string) {
    const names = (await readdir(assets)).filter((name) =>
        /^host-monitoring\.worker\.server-[a-zA-Z0-9_-]+\.js$/u.test(name),
    )
    if (names.length !== 1)
        throw new Error('B2 requires exactly one original built monitoring worker chunk')
    const path = assets + '/' + names[0]!
    if ((await stat(path)).size > 2 * 1024 * 1024)
        throw new Error('B2 monitoring chunk budget exceeded')
    const source = await readFile(path, 'utf8')
    if (!source.includes('hostMonitoringWorker') || source.includes('startHostMonitoringLifecycle'))
        throw new Error('B2 worker chunk does not expose the expected lazy lifecycle boundary')
    const loaded: Record<string, unknown> = await import(pathToFileURL(path).href)
    const candidates = Object.values(loaded).filter(
        (value): value is { start: () => unknown; stop: () => Promise<void> } =>
            typeof value === 'object' &&
            value !== null &&
            'start' in value &&
            'stop' in value &&
            typeof value.start === 'function' &&
            typeof value.stop === 'function',
    )
    if (candidates.length !== 1)
        throw new Error('B2 original monitoring worker export is ambiguous')
    const worker = candidates[0]!,
        start = worker.start
    let calls = 0
    // The original module graph can schedule server startup as soon as this import resolves.
    // Intercept synchronously, before any additional import or filesystem await.
    worker.start = () => {
        calls++
        return design === 'A' ? start.call(worker) : undefined
    }
    const changeSelector = monitoringB2OriginalImport(source, 'publishApplicationChange'),
        changePath = resolve(dirname(path), changeSelector.path)
    const change: Record<string, unknown> = await import(pathToFileURL(changePath).href)
    const publishChange = change[changeSelector.exported]
    if (typeof publishChange !== 'function')
        throw new Error('B2 original application publisher missing')
    const changeChunkSha256 = createHash('sha256')
        .update(await readFile(changePath, 'utf8'))
        .digest('hex')
    return {
        wake: () => {
            publishChange()
        },
        changeChunkSha256,
        proof: () => ({
            design,
            chunk: names[0]!,
            chunkSha256: createHash('sha256').update(source).digest('hex'),
            originalStartCalls: design === 'A' ? calls : 0,
            suppressedStartCalls: design === 'B2' ? calls : 0,
        }),
    }
}

/** Fixture-only replacement at the original serve.ts path imports the byte-identical preserved original entrypoint. */
async function startMonitoringB2Web(setPhase: (phase: string) => void) {
    setPhase('mode-seal')
    const input = await monitoringB2BootMode()
    const webKey = await monitoringB2WebKey()
    process.env.RENTNERPROXY_MONITORING_B2 = '1'
    process.env.RENTNERPROXY_RELIABILITY_ISOLATED = input.runId
    process.env.B2_APPLIANCE_FIXTURE = input.runId
    const design = input.design as 'A' | 'B2'
    const maintenance = createMonitoringB2AttachmentFence()
    let changeControl: ReturnType<typeof Bun.serve> | undefined
    process.once('rentnerproxy:shutdown', (pending: Promise<void>[]) => {
        if (changeControl) pending.push(changeControl.stop(true))
        pending.push(maintenance.drain(async () => {}))
    })
    if (design === 'A')
        process.once('rentnerproxy:maintenance', (pending: Promise<void>[]) => {
            if (changeControl) pending.push(changeControl.stop(true))
            pending.push(maintenance.drain(async () => {}))
        })
    // Register before importing the original worker: its dependency graph imports server.js
    // and can observe a root maintenance operation during application initialization.
    if (design === 'B2')
        process.once('rentnerproxy:maintenance', (pending: Promise<void>[]) => {
            if (changeControl) pending.push(changeControl.stop(true))
            pending.push(
                (async () => {
                    await maintenance.drain(async () => {
                        const runtime = JSON.parse(
                            await readFile('/tmp/monitoring-b2/runtime.json', 'utf8'),
                        ) as { storeId: string; controllerEpoch: string }
                        const procStat = await readFile('/proc/self/stat', 'utf8')
                        const startTicks = procStat
                            .slice(procStat.lastIndexOf(')') + 2)
                            .trim()
                            .split(/\s+/u)[19]
                        if (!startTicks || !/^\d+$/u.test(startTicks))
                            throw new Error('B2 startup drain identity unavailable')
                        await writeFile(
                            '/tmp/monitoring-b2/state/replay-drained.json',
                            JSON.stringify({
                                drained: true,
                                pid: process.pid,
                                startTicks,
                                storeId: runtime.storeId,
                                controllerEpoch: runtime.controllerEpoch,
                                neverAttached: true,
                            }),
                            { mode: 0o644 },
                        )
                    })
                    const response = await fetch('http://127.0.0.1:9020/quiesce', {
                        method: 'POST',
                        headers: {
                            authorization: 'Bearer ' + webKey,
                            'content-type': 'application/json',
                        },
                        body: '{}',
                        signal: AbortSignal.timeout(15000),
                    })
                    if (!response.ok) throw new Error('B2 startup native quiescence failed')
                    const proof: unknown = await response.json()
                    if (
                        !proof ||
                        typeof proof !== 'object' ||
                        !('quiesced' in proof) ||
                        proof.quiesced !== true ||
                        !('nativeStopped' in proof) ||
                        proof.nativeStopped !== true ||
                        !('activeNativeHeads' in proof) ||
                        proof.activeNativeHeads !== 0
                    )
                        throw new Error('B2 startup native quiescence not proven')
                })(),
            )
        })
    setPhase('worker-preload')
    const prepared = await prepareMonitoringB2Web(
        design,
        '/opt/rentnerproxy/web/web/dist/server/assets',
    )
    const original = '/opt/rentnerproxy/web/docker/web/serve.production.ts'
    setPhase('original-server-import')
    await import(pathToFileURL(original).href)
    const deadline = Date.now() + 10000
    setPhase('lifecycle-proof')
    while (
        Date.now() < deadline &&
        prepared.proof().originalStartCalls + prepared.proof().suppressedStartCalls === 0
    )
        await Bun.sleep(10)
    if (
        !maintenance.signal.aborted &&
        prepared.proof().originalStartCalls + prepared.proof().suppressedStartCalls !== 1
    )
        throw new Error('B2 web topology did not intercept exactly one scheduler start')
    const procStat = await readFile('/proc/self/stat', 'utf8'),
        startTicks = procStat
            .slice(procStat.lastIndexOf(')') + 2)
            .trim()
            .split(/\s+/u)[19]
    if (!startTicks || !/^\d+$/u.test(startTicks))
        throw new Error('B2 original web identity unavailable')
    let wakeCount = 0
    if (!maintenance.signal.aborted) {
        changeControl = Bun.serve({
            hostname: '127.0.0.1',
            port: 9023,
            idleTimeout: 2,
            maxRequestBodySize: 256,
            async fetch(request) {
                if (request.headers.get('authorization') !== 'Bearer ' + webKey)
                    return new Response(null, { status: 403 })
                if (request.method !== 'POST' || new URL(request.url).pathname !== '/wake')
                    return new Response(null, { status: 404 })
                if (maintenance.signal.aborted) return new Response(null, { status: 409 })
                const body = await Promise.race([
                    request.text().catch(() => null),
                    Bun.sleep(250).then(() => null),
                ])
                if (body === null) return new Response(null, { status: 408 })
                if (Buffer.byteLength(body) > 256) return new Response(null, { status: 413 })
                let parsed: unknown
                try {
                    parsed = JSON.parse(body)
                } catch {
                    return new Response(null, { status: 400 })
                }
                if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed))
                    return new Response(null, { status: 400 })
                const value = parsed as Record<string, unknown>,
                    now = Date.now()
                if (
                    Object.keys(value).toSorted().join(',') !== 'design,originMs,runId,windowId' ||
                    value.design !== design ||
                    value.runId !== input.runId ||
                    typeof value.windowId !== 'string' ||
                    !/^[a-z0-9-]{1,40}$/u.test(value.windowId) ||
                    !Number.isSafeInteger(value.originMs) ||
                    Number(value.originMs) <= now ||
                    Number(value.originMs) > now + 5000
                )
                    return new Response(null, { status: 400 })
                if (maintenance.signal.aborted) return new Response(null, { status: 409 })
                prepared.wake()
                wakeCount++
                return Response.json({
                    design,
                    runId: input.runId,
                    windowId: value.windowId,
                    originMs: value.originMs,
                    publishedAtMs: now,
                    webIdentity: { pid: process.pid, startTicks },
                    originalChangeChunkSha256: prepared.changeChunkSha256,
                    wakeCount,
                })
            },
        })
    }
    await writeFile(
        '/tmp/monitoring-b2/state/web-proof.json',
        JSON.stringify({
            ...prepared.proof(),
            restoredPaused: input.restoredPaused,
            maintenancePaused: maintenance.signal.aborted,
        }),
        {
            mode: 0o644,
        },
    )
    if (design === 'B2' && input.attachReplay && !maintenance.signal.aborted) {
        setPhase('replay-attach')
        const runtime = '/opt/rentnerproxy/web/monitoring-b2-web-replay.js'
        const adapter = (await import(pathToFileURL(runtime).href)) as {
            attachMonitoringB2Replay: (
                workerPath: string,
                signal?: AbortSignal,
            ) => Promise<{ drain: () => Promise<void> } | undefined>
        }
        await maintenance.attach((signal) =>
            adapter.attachMonitoringB2Replay(
                '/opt/rentnerproxy/web/web/dist/server/assets/' + prepared.proof().chunk,
                signal,
            ),
        )
    }
}

/** Run the private startup wrapper and record only a fixed failure phase if original startup cannot finish. */
export async function runMonitoringB2Web() {
    let phase = 'initializing'
    try {
        await startMonitoringB2Web((value) => {
            phase = value
        })
    } catch (error) {
        const code = error && typeof error === 'object' && 'code' in error ? String(error.code) : ''
        const diagnostic = ['ENOENT', 'EACCES', 'EPERM', 'ERR_MODULE_NOT_FOUND'].includes(code)
            ? code
            : 'wrapper-failed'
        try {
            await writeFile(
                '/tmp/monitoring-b2/state/web-failure.json',
                JSON.stringify({ phase, diagnostic }),
                { mode: 0o644 },
            )
        } catch {
            /* A missing fixture directory cannot emit a failure receipt. */
        }
        throw new Error('B2 original web wrapper failed', { cause: error })
    }
}

if (import.meta.main) await runMonitoringB2Web()
