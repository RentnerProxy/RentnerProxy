// oxlint-disable no-await-in-loop -- Bounded waits observe serial React and router transitions.
import { afterEach, beforeEach, expect, jest, mock, spyOn, test } from 'bun:test'
import { GlobalRegistrator } from '@happy-dom/global-registrator'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { toast } from '@rentnerkev/toasts'
import { act, useState } from 'react'
import type { ReactNode } from 'react'
import { createRoot } from 'react-dom/client'
import type { Root } from 'react-dom/client'
import {
    createMemoryHistory,
    createRootRoute,
    createRoute,
    createRouter,
    Outlet,
    RouterProvider,
} from '@tanstack/react-router'
import { PERMISSIONS } from '@/config/permissions.config.ts'
import { RESTORE_COMPLETION_VISIBLE_MS } from '@/config/backups.config.ts'
import type { AuthenticatedUser } from '@/lib/Auth/Types/auth.types.ts'
import type { ManagedBackupMaintenance } from '@/lib/Backups/Types/backups.types.ts'
import type { UserThemeMode } from '@/config/Types/theme-config.types.ts'
import withTestLanguage, { catalogs } from '@/tests/Helpers/withTestLanguage.tsx'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

if (!GlobalRegistrator.isRegistered) GlobalRegistrator.register()
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })

// Keep the actual layout, restore hooks, identity sync and socket transport; isolate unrelated UI/services.
await mock.module('@/layouts/AuthenticatedLayout/Components/ApplicationShell/index.tsx', () => ({
    default: ({
        children,
        themeControl,
        themeMode,
    }: {
        children: ReactNode
        themeControl: ReactNode
        themeMode: UserThemeMode
    }) => (
        <div data-application-shell data-theme={themeMode}>
            {themeControl}
            {children}
        </div>
    ),
}))
await mock.module(
    '@/features/Admin/ProxyHostManagement/CertificateJobs/CertificateJobProgressObserver.tsx',
    () => ({ default: () => null }),
)
await mock.module('@/shared/QuickActions/Components/QuickActionsProvider.tsx', () => ({
    default: ({ children }: { children: ReactNode }) => <>{children}</>,
}))
await mock.module('@/shared/PageAlerts/PageAlertsProvider.tsx', () => ({
    default: ({ children }: { children: ReactNode }) => <>{children}</>,
}))
await mock.module('@/features/UserSettings/middleware.ts', () => ({
    updateCurrentUserThemeModeHandler: async ({ data }: { data: { themeMode: UserThemeMode } }) => {
        if (themeUpdatesAllowed) return { success: true, themeMode: data.themeMode }
        throw new Error('Unexpected preference mutation')
    },
    updateCurrentUserNavigationGroupHandler: async () => {
        throw new Error('Unexpected preference mutation')
    },
}))
await mock.module('@/features/Auth/middleware.ts', () => ({
    logoutHandler: async () => {
        throw new Error('Unexpected logout')
    },
}))
const { default: AuthenticatedLayout } = await import('@/layouts/AuthenticatedLayout/index.tsx')
let themeUpdatesAllowed = false
let identityLoad: Promise<void> | undefined

class FakeSocket extends EventTarget {
    static instances: FakeSocket[] = []
    static OPEN = 1
    static CLOSED = 3
    readyState = FakeSocket.OPEN
    sent: Array<{ type: string; topic?: string; query?: object }> = []
    constructor(readonly url: URL) {
        super()
        FakeSocket.instances.push(this)
    }
    send(value: string) {
        this.sent.push(JSON.parse(value))
    }
    open() {
        this.dispatchEvent(new Event('open'))
    }
    close() {
        this.readyState = FakeSocket.CLOSED
        this.dispatchEvent(new Event('close'))
    }
    deliver(maintenance: unknown, topic = 'backup-restore') {
        this.dispatchEvent(
            new MessageEvent('message', {
                data: JSON.stringify({
                    type: 'snapshot',
                    topic,
                    query: {},
                    payload: topic === 'backup-restore' ? { maintenance } : maintenance,
                }),
            }),
        )
    }
}

const originalSocket = globalThis.WebSocket
const uuid = (value: number) => `01900000-0000-7000-8000-${String(value).padStart(12, '0')}`
const user: AuthenticatedUser = {
    id: uuid(1),
    displayName: 'Viewer',
    email: 'viewer@example.test',
    profileImageVersion: null,
    roles: [],
    permissions: [PERMISSIONS.APP_ACCESS],
    language: 'en',
    themeMode: 'dark',
}
const running: ManagedBackupMaintenance = {
    id: uuid(2),
    requestId: uuid(3),
    actorId: uuid(4),
    actorName: 'Kevin',
    artifactId: uuid(5),
    metadataSha256: 'a'.repeat(64),
    operation: 'restore',
    state: 'running',
    phase: 'database',
    progressPercent: 42,
    requestedAt: 1_793_448_000_000,
    startedAt: 1_793_448_000_001,
    finishedAt: null,
    error: null,
}

function DraftRoute() {
    const [count, setCount] = useState(0)
    return (
        <div data-route-state>
            <input aria-label="Unfinished draft" defaultValue="original" />
            <button onClick={() => setCount((value) => value + 1)}>Draft count {count}</button>
        </div>
    )
}

function makeRouter() {
    const rootRoute = createRootRoute({ component: () => <Outlet /> })
    const authenticated = createRoute({
        getParentRoute: () => rootRoute,
        id: '_authenticated',
        beforeLoad: () =>
            identityLoad
                ? identityLoad.then(() => ({ user, sessionId: 'stable-session' }))
                : { user, sessionId: 'stable-session' },
        component: () => <AuthenticatedLayout user={user} />,
    })
    const route = createRoute({
        getParentRoute: () => authenticated,
        path: '/proxy-hosts',
        validateSearch: (value) => ({
            filter: String(value.filter ?? ''),
            page: Number(value.page ?? 1),
        }),
        component: DraftRoute,
    })
    return createRouter({
        routeTree: rootRoute.addChildren([authenticated.addChildren([route])]),
        history: createMemoryHistory({
            initialEntries: ['/proxy-hosts?filter=kept&page=3#details'],
        }),
    })
}

let root: Root | null = null
let client: QueryClient
let router: ReturnType<typeof makeRouter>
let container: HTMLDivElement
const restorers: Array<() => void> = []

async function waitFor(check: () => boolean) {
    for (let attempt = 0; attempt < 150; attempt++) {
        if (check()) return
        await act(async () => {
            await Bun.sleep(10)
        })
    }
    expect(check()).toBeTrue()
}
async function mount() {
    router = makeRouter()
    client = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } })
    container = document.createElement('div')
    document.body.append(container)
    root = createRoot(container)
    await act(async () => {
        root!.render(
            withTestLanguage(
                <QueryClientProvider client={client}>
                    <RouterProvider router={router} />
                </QueryClientProvider>,
            ),
        )
    })
    await waitFor(
        () =>
            !!container.querySelector('[data-route-state]') &&
            !container.querySelector('[data-route-state]')?.closest('[hidden]'),
    )
    await act(async () => FakeSocket.instances.at(-1)!.open())
}
async function deliver(value: unknown, topic?: string) {
    await act(async () => FakeSocket.instances.at(-1)!.deliver(value, topic))
}
function progress() {
    return container.querySelector<HTMLProgressElement>('#restore-progress')
}

beforeEach(() => {
    const success = spyOn(toast, 'success').mockReturnValue('restore-feedback-fixture')
    restorers.push(() => success.mockRestore())
    themeUpdatesAllowed = false
    identityLoad = undefined
    window.location.href = 'http://localhost/proxy-hosts?filter=kept&page=3#details'
    Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' })
    globalThis.WebSocket = FakeSocket as unknown as typeof WebSocket
    FakeSocket.instances = []
})
afterEach(async () => {
    await act(async () => root?.unmount())
    root = null
    for (const restore of restorers.splice(0)) restore()
    client?.clear()
    document.body.replaceChildren()
    globalThis.WebSocket = originalSocket
})

test('APP_ACCESS-only users see the global actor, backup and genuine checkpoint percentage', async () => {
    await mount()
    expect(user.permissions).not.toContain(PERMISSIONS.BACKUPS_VIEW)
    expect(FakeSocket.instances[0]!.sent).toContainEqual({
        type: 'subscribe',
        topic: 'backup-restore',
        query: {},
    })
    await deliver(running)
    await waitFor(() => progress()?.value === 42)
    expect(container.textContent).toContain('Kevin')
    expect(container.textContent).toContain(running.artifactId)
    expect(container.textContent).toContain('42%')
    expect(container.textContent).toContain('Restoring the database')
    expect(container.querySelector('[data-route-state]')?.closest('[hidden]')).not.toBeNull()
})

test.each(['failed', 'interrupted'] as const)(
    'a historical %s restore blocks the page without inventing details or progress',
    async (state) => {
        await mount()
        const historical = {
            id: running.id,
            operation: 'restore',
            state,
            phase: 'database',
            detailsUnavailable: true,
        }
        await deliver(historical)
        expect(container.querySelector('#restore-progress-title')).not.toBeNull()
        expect(container.textContent).toContain(
            'Restore failed or service recovery could not be confirmed',
        )
        expect(container.textContent).toContain('Restoring the database')
        expect(progress()).toBeNull()
        expect(container.textContent).not.toContain('Kevin')
        expect(container.textContent).not.toContain(running.artifactId)
        expect(container.textContent).not.toContain('0%')
        expect(container.querySelector('main button')).toBeNull()
        expect(container.querySelector('[data-route-state]')?.closest('[hidden]')).not.toBeNull()
        await deliver(null)
        await deliver({ ...historical, phase: 'validating' })
        await deliver({ ...historical, actorName: 'Invented operator', progressPercent: 0 })
        expect(container.querySelector('#restore-progress-title')).not.toBeNull()
        expect(container.textContent).toContain('Restoring the database')
        expect(container.textContent).not.toContain('Invented operator')
        expect(progress()).toBeNull()
    },
)

test('genuine full metadata enriches a historical restore and a later ID-only snapshot cannot erase it', async () => {
    await mount()
    const historical = {
        id: running.id,
        operation: 'restore',
        state: 'failed',
        phase: 'database',
        detailsUnavailable: true,
    }
    await deliver(historical)
    expect(container.querySelector('#restore-progress-title')).not.toBeNull()
    await deliver({ ...running, state: 'failed', error: 'execution_failed' })
    expect(progress()?.value).toBe(42)
    expect(container.textContent).toContain('Kevin')
    await deliver(historical)
    expect(progress()?.value).toBe(42)
    expect(container.textContent).toContain(running.artifactId)
})

test('a genuine later full recovery can retire a historical blocker despite missed intermediate snapshots', async () => {
    await mount()
    const historical = {
        id: running.id,
        operation: 'restore',
        state: 'failed',
        phase: 'database',
        detailsUnavailable: true,
    }
    const complete = {
        ...running,
        id: uuid(95),
        state: 'succeeded',
        phase: 'complete',
        progressPercent: 100,
    }
    const success = spyOn(toast, 'success').mockReturnValue('restore-feedback-fixture')
    const recovery = Promise.withResolvers<void>()
    const invalidate = spyOn(router, 'invalidate').mockImplementation(() => recovery.promise)
    restorers.push(() => {
        success.mockRestore()
        invalidate.mockRestore()
    })
    await deliver(historical)
    expect(container.querySelector('#restore-progress-title')).not.toBeNull()
    jest.useFakeTimers()
    try {
        await deliver(complete)
        expect(progress()?.value).toBe(100)
        expect(invalidate).toHaveBeenCalledTimes(1)
        await deliver(historical)
        expect(progress()?.value).toBe(100)
        await act(async () => jest.advanceTimersByTime(RESTORE_COMPLETION_VISIBLE_MS))
        expect(container.querySelector('#restore-progress-title')).not.toBeNull()
        await act(async () => recovery.resolve())
        expect(container.querySelector('#restore-progress-title')).toBeNull()
        expect(success).toHaveBeenCalledTimes(1)
        const invalidationsAfterRecovery = invalidate.mock.calls.length
        await deliver(historical)
        await deliver(running)
        await deliver(complete)
        expect(container.querySelector('#restore-progress-title')).toBeNull()
        expect(container.querySelector('[data-route-state]')?.closest('[hidden]')).toBeNull()
        expect(invalidate).toHaveBeenCalledTimes(invalidationsAfterRecovery)
    } finally {
        recovery.resolve()
        jest.useRealTimers()
    }
})

async function recoveryPreflightSnapshot(
    fresh: boolean,
    terminal: 'preflight' | 'complete' = 'preflight',
) {
    const output = await runBunScript(
        `
        import assert from 'node:assert/strict'
        import { createHash } from 'node:crypto'
        import { mock } from 'bun:test'
        const original = ${JSON.stringify(running)}
        const unsafe = { id: original.id, operation: 'restore', state: 'failed', phase: 'database' }
        const complete = { ...original, id: '${uuid(91)}', state: 'succeeded', phase: 'complete', progressPercent: 100, finishedAt: original.startedAt+1000 }
        const failed = { ...original, id: '${uuid(92)}', state: 'failed', phase: 'validating', progressPercent: 5, error: 'artifact_invalid', finishedAt: original.startedAt+2000 }
        const token = 'a'.repeat(43)
        let publication = { maintenance: { ...original, id: '${uuid(90)}', operation: 'delete', state: 'succeeded', phase: 'complete', progressPercent: 100, finishedAt: original.startedAt+500 }, restoreResults: [unsafe] }
        let controllerReads = 0
        mock.module('@tanstack/react-start/server-only', () => ({}))
        await import('node:fs/promises')
        mock.module('node:fs/promises', () => ({
            open: async path => path.endsWith('/progress.json') ? {
                stat: async () => ({ uid: 0, nlink: 1, mode: 0o100440, size: JSON.stringify(publication).length, isFile: () => true }),
                read: async buffer => ({ bytesRead: buffer.write(JSON.stringify(publication)) }), close: async () => {},
            } : { writeFile: async () => {}, sync: async () => {}, close: async () => {} },
            rename: async () => {}, unlink: async () => {},
        }))
        mock.module('./db/index.ts', () => ({ db: { execute: async () => [{ tokenHash: createHash('sha256').update(token).digest('hex'), expiresAt: new Date(Date.now()+60000) }] }, closeDatabaseForRestoreMaintenance: async () => {} }))
        mock.module('./server/Foundation/worker-lifecycle.server.ts', () => ({ stopWorkersForRestoreMaintenance: async () => {} }))
        mock.module('./server/env.server.ts', () => ({ getPublicOrigin: () => 'https://proxy.example' }))
        mock.module('./server/Auth/Access/authorization.service.ts', () => ({ requirePermissionService: async permission => { assert.equal(permission, 'app.access') }, requireSessionService: async () => { throw new Error('Unexpected session read') }, requireSessionPermission: async () => {} }))
        mock.module('./server/WebSockets/applicationChanges.ts', () => ({ getApplicationRevision: () => 'current' }))
        mock.module('./server/Admin/Backups/backups-demo.server.ts', () => ({ getLocalBackupDemoStatus: async () => null }))
        mock.module('./server/Controller/backups.server.ts', () => ({ backupsController: { maintenance: async () => { controllerReads++; return publication.maintenance } } }))
        const modules = {
            './server/Admin/Backups/backups.service.ts': ['getBackupsStatusService'],
            './server/Admin/ProxyAccessLogs/proxy-access-logs.service.ts': ['getProxyAccessLogsService'],
            './server/Audit/audit-reader.service.ts': ['listAuditEventsService'],
            './server/Foundation/health.service.ts': ['checkFoundationHealthService'],
            './server/Admin/CertificateManagement/certificates.service.ts': ['getCertificatesService'],
            './server/Admin/DnsConnections/dns-connections.service.ts': ['getDnsConnectionsService'],
            './server/Admin/Ddns/ddns.service.ts': ['getDdnsStatusService'],
            './server/Admin/DockerDiscovery/discovery-management.service.ts': ['getDiscoveryOverviewService'],
            './server/WebSockets/proxy-live-revision.server.ts': ['getProxyLiveRevisionService'],
            './server/ForwardAuth/forward-auth-summary.service.ts': ['getForwardAuthSummaryService'],
            './server/Admin/CrowdSec/crowdsec.service.ts': ['getCrowdSecConfigurationService', 'getCrowdSecDashboardService'],
        }
        for (const [path, names] of Object.entries(modules)) mock.module(path, () => Object.fromEntries(names.map(name => [name, async () => { throw new Error('Unrelated service called') }])))
        const runtime = await import('./server/Backups/restore-maintenance.server.ts')
        const { getLiveSnapshotResponse } = await import('./server/WebSockets/realtimeSnapshots.service.ts')
        if (!${fresh}) {
            assert.deepEqual(await runtime.getActiveRestoreMaintenance(), { ...unsafe, detailsUnavailable: true })
            for (let tick = 0; tick < 40; tick++) await Promise.resolve()
        }
        publication = { maintenance: '${terminal}' === 'complete' ? complete : failed, restoreResults: [unsafe, { id: complete.id, operation: 'restore', state: 'succeeded', phase: 'complete' }, ...('${terminal}' === 'complete' ? [] : [{ id: failed.id, operation: 'restore', state: 'failed', phase: 'validating' }])] }
        const request = new Request('https://proxy.example/api/live-snapshot?topic=backup-restore', { headers: { origin: 'https://proxy.example', cookie: 'rentnerproxy_session='+token } })
        const response = await getLiveSnapshotResponse(request)
        assert.equal(response.status, 200)
        const payload = await response.json()
        assert.equal(runtime.isRestoreMaintenanceFenced(), !${fresh})
        assert.equal(controllerReads, ${fresh} ? 1 : 0)
        assert.equal((await runtime.withRestoreMaintenanceFence(new Request('https://proxy.example/change', { method: 'POST' }), async () => new Response())).status, ${fresh} ? 200 : 503)
        console.log(JSON.stringify(payload))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    return JSON.parse(output.trim().split('\n').at(-1)!) as { maintenance: unknown }
}

test('historical provenance survives full failure details and accepts a verified later complete recovery', async () => {
    const [blocked, recovered] = await Promise.all([
        recoveryPreflightSnapshot(false),
        recoveryPreflightSnapshot(true, 'complete'),
    ])
    await mount()
    const gate = Promise.withResolvers<void>()
    const success = spyOn(toast, 'success').mockReturnValue('verified-recovery')
    const invalidate = spyOn(router, 'invalidate').mockImplementation(() => gate.promise)
    restorers.push(() => {
        success.mockRestore()
        invalidate.mockRestore()
    })
    await deliver(blocked.maintenance)
    await deliver({ ...running, state: 'failed', error: 'execution_failed' })
    expect(progress()?.value).toBe(42)
    jest.useFakeTimers()
    try {
        await deliver(recovered.maintenance)
        expect(progress()?.value).toBe(100)
        expect(invalidate).toHaveBeenCalledTimes(1)
        expect(success).not.toHaveBeenCalled()
        await act(async () => gate.resolve())
        await act(async () => jest.advanceTimersByTime(RESTORE_COMPLETION_VISIBLE_MS))
        expect(container.querySelector('#restore-progress-title')).toBeNull()
        expect(success).toHaveBeenCalledTimes(1)
        await deliver(blocked.maintenance)
        await deliver(running)
        expect(container.querySelector('#restore-progress-title')).toBeNull()
    } finally {
        gate.resolve()
        jest.useRealTimers()
    }
})

test('a failed preflight refresh retries without another deduplicated snapshot and keeps dismissal blocked', async () => {
    const [blocked, recovered] = await Promise.all([
        recoveryPreflightSnapshot(false),
        recoveryPreflightSnapshot(true),
    ])
    await mount()
    const gate = Promise.withResolvers<void>()
    const success = spyOn(toast, 'success').mockReturnValue('unused')
    const invalidate = spyOn(router, 'invalidate')
        .mockRejectedValueOnce(new Error('Temporary refresh failure'))
        .mockImplementation(() => gate.promise)
    restorers.push(() => {
        success.mockRestore()
        invalidate.mockRestore()
    })
    await deliver(blocked.maintenance)
    jest.useFakeTimers()
    try {
        await deliver(recovered.maintenance)
        expect(invalidate).toHaveBeenCalledTimes(1)
        expect(container.querySelector('main button')).toBeNull()
        await act(async () => jest.advanceTimersByTime(999))
        expect(invalidate).toHaveBeenCalledTimes(1)
        await act(async () => jest.advanceTimersByTime(1))
        expect(invalidate).toHaveBeenCalledTimes(2)
        expect(container.querySelector('main button')).toBeNull()
        expect(container.querySelector('[data-route-state]')?.closest('[hidden]')).not.toBeNull()
        await act(async () => gate.resolve())
        expect(progress()?.value).toBe(5)
        expect(container.querySelector('main button')).not.toBeNull()
        expect(success).not.toHaveBeenCalled()
    } finally {
        gate.resolve()
        jest.useRealTimers()
    }
})

test('refresh retries coalesce and back off to thirty seconds, then unmount cancels the owned wake', async () => {
    const [blocked, recovered] = await Promise.all([
        recoveryPreflightSnapshot(false),
        recoveryPreflightSnapshot(true),
    ])
    await mount()
    const invalidate = spyOn(router, 'invalidate').mockRejectedValue(
        new Error('Refresh remains unavailable'),
    )
    restorers.push(() => invalidate.mockRestore())
    await deliver(blocked.maintenance)
    jest.useFakeTimers()
    try {
        await deliver(recovered.maintenance)
        let attempts = 1
        for (const wait of [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]) {
            const timers = jest.getTimerCount()
            await deliver(recovered.maintenance)
            expect(invalidate).toHaveBeenCalledTimes(attempts)
            expect(jest.getTimerCount()).toBe(timers)
            await act(async () => jest.advanceTimersByTime(wait - 1))
            expect(invalidate).toHaveBeenCalledTimes(attempts)
            await deliver(recovered.maintenance)
            expect(invalidate).toHaveBeenCalledTimes(attempts)
            expect(jest.getTimerCount()).toBe(timers)
            await act(async () => jest.advanceTimersByTime(1))
            expect(invalidate).toHaveBeenCalledTimes(++attempts)
            expect(progress()).toBeNull()
            expect(container.querySelector('main button')).toBeNull()
            expect(
                container.querySelector('[data-route-state]')?.closest('[hidden]'),
            ).not.toBeNull()
        }
        await act(async () => root?.unmount())
        root = null
        await act(async () => jest.advanceTimersByTime(60_000))
        expect(invalidate).toHaveBeenCalledTimes(attempts)
    } finally {
        jest.useRealTimers()
    }
})

test('a newer restore cancels a failed refresh wake and clears the old historical recovery eligibility', async () => {
    const [blocked, recovered] = await Promise.all([
        recoveryPreflightSnapshot(false),
        recoveryPreflightSnapshot(true),
    ])
    await mount()
    const invalidate = spyOn(router, 'invalidate').mockRejectedValue(
        new Error('Refresh unavailable'),
    )
    restorers.push(() => invalidate.mockRestore())
    await deliver(blocked.maintenance)
    jest.useFakeTimers()
    try {
        await deliver(recovered.maintenance)
        const newer = { ...running, id: uuid(93), progressPercent: 44 }
        await deliver(newer)
        await act(async () => jest.advanceTimersByTime(60_000))
        expect(invalidate).toHaveBeenCalledTimes(1)
        expect(progress()?.value).toBe(44)
        await deliver(recovered.maintenance)
        await deliver({
            ...running,
            id: uuid(94),
            state: 'succeeded',
            phase: 'complete',
            progressPercent: 100,
        })
        expect(progress()?.value).toBe(44)
        expect(invalidate).toHaveBeenCalledTimes(1)
    } finally {
        jest.useRealTimers()
    }
})

test('a failed completed-recovery refresh retries without another snapshot and waits for actual route recovery', async () => {
    const [blocked, recovered] = await Promise.all([
        recoveryPreflightSnapshot(false),
        recoveryPreflightSnapshot(true, 'complete'),
    ])
    await mount()
    const gate = Promise.withResolvers<void>()
    const success = spyOn(toast, 'success').mockReturnValue('verified-recovery')
    const invalidate = spyOn(router, 'invalidate')
        .mockRejectedValueOnce(new Error('Temporary refresh failure'))
        .mockImplementation(() => gate.promise)
    restorers.push(() => {
        success.mockRestore()
        invalidate.mockRestore()
    })
    await deliver(blocked.maintenance)
    await deliver({ ...running, state: 'failed', error: 'execution_failed' })
    jest.useFakeTimers()
    try {
        await deliver(recovered.maintenance)
        expect(progress()?.value).toBe(100)
        expect(success).not.toHaveBeenCalled()
        await act(async () => jest.advanceTimersByTime(1_000))
        expect(invalidate).toHaveBeenCalledTimes(2)
        await act(async () => jest.advanceTimersByTime(RESTORE_COMPLETION_VISIBLE_MS))
        expect(container.querySelector('#restore-progress-title')).not.toBeNull()
        expect(success).not.toHaveBeenCalled()
        await act(async () => gate.resolve())
        expect(container.querySelector('#restore-progress-title')).toBeNull()
        expect(success).toHaveBeenCalledTimes(1)
    } finally {
        gate.resolve()
        jest.useRealTimers()
    }
})

test('a root-proven recovery hidden by a later preflight failure refreshes caches before dismissal without a success toast', async () => {
    const [blocked, recovered] = await Promise.all([
        recoveryPreflightSnapshot(false),
        recoveryPreflightSnapshot(true),
    ])
    await mount()
    const route = container.querySelector('[data-route-state]')!
    client.setQueryData(['before-hidden-recovery'], 'stale')
    const gate = Promise.withResolvers<void>()
    const success = spyOn(toast, 'success').mockReturnValue('unused')
    const invalidate = spyOn(router, 'invalidate').mockImplementation(() => gate.promise)
    const cancel = spyOn(client, 'cancelQueries')
    const clear = spyOn(client, 'clear')
    restorers.push(() => {
        success.mockRestore()
        invalidate.mockRestore()
        cancel.mockRestore()
        clear.mockRestore()
    })
    try {
        await deliver(blocked.maintenance)
        expect(container.querySelector('#restore-progress-title')).not.toBeNull()
        await deliver(recovered.maintenance)
        await deliver(recovered.maintenance)
        expect(progress()).toBeNull()
        expect(cancel).toHaveBeenCalledTimes(1)
        expect(clear).toHaveBeenCalledTimes(1)
        expect(client.getQueryData(['before-hidden-recovery'])).toBeUndefined()
        expect(invalidate).toHaveBeenCalledTimes(1)
        expect(container.querySelector('main button')).toBeNull()
        expect(route.closest('[hidden]')).not.toBeNull()
        await deliver(blocked.maintenance)
        expect(progress()).toBeNull()
        await act(async () => gate.resolve())
        expect(progress()?.value).toBe(5)
        const dismiss = container.querySelector<HTMLButtonElement>('main button')
        expect(dismiss).not.toBeNull()
        expect(success).not.toHaveBeenCalled()
        await act(async () => dismiss!.click())
        expect(route.closest('[hidden]')).toBeNull()
        await deliver(blocked.maintenance)
        await deliver(recovered.maintenance)
        expect(container.querySelector('#restore-progress-title')).toBeNull()
        expect(success).not.toHaveBeenCalled()
    } finally {
        gate.resolve()
    }
})

test('failed hidden-recovery refresh retains the blocker and retries the same root-authorized snapshot', async () => {
    const [blocked, recovered] = await Promise.all([
        recoveryPreflightSnapshot(false),
        recoveryPreflightSnapshot(true),
    ])
    await mount()
    const gate = Promise.withResolvers<void>()
    const invalidate = spyOn(router, 'invalidate')
        .mockRejectedValueOnce(new Error('Refresh unavailable'))
        .mockImplementation(() => gate.promise)
    restorers.push(() => invalidate.mockRestore())
    await deliver(blocked.maintenance)
    jest.useFakeTimers()
    try {
        await deliver(recovered.maintenance)
        expect(invalidate).toHaveBeenCalledTimes(1)
        expect(progress()).toBeNull()
        expect(container.querySelector('main button')).toBeNull()
        expect(container.querySelector('[data-route-state]')?.closest('[hidden]')).not.toBeNull()
        const timers = jest.getTimerCount()
        await deliver(recovered.maintenance)
        expect(invalidate).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(timers)
        await act(async () => jest.advanceTimersByTime(999))
        await deliver(recovered.maintenance)
        expect(invalidate).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(timers)
        await act(async () => jest.advanceTimersByTime(1))
        expect(invalidate).toHaveBeenCalledTimes(2)
        await act(async () => gate.resolve())
        expect(progress()?.value).toBe(5)
        expect(container.querySelector('main button')).not.toBeNull()
    } finally {
        gate.resolve()
        jest.useRealTimers()
    }
})

test.each(['failed complete refresh', 'historical blocker'] as const)(
    'a newer restore inherits the refresh obligation from %s before any preflight dismissal',
    async (origin) => {
        const blocked =
            origin === 'historical blocker' ? await recoveryPreflightSnapshot(false) : null
        await mount()
        const gate = Promise.withResolvers<void>()
        const success = spyOn(toast, 'success').mockReturnValue('unused')
        const invalidate = spyOn(router, 'invalidate')
        if (origin === 'failed complete refresh')
            invalidate.mockRejectedValueOnce(new Error('Refresh failed'))
        invalidate.mockImplementation(() => gate.promise)
        restorers.push(() => {
            success.mockRestore()
            invalidate.mockRestore()
        })
        if (blocked) await deliver(blocked.maintenance)
        else await deliver(running)
        jest.useFakeTimers()
        try {
            if (!blocked) {
                await deliver({
                    ...running,
                    state: 'succeeded',
                    phase: 'complete',
                    progressPercent: 100,
                })
                expect(invalidate).toHaveBeenCalledTimes(1)
            }
            const newer = { ...running, id: uuid(97), phase: 'validating', progressPercent: 5 }
            await deliver(newer)
            await deliver({ ...newer, state: 'failed', error: 'artifact_invalid' })
            expect(container.querySelector('main button') === null).toBe(true)
            expect(
                container.querySelector('[data-route-state]')?.closest('[hidden]'),
            ).not.toBeNull()
            expect(invalidate).toHaveBeenCalledTimes(blocked ? 1 : 2)
            expect(success).not.toHaveBeenCalled()
            await act(async () => gate.resolve())
            expect(container.querySelector('main button')).not.toBeNull()
            expect(success).not.toHaveBeenCalled()
        } finally {
            gate.resolve()
            jest.useRealTimers()
        }
    },
)

test('hidden-recovery refresh cannot overwrite a newer restore or revive its retired predecessor', async () => {
    const [blocked, recovered] = await Promise.all([
        recoveryPreflightSnapshot(false),
        recoveryPreflightSnapshot(true),
    ])
    await mount()
    const gate = Promise.withResolvers<void>()
    const invalidate = spyOn(router, 'invalidate').mockImplementation(() => gate.promise)
    restorers.push(() => invalidate.mockRestore())
    try {
        await deliver(blocked.maintenance)
        await deliver(recovered.maintenance)
        expect(invalidate).toHaveBeenCalledTimes(1)
        const newer = { ...running, id: uuid(93), progressPercent: 43 }
        await deliver(newer)
        expect(progress()?.value).toBe(43)
        await act(async () => gate.resolve())
        expect(progress()?.value).toBe(43)
        await deliver(recovered.maintenance)
        await deliver(blocked.maintenance)
        expect(progress()?.value).toBe(43)
        expect(container.querySelector('main button')).toBeNull()
        expect(invalidate).toHaveBeenCalledTimes(1)
    } finally {
        gate.resolve()
    }
})

test('restore preserves the route, search, scroll and mounted drafts until confirmed cache refresh completes', async () => {
    await mount()
    const originalDocumentScroll = Object.getOwnPropertyDescriptor(window, 'scrollY')
    const originalDocumentHeight = Object.getOwnPropertyDescriptor(
        document.documentElement,
        'scrollHeight',
    )
    const originalViewportHeight = Object.getOwnPropertyDescriptor(window, 'innerHeight')
    const originalObserver = globalThis.ResizeObserver
    let documentTop = 1500
    let documentHeight = 4000
    let layoutChanged: (() => void) | undefined
    Object.defineProperty(document.documentElement, 'scrollHeight', {
        configurable: true,
        get: () => documentHeight,
    })
    Object.defineProperty(window, 'innerHeight', { configurable: true, value: 1000 })
    globalThis.ResizeObserver = class {
        constructor(callback: ResizeObserverCallback) {
            layoutChanged = () => callback([], this as unknown as ResizeObserver)
        }
        observe() {}
        disconnect() {}
    } as unknown as typeof ResizeObserver
    Object.defineProperty(window, 'scrollY', { configurable: true, get: () => documentTop })
    const documentScroll = spyOn(window, 'scrollTo').mockImplementation((options) => {
        if (typeof options === 'object')
            documentTop = Math.min(
                options.top ?? documentTop,
                Math.max(0, documentHeight - window.innerHeight),
            )
    })
    restorers.push(() => {
        documentScroll.mockRestore()
        if (originalDocumentScroll) Object.defineProperty(window, 'scrollY', originalDocumentScroll)
        else Reflect.deleteProperty(window, 'scrollY')
        if (originalDocumentHeight)
            Object.defineProperty(document.documentElement, 'scrollHeight', originalDocumentHeight)
        else Reflect.deleteProperty(document.documentElement, 'scrollHeight')
        if (originalViewportHeight)
            Object.defineProperty(window, 'innerHeight', originalViewportHeight)
        else Reflect.deleteProperty(window, 'innerHeight')
        globalThis.ResizeObserver = originalObserver
    })
    const routeState = container.querySelector<HTMLElement>('[data-route-state]')!
    const draft = routeState.querySelector('input')!
    draft.value = 'unfinished work'
    routeState.scrollTop = 327
    await act(async () => routeState.querySelector('button')!.click())
    const originalHref = router.state.location.href
    client.setQueryData(['before-restore'], { stale: true })
    const calls: string[] = []
    const clear = client.clear.bind(client)
    const clearSpy = spyOn(client, 'clear').mockImplementation(() => {
        calls.push('clear')
        documentHeight = 1100
        clear()
    })
    const cancelSpy = spyOn(client, 'cancelQueries').mockImplementation(async () => {
        calls.push('cancel')
    })
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
        release = resolve
    })
    const invalidate = spyOn(router, 'invalidate').mockImplementation(async () => {
        calls.push('invalidate')
        expect(client.getQueryData(['before-restore'])).toBeUndefined()
        await gate
    })
    const navigate = spyOn(router, 'navigate')
    restorers.push(() => {
        clearSpy.mockRestore()
        cancelSpy.mockRestore()
        invalidate.mockRestore()
        navigate.mockRestore()
    })
    await deliver(running)
    await waitFor(() => progress()?.value === 42)
    documentTop = 0
    expect(documentScroll).not.toHaveBeenCalled()
    expect(calls).toEqual([])
    await deliver({
        ...running,
        state: 'succeeded',
        phase: 'complete',
        progressPercent: 100,
        finishedAt: running.startedAt! + 1000,
    })
    await waitFor(() => calls.includes('invalidate'))
    expect(calls.slice(0, 3)).toEqual(['cancel', 'clear', 'invalidate'])
    expect(progress()?.value).toBe(100)
    expect(router.state.location.href).toBe(originalHref)
    expect(routeState.scrollTop).toBe(327)
    await act(async () => release())
    await waitFor(() => !progress())
    await waitFor(() => !routeState.closest('[hidden]'))
    expect(router.state.location.href).toBe(originalHref)
    expect(router.state.location.search).toEqual({ filter: 'kept', page: 3 })
    expect(navigate).not.toHaveBeenCalled()
    expect(container.querySelector('[data-route-state]')).toBe(routeState)
    expect(container.querySelector('input')).toBe(draft)
    expect(draft.value).toBe('unfinished work')
    expect(routeState.textContent).toContain('Draft count 1')
    expect(routeState.scrollTop).toBe(327)
    expect(documentScroll).not.toHaveBeenCalled()
    documentHeight = 4000
    await act(async () => layoutChanged!())
    expect(documentTop).toBe(1500)
})

test('empty, invalid and disconnected snapshots retain the last confirmed stage without advancing', async () => {
    await mount()
    const earlier = {
        ...running,
        id: uuid(98),
        requestedAt: running.requestedAt - 1,
        progressPercent: 5,
    }
    await deliver(earlier)
    await waitFor(() => progress()?.value === 5)
    await deliver(running)
    await waitFor(() => progress()?.value === 42)
    await deliver(null)
    await deliver({ ...running, progressPercent: 101 })
    await deliver({ ...running, operation: 'delete', progressPercent: 100, state: 'succeeded' })
    await deliver({ ...running, phase: 'validating', progressPercent: 5 })
    await deliver(earlier)
    expect(progress()?.value).toBe(42)
    await act(async () => FakeSocket.instances.at(-1)!.close())
    await waitFor(
        () => container.textContent?.includes('completion has not yet been confirmed') === true,
    )
    expect(progress()?.value).toBe(42)
    expect(container.textContent).not.toContain('Restore and service recovery confirmed')
})

test('a new recovery operation remains visible after the appliance clock moves backwards', async () => {
    await mount()
    await deliver(running)
    await waitFor(() => progress()?.value === 42)
    await deliver({ ...running, state: 'failed', error: 'execution_failed' })
    const recovery = {
        ...running,
        id: uuid(97),
        artifactId: uuid(96),
        requestedAt: 1,
        phase: 'preparing',
        progressPercent: 1,
    }
    await deliver(recovery)
    await waitFor(() => progress()?.value === 1)
    expect(container.textContent).toContain(recovery.artifactId)
    await deliver(running)
    expect(progress()?.value).toBe(1)
    await deliver({ ...recovery, phase: 'database', progressPercent: 30 })
    await waitFor(() => progress()?.value === 30)
})

test('confirmed 100 percent stays visible before one localized completion toast, with no navigation', async () => {
    await mount()
    const success = spyOn(toast, 'success').mockReturnValue('restore-feedback-fixture')
    const recovery = Promise.withResolvers<void>()
    const invalidate = spyOn(router, 'invalidate').mockImplementation(() => recovery.promise)
    restorers.push(() => {
        success.mockRestore()
        invalidate.mockRestore()
    })
    await deliver(running)
    await waitFor(() => progress()?.value === 42)
    jest.useFakeTimers()
    try {
        const complete = { ...running, state: 'succeeded', phase: 'complete', progressPercent: 100 }
        await deliver(complete)
        await deliver(complete)
        expect(progress()?.value).toBe(100)
        expect(success).not.toHaveBeenCalled()
        expect(invalidate).toHaveBeenCalledTimes(1)
        await act(async () => jest.advanceTimersByTime(RESTORE_COMPLETION_VISIBLE_MS - 1))
        expect(progress()?.value).toBe(100)
        await act(async () => jest.advanceTimersByTime(1))
        expect(progress()?.value).toBe(100)
        expect(success).not.toHaveBeenCalled()
        await act(async () => recovery.resolve())
        expect(progress()).toBeNull()
        expect(success).toHaveBeenCalledTimes(1)
        expect(success.mock.calls[0]?.[0]).toBe(catalogs.en.admin.backups.messages.restored)
        expect(router.state.location.pathname).toBe('/proxy-hosts')
    } finally {
        recovery.resolve()
        jest.useRealTimers()
    }
})

test('terminal failures stay visible and unrelated or historical successes cannot dismiss them', async () => {
    await mount()
    const clear = spyOn(client, 'clear')
    const invalidate = spyOn(router, 'invalidate')
    restorers.push(() => {
        clear.mockRestore()
        invalidate.mockRestore()
    })
    await deliver({ ...running, state: 'succeeded', phase: 'complete', progressPercent: 100 })
    await deliver({ ...running, state: 'failed', error: 'execution_failed' })
    expect(progress()).toBeNull()
    await deliver(running)
    await waitFor(() => progress()?.value === 42)
    await deliver({
        ...running,
        state: 'failed',
        error: 'execution_failed',
        finishedAt: running.startedAt! + 1000,
    })
    await waitFor(
        () =>
            container.textContent?.includes(
                'Restore failed or service recovery could not be confirmed',
            ) === true,
    )
    await deliver({
        ...running,
        id: uuid(99),
        state: 'succeeded',
        phase: 'complete',
        progressPercent: 100,
    })
    await deliver(null)
    expect(progress()?.value).toBe(42)
    expect(clear).not.toHaveBeenCalled()
    expect(invalidate).not.toHaveBeenCalled()
})

test('application identity snapshots are suspended throughout the destructive restore', async () => {
    await mount()
    const socket = FakeSocket.instances.at(-1)!
    expect(socket.sent).toContainEqual({ type: 'subscribe', topic: 'app-events', query: {} })
    await deliver({ revision: 'before', userVersion: 'viewer' }, 'app-events')
    await deliver(running)
    await waitFor(() => progress()?.value === 42)
    expect(socket.sent).toContainEqual({ type: 'unsubscribe', topic: 'app-events' })
    const invalidate = spyOn(router, 'invalidate')
    restorers.push(() => invalidate.mockRestore())
    await deliver({ revision: 'restored-database', userVersion: 'different' }, 'app-events')
    expect(invalidate).not.toHaveBeenCalled()
    expect(progress()?.value).toBe(42)
})

test('brief identity checks never show the loading screen and each check gets a fresh buffer', async () => {
    await mount()
    await deliver({ revision: 'before', userVersion: 'viewer' }, 'app-events')
    const first = Promise.withResolvers<void>()
    const second = Promise.withResolvers<void>()
    const invalidate = spyOn(router, 'invalidate')
        .mockImplementationOnce(() => first.promise)
        .mockImplementationOnce(() => second.promise)
    restorers.push(() => invalidate.mockRestore())
    jest.useFakeTimers()
    try {
        await deliver({ revision: 'first', userVersion: 'viewer' }, 'app-events')
        expect(container.querySelector('[data-route-state]')?.closest('[hidden]')).not.toBeNull()
        await act(async () => jest.advanceTimersByTime(900))
        expect(container.querySelector('h1')).toBeNull()
        await act(async () => first.resolve())
        expect(container.querySelector('[data-route-state]')?.closest('[hidden]')).toBeNull()

        await deliver({ revision: 'second', userVersion: 'viewer' }, 'app-events')
        await act(async () => jest.advanceTimersByTime(100))
        expect(container.querySelector('h1')).toBeNull()
        await act(async () => jest.advanceTimersByTime(899))
        expect(container.querySelector('h1')).toBeNull()
        await act(async () => jest.advanceTimersByTime(1))
        expect(container.querySelector('h1')?.textContent).toBe(catalogs.en.system.loading.title)

        await act(async () => second.resolve())
        expect(container.querySelector('h1')).toBeNull()
        expect(container.querySelector('[data-route-state]')?.closest('[hidden]')).toBeNull()
        await act(async () => jest.advanceTimersByTime(2_000))
        expect(container.querySelector('h1')).toBeNull()
    } finally {
        await act(async () => {
            first.resolve()
            second.resolve()
        })
        jest.useRealTimers()
    }
})

test.each(['theme', 'application'] as const)(
    '%s updates keep the mounted page visible during a slow refresh',
    async (kind) => {
        await mount()
        const success = spyOn(toast, 'success').mockReturnValue('theme-feedback-fixture')
        restorers.push(() => success.mockRestore())
        const identityVersion = 'a'.repeat(64)
        await deliver({ revision: 'before', userVersion: 'dark', identityVersion }, 'app-events')
        const routeState = container.querySelector<HTMLElement>('[data-route-state]')!
        const draft = routeState.querySelector('input')!
        draft.value = 'keep this draft'
        themeUpdatesAllowed = true
        const pending = Promise.withResolvers<void>()
        identityLoad = pending.promise
        const expectedTheme = kind === 'theme' ? 'light' : 'dark'
        jest.useFakeTimers()
        try {
            if (kind === 'theme') {
                await act(async () =>
                    container.querySelector<HTMLButtonElement>('[role="switch"]')!.click(),
                )
            }
            expect(
                container.querySelector('[data-application-shell]')?.getAttribute('data-theme'),
            ).toBe(expectedTheme)
            await deliver(
                { revision: 'changed', userVersion: expectedTheme, identityVersion },
                'app-events',
            )
            await act(async () => jest.advanceTimersByTime(1_500))
            expect(routeState.closest('[hidden]')).toBeNull()
            expect(container.querySelector('h1')).toBeNull()
            expect(container.querySelector('[data-route-state]')).toBe(routeState)
            expect(draft.value).toBe('keep this draft')
            await act(async () => pending.resolve())
            await act(async () => jest.advanceTimersByTime(2_000))
            expect(container.querySelector('[data-route-state]')).toBe(routeState)
            expect(routeState.closest('[hidden]')).toBeNull()
            expect(container.querySelector('h1')).toBeNull()
            expect(
                container.querySelector('[data-application-shell]')?.getAttribute('data-theme'),
            ).toBe(expectedTheme)
        } finally {
            await act(async () => pending.resolve())
            jest.useRealTimers()
        }
    },
)

test.each(['pending', 'failed'] as const)(
    'restore takes precedence over a %s identity check and its loading or error screen',
    async (identityState) => {
        await mount()
        await deliver({ revision: 'before', userVersion: 'viewer' }, 'app-events')
        const gate = Promise.withResolvers<void>()
        const invalidate = spyOn(router, 'invalidate').mockImplementation(() => gate.promise)
        restorers.push(() => invalidate.mockRestore())
        try {
            await deliver({ revision: 'changed', userVersion: 'viewer' }, 'app-events')
            await waitFor(
                () =>
                    container.querySelector('h1')?.textContent === catalogs.en.system.loading.title,
            )
            if (identityState === 'failed') {
                await act(async () => gate.reject(new Error('Synthetic identity failure')))
                await waitFor(() => !!container.querySelector('main[role="alert"]'))
            }
            await deliver(running)
            await waitFor(() => progress()?.value === 42)
            expect(container.querySelectorAll('main')).toHaveLength(1)
            expect(container.querySelector('main')?.getAttribute('aria-labelledby')).toBe(
                'restore-progress-title',
            )
            expect(container.textContent).not.toContain(catalogs.en.system.loading.title)
            expect(container.querySelector('main[role="alert"]')).toBeNull()
        } finally {
            await act(async () => gate.resolve())
        }
    },
)

test.each(['queued', 'preparing', 'validating'] as const)(
    'a failed %s preflight can return to the preserved page without claiming successful restore',
    async (phase) => {
        await mount()
        const routeState = container.querySelector<HTMLElement>('[data-route-state]')!
        const draft = routeState.querySelector('input')!
        draft.value = 'preflight draft'
        routeState.scrollTop = 219
        const originalHref = router.state.location.href
        client.setQueryDefaults(['preflight-cache'], { gcTime: Infinity })
        client.setQueryData(['preflight-cache'], { preserved: true })
        const clear = spyOn(client, 'clear')
        restorers.push(() => clear.mockRestore())
        await deliver({
            ...running,
            phase,
            state: phase === 'queued' ? 'queued' : 'running',
            progressPercent: 5,
        })
        await waitFor(() => progress()?.value === 5)
        const failure = {
            ...running,
            phase,
            state: 'failed',
            progressPercent: 5,
            error: 'artifact_invalid',
            finishedAt: running.startedAt! + 1000,
        }
        await deliver(failure)
        const dismissButton = () =>
            container.querySelector<HTMLButtonElement>(
                'main[aria-labelledby="restore-progress-title"] button',
            )
        await waitFor(() => !!dismissButton())
        expect(container.textContent).toContain(
            'Restore failed or service recovery could not be confirmed',
        )
        expect(container.textContent).not.toContain('Restore and service recovery confirmed')
        await act(async () => dismissButton()!.click())
        await waitFor(() => !progress() && !routeState.closest('[hidden]'))
        expect(clear).not.toHaveBeenCalled()
        expect(client.getQueryData(['preflight-cache'])).toEqual({ preserved: true })
        expect(container.querySelector('[data-route-state]')).toBe(routeState)
        expect(container.querySelector('input')).toBe(draft)
        expect(draft.value).toBe('preflight draft')
        expect(routeState.scrollTop).toBe(219)
        expect(router.state.location.href).toBe(originalHref)
        await deliver(failure)
        expect(progress()).toBeNull()
        expect(container.textContent).not.toContain('Restore and service recovery confirmed')
    },
)

test.each(['quiescing', 'database', 'controller', 'crowdsec', 'secrets', 'restarting'] as const)(
    'a failed destructive %s restore keeps its recovery screen without a return button',
    async (phase) => {
        await mount()
        await deliver({ ...running, phase })
        await waitFor(() => !!progress())
        await deliver({
            ...running,
            phase,
            state: 'failed',
            error: 'execution_failed',
            finishedAt: running.startedAt! + 1000,
        })
        await waitFor(
            () =>
                container.textContent?.includes(
                    'Restore failed or service recovery could not be confirmed',
                ) === true,
        )
        expect(
            container.querySelector('main[aria-labelledby="restore-progress-title"] button'),
        ).toBeNull()
        expect(container.querySelector('[data-route-state]')?.closest('[hidden]')).not.toBeNull()
        expect(progress()?.value).toBe(42)
    },
)

test('an unauthorized socket revalidates identity while retaining the unconfirmed restore screen', async () => {
    await mount()
    await deliver(running)
    await waitFor(() => progress()?.value === 42)
    client.setQueryDefaults(['held-restore-cache'], { gcTime: Infinity })
    client.setQueryData(['held-restore-cache'], 'awaiting confirmation')
    const clear = spyOn(client, 'clear')
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
        release = resolve
    })
    const invalidate = spyOn(router, 'invalidate').mockImplementation(async () => {
        await gate
    })
    restorers.push(() => {
        clear.mockRestore()
        invalidate.mockRestore()
    })
    await act(async () =>
        FakeSocket.instances.at(-1)!.dispatchEvent(
            new MessageEvent('message', {
                data: JSON.stringify({ type: 'unauthorized' }),
            }),
        ),
    )
    await waitFor(() => invalidate.mock.calls.length > 0)
    expect(invalidate).toHaveBeenCalledWith({ sync: true })
    expect(progress()?.value).toBe(42)
    expect(clear).not.toHaveBeenCalled()
    expect(client.getQueryData(['held-restore-cache'])).toBe('awaiting confirmation')
    expect(container.textContent).not.toContain('Restore and service recovery confirmed')
    await act(async () => release())
    expect(progress()?.value).toBe(42)
})
