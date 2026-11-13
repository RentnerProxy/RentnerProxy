import { afterEach, beforeEach, describe, expect, mock, spyOn, test } from 'bun:test'
import { GlobalRegistrator } from '@happy-dom/global-registrator'
import type { Root } from 'react-dom/client'
import type { ProxyAccessLogsQuery, ProxyAccessLogsResult } from '@/lib/Admin/ProxyAccessLogs/Types/proxy-access-logs.types.ts'
import type { AuditEventsQuery, AuditEventsResult } from '@/lib/Admin/AuditLogs/Types/audit-events.types.ts'
import { PERMISSIONS } from '@/config/permissions.config.ts'
import { LIVE_READ_RETRY_INITIAL_MS, LIVE_READ_RETRY_MAX_MS } from '@/config/realtime.config.ts'
import withTestLanguage from '@/tests/Helpers/withTestLanguage.tsx'

if (!GlobalRegistrator.isRegistered) GlobalRegistrator.register()
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
const { QueryClient, QueryClientProvider } = await import('@tanstack/react-query')
const { act } = await import('react')
const { createRoot } = await import('react-dom/client')
const { default: withTestPageAlerts } = await import('@/tests/Helpers/withTestPageAlerts.tsx')

let failureStatus: number | null = null
let requests: object[] = []
let historicalReads = 0
function historicalRead(request: object): void {
    requests.push(request)
    historicalReads += 1
    if (failureStatus) throw Object.assign(new Error('Temporary log failure'), { status: failureStatus })
}
const accessRead = mock(async ({ data }: { data: ProxyAccessLogsQuery }): Promise<ProxyAccessLogsResult> => {
    if ((data.offset ?? 0) > 0) historicalRead(data)
    return {
        entries: [{ timestamp: '2026-09-12T12:00:00.000Z', host: 'example.test', method: 'GET',
            path: (data.offset ?? 0) > 0 ? '/historical-record' : '/first-record', status: 200,
            durationMs: 1, clientIp: '192.0.2.1', upstream: '127.0.0.1:8080', bytes: 1, protocol: 'HTTP/2' }],
        total: 31, limit: data.limit ?? 15, offset: data.offset ?? 0, hasMore: true,
        availableHosts: ['example.test'], availableStatuses: [200], truncated: false,
        snapshot: data.snapshot ?? 'stable-snapshot', snapshotExpiresAt: '2099-01-01T00:00:00.000Z', snapshotReset: false,
    }
})
const auditRead = mock(async ({ data }: { data: AuditEventsQuery }): Promise<AuditEventsResult> => {
    if (data.cursor) historicalRead(data)
    return {
        events: [{ id: '018f2f52-7c1b-7cc0-9f3c-6a9952c54023', timestamp: '2026-09-12T12:00:00.000Z',
            actorUserId: null, actorKind: 'system', actorDisplayName: null, action: 'update',
            resource: 'proxy-host', targetId: null, result: 'success',
            metadata: { changedFields: ['upstream'] } }],
        limit: 100, nextCursor: data.cursor ? null : 'stable-cursor', hasMore: !data.cursor,
    }
})
await mock.module('@/features/Admin/ProxyAccessLogs/middleware.ts', () => ({ getProxyAccessLogsHandler: accessRead }))
await mock.module('@/features/Admin/AuditLogs/middleware.ts', () => ({ getAuditEventsHandler: auditRead, getAuditActorOptionsHandler: async () => [] }))
const live = mock((_options: { enabled: boolean }) => 'inactive')
await mock.module('@/shared/Live/Hooks/useLiveQuery.ts', () => ({ default: live }))
const { default: AccessPage } = await import('@/features/Admin/ProxyAccessLogs/index.tsx')
const { default: AuditPage } = await import('@/features/Admin/AuditLogs/index.tsx')

let root: Root
let container: HTMLDivElement
let client: InstanceType<typeof QueryClient>
const realSetTimeout = window.setTimeout.bind(window)
const realClearTimeout = window.clearTimeout.bind(window)
let timerSpy: ReturnType<typeof spyOn<typeof window, 'setTimeout'>>
let clearSpy: ReturnType<typeof spyOn<typeof window, 'clearTimeout'>>
const timers = new Map<unknown, { delay: number; callback: () => void }>()
let nextTimer = -1
async function settle() {
    await act(async () => {
        await new Promise((resolve) => realSetTimeout(resolve, 0))
        await new Promise((resolve) => realSetTimeout(resolve, 0))
    })
}
async function retry() {
    const [handle, timer] = [...timers.entries()][0]!
    timers.delete(handle)
    await act(async () => timer.callback())
    await settle()
}

beforeEach(() => {
    requests = []
    historicalReads = 0
    failureStatus = null
    timers.clear()
    live.mockClear()
    timerSpy = spyOn(window, 'setTimeout').mockImplementation(((callback: () => void, delay?: number) => {
        if (delay !== undefined && delay >= LIVE_READ_RETRY_INITIAL_MS && delay <= LIVE_READ_RETRY_MAX_MS) {
            const handle = nextTimer--
            timers.set(handle, { delay, callback })
            return handle
        }
        return realSetTimeout(callback, delay)
    }) as typeof window.setTimeout)
    clearSpy = spyOn(window, 'clearTimeout').mockImplementation((handle) => {
        if (handle !== undefined && timers.delete(handle)) return
        realClearTimeout(handle as Parameters<typeof realClearTimeout>[0])
    })
    client = new QueryClient({ defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false, refetchOnReconnect: false } } })
    container = document.createElement('div')
    document.body.append(container)
    root = createRoot(container)
})
afterEach(async () => {
    await act(async () => root.unmount())
    client.clear()
    container.remove()
    timerSpy.mockRestore()
    clearSpy.mockRestore()
    timers.clear()
})

describe.each([
    { name: 'access logs', Page: AccessPage, permission: PERMISSIONS.PROXY_ACCESS_LOGS_VIEW, next: 'Go to next page' },
    { name: 'audit logs', Page: AuditPage, permission: PERMISSIONS.AUDIT_LOGS_VIEW, next: 'Next page' },
])('$name historical recovery', ({ Page, permission, next }) => {
    async function openHistorical(status: number) {
        await act(async () => root.render(withTestLanguage(withTestPageAlerts(
            <QueryClientProvider client={client}><Page permissions={[permission]} /></QueryClientProvider>,
        ))))
        await settle()
        const button = container.querySelector<HTMLButtonElement>(`button[aria-label="${next}"]`)
        expect(button?.disabled).toBe(false)
        expect(timers.size).toBe(0)
        failureStatus = status
        await act(async () => button!.click())
        await settle()
        expect(historicalReads).toBe(1)
        expect(live.mock.lastCall?.[0].enabled).toBe(false)
    }

    test('recovers transient failures in place with bounded backoff and the same snapshot/cursor', async () => {
        await openHistorical(500)
        expect([...timers.values()].map((timer) => timer.delay)).toEqual([LIVE_READ_RETRY_INITIAL_MS])
        await retry()
        expect(historicalReads).toBe(2)
        expect([...timers.values()].map((timer) => timer.delay)).toEqual([LIVE_READ_RETRY_INITIAL_MS * 2])
        failureStatus = null
        await retry()
        expect(historicalReads).toBe(3)
        expect(requests[1]).toEqual(requests[0])
        expect(requests[2]).toEqual(requests[0])
        expect(container.querySelector(`button[aria-label="${next}"]`)).not.toBeNull()
        expect(timers.size).toBe(0)
    })

    test.each([401, 403, 404])('does not retry terminal HTTP %i', async (status) => {
        await openHistorical(status)
        expect(timers.size).toBe(0)
        await settle()
        expect(historicalReads).toBe(1)
    })
})
