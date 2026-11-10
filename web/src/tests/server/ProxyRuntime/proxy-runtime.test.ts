import { UPSTREAM_FAILOVER_DEFAULTS } from '@/config/upstream-pools.config.ts'
import { afterEach, describe, expect, mock, spyOn, test } from 'bun:test'

import {
    getProxyRuntimeStatus,
    applyProxyRuntimeConfiguration,
    applyProxyRuntimeConfigurationResult,
    previewProxyConfiguration,
    previewProxyHostConfiguration,
} from '@/server/Controller/proxy.server.ts'
import { checkControllerHealth } from '@/server/Controller/health.server.ts'
import { createProxyReconciler } from '@/server/ProxyRuntime/proxy-reconcile.ts'
import {
    compareProxyRuntimeStatus,
    createProxyRuntimeSnapshot,
    MAX_RUNTIME_PROXY_HOSTS,
} from '@/server/ProxyRuntime/proxy-runtime-snapshot.ts'
import type {
    ProxyRuntimeHost,
    ProxyRuntimeSnapshot,
} from '@/server/ProxyRuntime/Types/proxy-runtime.types.ts'

type SnapshotInputHost = ProxyRuntimeHost & { readonly enabled: boolean }

const BASE_ID = '018f2f52-7c1b-7cc0-9f3c-6a9952c54019'

test('paused native development leaves proxy changes pending without background requests', async () => {
    const loadSnapshot = mock(async () => snapshot())
    const applySnapshot = mock(async () => null)
    const checkDrift = mock(async () => true)
    const reconciler = createProxyReconciler({
        enabled: false,
        loadSnapshot,
        applySnapshot,
        checkDrift,
    })

    try {
        reconciler.start()
        await reconciler.checkDrift()
        expect(await reconciler()).toBe('pending')
        expect(loadSnapshot).not.toHaveBeenCalled()
        expect(applySnapshot).not.toHaveBeenCalled()
        expect(checkDrift).not.toHaveBeenCalled()
    } finally {
        await reconciler.stop()
    }
})
const originalEnvironment = new Map<string, string | undefined>()
const CONTROLLER_ENVIRONMENT = ['RENTNERPROXY_CONTROLLER_URL', 'RENTNERPROXY_CONTROLLER_TOKEN']

for (const variable of CONTROLLER_ENVIRONMENT) {
    originalEnvironment.set(variable, process.env[variable])
}

function inputHost(overrides: Partial<SnapshotInputHost> = {}): SnapshotInputHost {
    return {
        id: BASE_ID,
        domains: ['demo.test'],
        enabled: true,
        forwardScheme: 'http',
        forwardHost: 'backend.internal',
        forwardPort: 4_000,
        ...overrides,
    }
}

function snapshot(overrides: Partial<SnapshotInputHost> = {}): ProxyRuntimeSnapshot {
    return createProxyRuntimeSnapshot([inputHost(overrides)])
}

function restoreControllerEnvironment(): void {
    for (const variable of CONTROLLER_ENVIRONMENT) {
        const originalValue = originalEnvironment.get(variable)
        if (originalValue === undefined) delete process.env[variable]
        else process.env[variable] = originalValue
    }
}

afterEach(() => {
    restoreControllerEnvironment()
})

describe('proxy runtime snapshots', () => {
    test('produces the versioned known-vector SHA-256 revision', () => {
        const result = createProxyRuntimeSnapshot([
            inputHost({ domains: ['www.demo.test', 'demo.test'] }),
        ])

        expect(result).toEqual({
            version: 7,
            revision: 'sha256:7b3e586f596ea7a1ffacad33824b96234e23802d12f3741e73025bf8a23a4a06',
            proxyHosts: [
                {
                    id: BASE_ID,
                    domains: ['demo.test', 'www.demo.test'],
                    forwardScheme: 'http',
                    forwardHost: 'backend.internal',
                    forwardPort: 4_000,
                },
            ],
            redirectHosts: [],
            httpSettings: {},
            trustedCas: [],
        })
    })

    test('matches the Rust Forward Auth revision with IP rules and a gateway path', () => {
        const result = createProxyRuntimeSnapshot([
            inputHost({
                id: '018f4b4a-7d1f-7abc-8def-0123456789ab',
                domains: ['a.example'],
                forwardHost: '127.0.0.1',
                forwardPort: 8080,
                accessPolicy: {
                    id: '0198d98a-0000-7000-8000-000000000001',
                    mode: 'combined',
                    combination: 'all',
                    forwardAuth: {
                        endpoint: 'http://auth.example.test/check',
                        timeoutSeconds: 5,
                        requestHeaders: ['Cookie'],
                        responseHeaders: ['Remote-User'],
                        gatewayPathPrefix: '/outpost/',
                    },
                    ipRules: {
                        defaultAction: 'deny',
                        allow: ['192.0.2.0/24'],
                        deny: [],
                    },
                },
            }),
        ])
        expect(result.revision).toBe(
            'sha256:32b018cab495353f71733a5f7d85a17141cd6c09b8e94ca3d6ef94f67af77dac',
        )
    })

    test('filters disabled hosts and sorts hosts and domains deterministically', () => {
        const enabledSecond = inputHost({
            id: '018f2f52-7c1b-7cc0-9f3c-6a9952c54020',
            domains: ['z.demo.test', 'a.demo.test'],
        })
        const disabled = inputHost({
            id: '018f2f52-7c1b-7cc0-9f3c-6a9952c54021',
            domains: ['disabled.demo.test'],
            enabled: false,
        })
        const result = createProxyRuntimeSnapshot([enabledSecond, disabled, inputHost()])

        expect(result.proxyHosts.map(({ id }) => id)).toEqual([BASE_ID, enabledSecond.id])
        expect(result.proxyHosts[1]?.domains).toEqual(['a.demo.test', 'z.demo.test'])
        expect(
            result.proxyHosts.some(({ domains }) => domains.includes('disabled.demo.test')),
        ).toBeFalse()
    })

    test('is stable across input ordering and changes when runtime content changes', () => {
        const first = createProxyRuntimeSnapshot([
            inputHost({ domains: ['z.demo.test', 'demo.test'] }),
            inputHost({
                id: '018f2f52-7c1b-7cc0-9f3c-6a9952c54020',
                domains: ['api.demo.test'],
                forwardPort: 4_001,
            }),
        ])
        const reordered = createProxyRuntimeSnapshot([
            inputHost({
                id: '018f2f52-7c1b-7cc0-9f3c-6a9952c54020',
                domains: ['api.demo.test'],
                forwardPort: 4_001,
            }),
            inputHost({ domains: ['demo.test', 'z.demo.test'] }),
        ])
        const changed = createProxyRuntimeSnapshot([
            inputHost({ domains: ['z.demo.test', 'demo.test'], forwardPort: 4_002 }),
            inputHost({
                id: '018f2f52-7c1b-7cc0-9f3c-6a9952c54020',
                domains: ['api.demo.test'],
                forwardPort: 4_001,
            }),
        ])

        expect(reordered).toEqual(first)
        expect(changed.revision).not.toBe(first.revision)
    })

    test('rejects duplicate runtime identities, injection values, and excessive limits', () => {
        expect(() =>
            createProxyRuntimeSnapshot([
                inputHost(),
                inputHost({ id: BASE_ID, domains: ['other.demo.test'] }),
            ]),
        ).toThrow()
        expect(() =>
            createProxyRuntimeSnapshot([
                inputHost({ domains: ['same.demo.test', 'same.demo.test'] }),
            ]),
        ).toThrow()
        expect(() =>
            createProxyRuntimeSnapshot([
                inputHost({ domains: ['demo.test\nserver { return 200; }'] }),
            ]),
        ).toThrow()
        expect(() =>
            createProxyRuntimeSnapshot([
                inputHost({ forwardHost: 'backend.internal; proxy_pass http://evil' }),
            ]),
        ).toThrow()

        const tooManyHosts = Array.from({ length: MAX_RUNTIME_PROXY_HOSTS + 1 }, (_, index) =>
            inputHost({
                id: `018f2f52-7c1b-7cc0-9f3c-${String(index).padStart(12, '0')}`,
                domains: [`host-${index}.demo.test`],
            }),
        )
        expect(() => createProxyRuntimeSnapshot(tooManyHosts)).toThrow()
    })

    test('compares desired and active runtime status safely', () => {
        const desiredRevision = snapshot().revision
        expect(compareProxyRuntimeStatus(desiredRevision, null)).toMatchObject({
            state: 'unavailable',
            desiredRevision,
        })
        expect(
            compareProxyRuntimeStatus(desiredRevision, {
                available: true,
                running: true,
                activeRevision: desiredRevision,
                lastApplyAt: null,
            }),
        ).toMatchObject({ state: 'synced', desiredRevision })
        expect(
            compareProxyRuntimeStatus(desiredRevision, {
                available: true,
                running: true,
                activeRevision: 'sha256:' + '0'.repeat(64),
                lastApplyAt: null,
            }),
        ).toMatchObject({ state: 'pending', desiredRevision })
    })
})

describe('proxy runtime reconciliation', () => {
    test('coalesces concurrent requests and applies a latest snapshot before success', async () => {
        const first = snapshot()
        const latest = snapshot({ forwardPort: 4_001 })
        let current = first
        let applyStarted!: () => void
        let releaseApply!: () => void
        const started = new Promise<void>((resolve) => {
            applyStarted = resolve
        })
        const gate = new Promise<void>((resolve) => {
            releaseApply = resolve
        })
        const applied: string[] = []
        const reconciler = createProxyReconciler({
            loadSnapshot: async () => current,
            applySnapshot: async (value) => {
                applied.push(value.revision)
                if (applied.length === 1) {
                    current = latest
                    applyStarted()
                    await gate
                }
                return { status: 'applied', activeRevision: value.revision, lastApplyAt: null }
            },
        })

        const firstRequest = reconciler()
        await started
        const secondRequest = reconciler()
        expect(secondRequest).toBe(firstRequest)
        releaseApply()

        expect(await firstRequest).toBe('applied')
        expect(applied).toEqual([first.revision, latest.revision])
    })

    test('returns pending for a stalled read and applies only after it is released', async () => {
        const current = snapshot()
        let releaseRead!: (value: ProxyRuntimeSnapshot) => void
        const stalled = new Promise<ProxyRuntimeSnapshot>((resolve) => {
            releaseRead = resolve
        })
        let reads = 0
        let applies = 0
        const reconcile = createProxyReconciler(
            {
                loadSnapshot: async () => {
                    reads += 1
                    return reads === 1 ? stalled : current
                },
                applySnapshot: async (value) => {
                    applies += 1
                    return { status: 'applied', activeRevision: value.revision, lastApplyAt: null }
                },
            },
            25,
        )

        expect(await reconcile()).toBe('pending')
        expect(applies).toBe(0)
        releaseRead(current)
        await Bun.sleep(0)
        expect(applies).toBeGreaterThanOrEqual(1)
        expect(await reconcile()).toBe('applied')
        expect(applies).toBe(2)
    })

    test('keeps a stalled post-apply confirmation within the same deadline', async () => {
        const current = snapshot()
        let reads = 0
        const reconcile = createProxyReconciler(
            {
                loadSnapshot: async () => {
                    reads += 1
                    return reads === 1
                        ? current
                        : new Promise<ProxyRuntimeSnapshot>(() => undefined)
                },
                applySnapshot: async (value) => ({
                    status: 'applied',
                    activeRevision: value.revision,
                    lastApplyAt: null,
                }),
            },
            25,
        )

        expect(await reconcile()).toBe('pending')
        expect(reads).toBe(2)
    })
    test('reports a failed attempt as pending and keeps retrying until stopped', async () => {
        let attempts = 0
        const retrying = createProxyReconciler(
            {
                loadSnapshot: async () => snapshot(),
                applySnapshot: async () => {
                    attempts += 1
                    return null
                },
            },
            25,
        )
        expect(await retrying()).toBe('pending')
        expect(attempts).toBeGreaterThan(0)
        await retrying.stop()

        const failingLoad = createProxyReconciler(
            {
                loadSnapshot: async () => Promise.reject(new Error('database unavailable')),
                applySnapshot: async () => {
                    throw new Error('must not apply')
                },
            },
            25,
        )
        expect(await failingLoad()).toBe('pending')
        await failingLoad.stop()
    })

    test('recovers after a transient controller failure without a new request', async () => {
        let failures = 1
        let acknowledgeRecovery!: () => void
        const recovered = new Promise<void>((resolve) => {
            acknowledgeRecovery = resolve
        })
        const appliedRevisions: string[] = []
        const reconciler = createProxyReconciler(
            {
                loadSnapshot: async () => snapshot(),
                applySnapshot: async (value) => {
                    if (failures > 0) {
                        failures -= 1
                        return null
                    }
                    appliedRevisions.push(value.revision)
                    acknowledgeRecovery()
                    return { status: 'applied', activeRevision: value.revision, lastApplyAt: null }
                },
            },
            3_000,
        )
        expect(await reconciler()).toBe('pending')
        await recovered
        expect(appliedRevisions).toEqual([snapshot().revision])
        await reconciler.stop()
    })

    test('does not reapply a healthy runtime during drift checks', async () => {
        let applies = 0
        let releaseApply!: () => void
        const applyFinished = new Promise<void>((resolve) => {
            releaseApply = resolve
        })
        const reconciler = createProxyReconciler({
            loadSnapshot: async () => snapshot(),
            checkDrift: async () => false,
            applySnapshot: async (value) => {
                applies += 1
                releaseApply()
                return { status: 'applied', activeRevision: value.revision, lastApplyAt: null }
            },
        })
        reconciler.start()
        await applyFinished
        await reconciler.checkDrift()
        expect(applies).toBe(1)
        await reconciler.stop()
    })

    test('stops during a database read without starting a new apply', async () => {
        const current = snapshot()
        let releaseRead!: () => void
        let readStarted!: () => void
        const readGate = new Promise<void>((resolve) => (releaseRead = resolve))
        const readObserved = new Promise<void>((resolve) => (readStarted = resolve))
        let applies = 0
        const reconciler = createProxyReconciler({
            loadSnapshot: async () => {
                readStarted()
                await readGate
                return current
            },
            applySnapshot: async () => {
                applies += 1
                return null
            },
        })
        reconciler.start()
        await readObserved
        const stopping = reconciler.stop()
        releaseRead()
        await stopping
        expect(applies).toBe(0)
    })

    test('waits for an in-flight controller apply during shutdown', async () => {
        const current = snapshot()
        let releaseApply!: () => void
        let applyStarted!: () => void
        const applyGate = new Promise<void>((resolve) => (releaseApply = resolve))
        const applyObserved = new Promise<void>((resolve) => (applyStarted = resolve))
        const reconciler = createProxyReconciler({
            loadSnapshot: async () => current,
            applySnapshot: async (value) => {
                applyStarted()
                await applyGate
                return { status: 'applied', activeRevision: value.revision, lastApplyAt: null }
            },
        })
        reconciler.start()
        await applyObserved
        let stopped = false
        const stopping = reconciler.stop().then(() => (stopped = true))
        await Bun.sleep(0)
        expect(stopped).toBeFalse()
        releaseApply()
        await stopping
        expect(stopped).toBeTrue()
    })
})

describe('controller runtime client', () => {
    test.each([
        [422, 'validation_failed', false],
        [400, 'invalid_configuration', false],
        [413, 'invalid_configuration', false],
        [401, 'unauthorized', true],
        [403, 'unauthorized', true],
        [404, 'not_found', true],
        [409, 'busy', true],
        [503, 'runtime_unavailable', true],
        [502, 'apply_failed', true],
    ] as const)('preserves safe apply diagnostics for HTTP %s', async (status, code, retryable) => {
        process.env.RENTNERPROXY_CONTROLLER_URL = 'http://127.0.0.1:8081'
        process.env.RENTNERPROXY_CONTROLLER_TOKEN = 'A'.repeat(32)
        const fetchMock = spyOn(globalThis, 'fetch').mockImplementation((async () =>
            Response.json({ error: code }, { status })) as unknown as typeof fetch)
        try {
            expect(await applyProxyRuntimeConfigurationResult(snapshot())).toEqual({
                ok: false,
                kind: 'http',
                status,
                code,
                retryable,
            })
            expect(await applyProxyRuntimeConfiguration(snapshot())).toBeNull()
        } finally {
            fetchMock.mockRestore()
        }
    })

    test('does not preserve arbitrary error bodies or exception messages in apply diagnostics', async () => {
        process.env.RENTNERPROXY_CONTROLLER_URL = 'http://127.0.0.1:8081'
        process.env.RENTNERPROXY_CONTROLLER_TOKEN = 'A'.repeat(32)
        const fetchMock = spyOn(globalThis, 'fetch').mockResolvedValue(
            Response.json({ error: 'private_payload' }, { status: 422 }),
        )
        try {
            expect(await applyProxyRuntimeConfigurationResult(snapshot())).toEqual({
                ok: false,
                kind: 'http',
                status: 422,
                code: null,
                retryable: true,
            })
            fetchMock.mockRejectedValue(new Error('token=private'))
            expect(await applyProxyRuntimeConfigurationResult(snapshot())).toEqual({
                ok: false,
                kind: 'network',
                status: null,
                code: null,
                retryable: true,
            })
        } finally {
            fetchMock.mockRestore()
        }
    })

    test.each(['configuration', 401, 403, 404] as const)(
        'recovers from %s without changing desired state or requesting another apply',
        async (failure) => {
            process.env.RENTNERPROXY_CONTROLLER_URL = 'http://127.0.0.1:8081'
            process.env.RENTNERPROXY_CONTROLLER_TOKEN =
                failure === 'configuration' ? '' : 'A'.repeat(32)
            const current = snapshot()
            const success = {
                status: 'applied',
                activeRevision: current.revision,
                lastApplyAt: null,
            }
            const fetchMock = spyOn(globalThis, 'fetch').mockResolvedValue(
                failure === 'configuration'
                    ? Response.json(success)
                    : Response.json(
                          { error: failure === 404 ? 'not_found' : 'unauthorized' },
                          { status: failure },
                      ),
            )
            const reconciler = createProxyReconciler({
                loadSnapshot: async () => current,
                applySnapshot: applyProxyRuntimeConfigurationResult,
            })
            try {
                expect(await reconciler()).toBe('pending')
                expect(fetchMock).toHaveBeenCalledTimes(failure === 'configuration' ? 0 : 1)
                process.env.RENTNERPROXY_CONTROLLER_TOKEN = 'B'.repeat(32)
                fetchMock.mockResolvedValue(Response.json(success))
                await Bun.sleep(1_100)
                expect(fetchMock).toHaveBeenCalledTimes(failure === 'configuration' ? 1 : 2)
                expect(fetchMock.mock.calls.at(-1)?.[1]?.headers).toMatchObject({
                    authorization: 'Bearer ' + 'B'.repeat(32),
                })
            } finally {
                await reconciler.stop()
                fetchMock.mockRestore()
            }
        },
    )

    test('carries a real controller rejection through the reconciler without logging payloads', async () => {
        process.env.RENTNERPROXY_CONTROLLER_URL = 'http://127.0.0.1:8081'
        process.env.RENTNERPROXY_CONTROLLER_TOKEN = 'A'.repeat(32)
        const fetchMock = spyOn(globalThis, 'fetch').mockImplementation((async () =>
            Response.json(
                { error: 'validation_failed' },
                { status: 422 },
            )) as unknown as typeof fetch)
        const warnings = spyOn(console, 'warn').mockImplementation(() => undefined)
        const reconciler = createProxyReconciler({
            loadSnapshot: async () => snapshot(),
            applySnapshot: applyProxyRuntimeConfigurationResult,
        })
        try {
            expect(await reconciler()).toBe('pending')
            expect(warnings).toHaveBeenCalledWith('[proxy-runtime] reconcile rejected', {
                code: 'validation_failed',
                status: 422,
                retryable: false,
            })
            expect(fetchMock).toHaveBeenCalledTimes(1)
            fetchMock.mockResolvedValue(
                new Response('{', { headers: { 'content-type': 'application/json' } }),
            )
            expect(await applyProxyRuntimeConfigurationResult(snapshot())).toEqual({
                ok: false,
                kind: 'invalid_response',
                status: 200,
                code: null,
                retryable: true,
            })
        } finally {
            await reconciler.stop()
            warnings.mockRestore()
            fetchMock.mockRestore()
        }
    })

    test('parks rejected revisions, accepts explicit retries and applies changed desired state', async () => {
        let desired = snapshot()
        let rejected = true
        const applySnapshot = mock(async (current: ProxyRuntimeSnapshot) =>
            rejected
                ? {
                      ok: false as const,
                      kind: 'http' as const,
                      status: 422,
                      code: 'validation_failed',
                      retryable: false,
                  }
                : {
                      ok: true as const,
                      value: {
                          status: 'applied' as const,
                          activeRevision: current.revision,
                          lastApplyAt: null,
                      },
                  },
        )
        const reconciler = createProxyReconciler({
            loadSnapshot: async () => desired,
            applySnapshot,
            checkDrift: async () => true,
        })
        try {
            expect(await reconciler()).toBe('pending')
            await Bun.sleep(1_100)
            expect(applySnapshot).toHaveBeenCalledTimes(1)
            await reconciler.checkDrift()
            await Bun.sleep(20)
            expect(applySnapshot).toHaveBeenCalledTimes(1)
            expect(await reconciler()).toBe('pending')
            expect(applySnapshot).toHaveBeenCalledTimes(2)
            rejected = false
            desired = snapshot({ forwardPort: 4_001 })
            await reconciler.checkDrift()
            await Bun.sleep(20)
            expect(applySnapshot).toHaveBeenCalledTimes(3)
            expect(await reconciler()).toBe('applied')
        } finally {
            await reconciler.stop()
        }
    })

    test('retries typed transient apply failures without a new request and stops cleanly', async () => {
        const current = snapshot()
        const applySnapshot = mock(async () =>
            applySnapshot.mock.calls.length === 1
                ? {
                      ok: false as const,
                      kind: 'http' as const,
                      status: 409,
                      code: 'busy',
                      retryable: true,
                  }
                : {
                      ok: true as const,
                      value: {
                          status: 'applied' as const,
                          activeRevision: current.revision,
                          lastApplyAt: null,
                      },
                  },
        )
        const reconciler = createProxyReconciler({
            loadSnapshot: async () => current,
            applySnapshot,
        })
        try {
            expect(await reconciler()).toBe('pending')
            await Bun.sleep(1_100)
            expect(applySnapshot).toHaveBeenCalledTimes(2)
        } finally {
            await reconciler.stop()
        }
    })
    test.each([
        ['http://controller.example:8081', false],
        ['https://controller.example:8443', true],
        ['http://127.0.0.1:8081', true],
    ])('protects Basic Auth snapshot transport to %s', async (url, allowed) => {
        process.env.RENTNERPROXY_CONTROLLER_URL = url
        process.env.RENTNERPROXY_CONTROLLER_TOKEN = 'A'.repeat(32)
        const current = snapshot({
            accessPolicy: {
                id: '0198d98a-0000-7000-8000-000000000001',
                mode: 'authenticated',
                combination: null,
                basicAuth: {
                    accounts: [
                        {
                            username: 'alice',
                            passwordHash:
                                '$argon2id$v=19$m=47104,t=1,p=1$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA$MrQeoLQVkaRjr94luEbHZECFRREjHzNciGTu9rBCN+Y',
                        },
                    ],
                },
            },
        })
        const fetchMock = spyOn(globalThis, 'fetch').mockImplementation((async () =>
            Response.json({
                status: 'applied',
                activeRevision: current.revision,
                lastApplyAt: null,
                config: '{}',
                revision: current.revision,
            })) as unknown as typeof fetch)
        try {
            const results = await Promise.all([
                applyProxyRuntimeConfiguration(current),
                previewProxyConfiguration(current),
                previewProxyHostConfiguration(BASE_ID, current),
            ])
            expect(fetchMock).toHaveBeenCalledTimes(allowed ? 3 : 0)
            for (const result of results) {
                if (allowed) expect(result).not.toBeNull()
                else expect(result).toBeNull()
            }
        } finally {
            fetchMock.mockRestore()
        }
    })

    test.each([
        { restarts: 4, accepted: true },
        { restarts: -1, accepted: false },
        { restarts: Number.MAX_SAFE_INTEGER + 1, accepted: false },
    ])(
        'validates recovery observations without duplicating the native restart budget: %j',
        async ({ restarts, accepted }) => {
            process.env.RENTNERPROXY_CONTROLLER_URL = 'https://controller.example:8081'
            process.env.RENTNERPROXY_CONTROLLER_TOKEN = 'A'.repeat(32)
            const recovery = {
                workerRunning: true,
                completedTicks: 10,
                lastCompletedAgeSeconds: 1,
                circuitOpen: false,
                restartsInWindow: restarts,
                faults: 2,
                consecutiveProbeFailures: 1,
            }
            const fetchMock = spyOn(globalThis, 'fetch').mockResolvedValue(
                Response.json({
                    available: true,
                    running: true,
                    activeRevision: snapshot().revision,
                    lastApplyAt: null,
                    recovery,
                }),
            )
            try {
                const status = await getProxyRuntimeStatus()
                if (accepted) expect(status?.recovery).toEqual(recovery)
                else expect(status).toBeNull()
            } finally {
                fetchMock.mockRestore()
            }
        },
    )

    test('keeps health unauthenticated while authenticating every internal endpoint', async () => {
        process.env.RENTNERPROXY_CONTROLLER_URL = 'https://controller.example:8081'
        process.env.RENTNERPROXY_CONTROLLER_TOKEN = 'A'.repeat(32)
        const current = snapshot()
        const fetchMock = spyOn(globalThis, 'fetch')
        fetchMock.mockImplementation((async (
            _input: Parameters<typeof fetch>[0],
            init: Parameters<typeof fetch>[1],
        ) => {
            const path = new URL(String(_input)).pathname
            const body =
                path === '/health'
                    ? { status: 'ok', service: 'rentnerproxy-controller', version: '0.0.0' }
                    : path === '/internal/v1/proxy/status'
                      ? {
                            available: true,
                            running: true,
                            activeRevision: current.revision,
                            lastApplyAt: null,
                        }
                      : { status: 'applied', activeRevision: current.revision, lastApplyAt: null }
            expect(init?.redirect).toBe('error')
            return new Response(JSON.stringify(body), {
                headers: { 'content-type': 'application/json' },
            })
        }) as unknown as typeof fetch)

        expect(await checkControllerHealth()).toEqual({ state: 'connected' })
        expect(await getProxyRuntimeStatus()).toMatchObject({ activeRevision: current.revision })
        expect(await applyProxyRuntimeConfiguration(current)).toMatchObject({
            activeRevision: current.revision,
        })
        const healthRequest = fetchMock.mock.calls[0]?.[1]
        const statusRequest = fetchMock.mock.calls[1]?.[1]
        const applyRequest = fetchMock.mock.calls[2]?.[1]
        expect(healthRequest?.headers).not.toHaveProperty('authorization')
        expect(statusRequest?.headers).toMatchObject({ authorization: 'Bearer ' + 'A'.repeat(32) })
        expect(applyRequest?.headers).toMatchObject({ authorization: 'Bearer ' + 'A'.repeat(32) })
        expect(fetchMock).toHaveBeenCalledTimes(3)
        fetchMock.mockRestore()
    })

    test('refuses privileged requests without a valid token on non-loopback controllers', async () => {
        process.env.RENTNERPROXY_CONTROLLER_URL = 'https://controller.example:8081'
        delete process.env.RENTNERPROXY_CONTROLLER_TOKEN
        const fetchMock = spyOn(globalThis, 'fetch').mockRejectedValue(
            new Error('Unexpected network call in controller token validation test.'),
        )
        try {
            expect(await getProxyRuntimeStatus()).toBeNull()
            expect(await applyProxyRuntimeConfiguration(snapshot())).toBeNull()
            process.env.RENTNERPROXY_CONTROLLER_TOKEN = 'too-short'
            expect(await getProxyRuntimeStatus()).toBeNull()
            expect(fetchMock).not.toHaveBeenCalled()
        } finally {
            fetchMock.mockRestore()
        }
    })

    test('rejects invalid, oversized, redirected, and revision-mismatched responses without leaking errors', async () => {
        process.env.RENTNERPROXY_CONTROLLER_URL = 'https://controller.example:8081'
        process.env.RENTNERPROXY_CONTROLLER_TOKEN = 'B'.repeat(32)
        const current = snapshot()
        const warnings = spyOn(console, 'warn').mockImplementation(() => undefined)
        const fetchMock = spyOn(globalThis, 'fetch').mockRejectedValue(new Error('token=private'))
        expect(await getProxyRuntimeStatus()).toBeNull()
        expect(fetchMock).toHaveBeenCalledTimes(1)
        expect(warnings.mock.calls.flat().join(' ')).not.toContain('private')
        fetchMock.mockRestore()
        warnings.mockRestore()

        const responses = [
            new Response('x'.repeat(4_097), { headers: { 'content-type': 'application/json' } }),
            new Response(
                JSON.stringify({
                    status: 'applied',
                    activeRevision: 'sha256:' + '0'.repeat(64),
                    lastApplyAt: null,
                }),
                {
                    headers: { 'content-type': 'application/json' },
                },
            ),
        ]
        const responseMock = spyOn(globalThis, 'fetch')
        responseMock.mockImplementation((async (
            _input: Parameters<typeof fetch>[0],
            init: Parameters<typeof fetch>[1],
        ) => {
            expect(init?.redirect).toBe('error')
            return (
                responses.shift() ??
                new Response('{}', { headers: { 'content-type': 'text/plain' } })
            )
        }) as unknown as typeof fetch)
        expect(await getProxyRuntimeStatus()).toBeNull()
        expect(await applyProxyRuntimeConfiguration(current)).toBeNull()
        expect(responseMock).toHaveBeenCalledTimes(2)
        responseMock.mockRestore()
    })
})

describe('upstream pool snapshot contract', () => {
    const members = [
        { host: 'backend.internal', port: 4000 },
        { host: 'secondary.internal', port: 4001 },
    ]
    test('uses v8 only for active pools and appends the canonical field after existing fields', () => {
        const legacy = snapshot()
        const pooled = snapshot({ upstreamPool: { policy: 'round_robin', members } })
        expect(legacy.version).toBe(7)
        expect(pooled.version).toBe(8)
        expect(
            snapshot({
                domains: ['www.demo.test', 'demo.test'],
                upstreamPool: { policy: 'round_robin', members },
            }).revision,
        ).toBe('sha256:ab553eed513e20dae33d9e0e41c2d2bf73505b6da121bc2a99ed0ee96a7b6841')
        expect(pooled.proxyHosts[0]?.upstreamPool).toEqual({ policy: 'round_robin', members })
        expect(Object.keys(pooled.proxyHosts[0]!).at(-1)).toBe('upstreamPool')
        expect(
            createProxyRuntimeSnapshot([
                inputHost(),
                inputHost({
                    id: '018f2f52-7c1b-7cc0-9f3c-6a9952c54020',
                    enabled: false,
                    upstreamPool: { policy: 'first', members },
                }),
            ]),
        ).toEqual(legacy)
    })
    test('includes pool policy, order and secondary targets in the revision', () => {
        const pooled = snapshot({ upstreamPool: { policy: 'round_robin', members } })
        const first = snapshot({ upstreamPool: { policy: 'first', members } })
        const changed = snapshot({
            upstreamPool: {
                policy: 'round_robin',
                members: [members[0]!, { host: 'changed.internal', port: 4001 }],
            },
        })
        const reversed = snapshot({
            forwardHost: 'secondary.internal',
            forwardPort: 4001,
            upstreamPool: { policy: 'round_robin', members: members.toReversed() },
        })
        expect(
            new Set([pooled.revision, first.revision, changed.revision, reversed.revision]).size,
        ).toBe(4)
        expect(() =>
            snapshot({ upstreamPool: { policy: 'first', members: members.toReversed() } }),
        ).toThrow()
    })
    test('rejects a secondary IP with verified TLS unless a shared identity is explicit', () => {
        const upstreamPool = {
            policy: 'first' as const,
            members: [members[0]!, { host: '192.0.2.2', port: 443 }],
        }
        expect(() => snapshot({ forwardScheme: 'https', upstreamPool })).toThrow()
        expect(
            snapshot({
                forwardScheme: 'https',
                upstreamPool,
                upstreamTls: { verify: true, serverName: 'shared.internal', trustedCaId: null },
            }).version,
        ).toBe(8)
        expect(
            snapshot({
                forwardScheme: 'https',
                upstreamPool,
                upstreamTls: { verify: false, serverName: null, trustedCaId: null },
            }).version,
        ).toBe(8)
    })
})

test('failover uses canonical v9 while preserving v7 and v8 payload hashes', () => {
    const members = [
        { host: 'backend.internal', port: 4000 },
        { host: 'secondary.internal', port: 4001 },
    ]
    const base = { domains: ['www.demo.test', 'demo.test'] }
    const legacy = snapshot(base)
    const pooled = snapshot({ ...base, upstreamPool: { policy: 'round_robin', members } })
    expect(legacy.version).toBe(7)
    expect(legacy.revision).toBe(
        'sha256:7b3e586f596ea7a1ffacad33824b96234e23802d12f3741e73025bf8a23a4a06',
    )
    expect(pooled.version).toBe(8)
    expect(pooled.revision).toBe(
        'sha256:ab553eed513e20dae33d9e0e41c2d2bf73505b6da121bc2a99ed0ee96a7b6841',
    )
    const failover = snapshot({
        ...base,
        upstreamPool: { policy: 'round_robin', members, failover: UPSTREAM_FAILOVER_DEFAULTS },
    })
    expect(failover.version).toBe(9)
    expect(failover.revision).toBe(
        'sha256:f3228530e6b0bc0b0c2aa22c401dc2b5358e809fbb2eb03fbc47abf2111aa22b',
    )
    expect(Object.keys(failover.proxyHosts[0]!).at(-1)).toBe('upstreamPool')
    const changed = snapshot({
        ...base,
        upstreamPool: {
            policy: 'round_robin',
            members,
            failover: {
                ...UPSTREAM_FAILOVER_DEFAULTS,
                active: { ...UPSTREAM_FAILOVER_DEFAULTS.active, path: '/ready' },
            },
        },
    })
    expect(changed.revision).not.toBe(failover.revision)
})
