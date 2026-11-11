// oxlint-disable no-await-in-loop -- Drive deterministic clock turns between asynchronous watcher/transaction boundaries.
import { afterEach, expect, jest, mock, spyOn, test } from 'bun:test'
import {
    createDiscoveryObserver,
    DiscoveryFinalizationError,
    DiscoverySuperseded,
} from '@/server/Admin/DockerDiscovery/discovery-observer.ts'
import type {
    DiscoveryObservation,
    DiscoveryObserverDependencies,
} from '@/server/Admin/DockerDiscovery/Types/discovery-observer.types.ts'
import type { DockerDiscoveryObservation } from '@/lib/DockerDiscovery/Types/docker-discovery.types.ts'
import { discoveryCandidate, discoverySnapshot } from '@/tests/Helpers/discoveryFixture.ts'

afterEach(() => {
    jest.useRealTimers()
    mock.restore()
})
const observed = (generation = 1, runtimeId = 'a'.repeat(32)): DiscoveryObservation => ({
    cursor: { runtimeId, generation },
    snapshot: discoverySnapshot([
        discoveryCandidate({ domains: [`${runtimeId[0]}-${generation}.example.com`] }),
    ]),
})
async function settle() {
    for (let turn = 0; turn < 60; turn++) await Promise.resolve()
}
async function advance(ms: number) {
    await settle()
    jest.advanceTimersByTime(ms)
    await settle()
}

function fixture(overrides: Partial<DiscoveryObserverDependencies> = {}) {
    jest.useFakeTimers({ now: 1000 })
    let enabled = true
    let latest = observed()
    let invalidate: (() => void) | undefined
    const requests: {
        signal: AbortSignal
        reply: (value: DockerDiscoveryObservation) => void
        fail: () => void
    }[] = []
    const dependencies = {
        enabled: mock(overrides.enabled ?? (async () => enabled)),
        read: mock(overrides.read ?? (async () => latest)),
        watch: mock<DiscoveryObserverDependencies['watch']>(
            overrides.watch ??
                ((cursor, signal) =>
                    new Promise<DockerDiscoveryObservation>((resolve, reject) => {
                        const finish = (value: DockerDiscoveryObservation) => {
                            clearTimeout(timer)
                            signal.removeEventListener('abort', cancel)
                            resolve(value)
                        }
                        const cancel = () => {
                            clearTimeout(timer)
                            signal.removeEventListener('abort', cancel)
                            reject(new Error('cancelled'))
                        }
                        const timer = setTimeout(() => finish({ cursor, snapshot: null }), 25_000)
                        signal.addEventListener('abort', cancel, { once: true })
                        requests.push({
                            signal,
                            reply: finish,
                            fail: () => {
                                cancel()
                            },
                        })
                        if (signal.aborted) cancel()
                    })),
        ),
        reconcile: mock<DiscoveryObserverDependencies['reconcile']>(
            overrides.reconcile ??
                (async (_snapshot, _signal, assertCurrent) => {
                    assertCurrent()
                    return { enabled: true, again: false }
                }),
        ),
        subscribe:
            overrides.subscribe ??
            ((listener: () => void) => {
                invalidate = listener
                return () => {
                    invalidate = undefined
                }
            }),
        failed: mock(overrides.failed ?? (async () => {})),
    }
    const worker = createDiscoveryObserver(dependencies)
    return {
        worker,
        dependencies,
        requests,
        enabled(value: boolean) {
            enabled = value
        },
        latest(value: DiscoveryObservation) {
            latest = value
        },
        invalidate() {
            invalidate?.()
        },
        reply(index: number, observation: DiscoveryObservation) {
            latest = observation
            requests[index]!.reply({ cursor: observation.cursor!, snapshot: observation.snapshot })
        },
    }
}

test.each([false, true])(
    'idle discovery uses one safety read per minute, enabled: %s',
    async (enabled) => {
        const f = fixture()
        f.enabled(enabled)
        try {
            f.worker.start()
            await settle()
            for (let second = 0; second < 60; second += 2) await advance(2000)
            expect(f.dependencies.enabled).toHaveBeenCalledTimes(2) // Startup plus one 60-second fallback.
            expect(f.dependencies.read).toHaveBeenCalledTimes(enabled ? 2 : 0)
            expect(f.dependencies.reconcile).toHaveBeenCalledTimes(enabled ? 2 : 0)
            expect(f.dependencies.watch.mock.calls.length).toBeLessThanOrEqual(enabled ? 3 : 0)
        } finally {
            await f.worker.stop()
        }
    },
)

test('disabled-to-enabled changes wake after commit; external changes recover even without a hint', async () => {
    const f = fixture()
    f.enabled(false)
    try {
        f.worker.start()
        await settle()
        f.enabled(true)
        f.invalidate()
        await advance(499)
        expect(f.dependencies.read).not.toHaveBeenCalled()
        await advance(1)
        expect(f.dependencies.reconcile).toHaveBeenCalledTimes(1)
        f.enabled(false)
        f.invalidate()
        await advance(500)
        expect(f.requests[0]!.signal.aborted).toBe(true)
        f.enabled(true) // Simulate an external SQL/configuration change with a lost hint.
        await advance(59_000)
        expect(f.dependencies.reconcile).toHaveBeenCalledTimes(2)
    } finally {
        await f.worker.stop()
    }
})

test('a flood holds one running pass plus one latest snapshot, and dirty work gets a follow-up', async () => {
    const first = Promise.withResolvers<{ enabled: boolean; again: boolean }>()
    let inFlight = 0
    let maximum = 0
    let calls = 0
    const f = fixture({
        reconcile: async (_snapshot, _signal, guard) => {
            guard()
            inFlight++
            maximum = Math.max(maximum, inFlight)
            if (++calls === 2) await first.promise
            inFlight--
            return { enabled: true, again: false }
        },
    })
    try {
        f.worker.start()
        await settle()
        f.reply(0, observed(2))
        await advance(500)
        expect(calls).toBe(2)
        for (let generation = 3; generation <= 50; generation++) {
            f.reply(f.requests.length - 1, observed(generation))
            await advance(50)
        }
        expect(calls).toBe(2)
        first.resolve({ enabled: true, again: false })
        await settle()
        await advance(500)
        expect(calls).toBe(3)
        expect(maximum).toBe(1)
        expect(f.dependencies.reconcile.mock.calls[2]![0]).toEqual(observed(50).snapshot)
    } finally {
        first.resolve({ enabled: true, again: false })
        await f.worker.stop()
    }
})

test.each([false, true])(
    'late replies cannot replace a newer generation or runtime identity (%s)',
    async (reset) => {
        const f = fixture()
        try {
            f.worker.start()
            await settle()
            const late = Promise.withResolvers<DiscoveryObservation>()
            f.dependencies.read.mockImplementationOnce(() => late.promise)
            const forced = f.worker.trigger()
            await settle()
            const newest = observed(reset ? 1 : 2, reset ? 'b'.repeat(32) : 'a'.repeat(32))
            f.reply(0, newest)
            await advance(50)
            late.resolve(observed(1))
            await forced
            expect(f.dependencies.reconcile).toHaveBeenCalledTimes(1)
            await advance(500)
            expect(f.dependencies.reconcile).toHaveBeenCalledTimes(2)
            expect(f.dependencies.reconcile.mock.calls[1]![0]).toEqual(newest.snapshot)
            f.requests
                .at(-1)!
                .reply({ cursor: observed(0).cursor!, snapshot: observed(0).snapshot })
            await advance(500)
            expect(f.dependencies.reconcile).toHaveBeenCalledTimes(2)
            expect(f.dependencies.failed).not.toHaveBeenCalled()
        } finally {
            await f.worker.stop()
        }
    },
)

test('generation admission rejects a snapshot superseded while waiting for the database lock', async () => {
    const entered = Promise.withResolvers<void>()
    const admit = Promise.withResolvers<void>()
    let rejected = false
    let calls = 0
    const f = fixture({
        reconcile: async (_snapshot, _signal, guard) => {
            if (++calls === 2) {
                entered.resolve()
                await admit.promise
                try {
                    guard()
                } catch (error) {
                    rejected = error instanceof DiscoverySuperseded
                    throw error
                }
            } else guard()
            return { enabled: true, again: false }
        },
    })
    try {
        f.worker.start()
        await settle()
        const forced = f.worker.trigger()
        await entered.promise
        f.reply(0, observed(2))
        await advance(50)
        admit.resolve()
        await forced
        expect(rejected).toBe(true)
        await advance(500)
        expect(calls).toBe(3)
        expect(f.dependencies.failed).not.toHaveBeenCalled()
    } finally {
        admit.resolve()
        await f.worker.stop()
    }
})

test('watch reconnects are bounded, resync the newest state and stop cancels the subscription', async () => {
    const warning = spyOn(console, 'warn').mockImplementation(() => {})
    const f = fixture()
    try {
        f.worker.start()
        await settle()
        f.requests[0]!.fail()
        await settle()
        await advance(1999)
        expect(f.requests).toHaveLength(1)
        await advance(1)
        expect(f.requests).toHaveLength(2)
        f.reply(1, observed(5))
        await advance(500)
        expect(f.dependencies.reconcile).toHaveBeenCalledTimes(2)
        await f.worker.stop()
        expect(f.requests.at(-1)!.signal.aborted).toBe(true)
        const before = f.dependencies.reconcile.mock.calls.length
        f.invalidate()
        await advance(60_000)
        expect(f.dependencies.reconcile).toHaveBeenCalledTimes(before)
        expect(warning).toHaveBeenCalledTimes(1)
    } finally {
        await f.worker.stop()
        warning.mockRestore()
    }
})

test.each(['read', 'reconcile'] as const)(
    'a failed initial %s recovers with bounded resync before any subscription exists',
    async (phase) => {
        const f = fixture()
        spyOn(console, 'warn').mockImplementation(() => {})
        f.dependencies[phase].mockRejectedValueOnce(new Error('temporary failure'))
        try {
            await f.worker.start()
            expect(f.dependencies.failed).toHaveBeenCalledTimes(1)
            expect(f.requests).toHaveLength(0)
            await advance(1999)
            expect(f.dependencies.read).toHaveBeenCalledTimes(1)
            await advance(1)
            expect(f.dependencies.read).toHaveBeenCalledTimes(2)
            expect(f.requests).toHaveLength(1)
            expect(f.dependencies.failed).toHaveBeenCalledTimes(1)
        } finally {
            await f.worker.stop()
        }
    },
)

test('legacy controllers use bounded polling and never start an unsupported subscription', async () => {
    const f = fixture()
    f.latest({ cursor: null, snapshot: observed().snapshot })
    try {
        f.worker.start()
        await settle()
        await advance(60_000)
        expect(f.dependencies.read).toHaveBeenCalledTimes(2)
        expect(f.dependencies.watch).not.toHaveBeenCalled()
    } finally {
        await f.worker.stop()
    }
})

test('bounded fair batches continue without another Docker event and yield between passes', async () => {
    let passes = 0
    const f = fixture({
        reconcile: async (_snapshot, _signal, guard) => {
            guard()
            passes++
            return { enabled: true, again: passes < 3 }
        },
    })
    try {
        f.worker.start()
        await settle()
        await advance(1999)
        expect(passes).toBe(1)
        await advance(1)
        expect(passes).toBe(2)
        await advance(2000)
        expect(passes).toBe(3)
        await advance(2000)
        expect(passes).toBe(3)
    } finally {
        await f.worker.stop()
    }
})

test('stop aborts collection admission and drains an in-flight pass before returning', async () => {
    const reading = Promise.withResolvers<DiscoveryObservation>()
    const f = fixture({ read: () => reading.promise })
    f.worker.start()
    await settle()
    let drained = false
    const stopping = f.worker.stop().then(() => {
        drained = true
    })
    await settle()
    expect(drained).toBe(false)
    reading.resolve(observed())
    await stopping
    expect(f.dependencies.reconcile).not.toHaveBeenCalled()
    expect(f.dependencies.watch).not.toHaveBeenCalled()
})

test('a failed admitted receipt rejects repeated stops after collection aborts', async () => {
    const finalizing = Promise.withResolvers<{ enabled: boolean; again: boolean }>()
    const error = new DiscoveryFinalizationError(new Error('receipt unavailable'))
    const f = fixture({ reconcile: () => finalizing.promise })
    const starting = f.worker.start()
    await settle()
    const stopping = f.worker.stop()
    void stopping.catch(() => undefined)
    finalizing.reject(error)
    await starting
    await expect(stopping).rejects.toBe(error)
    await expect(f.worker.stop()).rejects.toBe(error)
    expect(f.dependencies.failed).not.toHaveBeenCalled()
})

test.each([false, true])(
    'receipt failure remains a drain failure until its durable finalization is confirmed: %s',
    async (receiptSettled) => {
        const error = new DiscoveryFinalizationError(new Error('receipt unavailable'))
        const f = fixture({
            reconcile: async () => {
                throw error
            },
        })
        const warning = spyOn(console, 'warn').mockImplementation(() => undefined)
        await f.worker.start()
        f.dependencies.reconcile.mockResolvedValue({ enabled: true, again: false, receiptSettled })
        await f.worker.trigger()
        if (receiptSettled) await f.worker.stop()
        else await expect(f.worker.stop()).rejects.toBe(error)
        expect(f.dependencies.failed).toHaveBeenCalledTimes(1)
        warning.mockRestore()
    },
)
