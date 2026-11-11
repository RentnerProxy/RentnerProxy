import { expect, spyOn, test } from 'bun:test'
import { ReliabilityError } from '../../../../../../scripts/runtime-reliability/harness.ts'
import {
    backgroundPauseGuard,
    backgroundPauseMarker,
    backgroundPauseProcessGuard,
    backgroundPauseDetachedScript,
    checkBackgroundCancellation,
    withBackgroundPause,
} from '../../../../../../scripts/runtime-scale/background-pause.ts'

test('detached entrypoint keeps PID, identity ticks and marker entirely in argv and rejects code-like arguments', async () => {
    const script = `await (${backgroundPauseProcessGuard.toString()})(async d=>{if(await d.identity())process.exit(2);if(await d.requested())process.exit(3)}).catch(()=>process.exit(1))`
    const marker = backgroundPauseMarker('fixture')
    const valid = Bun.spawn(
        [process.execPath, '--no-env-file', '-e', script, '999999999', '123456', marker],
        { stdout: 'ignore', stderr: 'ignore' },
    )
    expect(await valid.exited).toBe(0)
    const invalid = await Promise.all(
        [
            ['2);process.exit(0)//', '123456', marker],
            ['999999999', '0;process.exit(0)', marker],
            ['999999999', '123456', marker + ';process.exit(0)//'],
        ].map(async (args) => {
            const child = Bun.spawn(
                [process.execPath, '--no-env-file', '-e', backgroundPauseDetachedScript, ...args],
                { stdout: 'ignore', stderr: 'ignore' },
            )
            return child.exited
        }),
    )
    expect(invalid).toEqual([1, 1, 1])
    expect(backgroundPauseDetachedScript).not.toContain(marker)
    expect(backgroundPauseDetachedScript).not.toContain('123456')
})

function guardFixture() {
    let now = 0
    let requested = false
    let live = true
    const events: string[] = []
    const dependencies = {
        identity: async () => live,
        signal: (signal: 'SIGSTOP' | 'SIGCONT') => {
            events.push(signal)
        },
        write: async (state: string) => {
            events.push(state)
        },
        requested: async () => requested,
        now: () => now,
        sleep: async () => {
            now += 100
        },
    }
    return {
        dependencies,
        events,
        request: () => {
            requested = true
        },
        replace: () => {
            live = false
        },
        advance: () => {
            now += 90000
        },
    }
}

test('each pause has its own run-scoped readiness and resume marker', () => {
    const markers = Array.from({ length: 100 }, () => backgroundPauseMarker('fixture'))
    expect(new Set(markers).size).toBe(100)
    expect(
        markers.every((marker) => /^\/tmp\/background-resume-fixture-[0-9a-f-]{36}$/u.test(marker)),
    ).toBe(true)
})

test('an aborted scenario is classified separately from retryable command failures', () => {
    const signal = AbortSignal.abort()
    try {
        checkBackgroundCancellation(signal)
        throw new Error('Expected cancellation')
    } catch (error) {
        expect(error).toBeInstanceOf(ReliabilityError)
        expect((error as ReliabilityError).category).toBe('cancelled')
    }
    expect(() => checkBackgroundCancellation(new AbortController().signal)).not.toThrow()
})

test('normal resume terminates the guard before a later pause can begin', async () => {
    const first = guardFixture()
    first.dependencies.sleep = async () => {
        first.request()
    }
    await backgroundPauseGuard(first.dependencies)
    expect(first.events).toEqual(['SIGSTOP', 'armed', 'SIGCONT', 'resumed'])
    const second = guardFixture()
    second.dependencies.sleep = async () => {
        first.advance()
        expect(first.events).toEqual(['SIGSTOP', 'armed', 'SIGCONT', 'resumed'])
        second.request()
    }
    await backgroundPauseGuard(second.dependencies)
    expect(second.events).toEqual(['SIGSTOP', 'armed', 'SIGCONT', 'resumed'])
})

test('a host crash still gets one bounded emergency resume', async () => {
    const fixture = guardFixture()
    await backgroundPauseGuard(fixture.dependencies, 200)
    expect(fixture.events).toEqual(['SIGSTOP', 'armed', 'SIGCONT', 'resumed'])
})

test('a recycled PID is never stopped or resumed', async () => {
    const initial = guardFixture()
    initial.replace()
    await backgroundPauseGuard(initial.dependencies)
    expect(initial.events).toEqual(['stale'])
    const replaced = guardFixture()
    replaced.dependencies.sleep = async () => {
        replaced.replace()
        replaced.request()
    }
    await backgroundPauseGuard(replaced.dependencies)
    expect(replaced.events).toEqual(['SIGSTOP', 'armed', 'stale'])
})

test('guard failure after stop resumes before surfacing failure', async () => {
    const fixture = guardFixture()
    fixture.dependencies.sleep = async () => {
        throw new Error('fixture')
    }
    await expect(backgroundPauseGuard(fixture.dependencies)).rejects.toThrow('fixture')
    expect(fixture.events).toEqual(['SIGSTOP', 'armed', 'SIGCONT', 'resumed'])
})

for (const phase of ['before', 'arming', 'operation', 'operation-failure'] as const) {
    test('pause cancellation/failure at ' + phase + ' never skips required resume', async () => {
        const abort = new AbortController()
        const events: string[] = []
        if (phase === 'before') abort.abort()
        await expect(
            withBackgroundPause(
                abort.signal,
                async () => {
                    events.push('arm')
                    if (phase === 'arming') abort.abort()
                },
                async () => {
                    events.push('operation')
                    if (phase === 'operation-failure') throw new Error('fixture')
                    abort.abort()
                },
                async () => {
                    events.push('resume')
                },
            ),
        ).rejects.toThrow()
        expect(events).toEqual(
            phase === 'before'
                ? []
                : phase === 'arming'
                  ? ['arm', 'resume']
                  : ['arm', 'operation', 'resume'],
        )
        if (phase !== 'operation-failure')
            expect(() => checkBackgroundCancellation(abort.signal)).toThrow('cancelled')
    })
}

test('the exact detached guard serialization runs without module closures', async () => {
    const child = Bun.spawn(
        [
            process.execPath,
            '--no-env-file',
            '-e',
            `let state='';const signals=[];await (${backgroundPauseGuard.toString()})({identity:async()=>true,signal:s=>signals.push(s),write:async s=>{state=s},requested:async()=>true,now:()=>0,sleep:async()=>{}});if(state!=='resumed'||signals.join(',')!=='SIGSTOP,SIGCONT')process.exit(1)`,
        ],
        { stdout: 'ignore', stderr: 'ignore' },
    )
    expect(await child.exited).toBe(0)
})

test('a resume failure keeps the primary failure identity, including falsy failures', async () => {
    const log = spyOn(console, 'error').mockImplementation(() => {})
    const cleanup = new Error('private cleanup detail')
    try {
        const failures = [new Error('private operation detail'), null, undefined, false, 0, '']
        const outcomes = await Promise.all(
            failures.map((primary) =>
                withBackgroundPause(
                    new AbortController().signal,
                    async () => {},
                    async () => {
                        throw primary
                    },
                    async () => {
                        throw cleanup
                    },
                ).then(
                    () => ({ fulfilled: true }),
                    (reason: unknown) => ({ reason }),
                ),
            ),
        )
        expect(outcomes).toEqual(failures.map((reason) => ({ reason })))
        expect(
            log.mock.calls.every(
                ([message]) => message === 'Background pause cleanup failed after primary failure',
            ),
        ).toBe(true)
        await expect(
            withBackgroundPause(
                new AbortController().signal,
                async () => {},
                async () => 1,
                async () => {
                    throw cleanup
                },
            ),
        ).rejects.toBe(cleanup)
    } finally {
        log.mockRestore()
    }
})
