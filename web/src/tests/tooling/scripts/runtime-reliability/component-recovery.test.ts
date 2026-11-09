import { expect, test } from 'bun:test'
import { recoverReliabilityComponent } from '../../../../../../scripts/runtime-reliability/component-recovery.ts'
import type {
    ApplianceProcess,
    ComponentRecoveryDependencies,
    ComponentRecoverySnapshot,
} from '../../../../../../scripts/runtime-reliability/Types/component-recovery.types.ts'

/** Model process ownership without wall-clock sleeps or a Docker daemon. */
function snapshot(
    startedAt = 'original',
    webTicks = '1',
    controllerTicks = '2',
    caddyTicks = '3',
): ComponentRecoverySnapshot {
    return {
        running: true,
        startedAt,
        processes: [
            { pid: 10, startTicks: webTicks, uid: 10002, role: 'web', state: 'S' },
            { pid: 11, startTicks: controllerTicks, uid: 10001, role: 'controller', state: 'S' },
            { pid: 12, startTicks: caddyTicks, uid: 10001, role: 'caddy', state: 'S' },
        ],
    }
}

/** Advance only the injected polling clock while retaining the final observable state. */
function fixture(states: ComponentRecoverySnapshot[], readiness: boolean[] = [true]) {
    let now = 0
    let current: ComponentRecoverySnapshot | undefined
    const events: string[] = []
    const signals: ApplianceProcess[] = []
    const dependencies: ComponentRecoveryDependencies = {
        snapshot: async () => {
            current = states.length > 1 ? states.shift()! : states[0]!
            events.push('snapshot:' + current.startedAt + ':' + current.running)
            return current
        },
        signal: async (target) => {
            signals.push(target)
            events.push('signal')
        },
        start: async () => {
            expect(current?.running).toBe(false)
            events.push('start')
        },
        ready: async () => {
            events.push('ready')
            return readiness.length > 1 ? readiness.shift()! : readiness[0]!
        },
        now: () => now,
        sleep: async (milliseconds) => {
            now += milliseconds
        },
        timeoutError: (label) => new Error('timeout: ' + label),
    }
    return { dependencies, events, signals }
}

test('web recovery waits for old identity replacement even while its old readiness would pass', async () => {
    const f = fixture(
        [snapshot(), snapshot(), snapshot(), snapshot('original', '10')],
        [false, true],
    )
    await recoverReliabilityComponent('web', f.dependencies)
    expect(f.signals).toEqual([snapshot().processes[0]!])
    expect(f.events).not.toContain('start')
    expect(f.events.indexOf('ready')).toBeGreaterThan(4)
    expect(f.events.filter((event) => event === 'ready')).toHaveLength(2)
})

test('readiness cannot bypass a web process that never exits', async () => {
    const f = fixture([snapshot()])
    await expect(recoverReliabilityComponent('web', f.dependencies)).rejects.toThrow(
        'isolated web process replacement',
    )
    expect(f.events).not.toContain('ready')
    expect(f.events).not.toContain('start')
})

test('controller recovery observes fail-stop before explicit start and tolerates startup migration ownership', async () => {
    const stopped = { running: false, startedAt: 'original', processes: [] }
    const f = fixture(
        [
            snapshot(),
            snapshot(),
            stopped,
            snapshot('restarted', '20', '21', '22'),
            snapshot('restarted', '30', '21', '22'),
        ],
        [false, true, true],
    )
    await recoverReliabilityComponent('controller', f.dependencies)
    expect(f.signals).toEqual([snapshot().processes[1]!])
    expect(f.events.indexOf('start')).toBeGreaterThan(f.events.indexOf('snapshot:original:false'))
    expect(f.events.indexOf('ready')).toBeGreaterThan(f.events.indexOf('start'))
})

test.each([
    ['appliance exit', { running: false, startedAt: 'original', processes: [] }],
    ['appliance restart', snapshot('restarted', '10')],
    ['controller replacement', snapshot('original', '10', '20')],
    ['Caddy replacement', snapshot('original', '10', '2', '30')],
    [
        'parallel UID writers',
        {
            ...snapshot('original', '10'),
            processes: [...snapshot('original', '10').processes, snapshot().processes[0]!],
        },
    ],
] as const)('web recovery rejects %s before admitting readiness', async (_label, invalid) => {
    const f = fixture([snapshot(), { ...invalid, processes: [...invalid.processes] }])
    await expect(recoverReliabilityComponent('web', f.dependencies)).rejects.toThrow()
    expect(f.events).not.toContain('ready')
})

test('controller cannot restart locally while its original Caddy remains', async () => {
    const f = fixture([snapshot(), snapshot('original', '1', '20')])
    await expect(recoverReliabilityComponent('controller', f.dependencies)).rejects.toThrow(
        'controller must not restart locally',
    )
    expect(f.events).not.toContain('start')
    expect(f.events).not.toContain('ready')
})

test('readiness polling still rejects a second web replacement after the identity barrier', async () => {
    const f = fixture([snapshot(), snapshot('original', '10'), snapshot('original', '11')])
    await expect(recoverReliabilityComponent('web', f.dependencies)).rejects.toThrow(
        'ownership must stay stable',
    )
    expect(f.events).not.toContain('ready')
})

test('Caddy recovery replaces its start-tick identity without replacing web or controller', async () => {
    const f = fixture([snapshot(), snapshot(), snapshot('original', '1', '2', '30')])
    await recoverReliabilityComponent('caddy', f.dependencies)
    expect(f.signals).toEqual([snapshot().processes[2]!])
    expect(f.events).not.toContain('start')
    expect(f.events).toContain('ready')
})

test('controller readiness crossing migration-to-server transition retries with the fresh web identity', async () => {
    const stopped = { running: false, startedAt: 'original', processes: [] }
    const f = fixture([
        snapshot(),
        stopped,
        snapshot('restarted', '20', '21', '22'),
        snapshot('restarted', '30', '21', '22'),
        snapshot('restarted', '30', '21', '22'),
    ])
    await recoverReliabilityComponent('controller', f.dependencies)
    expect(f.events.filter((event) => event === 'ready')).toHaveLength(3)
    expect(f.events.filter((event) => event === 'start')).toHaveLength(1)
})

test('controller readiness crossing a native ownership change fails instead of accepting the new owner', async () => {
    const stopped = { running: false, startedAt: 'original', processes: [] }
    const f = fixture([
        snapshot(),
        stopped,
        snapshot('restarted', '20', '21', '22'),
        snapshot('restarted', '30', '21', '23'),
    ])
    await expect(recoverReliabilityComponent('controller', f.dependencies)).rejects.toThrow(
        'ownership must stay stable',
    )
})

test('a successful readiness response cannot hide a web replacement during its request', async () => {
    const f = fixture([
        snapshot(),
        snapshot('original', '10'),
        snapshot('original', '10'),
        snapshot('original', '11'),
    ])
    await expect(recoverReliabilityComponent('web', f.dependencies)).rejects.toThrow(
        'ownership must stay stable',
    )
    expect(f.events.filter((event) => event === 'ready')).toHaveLength(1)
})
