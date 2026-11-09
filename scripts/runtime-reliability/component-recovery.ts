// oxlint-disable no-await-in-loop -- Process ownership must settle before readiness or fixture work is admitted.
import assert from 'node:assert/strict'
import type {
    ApplianceProcess,
    ComponentRecoveryDependencies,
    ComponentRecoverySnapshot,
} from './Types/component-recovery.types.ts'

/** Include start ticks so a recycled PID cannot satisfy ownership or replacement checks. */
function identity(process: ApplianceProcess): string {
    return process.pid + ':' + process.startTicks
}

/** Require one live owner; duplicate UID writers must never be mistaken for recovery. */
function owner(
    snapshot: ComponentRecoverySnapshot,
    role: ApplianceProcess['role'],
): ApplianceProcess {
    const matches = snapshot.processes.filter((process) => process.role === role)
    assert.equal(matches.length, 1, 'component recovery requires one process per role')
    assert.notEqual(matches[0]!.state, 'Z', 'recovered owner must be live')
    return matches[0]!
}

/** Reject boundary or native ownership changes across an asynchronous readiness request. */
function assertStableOwners(
    current: ComponentRecoverySnapshot,
    expected: ComponentRecoverySnapshot,
    roles: readonly ApplianceProcess['role'][] = ['web', 'controller', 'caddy'],
): void {
    assert.equal(current.running, true)
    assert.equal(current.startedAt, expected.startedAt)
    for (const role of roles)
        assert.equal(
            identity(owner(current, role)),
            identity(owner(expected, role)),
            'ownership must stay stable through readiness',
        )
}

/** Poll observable state within a deadline without swallowing assertion failures as transient errors. */
async function waitForState(
    check: () => Promise<boolean>,
    timeoutMs: number,
    label: string,
    dependencies: ComponentRecoveryDependencies,
): Promise<void> {
    const now = dependencies.now ?? (() => performance.now())
    const sleep = dependencies.sleep ?? Bun.sleep
    const deadline = now() + timeoutMs
    while (now() <= deadline) {
        if (await check()) {
            if (now() <= deadline) return
            break
        }
        const remaining = deadline - now()
        if (remaining <= 0) break
        await sleep(Math.min(250, remaining))
    }
    throw dependencies.timeoutError(label)
}

/** Separate leaf-process replacement from controller fail-stop before admitting readiness or fixture writers. */
export async function recoverReliabilityComponent(
    component: 'web' | 'controller' | 'caddy',
    dependencies: ComponentRecoveryDependencies,
): Promise<void> {
    const before = await dependencies.snapshot()
    assert.ok(before?.running, 'component recovery starts from a running appliance')
    const oldWeb = owner(before, 'web')
    const oldController = owner(before, 'controller')
    const oldCaddy = owner(before, 'caddy')
    const target = component === 'web' ? oldWeb : component === 'caddy' ? oldCaddy : oldController
    await dependencies.signal(target)
    let recovered: ComponentRecoverySnapshot | undefined
    let readinessDeadline: number | undefined
    if (component !== 'controller') {
        await waitForState(
            async () => {
                const current = await dependencies.snapshot()
                if (!current) return false
                assert.equal(current.running, true, 'leaf recovery must not exit the appliance')
                assert.equal(
                    current.startedAt,
                    before.startedAt,
                    'web recovery preserves the appliance start identity',
                )
                assert.equal(identity(owner(current, 'controller')), identity(oldController))
                if (component === 'web')
                    assert.equal(identity(owner(current, 'caddy')), identity(oldCaddy))
                else assert.equal(identity(owner(current, 'web')), identity(oldWeb))
                const web = current.processes.filter((process) => process.role === 'web')
                assert.ok(
                    web.length <= 1,
                    'parallel UID 10002 writers are forbidden during web recovery',
                )
                const candidates = current.processes.filter((process) => process.role === component)
                assert.ok(candidates.length <= 1, 'duplicate recovering owners are forbidden')
                if (
                    !candidates.length ||
                    identity(candidates[0]!) === identity(target) ||
                    candidates[0]!.state === 'Z'
                )
                    return false
                recovered = current
                return true
            },
            60_000,
            component === 'web'
                ? 'isolated web process replacement'
                : 'isolated Caddy process replacement',
            dependencies,
        )
    } else {
        await waitForState(
            async () => {
                const current = await dependencies.snapshot()
                if (!current) return false
                assert.equal(
                    current.startedAt,
                    before.startedAt,
                    'controller recovery requires explicit harness start',
                )
                assert.ok(
                    current.processes
                        .filter((process) => process.role === 'controller')
                        .every((process) => identity(process) === identity(oldController)),
                    'controller must not restart locally',
                )
                assert.ok(
                    current.processes
                        .filter((process) => process.role === 'caddy')
                        .every((process) => identity(process) === identity(oldCaddy)),
                    'Caddy must not restart alongside a failed controller',
                )
                return !current.running
            },
            45_000,
            'controller failure stops the appliance',
            dependencies,
        )
        await dependencies.start()
        readinessDeadline = (dependencies.now?.() ?? performance.now()) + 150_000
        await waitForState(
            async () => {
                const current = await dependencies.snapshot()
                if (!current?.running) return false
                if (current.startedAt === before.startedAt) return false
                const controllers = current.processes.filter(
                    (process) => process.role === 'controller',
                )
                const caddies = current.processes.filter((process) => process.role === 'caddy')
                assert.ok(
                    controllers.length <= 1 && caddies.length <= 1,
                    'duplicate native owners are forbidden',
                )
                if (!controllers.length || !caddies.length) return false
                assert.notEqual(identity(owner(current, 'controller')), identity(oldController))
                assert.notEqual(identity(owner(current, 'caddy')), identity(oldCaddy))
                const web = current.processes.filter((process) => process.role === 'web')
                assert.ok(
                    web.length <= 1,
                    'parallel UID 10002 writers are forbidden after appliance start',
                )
                if (!web.length || web[0]!.state === 'Z') return false
                assert.notEqual(identity(web[0]!), identity(oldWeb))
                // Startup migrations can briefly own the web UID before the actual server is launched.
                if (!(await dependencies.ready())) return false
                const confirmed = await dependencies.snapshot()
                if (!confirmed) return false
                assertStableOwners(confirmed, current, ['controller', 'caddy'])
                const confirmedWeb = confirmed.processes.filter((process) => process.role === 'web')
                assert.ok(
                    confirmedWeb.length <= 1,
                    'parallel UID 10002 writers are forbidden after appliance start',
                )
                if (
                    !confirmedWeb.length ||
                    confirmedWeb[0]!.state === 'Z' ||
                    identity(confirmedWeb[0]!) !== identity(web[0]!)
                )
                    return false
                recovered = confirmed
                return true
            },
            150_000,
            'new appliance process owners',
            dependencies,
        )
    }
    assert.ok(recovered)
    const expected = recovered
    await waitForState(
        async () => {
            const current = await dependencies.snapshot()
            if (!current) return false
            assertStableOwners(current, expected)
            if (!(await dependencies.ready())) return false
            const confirmed = await dependencies.snapshot()
            if (!confirmed) return false
            assertStableOwners(confirmed, expected)
            return true
        },
        readinessDeadline === undefined
            ? 150_000
            : Math.max(0, readinessDeadline - (dependencies.now?.() ?? performance.now())),
        'component readiness after ownership recovery',
        dependencies,
    )
}
