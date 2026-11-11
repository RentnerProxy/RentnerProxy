import { expect, mock, test } from 'bun:test'
import { createHash } from 'node:crypto'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

import type { ManagedBackupMaintenance } from '@/lib/Backups/Types/backups.types.ts'

const token = 'a'.repeat(43)
const tokenHash = createHash('sha256').update(token).digest('hex')
const initial: ManagedBackupMaintenance = {
    id: '01900000-0000-7000-8000-000000000001',
    requestId: '01900000-0000-7000-8000-000000000002',
    actorId: '01900000-0000-7000-8000-000000000003',
    actorName: 'Restore operator',
    artifactId: '01900000-0000-7000-8000-000000000004',
    metadataSha256: 'a'.repeat(64),
    operation: 'restore',
    state: 'queued',
    phase: 'queued',
    progressPercent: 0,
    requestedAt: 100,
    startedAt: null,
    finishedAt: null,
    error: null,
}
let status = initial
const stopWorkers = mock(async () => undefined)
const closeDatabase = mock(async () => undefined)
const execute = mock(async () => [{ tokenHash, expiresAt: new Date(Date.now() + 60_000) }])
const writeFile = mock(async (_contents: string) => undefined)
const sync = mock(async () => undefined)
const close = mock(async () => undefined)
const open = mock(async (_path: string, _flags: string, _mode: number) => ({
    writeFile,
    sync,
    close,
}))
const rename = mock(async (_from: string, _to: string) => undefined)
mock.module('node:fs/promises', () => ({ open, rename, unlink: async () => undefined }))
mock.module('@/server/Backups/restore-maintenance-status.server.ts', () => ({
    readRestoreMaintenanceStatus: async () => status,
    readRestoreMaintenanceObservation: async () => ({ maintenance: status, restoreResults: [] }),
}))
mock.module('@/server/Foundation/worker-lifecycle.server.ts', () => ({
    stopWorkersForRestoreMaintenance: stopWorkers,
}))
mock.module('@/db/index.ts', () => ({
    db: { execute },
    closeDatabaseForRestoreMaintenance: closeDatabase,
}))

const runtime = await import('@/server/Backups/restore-maintenance.server.ts')
const next = async () => new Response('normal operation')

async function runtimeProbe(script: string) {
    return runBunScript(
        `
        import assert from 'node:assert/strict'
        import { mock } from 'bun:test'
        mock.module('@tanstack/react-start/server-only', () => ({}))
        const original = ${JSON.stringify(initial)}
        const unsafe = { id: original.id, operation: 'restore', state: 'failed', phase: 'database' }
        const recovery = { ...original, id: '01900000-0000-7000-8000-000000000000', state: 'succeeded', phase: 'complete' }
        const deleted = { ...original, id: '01900000-0000-7000-8000-000000000005', operation: 'delete', state: 'succeeded' }
        let observation = { maintenance: deleted, restoreResults: [unsafe] }
        const acknowledged = Promise.withResolvers()
        let stopGate = null
        const calls = { reads: 0, stops: 0, closes: 0, prepares: 0, writes: 0, acknowledgements: [] }
        mock.module('./server/Backups/restore-maintenance-status.server.ts', () => ({
            readRestoreMaintenanceStatus: async () => { calls.reads++; return observation.maintenance },
            readRestoreMaintenanceObservation: async () => { calls.reads++; return observation },
        }))
        mock.module('./server/Foundation/worker-lifecycle.server.ts', () => ({ stopWorkersForRestoreMaintenance: async () => { calls.stops++; await stopGate?.promise } }))
        mock.module('./db/index.ts', () => ({
            db: { execute: async () => { calls.prepares++; return [] } },
            closeDatabaseForRestoreMaintenance: async () => { calls.closes++ },
        }))
        // Load the builtin namespace before replacing its private acknowledgement IO.
        await import('node:fs/promises')
        mock.module('node:fs/promises', () => ({
            open: async () => ({ writeFile: async text => { calls.acknowledgements.push(JSON.parse(text).id); acknowledged.resolve() }, sync: async () => {}, close: async () => {} }),
            rename: async () => {}, unlink: async () => {},
        }))
        const runtime = await import('./server/Backups/restore-maintenance.server.ts')
        const flush = async () => { for (let index = 0; index < 40; index++) await Promise.resolve() }
        const mutation = new Request('http://localhost/change', { method: 'POST' })
        const next = async () => { calls.writes++; return new Response('admitted') }
        ${script}
        console.log(JSON.stringify(calls))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
}

test.each(['fresh delete', 'fresh preflight', 'observed delete'] as const)(
    'HTTP admission retains a hidden destructive restore across %s and later operations',
    async (scenario) => {
        const output = await runtimeProbe(`
            if ('${scenario}' === 'observed delete') {
                observation = { maintenance: original, restoreResults: [] }
                assert.equal((await runtime.getActiveRestoreMaintenance()).id, original.id)
                await flush()
                observation = { maintenance: deleted, restoreResults: [unsafe] }
            }
            if ('${scenario}' === 'fresh preflight') observation.maintenance = { ...recovery, state: 'failed', phase: 'validating' }
            const active = await runtime.getActiveRestoreMaintenance()
            await flush()
            assert.equal(runtime.isRestoreMaintenanceFenced(), true)
            assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 503)
            assert.equal(calls.writes, 0)
            assert.equal(calls.stops, 1)
            assert.equal(calls.closes, 1)
            await acknowledged.promise
            assert.deepEqual(calls.acknowledgements, [unsafe.id])
            if ('${scenario}' === 'observed delete') assert.equal(active.id, unsafe.id)
            else assert.equal(active, null) // An ID-only receipt cannot invent the old actor/artifact.
            observation = { maintenance: { ...recovery, state: 'failed', phase: 'validating' }, restoreResults: [unsafe] }
            assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 503)
            observation = { maintenance: recovery, restoreResults: [unsafe, recovery] }
            assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 503)
            assert.equal(runtime.isRestoreMaintenanceFenced(), true) // This old process already closed SQL.
            assert.equal(calls.writes, 0)
        `)
        expect(JSON.parse(output.trim().split('\n').at(-1)!)).toMatchObject({
            stops: 1,
            closes: 1,
            writes: 0,
        })
    },
)

test('a first verified snapshot with a later complete recovery admits HTTP despite an older failure', async () => {
    const output = await runtimeProbe(`
        observation.restoreResults.push(recovery)
        assert.equal(await runtime.getActiveRestoreMaintenance(), null)
        assert.equal(calls.reads, 1)
        assert.equal(runtime.isRestoreMaintenanceFenced(), false)
        assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 200)
        assert.equal(calls.stops, 0)
        assert.equal(calls.closes, 0)
    `)
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toMatchObject({
        writes: 1,
        stops: 0,
        closes: 0,
    })
})

test('a new process prepares the replacement restore while restarting and releases only after complete recovery', async () => {
    const output = await runtimeProbe(`
        const restarting = { ...recovery, state: 'running', phase: 'restarting' }
        observation.maintenance = restarting
        assert.equal((await runtime.getActiveRestoreMaintenance()).id, restarting.id)
        await flush()
        assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 503)
        observation.maintenance = { ...restarting, state: 'failed' }
        assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 503)
        assert.equal((await runtime.withRestoreMaintenanceFence(new Request('http://localhost/health/ready'), next)).status, 200)
        assert.equal(calls.stops, 0)
        assert.equal(calls.closes, 0)
        assert.deepEqual(calls.acknowledgements, [])
        observation = { maintenance: recovery, restoreResults: [unsafe, recovery] }
        assert.equal(await runtime.getActiveRestoreMaintenance(), null)
        assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 200)
        assert.equal(runtime.isRestoreMaintenanceFenced(), false)
        assert.equal(calls.stops, 0)
        assert.equal(calls.closes, 0)
    `)
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toMatchObject({
        writes: 2,
        stops: 0,
        closes: 0,
    })
})

test.each(['succeeded', 'failed'] as const)(
    'replacement bootstrap for a %s restore cannot suppress the next restore drain or acknowledgement',
    async (terminalState) => {
        const output = await runtimeProbe(`
            const first = { ...recovery, state: 'running', phase: 'restarting' }
            const ready = new Request('http://localhost/health/ready')
            observation = { maintenance: first, restoreResults: [] }
            await runtime.getActiveRestoreMaintenance()
            await flush()
            assert.equal((await runtime.withRestoreMaintenanceFence(ready, next)).status, 200)
            assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 503)
            const receipt = { ...first, state: '${terminalState}', phase: '${terminalState}' === 'succeeded' ? 'complete' : 'restarting' }
            observation = { maintenance: original, restoreResults: [receipt] }
            assert.equal((await runtime.getActiveRestoreMaintenance()).id, original.id)
            await flush()
            assert.equal((await runtime.withRestoreMaintenanceFence(ready, next)).status, 503)
            assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 503)
            assert.equal(calls.stops, 0)
            assert.equal(calls.closes, 0)
            assert.deepEqual(calls.acknowledgements, [])
            stopGate = Promise.withResolvers()
            observation.maintenance = { ...original, state: 'running', phase: 'quiescing' }
            await runtime.getActiveRestoreMaintenance()
            await flush()
            assert.equal(calls.stops, 1)
            assert.equal(calls.closes, 0)
            assert.deepEqual(calls.acknowledgements, [])
            stopGate.resolve()
            await acknowledged.promise
            await flush()
            assert.equal(calls.closes, 1)
            assert.deepEqual(calls.acknowledgements, [original.id])
            await runtime.getActiveRestoreMaintenance()
            assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 503)
            observation = { maintenance: { ...original, state: 'succeeded', phase: 'complete' }, restoreResults: [receipt, { ...original, state: 'succeeded', phase: 'complete' }] }
            assert.equal((await runtime.withRestoreMaintenanceFence(ready, next)).status, 503)
            assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 503)
            assert.equal(calls.stops, 1)
            assert.equal(calls.closes, 1)
            assert.deepEqual(calls.acknowledgements, [original.id])
        `)
        expect(JSON.parse(output.trim().split('\n').at(-1)!)).toMatchObject({
            stops: 1,
            closes: 1,
            writes: 1,
            acknowledgements: [initial.id],
        })
    },
)

test('a failed replacement bootstrap remains admission-fenced when the next restore fails preflight', async () => {
    const output = await runtimeProbe(`
        const first = { ...recovery, state: 'running', phase: 'restarting' }
        observation = { maintenance: first, restoreResults: [] }
        await runtime.getActiveRestoreMaintenance()
        await flush()
        observation = { maintenance: original, restoreResults: [{ ...first, state: 'failed' }] }
        await runtime.getActiveRestoreMaintenance()
        await flush()
        observation.maintenance = { ...original, state: 'failed', phase: 'validating' }
        assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 503)
        assert.equal((await runtime.withRestoreMaintenanceFence(new Request('http://localhost/health/ready'), next)).status, 503)
        assert.equal(calls.stops, 0)
        assert.equal(calls.closes, 0)
        assert.deepEqual(calls.acknowledgements, [])
    `)
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toMatchObject({
        stops: 0,
        closes: 0,
        writes: 0,
    })
})

test('a proven preflight failure permits the next operation to prepare and quiesce under its own ID', async () => {
    const output = await runtimeProbe(`
        observation = { maintenance: original, restoreResults: [] }
        await runtime.getActiveRestoreMaintenance()
        await flush()
        const nextRestore = { ...original, id: recovery.id }
        observation = { maintenance: nextRestore, restoreResults: [{ ...unsafe, phase: 'validating' }] }
        assert.equal((await runtime.getActiveRestoreMaintenance()).id, nextRestore.id)
        await flush()
        assert.equal((await runtime.withRestoreMaintenanceFence(mutation, next)).status, 503)
        assert.equal(calls.stops, 0)
        observation.maintenance = { ...nextRestore, state: 'running', phase: 'quiescing' }
        await runtime.getActiveRestoreMaintenance()
        await acknowledged.promise
        assert.deepEqual(calls.acknowledgements, [nextRestore.id])
        assert.equal(calls.stops, 1)
        assert.equal(calls.closes, 1)
    `)
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toMatchObject({
        stops: 1,
        closes: 1,
        writes: 0,
    })
})

test('validation failures release the admission fence without stopping services; destructive acknowledgement follows an actual drain', async () => {
    const viewer = new Request('http://localhost/api/live-snapshot', {
        headers: { cookie: 'rentnerproxy_session=' + token },
    })
    const mutation = new Request('http://localhost/change', { method: 'POST' })
    await runtime.getActiveRestoreMaintenance()
    await Bun.sleep(1)
    expect(runtime.isRestoreMaintenanceFenced()).toBe(true)
    expect(runtime.authorizeMaintenanceSnapshot(viewer)).toBe(true)
    expect((await runtime.withRestoreMaintenanceFence(mutation, next)).status).toBe(503)
    expect(stopWorkers).not.toHaveBeenCalled()
    expect(closeDatabase).not.toHaveBeenCalled()
    expect(writeFile).not.toHaveBeenCalled()
    status = {
        ...initial,
        state: 'failed',
        phase: 'validating',
        error: 'artifact_invalid',
        finishedAt: 200,
    }
    await runtime.getActiveRestoreMaintenance()
    expect(runtime.isRestoreMaintenanceFenced()).toBe(false)
    expect(runtime.authorizeMaintenanceSnapshot(viewer)).toBe(false)
    expect((await runtime.withRestoreMaintenanceFence(mutation, next)).status).toBe(200)
    expect(stopWorkers).not.toHaveBeenCalled()
    expect(closeDatabase).not.toHaveBeenCalled()
    expect(writeFile).not.toHaveBeenCalled()

    status = { ...initial, id: '01900000-0000-7000-8000-000000000005' }
    await runtime.getActiveRestoreMaintenance()
    await Bun.sleep(1)
    status = { ...status, state: 'running', phase: 'quiescing', progressPercent: 20 }
    await runtime.getActiveRestoreMaintenance()
    await Bun.sleep(1)
    expect(stopWorkers).toHaveBeenCalledTimes(1)
    expect(closeDatabase).toHaveBeenCalledTimes(1)
    expect(writeFile).toHaveBeenCalledTimes(1)
    expect(JSON.parse(writeFile.mock.calls[0]![0])).toMatchObject({ id: status.id })
    expect(rename).toHaveBeenCalledTimes(1)
    expect(rename.mock.calls[0]![1]).toBe('/run/rentnerproxy/maintenance/web-quiesced.json')
    expect(runtime.authorizeMaintenanceSnapshot(viewer)).toBe(true)
    expect(stopWorkers.mock.invocationCallOrder[0]!).toBeLessThan(
        closeDatabase.mock.invocationCallOrder[0]!,
    )
    expect(closeDatabase.mock.invocationCallOrder[0]!).toBeLessThan(
        writeFile.mock.invocationCallOrder[0]!,
    )
    status = { ...status, state: 'failed', error: 'execution_failed' }
    await runtime.getActiveRestoreMaintenance()
    expect(runtime.isRestoreMaintenanceFenced()).toBe(true)
    expect((await runtime.withRestoreMaintenanceFence(mutation, next)).status).toBe(503)
})
