import { expect, test } from 'bun:test'
import { createDockerDiscoveryController } from '@/server/Controller/docker-discovery.server.ts'
import type { DockerDiscoverySnapshot } from '@/lib/DockerDiscovery/Types/docker-discovery.types.ts'
import { dockerDiscoverySnapshotSchema } from '@/lib/DockerDiscovery/validation.ts'
const snapshot: DockerDiscoverySnapshot = {
    configuration: { version: 1, revision: 1, settings: { enabled: false, endpoint: null } },
    status: 'disabled',
    stale: true,
    error: null,
    candidates: [],
}

test('endpoint inspection is a bounded confidential POST with strictly redacted candidates', async () => {
    const controller = createDockerDiscoveryController(async (path, options) => {
        expect(path).toBe('/internal/v1/discovery/docker/inspect')
        expect(options).toMatchObject({
            privileged: true,
            confidential: true,
            method: 'POST',
            timeoutMs: 35000,
            responseLimit: 1048576,
        })
        expect(JSON.parse(options.body ?? '{}')).toEqual({
            endpoint: 'http://docker-proxy:2375',
            actorUserId: 'actor',
        })
        return { candidates: [] }
    })
    expect(
        await controller.inspect({ endpoint: 'http://docker-proxy:2375', actorUserId: 'actor' }),
    ).toEqual({ candidates: [] })
    const raw = createDockerDiscoveryController(async () => ({
        candidates: [],
        private: 'SYNTHETIC_SECRET',
    }))
    await expect(
        raw.inspect({ endpoint: 'http://docker-proxy:2375', actorUserId: 'actor' }),
    ).rejects.toThrow('controller_unavailable')
})

test('Docker discovery uses confidential bearer-authenticated bounded paths and validates save acknowledgments', async () => {
    const controller = createDockerDiscoveryController(async (path, options) => {
        expect(options).toMatchObject({
            privileged: true,
            confidential: true,
            timeoutMs: 10000,
            responseLimit: 1048576,
            acceptNonOkJson: true,
        })
        expect(path).toBe('/internal/v1/discovery/docker/config')
        expect(options.method).toBe('PUT')
        expect(JSON.parse(options.body!)).toEqual({
            actorUserId: 'actor',
            expectedRevision: 0,
            configuration: snapshot.configuration.settings,
            acknowledgeRestore: false,
        })
        return snapshot
    })
    expect(
        await controller.save({
            actorUserId: 'actor',
            expectedRevision: 0,
            configuration: snapshot.configuration.settings,
            acknowledgeRestore: false,
        }),
    ).toEqual(snapshot)
    const stale = createDockerDiscoveryController(async () => snapshot)
    await expect(
        stale.save({
            actorUserId: 'actor',
            expectedRevision: 2,
            configuration: snapshot.configuration.settings,
            acknowledgeRestore: false,
        }),
    ).rejects.toThrow('controller_unavailable')
})
test('Docker discovery rejects unknown fields and does not serialize raw Docker error responses', async () => {
    const bad = createDockerDiscoveryController(async () => ({
        ...snapshot,
        Env: ['SYNTHETIC_SECRET'],
    }))
    await expect(bad.status()).rejects.toThrow(
        'admin.dockerDiscovery.errors.controller_unavailable',
    )
    const error = createDockerDiscoveryController(async () => ({ error: 'conflict' }))
    await expect(error.status()).rejects.toThrow('admin.dockerDiscovery.errors.conflict')
    const raw = createDockerDiscoveryController(async () => ({ error: 'private Docker metadata' }))
    await expect(raw.status()).rejects.toThrow('controller_unavailable')
})

test('the shared Rust management fixture passes the web boundary without fields being dropped', async () => {
    const fixture: unknown = await Bun.file(
        new URL(
            '../../../../../core/tests/fixtures/docker-discovery-snapshot.json',
            import.meta.url,
        ),
    ).json()
    expect(fixture).toEqual(dockerDiscoverySnapshotSchema.parse(fixture))
})

test('change watches use authenticated bounded GETs, safe cursors and caller cancellation', async () => {
    const signal = new AbortController().signal
    const cursor = { runtimeId: 'a'.repeat(32), generation: Number.MAX_SAFE_INTEGER }
    const controller = createDockerDiscoveryController(async (path, options) => {
        expect(path).toBe(
            `/internal/v1/discovery/docker/watch?runtimeId=${cursor.runtimeId}&generation=${cursor.generation}`,
        )
        expect(options).toMatchObject({
            privileged: true,
            confidential: true,
            timeoutMs: 30_000,
            responseLimit: 1_048_576,
            signal,
        })
        expect(options.method).toBeUndefined()
        expect(options.body).toBeUndefined()
        return { cursor, snapshot: null }
    })
    expect(await controller.watch(cursor, signal)).toEqual({ cursor, snapshot: null })
    const initial = createDockerDiscoveryController(async (path) => {
        expect(path).toBe('/internal/v1/discovery/docker/watch')
        return { cursor, snapshot }
    })
    expect((await initial.watch(null, signal)).snapshot).toEqual(snapshot)
})

test('watch responses reject invalid identities, unsafe generations, private fields and inconsistent heartbeats', async () => {
    const signal = new AbortController().signal
    const cursor = { runtimeId: 'a'.repeat(32), generation: 1 }
    const values: unknown[] = [
        { cursor: { ...cursor, generation: Number.MAX_SAFE_INTEGER + 1 }, snapshot },
        { cursor: { ...cursor, runtimeId: 'unsafe' }, snapshot },
        { cursor, snapshot, secret: 'must-not-leave' },
        { cursor, snapshot: { ...snapshot, Env: ['must-not-leave'] } },
        { cursor: { ...cursor, generation: 2 }, snapshot: null },
        { error: 'private Docker payload' },
    ]
    await Promise.all(
        values.map(async (value) => {
            const controller = createDockerDiscoveryController(async () => value)
            await expect(controller.watch(cursor, signal)).rejects.toThrow('controller_unavailable')
        }),
    )
    const unavailable = createDockerDiscoveryController(async () => ({
        error: 'endpoint_unavailable',
    }))
    await expect(unavailable.watch(cursor, signal)).rejects.toThrow('endpoint_unavailable')
    const noSnapshot = createDockerDiscoveryController(async () => ({ cursor, snapshot: null }))
    await expect(noSnapshot.watch(null, signal)).rejects.toThrow('controller_unavailable')
})
