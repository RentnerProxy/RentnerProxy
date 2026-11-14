import { describe, expect, test } from 'bun:test'
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
    blogBytes,
    blogReady,
    buildReleaseBlog,
    prepareReleaseBlog,
} from '../../../../../.github/scripts/release/generate-release-blog.ts'
import {
    parseBlogAsset,
    publishReleaseBlog,
} from '../../../../../.github/scripts/release/publish-release-blog.ts'
import type {
    BlogAsset,
    BlogPublicationClient,
} from '../../../../../.github/scripts/release/Types/release-blog.types.ts'

const readiness = {
    schemaVersion: 1,
    releaseId: 123,
    tag: 'v1.0.0-beta.3',
    revision: 'a'.repeat(40),
    channel: 'beta',
    publishedAt: '2026-11-14T12:00:00Z',
    image: {
        repository: 'ghcr.io/rentnerproxy/platform',
        digest: `sha256:${'b'.repeat(64)}`,
        platforms: ['linux/amd64', 'linux/arm64'],
    },
}

function source() {
    return {
        contentRevision: 1,
        locales: {
            de: {
                title: 'Eine neue Version',
                summary: 'Die neue Version ist verfügbar.',
                sections: [
                    {
                        id: 'changes',
                        title: 'Änderungen',
                        paragraphs: ['Hier stehen die geprüften Änderungen.'],
                        code: { language: 'bash', value: 'docker compose pull' },
                        links: [
                            {
                                label: 'Release',
                                url: 'https://github.com/RentnerProxy/platform/releases/tag/v1.0.0-beta.3',
                            },
                        ],
                    },
                ],
            },
            en: {
                title: 'A new version',
                summary: 'The new version is available.',
                sections: [
                    {
                        id: 'changes',
                        title: 'Changes',
                        paragraphs: ['Verified changes are described here.'],
                    },
                ],
            },
            es: {
                title: 'Una nueva versión',
                summary: 'La nueva versión está disponible.',
                sections: [
                    {
                        id: 'changes',
                        title: 'Cambios',
                        paragraphs: ['Aquí se describen los cambios verificados.'],
                    },
                ],
            },
            zh: {
                title: '新版本',
                summary: '新版本现已发布。',
                sections: [
                    { id: 'changes', title: '更新内容', paragraphs: ['此处介绍经过验证的更新。'] },
                ],
            },
        },
    }
}

function measurement() {
    return {
        id: 'latency',
        unit: 'ms',
        direction: 'lower',
        baseline: { tag: 'v1.0.0-beta.2', revision: 'c'.repeat(40), values: [10, 11, 12, 10, 11] },
        candidate: { tag: readiness.tag, revision: readiness.revision, values: [8, 9, 10, 8, 9] },
        measuredAt: readiness.publishedAt,
        environment: 'Isolated fixture, identical hardware',
        methodology: 'Five repeated measurements after warmup',
        runUrl: 'https://github.com/RentnerProxy/platform/actions/runs/123',
    }
}

function fixture() {
    const files = new Map<string, { asset: BlogAsset; bytes: Uint8Array }>()
    const events: string[] = []
    let fail = ''
    const client: BlogPublicationClient = {
        async verifyIdentity() {
            events.push('identity')
            if (fail === 'identity') throw new Error('identity')
        },
        async listAssets() {
            return [...files.values()].map((value) => value.asset)
        },
        async readAsset(asset) {
            events.push(`read:${asset.name}`)
            const file = files.get(asset.name)
            if (!file) throw new Error('missing')
            return fail === 'read' ? Buffer.from('corrupt') : file.bytes
        },
        async uploadAsset(name, bytes) {
            events.push(`upload:${name}`)
            if (fail === name) throw new Error('upload')
            const asset = { id: files.size + 1, name }
            files.set(name, { asset, bytes })
            return asset
        },
        async deleteAsset(asset) {
            events.push(`delete:${asset.name}`)
            files.delete(asset.name)
        },
    }
    return {
        files,
        events,
        client,
        failure(value: string) {
            fail = value
        },
    }
}

describe('release blog contract', () => {
    test('uses assessed release identity and keeps four authored locales and code as text', () => {
        const post = buildReleaseBlog(readiness, source())
        expect(post.locales.de.sections[0]?.code?.value).toBe('docker compose pull')
        expect(post.image).toEqual(readiness.image)
        expect(Object.keys(post.locales)).toEqual(['de', 'en', 'es', 'zh'])
        expect(blogReady(post, blogBytes(post), readiness.publishedAt).postAsset).toBe(
            'rentnerproxy-blog-post-r1.json',
        )
    })

    test.each(['locale', 'revision', 'section', 'url', 'controls', 'language', 'readiness'])(
        'rejects malformed authored contract: %s',
        (mode) => {
            const authored = source()
            const identity = structuredClone(readiness)
            if (mode === 'locale') Reflect.deleteProperty(authored.locales, 'zh')
            if (mode === 'revision') authored.contentRevision = 0
            if (mode === 'section') authored.locales.en.sections[0]!.id = '../escape'
            if (mode === 'url')
                authored.locales.de.sections[0]!.links[0]!.url = 'https://user:secret@example.com'
            if (mode === 'controls') authored.locales.en.title = 'hidden\u0000text'
            if (mode === 'language') authored.locales.de.sections[0]!.code.language = 'exec'
            if (mode === 'readiness') identity.image.digest = 'invalid'
            expect(() => buildReleaseBlog(identity, authored)).toThrow()
        },
    )

    test.each([
        'valid',
        'missing-label',
        'samples',
        'negative',
        'identity',
        'baseline',
        'url',
        'duplicate',
    ])('accepts only evidence-backed benchmark shape: %s', (mode) => {
        const authored = { ...source(), benchmarks: [measurement()] }
        for (const localized of Object.values(authored.locales))
            Object.assign(localized, {
                benchmarks: { latency: { title: 'Latency', description: 'Repeated measurements' } },
            })
        const metric = authored.benchmarks[0]!
        if (mode === 'missing-label') Reflect.deleteProperty(authored.locales.zh, 'benchmarks')
        if (mode === 'samples') metric.candidate.values = [1, 2]
        if (mode === 'negative') metric.candidate.values[0] = -1
        if (mode === 'identity') metric.candidate.revision = 'd'.repeat(40)
        if (mode === 'baseline') metric.baseline.tag = readiness.tag
        if (mode === 'url') metric.runUrl = 'javascript:alert(1)'
        if (mode === 'duplicate') authored.benchmarks.push(measurement())
        if (mode === 'valid')
            expect(
                buildReleaseBlog(readiness, authored).benchmarks?.[0]?.candidate.values,
            ).toHaveLength(5)
        else expect(() => buildReleaseBlog(readiness, authored)).toThrow()
    })

    test('missing authored source skips cleanly and invalid tags fail before reading', async () => {
        const directory = await mkdtemp(join(tmpdir(), 'release-blog-'))
        try {
            expect(await prepareReleaseBlog(directory, directory, readiness.tag)).toBeUndefined()
            await expect(prepareReleaseBlog(directory, directory, '../secret')).rejects.toThrow()
            await mkdir(join(directory, '.github/release-content'), { recursive: true })
            await writeFile(
                join(directory, '.github/release-content', `${readiness.tag}.json`),
                JSON.stringify(source()),
            )
            await writeFile(join(directory, 'rentnerproxy-update.json'), JSON.stringify(readiness))
            const post = await prepareReleaseBlog(directory, directory, readiness.tag)
            expect(post).toBeDefined()
            expect(await readFile(join(directory, 'rentnerproxy-blog-post-r1.json'), 'utf8')).toBe(
                new TextDecoder().decode(blogBytes(post!)),
            )
        } finally {
            await rm(directory, { recursive: true, force: true })
        }
    })

    test.each([
        'valid',
        'source-only',
        'missing',
        'schema',
        'release-id',
        'tag',
        'revision',
        'digest',
        'duplicate',
        'samples',
        'oversized',
        'malformed',
    ])('binds optional benchmark artifact to the assessed candidate: %s', async (mode) => {
        const directory = await mkdtemp(join(tmpdir(), 'release-blog-evidence-'))
        try {
            await mkdir(join(directory, '.github/release-content'), { recursive: true })
            const authored = source()
            for (const localized of Object.values(authored.locales))
                Object.assign(localized, {
                    benchmarks: {
                        latency: { title: 'Latency', description: 'Repeated measurements' },
                    },
                })
            if (mode === 'duplicate' || mode === 'source-only')
                Object.assign(authored, { benchmarks: [measurement()] })
            await writeFile(
                join(directory, '.github/release-content', `${readiness.tag}.json`),
                JSON.stringify(authored),
            )
            await writeFile(join(directory, 'rentnerproxy-update.json'), JSON.stringify(readiness))
            const evidence = {
                schemaVersion: 1,
                releaseId: readiness.releaseId,
                tag: readiness.tag,
                revision: readiness.revision,
                imageDigest: readiness.image.digest,
                benchmarks: [measurement()],
            }
            if (mode === 'schema') evidence.schemaVersion = 2
            if (mode === 'release-id') evidence.releaseId += 1
            if (mode === 'tag') evidence.tag = 'v1.0.0-beta.4'
            if (mode === 'revision') evidence.revision = 'd'.repeat(40)
            if (mode === 'digest') evidence.imageDigest = `sha256:${'e'.repeat(64)}`
            if (mode === 'samples') evidence.benchmarks[0]!.candidate.values = [1]
            if (mode !== 'missing' && mode !== 'source-only')
                await writeFile(
                    join(directory, 'release-blog-benchmarks.json'),
                    mode === 'oversized'
                        ? ' '.repeat(1_048_577)
                        : mode === 'malformed'
                          ? '{'
                          : JSON.stringify(evidence),
                )
            if (mode === 'valid' || mode === 'source-only' || mode === 'missing') {
                const post = await prepareReleaseBlog(directory, directory, readiness.tag)
                expect(post).toBeDefined()
                if (mode === 'valid' || mode === 'source-only')
                    expect(post?.benchmarks?.[0]?.candidate).toEqual(
                        evidence.benchmarks[0]?.candidate,
                    )
                else {
                    expect(post?.benchmarks).toBeUndefined()
                    expect(
                        Object.values(post?.locales ?? {}).every(
                            (localized) => localized.benchmarks === undefined,
                        ),
                    ).toBeTrue()
                }
            } else {
                await expect(
                    prepareReleaseBlog(directory, directory, readiness.tag),
                ).rejects.toThrow()
                await expect(
                    readFile(join(directory, 'rentnerproxy-blog-post-r1.json')),
                ).rejects.toThrow()
            }
        } finally {
            await rm(directory, { recursive: true, force: true })
        }
    })
})

describe('blog publication commit marker', () => {
    test.each(['rentnerproxy-blog-post-r1.json', 'rentnerproxy-blog-ready.json'])(
        'recovers an incomplete exact publication asset after verifying identity: %s',
        async (name) => {
            for (const metadata of [
                { state: 'starter', size: 0 },
                { state: 'starter', size: 12 },
                { state: 'uploaded', size: 0 },
            ] as const) {
                const state = fixture()
                state.files.set(name, {
                    asset: parseBlogAsset({ id: 99, name, ...metadata }),
                    bytes: Buffer.alloc(metadata.size),
                })
                // oxlint-disable-next-line no-await-in-loop -- Complete each isolated retry lifecycle before the next fixture.
                await publishReleaseBlog(buildReleaseBlog(readiness, source()), state.client)
                const deletion = state.events.indexOf(`delete:${name}`)
                expect(deletion).toBeGreaterThan(0)
                expect(state.events[deletion - 1]).toBe('identity')
                expect(state.events.indexOf(`upload:${name}`)).toBeGreaterThan(deletion)
                expect(state.events.at(-1)).toBe('upload:rentnerproxy-blog-ready.json')
                expect(state.events.indexOf('read:rentnerproxy-blog-post-r1.json')).toBeLessThan(
                    state.events.length - 1,
                )
            }
        },
    )

    test.each(['identity', 'uploading', 'completed', 'replaced'])(
        'never deletes incomplete assets without stable metadata and fresh identity: %s',
        async (mode) => {
            const state = fixture()
            const name = 'rentnerproxy-blog-post-r1.json'
            const asset = parseBlogAsset({
                id: 99,
                name,
                size: 0,
                state: mode === 'uploading' ? 'uploading' : 'starter',
            })
            state.files.set(name, { asset, bytes: Buffer.alloc(0) })
            if (mode === 'identity') {
                let checks = 0
                state.client.verifyIdentity = async () => {
                    state.events.push('identity')
                    if (++checks === 2) throw new Error('identity changed')
                }
            }
            if (mode === 'completed' || mode === 'replaced') {
                let lists = 0
                state.client.listAssets = async () => {
                    lists += 1
                    return [
                        {
                            ...asset,
                            ...(lists === 1
                                ? {}
                                : mode === 'completed'
                                  ? { state: 'uploaded' as const, size: 100 }
                                  : { id: 100 }),
                        },
                    ]
                }
            }
            await expect(
                publishReleaseBlog(buildReleaseBlog(readiness, source()), state.client),
            ).rejects.toThrow()
            expect(
                state.events.some(
                    (event) => event.startsWith('delete:') || event.startsWith('upload:'),
                ),
            ).toBeFalse()
        },
    )

    test.each([
        { state: 'other', size: 0 },
        { state: 'starter', size: -1 },
        { state: 'uploaded', size: 0.5 },
        { state: 'uploaded', size: Number.NaN },
        { state: 'starter', size: 1_048_577 },
    ])('rejects malformed partial metadata without guessing recovery: %j', (metadata) => {
        expect(() =>
            parseBlogAsset({ id: 1, name: 'rentnerproxy-blog-post-r1.json', ...metadata }),
        ).toThrow('metadata')
    })

    test('publishes immutable bytes, verifies them, then commits marker; retry preserves marker', async () => {
        const state = fixture()
        const post = buildReleaseBlog(readiness, source())
        await publishReleaseBlog(post, state.client, readiness.publishedAt)
        expect(state.events).toEqual([
            'identity',
            'upload:rentnerproxy-blog-post-r1.json',
            'read:rentnerproxy-blog-post-r1.json',
            'identity',
            'upload:rentnerproxy-blog-ready.json',
        ])
        const marker = state.files.get('rentnerproxy-blog-ready.json')?.bytes
        state.events.length = 0
        await publishReleaseBlog(post, state.client)
        expect(state.events.some((event) => event.startsWith('upload:'))).toBeFalse()
        expect(state.files.get('rentnerproxy-blog-ready.json')?.bytes).toEqual(marker)
    })

    test.each(['identity', 'read', 'rentnerproxy-blog-post-r1.json'])(
        'never exposes a marker after failure: %s',
        async (mode) => {
            const state = fixture()
            state.failure(mode)
            await expect(
                publishReleaseBlog(buildReleaseBlog(readiness, source()), state.client),
            ).rejects.toThrow()
            expect(state.files.has('rentnerproxy-blog-ready.json')).toBeFalse()
        },
    )

    test('rejects mutated immutable content and regressing revisions; advances revisions marker last', async () => {
        const state = fixture()
        const post = buildReleaseBlog(readiness, source())
        await publishReleaseBlog(post, state.client)
        await expect(
            publishReleaseBlog(
                {
                    ...post,
                    locales: { ...post.locales, en: { ...post.locales.en, title: 'Changed' } },
                },
                state.client,
            ),
        ).rejects.toThrow()
        await publishReleaseBlog({ ...post, contentRevision: 2 }, state.client)
        expect(state.events.at(-1)).toBe('upload:rentnerproxy-blog-ready.json')
        await expect(publishReleaseBlog(post, state.client)).rejects.toThrow()
    })

    test('does not regress after recovery removes a marker or overwrite an orphaned immutable asset', async () => {
        const state = fixture()
        const post = buildReleaseBlog(readiness, source())
        await publishReleaseBlog({ ...post, contentRevision: 2 }, state.client)
        state.files.delete('rentnerproxy-blog-ready.json')
        await expect(publishReleaseBlog(post, state.client)).rejects.toThrow()
        state.files.clear()
        state.files.set('rentnerproxy-blog-post-r1.json', {
            asset: { id: 1, name: 'rentnerproxy-blog-post-r1.json' },
            bytes: Buffer.from('different'),
        })
        await expect(publishReleaseBlog(post, state.client)).rejects.toThrow()
        expect(state.files.has('rentnerproxy-blog-ready.json')).toBeFalse()
    })
})
