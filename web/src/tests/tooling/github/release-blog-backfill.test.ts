import { describe, expect, test } from 'bun:test'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
    parseBackfillArguments,
    prepareBackfillReleaseBlog,
    readBackfillContent,
} from '../../../../../.github/scripts/release/backfill-release-blog.ts'

import { blogBytes } from '../../../../../.github/scripts/release/generate-release-blog.ts'
import { publishReleaseBlog } from '../../../../../.github/scripts/release/publish-release-blog.ts'
import {
    renderReleaseNotes,
    renderBuildingReleaseNotes,
} from '../../../../../.github/scripts/lib/release-notes.ts'

function fixture() {
    const readiness = {
        schemaVersion: 1,
        releaseId: 123,
        tag: 'v1.0.0-beta.3.1',
        channel: 'beta',
        revision: 'a'.repeat(40),
        publishedAt: '2026-11-08T00:29:40Z',
        image: {
            repository: 'ghcr.io/rentnerproxy/platform',
            digest: `sha256:${'b'.repeat(64)}`,
            platforms: ['linux/amd64', 'linux/arm64'],
        },
    }
    const source = {
        contentRevision: 1,
        locales: Object.fromEntries(
            ['de', 'en', 'es', 'zh'].map((locale) => [
                locale,
                {
                    title: 'Release',
                    summary: 'Available now',
                    sections: [
                        { id: 'changes', title: 'Changes', paragraphs: ['Verified changes'] },
                    ],
                },
            ]),
        ),
    }
    const release = {
        id: readiness.releaseId,
        tag_name: readiness.tag,
        draft: false,
        prerelease: false,
        published_at: readiness.publishedAt,
        body: renderReleaseNotes({
            repository: 'RentnerProxy/platform',
            tagName: readiness.tag,
            channel: 'beta',
            publishedAt: readiness.publishedAt,
            image: readiness.image.repository,
            bannerRevision: readiness.revision,
            issues: [],
            initialRelease: false,
            config: { excludedLabels: [], highlightLabels: [], categories: [] },
        }).body,
    }
    const names = [
        'rentnerproxy-update.json',
        'INSTALL.txt',
        `RentnerProxy-${readiness.tag}-verification.tar.gz`,
        `RentnerProxy-${readiness.tag}-CHANGELOG.md`,
        ...['amd64', 'arm64'].flatMap((architecture) =>
            ['.deb', '.rpm', '-quadlet.tar.gz'].map(
                (suffix) => `rentnerproxy-${readiness.tag.slice(1)}-1-${architecture}${suffix}`,
            ),
        ),
    ]
    const assets = names.map((name, index) => ({
        id: index + 1,
        name,
        size: 100,
        state: 'uploaded',
    }))
    const image = {
        digest: readiness.image.digest,
        manifest: {
            schemaVersion: 2,
            manifests: ['amd64', 'arm64'].map((architecture) => ({
                digest: `sha256:${'c'.repeat(64)}`,
                platform: { os: 'linux', architecture },
            })),
        },
    }
    const events: string[] = []
    const evidence = {
        async release(tag: string) {
            events.push(`release:${tag}`)
            return release
        },
        async assets(id: number) {
            events.push(`assets:${id}`)
            return assets
        },
        async readAsset(id: number) {
            events.push(`readiness:${id}`)
            return readiness
        },
        async revision(tag: string) {
            events.push(`revision:${tag}`)
            return { sha: readiness.revision }
        },
        async image(reference: string) {
            events.push(`image:${reference}`)
            return image
        },
    }
    return { readiness, source, release, assets, image, evidence, events }
}

describe('operator release blog backfill', () => {
    test('requires explicit bounded flags and rejects malformed commands before external reads', async () => {
        expect(
            parseBackfillArguments(['--content', 'article.json', '--tag', 'v1.0.0-beta.3.1']),
        ).toEqual({ tag: 'v1.0.0-beta.3.1', content: 'article.json' })
        for (const commandArguments of [
            [],
            ['--tag', 'v1.0.0'],
            ['--tag', '../escape', '--content', 'a.json'],
            ['--tag', 'v1.0.0', '--content', 'a.json', '--tag', 'v1.0.1'],
            ['--repo', 'other/repo'],
        ])
            expect(() => parseBackfillArguments(commandArguments)).toThrow()
        const state = fixture()
        await expect(
            prepareBackfillReleaseBlog('../escape', state.source, state.evidence),
        ).rejects.toThrow()
        expect(state.events).toEqual([])
    })

    test('derives identity from published readiness and preserves original beta prerelease=false', async () => {
        const state = fixture()
        const result = await prepareBackfillReleaseBlog(
            state.readiness.tag,
            state.source,
            state.evidence,
        )
        expect(result.prerelease).toBeFalse()
        expect(result.post.releaseId).toBe(state.release.id)
        expect(result.post.publishedAt).toBe(state.release.published_at)
        expect(result.post.image).toEqual(state.readiness.image)
        expect(state.events).toEqual([
            `release:${state.readiness.tag}`,
            'assets:123',
            'readiness:1',
            `revision:${state.readiness.tag}`,
            `image:${state.readiness.image.repository}@${state.readiness.image.digest}`,
        ])
    })

    test.each([
        'draft',
        'tag',
        'release-id',
        'published-at',
        'revision',
        'missing-readiness',
        'duplicate',
        'oversized-readiness',
        'readiness-upload',
        'install',
        'verification',
        'package',
        'package-revision',
        'package-upload',
        'digest',
        'platform',
        'duplicate-platform',
        'manifest',
    ])('refuses incomplete or conflicting published evidence: %s', async (mode) => {
        const state = fixture()
        if (mode === 'draft') state.release.draft = true
        if (mode === 'tag') state.release.tag_name = 'v1.0.0'
        if (mode === 'release-id') state.release.id += 1
        if (mode === 'published-at') state.release.published_at = '2026-11-09T00:00:00Z'
        if (mode === 'revision') state.evidence.revision = async () => ({ sha: 'd'.repeat(40) })
        if (mode === 'missing-readiness') state.assets.shift()
        if (mode === 'duplicate') state.assets.push({ ...state.assets[0]! })
        if (mode === 'oversized-readiness') state.assets[0]!.size = 1_048_577
        if (mode === 'readiness-upload') state.assets[0]!.state = 'new'
        if (mode === 'install') state.assets.splice(1, 1)
        if (mode === 'verification') state.assets.splice(2, 1)
        if (mode === 'package') state.assets.pop()
        if (mode === 'package-revision')
            for (const asset of state.assets)
                asset.name = asset.name.replace('-1-arm64', '-2-arm64')
        if (mode === 'package-upload') state.assets.at(-1)!.state = 'new'
        if (mode === 'digest') state.image.digest = `sha256:${'d'.repeat(64)}`
        if (mode === 'platform') state.image.manifest.manifests.pop()
        if (mode === 'duplicate-platform')
            state.image.manifest.manifests.push(state.image.manifest.manifests[0]!)
        if (mode === 'manifest') state.image.manifest.schemaVersion = 1
        await expect(
            prepareBackfillReleaseBlog(state.readiness.tag, state.source, state.evidence),
        ).rejects.toThrow()
    })

    test.each([
        'building',
        'missing-body',
        'wrong-tag',
        'wrong-date',
        'missing-changelog',
        'unfinished-changelog',
    ])(
        'requires completed canonical final notes in addition to readiness and delivery assets: %s',
        async (mode) => {
            const state = fixture()
            if (mode === 'building')
                state.release.body = renderBuildingReleaseNotes({
                    repository: 'RentnerProxy/platform',
                    tagName: state.readiness.tag,
                    bannerRevision: state.readiness.revision,
                })
            if (mode === 'missing-body') state.release.body = ''
            if (mode === 'wrong-tag')
                state.release.body = state.release.body.replaceAll(
                    state.readiness.tag,
                    'v1.0.0-beta.9',
                )
            if (mode === 'wrong-date')
                state.release.body = state.release.body.replace('Published **', 'Published **2099 ')
            const changelog = state.assets.find((asset) => asset.name.endsWith('-CHANGELOG.md'))!
            if (mode === 'missing-changelog')
                state.assets.splice(state.assets.indexOf(changelog), 1)
            if (mode === 'unfinished-changelog') changelog.state = 'starter'
            await expect(
                prepareBackfillReleaseBlog(state.readiness.tag, state.source, state.evidence),
            ).rejects.toThrow()
            expect(state.events.some((event) => event.startsWith('image:'))).toBeFalse()
        },
    )

    test('revalidates final notes before publishing the blog commit marker', async () => {
        const state = fixture()
        const original = await prepareBackfillReleaseBlog(
            state.readiness.tag,
            state.source,
            state.evidence,
        )
        const uploaded: string[] = []
        await expect(
            publishReleaseBlog(original.post, {
                async verifyIdentity() {
                    await prepareBackfillReleaseBlog(
                        state.readiness.tag,
                        state.source,
                        state.evidence,
                    )
                },
                async listAssets() {
                    return []
                },
                async readAsset() {
                    return blogBytes(original.post)
                },
                async uploadAsset(name) {
                    uploaded.push(name)
                    state.release.body = renderBuildingReleaseNotes({
                        repository: 'RentnerProxy/platform',
                        tagName: state.readiness.tag,
                        bannerRevision: state.readiness.revision,
                    })
                    return { id: 100, name }
                },
                async deleteAsset() {
                    throw new Error('Unexpected deletion')
                },
            }),
        ).rejects.toThrow('completed final release notes')
        expect(uploaded).toEqual(['rentnerproxy-blog-post-r1.json'])
    })

    test('reads only regular bounded explicitly selected JSON content', async () => {
        const directory = await mkdtemp(join(tmpdir(), 'release-blog-content-'))
        try {
            const path = join(directory, 'article.json')
            await writeFile(path, JSON.stringify(fixture().source))
            expect(await readBackfillContent(path)).toEqual(fixture().source)
            await writeFile(path, ' '.repeat(1_048_577))
            await expect(readBackfillContent(path)).rejects.toThrow('bounded')
            await rm(path)
            await mkdir(path)
            await expect(readBackfillContent(path)).rejects.toThrow('regular')
            await expect(readBackfillContent(join(directory, '.env'))).rejects.toThrow('JSON')
        } finally {
            await rm(directory, { recursive: true, force: true })
        }
    })
})
