import { expect, spyOn, test } from 'bun:test'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import {
    createGitHubBlogClient,
    ghBlogCommand,
    ghBlogJson,
} from '../../../../../.github/scripts/release/publish-release-blog.ts'
import type { ReleaseBlogPost } from '../../../../../.github/scripts/release/Types/release-blog.types.ts'
import { renderReleaseNotes } from '../../../../../.github/scripts/lib/release-notes.ts'
import {
    blogRecord,
    blogTimestamp,
    buildReleaseBlog,
} from '../../../../../.github/scripts/release/generate-release-blog.ts'

const locale = { title: 'Release', summary: 'Summary', sections: [] }
const post: ReleaseBlogPost = {
    schemaVersion: 1,
    releaseId: 123,
    tag: 'v1.2.3',
    revision: 'a'.repeat(40),
    publishedAt: '2026-11-14T12:00:00Z',
    channel: 'stable',
    image: {
        repository: 'ghcr.io/rentnerproxy/platform',
        digest: `sha256:${'b'.repeat(64)}`,
        platforms: ['linux/amd64'],
    },
    contentRevision: 1,
    locales: { de: locale, en: locale, es: locale, zh: locale },
}
const asset = { id: 7, name: 'rentnerproxy-blog-post-r1.json', state: 'uploaded' as const, size: 3 }
const metadata = {
    id: post.releaseId,
    tag_name: post.tag,
    draft: false,
    prerelease: false,
    published_at: post.publishedAt,
}

test('authored blog input rejects nonobjects, impossible timestamps and duplicate section identities', () => {
    for (const value of [null, [], 'release'])
        expect(() => blogRecord(value)).toThrow('Expected a blog object')
    expect(() => blogTimestamp('2026-02-30T12:00:00Z')).toThrow('Invalid blog timestamp')
    const section = { id: 'changes', title: 'Changes', paragraphs: ['Verified changes'] }
    const invalidLocale = { ...locale, sections: [section, section] }
    expect(() =>
        buildReleaseBlog(post, {
            contentRevision: 1,
            locales: { ...post.locales, de: invalidLocale },
        }),
    ).toThrow('Duplicate blog section IDs')
    const benchmark = {
        id: 'latency',
        unit: 'ms',
        direction: 'lower',
        baseline: { tag: 'v1.2.2', revision: 'c'.repeat(40), values: [2, 2, 2, 2, 2] },
        candidate: { tag: post.tag, revision: post.revision, values: [1, 1, 1, 1, 1] },
        measuredAt: post.publishedAt,
        environment: 'fixture',
        methodology: 'fixture',
        runUrl: 'https://github.com/RentnerProxy/platform/actions/runs/1',
    }
    for (const [measurement, expected] of [
        [
            { ...benchmark, baseline: { ...benchmark.baseline, tag: 'HEAD' } },
            'Release tag must be Docker-compatible SemVer',
        ],
        [{ ...benchmark, direction: 'sideways' }, 'Invalid benchmark direction'],
    ] as const) {
        expect(() =>
            buildReleaseBlog(post, {
                contentRevision: 1,
                locales: post.locales,
                benchmarks: [measurement],
            }),
        ).toThrow(expected)
    }
    const labelsMissing = { ...locale, sections: [section], benchmarks: {} }
    expect(() =>
        buildReleaseBlog(post, {
            contentRevision: 1,
            locales: { ...post.locales, de: labelsMissing },
            benchmarks: [benchmark],
        }),
    ).toThrow('Localized benchmark labels must match all measurements')
})

test('GitHub blog transport bounds responses, hides command output on failure and parses JSON', async () => {
    let output: string | Uint8Array<ArrayBuffer> = ' {"ok":true} '
    let status = 0
    const commands: string[][] = []
    const spawn = spyOn(Bun, 'spawn').mockImplementation(((
        args: string[],
        options: { env: Record<string, string> },
    ) => {
        commands.push(args)
        expect(options.env.GH_HOST).toBe('github.com')
        return {
            exited: Promise.resolve(status),
            stdout: new Response(output).body,
            stderr: new Response('private diagnostic').body,
        }
    }) as typeof Bun.spawn)
    try {
        expect(await ghBlogJson(['api', 'fixture'])).toEqual({ ok: true })
        expect(commands).toEqual([['gh', 'api', 'fixture']])
        output = new Uint8Array(1_048_576)
        expect((await ghBlogCommand(['api', 'boundary'])).length).toBe(1_048_576)
        output = new Uint8Array(1_048_577)
        await expect(ghBlogCommand(['api', 'oversize'])).rejects.toThrow('response exceeds one MiB')
        status = 1
        await expect(ghBlogCommand(['api', 'failed'])).rejects.toThrow(
            /^GitHub blog publication command failed$/,
        )
        status = 0
        output = 'invalid json'
        await expect(ghBlogJson(['api', 'invalid'])).rejects.toBeInstanceOf(SyntaxError)
    } finally {
        spawn.mockRestore()
    }
})

test('GitHub blog adapter verifies identities and performs only explicit scoped asset operations', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'release-blog-adapter-'))
    const commands: string[][] = []
    let response: unknown = metadata
    let raw: Uint8Array<ArrayBuffer> | undefined
    const spawn = spyOn(Bun, 'spawn').mockImplementation(((args: string[]) => {
        commands.push(args)
        return {
            exited: Promise.resolve(0),
            stdout: new Response(raw ?? JSON.stringify(response)).body,
            stderr: new Response('').body,
        }
    }) as typeof Bun.spawn)
    let additionalChecks = 0
    const client = createGitHubBlogClient(post, directory, false, async () => {
        additionalChecks += 1
    })
    try {
        await client.verifyIdentity()
        expect(additionalChecks).toBe(1)
        expect(commands.at(-1)).toEqual(['gh', 'api', 'repos/RentnerProxy/platform/releases/123'])
        for (const changed of [
            { id: 124 },
            { tag_name: 'v1.2.4' },
            { draft: true },
            { prerelease: true },
            { published_at: '2026-11-15T12:00:00Z' },
        ]) {
            response = { ...metadata, ...changed }
            // oxlint-disable-next-line no-await-in-loop -- Each changed identity is checked against the same isolated fake transport.
            await expect(client.verifyIdentity()).rejects.toThrow('Release metadata changed')
        }
        expect(additionalChecks).toBe(1)
        response = metadata
        await expect(
            createGitHubBlogClient(post, directory, false, async () => {
                throw new Error('source changed')
            }).verifyIdentity(),
        ).rejects.toThrow('source changed')
        response = [
            [{ id: 99, name: 'CHANGELOG.md', size: 100, state: 'uploaded' }, asset],
            [{ id: 8, name: 'rentnerproxy-blog-ready.json', size: 0, state: 'starter' }],
        ]
        expect(await client.listAssets()).toEqual([
            asset,
            { id: 8, name: 'rentnerproxy-blog-ready.json', size: 0, state: 'starter' },
        ])
        expect(commands.at(-1)).toEqual([
            'gh',
            'api',
            'repos/RentnerProxy/platform/releases/123/assets?per_page=100',
            '--paginate',
            '--slurp',
        ])
        for (const invalid of [{}, [asset], [[asset, asset]], [[{ ...asset, state: 'unknown' }]]]) {
            response = invalid
            // oxlint-disable-next-line no-await-in-loop -- Invalid API listings must fail independently without publishing anything.
            await expect(client.listAssets()).rejects.toThrow()
        }
        raw = new Uint8Array([0, 255, 10])
        expect(await client.readAsset(asset)).toEqual(raw)
        expect(commands.at(-1)).toEqual([
            'gh',
            'api',
            'repos/RentnerProxy/platform/releases/assets/7',
            '-H',
            'Accept: application/octet-stream',
        ])
        raw = undefined
        response = [[asset]]
        const bytes = new Uint8Array([1, 2, 3])
        expect(await client.uploadAsset(asset.name, bytes)).toEqual(asset)
        expect(new Uint8Array(await readFile(join(directory, asset.name)))).toEqual(bytes)
        expect(commands.at(-2)).toEqual([
            'gh',
            'release',
            'upload',
            post.tag,
            join(directory, asset.name),
            '--repo',
            'RentnerProxy/platform',
        ])
        for (const listing of [[], [{ ...asset, state: 'starter' }], [{ ...asset, size: 0 }]]) {
            response = [listing]
            // oxlint-disable-next-line no-await-in-loop -- Successful CLI exit cannot substitute for a readable complete uploaded asset.
            await expect(client.uploadAsset(asset.name, bytes)).rejects.toThrow(
                'unavailable or incomplete',
            )
        }
        await client.deleteAsset(asset)
        expect(commands.at(-1)).toEqual([
            'gh',
            'api',
            '--method',
            'DELETE',
            'repos/RentnerProxy/platform/releases/assets/7',
        ])
    } finally {
        spawn.mockRestore()
        await rm(directory, { recursive: true, force: true })
    }
})

test('GitHub blog adapter resolves each benchmark baseline tag before accepting its revision', async () => {
    const benchmark = {
        id: 'latency',
        unit: 'ms',
        direction: 'lower' as const,
        baseline: { tag: 'v1.2.2', revision: 'c'.repeat(40), values: [2] },
        candidate: { tag: post.tag, revision: post.revision, values: [1] },
        measuredAt: post.publishedAt,
        environment: 'fixture',
        methodology: 'fixture',
        runUrl: 'https://github.com/RentnerProxy/platform/actions/runs/1',
    }
    let baselineRevision = benchmark.baseline.revision
    const commands: string[][] = []
    const spawn = spyOn(Bun, 'spawn').mockImplementation(((args: string[]) => {
        commands.push(args)
        const response = args[2]?.includes('/commits/') ? { sha: baselineRevision } : metadata
        return {
            exited: Promise.resolve(0),
            stdout: new Response(JSON.stringify(response)).body,
            stderr: new Response('').body,
        }
    }) as typeof Bun.spawn)
    try {
        const client = createGitHubBlogClient({ ...post, benchmarks: [benchmark] }, '.', false)
        await client.verifyIdentity()
        expect(commands.at(-1)).toEqual([
            'gh',
            'api',
            'repos/RentnerProxy/platform/commits/refs%2Ftags%2Fv1.2.2',
        ])
        baselineRevision = 'd'.repeat(40)
        await expect(client.verifyIdentity()).rejects.toThrow(
            'Benchmark baseline tag does not resolve',
        )
    } finally {
        spawn.mockRestore()
    }
})

test('release blog publication CLI validates pipeline identity before publishing immutable content and marker offline', async () => {
    const root = resolve(import.meta.dir, '../../../../..')
    const directory = await mkdtemp(join(tmpdir(), 'release-blog-cli-'))
    const artifacts = join(directory, 'rentnerproxy-release')
    const source = join(directory, 'source/.github/release-content')
    const preload = join(directory, 'offline-gh.ts')
    const entrypoint = join(root, '.github/scripts/release/publish-release-blog.ts')
    const authored = {
        contentRevision: post.contentRevision,
        locales: Object.fromEntries(
            Object.entries(post.locales).map(([key, value]) => [
                key,
                {
                    ...value,
                    sections: [
                        {
                            id: 'changes',
                            title: 'Changes',
                            paragraphs: ['Verified release changes'],
                        },
                    ],
                },
            ]),
        ),
    }
    const readiness = {
        schemaVersion: post.schemaVersion,
        releaseId: post.releaseId,
        tag: post.tag,
        revision: post.revision,
        publishedAt: post.publishedAt,
        channel: post.channel,
        image: post.image,
    }
    const completedRelease = {
        ...metadata,
        body: renderReleaseNotes({
            repository: 'RentnerProxy/platform',
            tagName: post.tag,
            channel: post.channel,
            publishedAt: post.publishedAt,
            image: post.image.repository,
            bannerRevision: post.revision,
            issues: [],
            config: { excludedLabels: [], highlightLabels: [], categories: [] },
            initialRelease: false,
        }).body,
    }
    const completedAssets = [
        'rentnerproxy-update.json',
        'INSTALL.txt',
        `RentnerProxy-${post.tag}-verification.tar.gz`,
        `RentnerProxy-${post.tag}-CHANGELOG.md`,
        ...['.deb', '.rpm', '-quadlet.tar.gz'].map(
            (suffix) => `rentnerproxy-${post.tag.slice(1)}-1-amd64${suffix}`,
        ),
    ].map((name, index) => ({ id: index + 101, name, state: 'uploaded', size: 100 }))
    const environment = {
        ...process.env,
        RUNNER_TEMP: directory,
        GITHUB_REPOSITORY: 'RentnerProxy/platform',
        RELEASE_TAG: post.tag,
        RELEASE_ID: String(post.releaseId),
        RELEASE_REVISION: post.revision,
        RELEASE_PUBLISHED_AT: post.publishedAt,
        RELEASE_CHANNEL: post.channel,
        RELEASE_IMAGE: post.image.repository,
        RELEASE_DIGEST: post.image.digest,
        RELEASE_PLATFORMS: post.image.platforms.join(','),
        RELEASE_PRERELEASE: 'false',
    }
    async function run(
        changes: Record<string, string> = {},
        entry = entrypoint,
        args: string[] = [],
    ) {
        const child = Bun.spawn(
            [process.execPath, '--no-env-file', '--preload', preload, entry, ...args],
            { cwd: directory, env: { ...environment, ...changes }, stdout: 'pipe', stderr: 'pipe' },
        )
        const [code, stdout, stderr] = await Promise.all([
            child.exited,
            new Response(child.stdout).text(),
            new Response(child.stderr).text(),
        ])
        return { code, stdout, stderr }
    }
    try {
        await mkdir(source, { recursive: true })
        await mkdir(artifacts, { recursive: true })
        await writeFile(join(source, post.tag + '.json'), JSON.stringify(authored))
        await writeFile(join(artifacts, 'rentnerproxy-update.json'), JSON.stringify(readiness))
        await writeFile(
            preload,
            `
import {readFileSync} from 'node:fs';
const assets=process.env.OFFLINE_BACKFILL?${JSON.stringify(completedAssets)}:[]; const bytes=new Map([[101,JSON.stringify(${JSON.stringify(readiness)})]]);
const which=Bun.which; Bun.which=(name)=>name==='docker'?'offline-docker':which(name);
Bun.spawn=(args)=>{let output='';
if(args[0]==='docker'){if(args.slice(1,4).join(' ')!=='buildx imagetools inspect'||args[4]!==${JSON.stringify(post.image.repository + '@' + post.image.digest)})throw new Error('Unexpected image inspection');
if(process.env.OFFLINE_DOCKER_FAIL)return {exited:Promise.resolve(1),stdout:new Response('private registry details').body,stderr:new Response('private registry error').body};
if(args[5]==='--format'&&args[6]==='{{.Manifest.Digest}}')output=${JSON.stringify(post.image.digest)};
else if(args[5]==='--raw')output=JSON.stringify({schemaVersion:2,manifests:[{digest:${JSON.stringify('sha256:' + 'c'.repeat(64))},platform:{os:'linux',architecture:'amd64'}}]});
else throw new Error('Unexpected Docker arguments');}
else if(args[0]!=='gh')throw new Error('Unexpected process');
else if(args[1]==='release'&&args[2]==='upload'){const path=args[4];const name=path.split(/[\\\\/]/).at(-1);const data=readFileSync(path);const id=assets.length+1;assets.push({id,name,size:data.length,state:'uploaded'});bytes.set(id,data)}
else if(args[1]==='api'&&(args[2]==='repos/RentnerProxy/platform/releases/123'||args[2]===${JSON.stringify('repos/RentnerProxy/platform/releases/tags/' + post.tag)}))output=JSON.stringify(${JSON.stringify(completedRelease)});
else if(args[1]==='api'&&args[2]===${JSON.stringify('repos/RentnerProxy/platform/commits/refs%2Ftags%2F' + post.tag)})output=JSON.stringify({sha:${JSON.stringify(post.revision)}});
else if(args[1]==='api'&&args[2]==='repos/RentnerProxy/platform/releases/123/assets?per_page=100')output=JSON.stringify([assets]);
else if(args[1]==='api'&&/^repos\\/RentnerProxy\\/platform\\/releases\\/assets\\/[0-9]+$/.test(args[2]))output=bytes.get(Number(args[2].split('/').at(-1)));
else throw new Error('Unexpected offline command');return {exited:Promise.resolve(0),stdout:new Response(output).body,stderr:new Response('').body}};
`,
        )
        expect(await run()).toMatchObject({ code: 0, stderr: '' })
        expect(
            JSON.parse(await readFile(join(artifacts, 'rentnerproxy-blog-ready.json'), 'utf8')),
        ).toMatchObject({ releaseId: post.releaseId, postAsset: asset.name })
        const invalid = await run({ RELEASE_ID: '999' })
        expect(invalid.code).toBe(1)
        expect(invalid.stderr).toContain(
            'Blog readiness differs from the release pipeline identity',
        )
        const prerelease = await run({ RELEASE_PRERELEASE: 'unknown' })
        expect(prerelease.stderr).toContain('Original release prerelease state is required')
        expect((await run({ GITHUB_REPOSITORY: 'untrusted/repository' })).stderr).toContain(
            'Official release runner is required',
        )
        const absent = await run({ RELEASE_TAG: 'v9.9.9' })
        expect(absent).toMatchObject({ code: 0, stderr: '' })
        expect(absent.stdout).toContain('No authored release blog content')
        const backfill = join(root, '.github/scripts/release/backfill-release-blog.ts')
        const backfillArgs = ['--tag', post.tag, '--content', join(source, post.tag + '.json')]
        const successfulBackfill = await run({ OFFLINE_BACKFILL: '1' }, backfill, backfillArgs)
        expect(successfulBackfill).toMatchObject({ code: 0, stderr: '' })
        expect(successfulBackfill.stdout).toContain(
            `Published release blog for ${post.tag} (release 123, content revision 1).`,
        )
        const registryFailure = await run(
            { OFFLINE_BACKFILL: '1', OFFLINE_DOCKER_FAIL: '1' },
            backfill,
            backfillArgs,
        )
        expect(registryFailure.code).toBe(1)
        expect(registryFailure.stderr).toContain('Published image inspection failed')
        expect(registryFailure.stderr).not.toContain('private registry')
        const invalidBackfill = await run({}, backfill, [])
        expect(invalidBackfill.code).toBe(1)
        expect(invalidBackfill.stderr).toContain('Release blog backfill failed:')
        const missing = await run({}, backfill, [
            '--tag',
            post.tag,
            '--content',
            join(directory, 'missing.json'),
        ])
        expect(missing.code).toBe(1)
        expect(missing.stderr).toContain('Release blog backfill failed:')
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})
