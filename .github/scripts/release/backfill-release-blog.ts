import { lstat, mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { resolve, join, dirname, basename } from 'node:path'
import { blogBytes, blogRecord, buildReleaseBlog } from './generate-release-blog.ts'
import {
    createGitHubBlogClient,
    ghBlogCommand,
    ghBlogJson,
    publishReleaseBlog,
} from './publish-release-blog.ts'
import { renderReleaseNotes } from '../lib/release-notes.ts'
import type { ReleaseBlogPost } from './Types/release-blog.types.ts'

export function parseBackfillArguments(arguments_: readonly string[]): {
    tag: string
    content: string
} {
    const values = new Map<string, string>()
    for (let index = 0; index < arguments_.length; index += 2) {
        const flag = arguments_[index]
        const value = arguments_[index + 1]
        if (
            (flag !== '--tag' && flag !== '--content') ||
            !value ||
            value.startsWith('--') ||
            values.has(flag)
        )
            throw new Error(
                'Usage: backfill-release-blog.ts --tag <release-tag> --content <article.json>',
            )
        values.set(flag, value)
    }
    const tag = values.get('--tag')
    const content = values.get('--content')
    if (
        !tag ||
        !content ||
        !/^v[0-9]+\.[0-9]+\.[0-9]+(?:-(?:alpha|beta)(?:\.[0-9A-Za-z-]+)*)?$/.test(tag) ||
        tag.length > 128
    )
        throw new Error('A valid explicit --tag and --content are required')
    return { tag, content }
}

export async function readBackfillContent(path: string): Promise<unknown> {
    if (!path.endsWith('.json')) throw new Error('Blog content must be a JSON file')
    const info = await lstat(path)
    if (!info.isFile() || info.nlink !== 1 || info.size < 1 || info.size > 1_048_576)
        throw new Error('Blog content must be a regular bounded JSON file')
    const bytes = await readFile(path)
    if (bytes.length > 1_048_576) throw new Error('Blog content exceeds one MiB')
    return JSON.parse(bytes.toString('utf8')) as unknown
}

/** Local operators use published evidence, never a reconstructed CI environment. */
export async function prepareBackfillReleaseBlog(
    tag: string,
    source: unknown,
    evidence: {
        release(tag: string): Promise<unknown>
        assets(releaseId: number): Promise<unknown[]>
        readAsset(assetId: number): Promise<unknown>
        revision(tag: string): Promise<unknown>
        image(reference: string): Promise<{ digest: string; manifest: unknown }>
    },
): Promise<{ post: ReleaseBlogPost; prerelease: boolean }> {
    // Validate the tag before it is interpolated into any external command or endpoint.
    parseBackfillArguments(['--tag', tag, '--content', 'article.json'])
    const release = blogRecord(await evidence.release(tag))
    if (
        release.tag_name !== tag ||
        release.draft !== false ||
        typeof release.prerelease !== 'boolean' ||
        typeof release.id !== 'number' ||
        !Number.isSafeInteger(release.id) ||
        release.id < 1
    )
        throw new Error('Backfill requires the exact published official release')
    const assets = (await evidence.assets(release.id)).map(blogRecord)
    if (new Set(assets.map((asset) => asset.name)).size !== assets.length)
        throw new Error('Duplicate release assets')
    const uploaded = (name: string) => {
        const asset = assets.find((value) => value.name === name)
        if (
            !asset ||
            asset.state !== 'uploaded' ||
            typeof asset.id !== 'number' ||
            !Number.isSafeInteger(asset.id) ||
            asset.id < 1 ||
            typeof asset.size !== 'number' ||
            !Number.isSafeInteger(asset.size) ||
            asset.size < 1
        )
            throw new Error(`Missing completed release asset: ${name}`)
        return asset
    }
    const ready = uploaded('rentnerproxy-update.json')
    if (Number(ready.size) > 1_048_576) throw new Error('Readiness asset exceeds one MiB')
    const readiness = await evidence.readAsset(Number(ready.id))
    const post = buildReleaseBlog(readiness, source)
    const commit = blogRecord(await evidence.revision(tag))
    if (
        post.releaseId !== release.id ||
        post.tag !== tag ||
        post.publishedAt !== release.published_at ||
        post.revision !== commit.sha
    )
        throw new Error('Readiness, published release and peeled tag identities differ')
    // The canonical final header binds the body to this channel, tag and publication date.
    // Readiness alone is insufficient: it is uploaded before the final notes edit can fail.
    const finalNotes = renderReleaseNotes({
        repository: 'RentnerProxy/platform',
        tagName: post.tag,
        channel: post.channel,
        publishedAt: post.publishedAt,
        image: post.image.repository,
        bannerRevision: post.revision,
        issues: [],
        config: { excludedLabels: [], highlightLabels: [], categories: [] },
        initialRelease: false,
    }).body.split('\n')
    const actualNotes =
        typeof release.body === 'string' ? release.body.replaceAll('\r\n', '\n').split('\n') : []
    if (actualNotes[2] !== finalNotes[2] || actualNotes[4] !== finalNotes[4])
        throw new Error(
            'Backfill requires completed final release notes for the published identity',
        )
    uploaded(`RentnerProxy-${tag}-CHANGELOG.md`)
    uploaded('INSTALL.txt')
    uploaded(`RentnerProxy-${tag}-verification.tar.gz`)
    let packageRevision: string | undefined
    for (const platform of post.image.platforms) {
        const architecture = platform.slice('linux/'.length)
        const stem = `rentnerproxy-${tag.slice(1)}-`
        const candidates = assets.filter(
            (asset) =>
                typeof asset.name === 'string' &&
                asset.name.startsWith(stem) &&
                asset.name.endsWith(`-${architecture}.deb`),
        )
        if (candidates.length !== 1)
            throw new Error('Missing or ambiguous release package revision')
        const name = String(candidates[0]!.name)
        const revision = name.slice(stem.length, -`-${architecture}.deb`.length)
        if (
            !/^[1-9][0-9]*$/.test(revision) ||
            (packageRevision !== undefined && revision !== packageRevision)
        )
            throw new Error('Inconsistent release package revisions')
        packageRevision = revision
        for (const suffix of ['.deb', '.rpm', '-quadlet.tar.gz'])
            uploaded(`${stem}${revision}-${architecture}${suffix}`)
    }
    const image = await evidence.image(`${post.image.repository}@${post.image.digest}`)
    const manifest = blogRecord(image.manifest)
    if (
        image.digest !== post.image.digest ||
        manifest.schemaVersion !== 2 ||
        !Array.isArray(manifest.manifests)
    )
        throw new Error('Published image digest or manifest differs from readiness')
    const platforms = manifest.manifests
        .map((value) => {
            const descriptor = blogRecord(value)
            const platform = blogRecord(descriptor.platform)
            if (
                typeof descriptor.digest !== 'string' ||
                !/^sha256:[0-9a-f]{64}$/.test(descriptor.digest)
            )
                throw new Error('Invalid published image descriptor')
            const name = `${String(platform.os)}/${String(platform.architecture)}`
            if (
                name === 'unknown/unknown' &&
                blogRecord(descriptor.annotations)['vnd.docker.reference.type'] !==
                    'attestation-manifest'
            )
                throw new Error('Unrecognized platformless image descriptor')
            return name
        })
        .filter((value) => value !== 'unknown/unknown')
        .toSorted()
    if (platforms.join(',') !== [...post.image.platforms].toSorted().join(','))
        throw new Error('Published image platforms differ from readiness')
    return { post, prerelease: release.prerelease }
}

async function inspectImage(reference: string): Promise<{ digest: string; manifest: unknown }> {
    if (!Bun.which('docker'))
        throw new Error('Docker with buildx imagetools is required to verify the published image')
    const run = async (arguments_: string[]) => {
        const child = Bun.spawn(
            ['docker', 'buildx', 'imagetools', 'inspect', reference, ...arguments_],
            { stdout: 'pipe', stderr: 'pipe' },
        )
        const [status, output] = await Promise.all([
            child.exited,
            new Response(child.stdout).text(),
            new Response(child.stderr).text(),
        ])
        if (status !== 0 || Buffer.byteLength(output) > 1_048_576)
            throw new Error(
                'Published image inspection failed; Docker buildx and registry access are required',
            )
        return output
    }
    const digest = (await run(['--format', '{{.Manifest.Digest}}'])).trim()
    const manifest: unknown = JSON.parse(await run(['--raw']))
    return { digest, manifest }
}

async function main(): Promise<void> {
    const { tag, content } = parseBackfillArguments(process.argv.slice(2))
    const source = await readBackfillContent(resolve(content))
    const evidence = {
        release: (value: string) =>
            ghBlogJson([
                'api',
                `repos/RentnerProxy/platform/releases/tags/${encodeURIComponent(value)}`,
            ]),
        async assets(releaseId: number): Promise<unknown[]> {
            const pages = await ghBlogJson([
                'api',
                `repos/RentnerProxy/platform/releases/${releaseId}/assets?per_page=100`,
                '--paginate',
                '--slurp',
            ])
            if (!Array.isArray(pages) || !pages.every(Array.isArray))
                throw new Error('Invalid release asset listing')
            return pages.flat() as unknown[]
        },
        async readAsset(assetId: number): Promise<unknown> {
            return JSON.parse(
                new TextDecoder().decode(
                    await ghBlogCommand([
                        'api',
                        `repos/RentnerProxy/platform/releases/assets/${assetId}`,
                        '-H',
                        'Accept: application/octet-stream',
                    ]),
                ),
            ) as unknown
        },
        revision: (value: string) =>
            ghBlogJson([
                'api',
                `repos/RentnerProxy/platform/commits/${encodeURIComponent(`refs/tags/${value}`)}`,
            ]),
        image: inspectImage,
    }
    const original = await prepareBackfillReleaseBlog(tag, source, evidence)
    const temporaryRoot = resolve(tmpdir())
    const directory = await mkdtemp(join(temporaryRoot, 'rentnerproxy-blog-backfill-'))
    if (
        dirname(resolve(directory)) !== temporaryRoot ||
        !basename(directory).startsWith('rentnerproxy-blog-backfill-')
    )
        throw new Error('Backfill temporary directory escaped its expected root')
    try {
        const client = createGitHubBlogClient(
            original.post,
            directory,
            original.prerelease,
            async () => {
                const current = await prepareBackfillReleaseBlog(tag, source, evidence)
                if (
                    current.prerelease !== original.prerelease ||
                    !Buffer.from(blogBytes(current.post)).equals(
                        Buffer.from(blogBytes(original.post)),
                    )
                )
                    throw new Error('Published release evidence changed during backfill')
            },
        )
        await publishReleaseBlog(original.post, client)
        process.stdout.write(
            `Published release blog for ${tag} (release ${original.post.releaseId}, content revision ${original.post.contentRevision}).\n`,
        )
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
}

if (import.meta.main) {
    try {
        await main()
    } catch (error) {
        process.stderr.write(
            `Release blog backfill failed: ${error instanceof Error ? error.message : 'unknown error'}\n`,
        )
        process.exitCode = 1
    }
}
