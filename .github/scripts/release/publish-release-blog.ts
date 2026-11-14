import { createHash } from 'node:crypto'
import { writeFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import {
    blogBytes,
    blogReady,
    blogRecord,
    blogRevision,
    prepareReleaseBlog,
} from './generate-release-blog.ts'
import type {
    BlogAsset,
    BlogPublicationClient,
    ReleaseBlogPost,
} from './Types/release-blog.types.ts'

const markerName = 'rentnerproxy-blog-ready.json'

/** A marker is the commit point; immutable content must be readable before it is uploaded. */
export async function publishReleaseBlog(
    post: ReleaseBlogPost,
    client: BlogPublicationClient,
    now = new Date().toISOString(),
): Promise<void> {
    const bytes = blogBytes(post)
    const marker = blogReady(post, bytes, now)
    await client.verifyIdentity()
    let assets = await client.listAssets()
    for (const existing of assets) {
        const match = /^rentnerproxy-blog-post-r([1-9][0-9]*)\.json$/.exec(existing.name)
        if (match && blogRevision(Number(match[1])) > post.contentRevision)
            throw new Error('Blog content revision is older than an immutable published post')
    }
    // Only recover this publication's names; another revision may belong to a concurrent publisher.
    for (const name of [marker.postAsset, markerName]) {
        const partial = assets.find((asset) => asset.name === name)
        if (partial?.state === 'uploading') throw new Error('Blog asset upload is still active')
        if (partial && (partial.state === 'starter' || partial.size === 0)) {
            // A second observation avoids deleting an upload that has completed since the listing.
            // oxlint-disable-next-line no-await-in-loop -- Recover at most this post and its marker, in order.
            const current = (await client.listAssets()).find((asset) => asset.name === name)
            if (
                !current ||
                current.id !== partial.id ||
                current.state !== partial.state ||
                current.size !== partial.size
            )
                throw new Error('Partial blog asset changed during recovery; retry publication')
            // oxlint-disable-next-line no-await-in-loop -- Every destructive recovery step requires a fresh identity check.
            await client.verifyIdentity()
            // oxlint-disable-next-line no-await-in-loop -- Delete only the exact unchanged incomplete asset after verification.
            await client.deleteAsset(current)
            assets = assets.filter((asset) => asset.id !== current.id)
        }
    }
    const existingMarker = assets.find((asset) => asset.name === markerName)
    if (existingMarker) {
        const previous = blogRecord(
            JSON.parse(new TextDecoder().decode(await client.readAsset(existingMarker))) as unknown,
        )
        if (
            previous.schemaVersion !== 1 ||
            previous.releaseId !== post.releaseId ||
            previous.tag !== post.tag ||
            previous.revision !== post.revision ||
            previous.publishedAt !== post.publishedAt
        )
            throw new Error('Existing blog marker has a conflicting release identity')
        const previousRevision = blogRevision(previous.contentRevision)
        if (
            previousRevision > post.contentRevision ||
            (previousRevision === post.contentRevision &&
                (previous.postSha256 !== marker.postSha256 ||
                    previous.postAsset !== marker.postAsset))
        )
            throw new Error('Blog content revisions cannot regress or change in place')
    }
    const existingPost = assets.find((asset) => asset.name === marker.postAsset)
    const published = existingPost ?? (await client.uploadAsset(marker.postAsset, bytes))
    const uploadedBytes = await client.readAsset(published)
    if (createHash('sha256').update(uploadedBytes).digest('hex') !== marker.postSha256)
        throw new Error('Immutable blog asset differs from the authored content')
    if (existingMarker) {
        const previous = blogRecord(
            JSON.parse(new TextDecoder().decode(await client.readAsset(existingMarker))) as unknown,
        )
        if (
            previous.contentRevision === marker.contentRevision &&
            previous.postSha256 === marker.postSha256
        )
            return
    }
    await client.verifyIdentity()
    if (existingMarker) await client.deleteAsset(existingMarker)
    await client.uploadAsset(markerName, Buffer.from(JSON.stringify(marker, null, 4) + '\n'))
}

export async function ghBlogCommand(arguments_: string[]): Promise<Uint8Array> {
    const child = Bun.spawn(['gh', ...arguments_], {
        stdout: 'pipe',
        stderr: 'pipe',
        env: { ...process.env, GH_HOST: 'github.com' },
    })
    const [status, output] = await Promise.all([
        child.exited,
        new Response(child.stdout).arrayBuffer(),
        new Response(child.stderr).text(),
    ])
    if (status !== 0) throw new Error('GitHub blog publication command failed')
    if (output.byteLength > 1_048_576) throw new Error('GitHub blog response exceeds one MiB')
    return new Uint8Array(output)
}

export function parseBlogAsset(value: unknown): BlogAsset {
    const record = blogRecord(value)
    if (
        typeof record.id !== 'number' ||
        !Number.isSafeInteger(record.id) ||
        record.id < 1 ||
        typeof record.name !== 'string' ||
        typeof record.size !== 'number' ||
        !Number.isSafeInteger(record.size) ||
        record.size < 0 ||
        record.size > 1_048_576 ||
        (record.state !== 'uploaded' && record.state !== 'starter' && record.state !== 'uploading')
    )
        throw new Error('Invalid blog asset metadata')
    return { id: record.id, name: record.name, state: record.state, size: record.size }
}

export async function ghBlogJson(arguments_: string[]): Promise<unknown> {
    return JSON.parse(new TextDecoder().decode(await ghBlogCommand(arguments_))) as unknown
}

export function createGitHubBlogClient(
    post: ReleaseBlogPost,
    directory: string,
    prerelease: boolean,
    verifyAdditionalIdentity?: () => Promise<void>,
): BlogPublicationClient {
    const endpoint = `repos/RentnerProxy/platform/releases/${post.releaseId}`
    const client: BlogPublicationClient = {
        async verifyIdentity() {
            const release = blogRecord(await ghBlogJson(['api', endpoint]))
            if (
                release.id !== post.releaseId ||
                release.tag_name !== post.tag ||
                release.draft !== false ||
                release.prerelease !== prerelease ||
                release.published_at !== post.publishedAt
            )
                throw new Error('Release metadata changed before blog publication')
            await verifyAdditionalIdentity?.()
            // A syntactically valid baseline is not evidence that the claimed release source exists.
            for (const benchmark of post.benchmarks ?? []) {
                const baseline = blogRecord(
                    // oxlint-disable-next-line no-await-in-loop -- Bound GitHub process concurrency while verifying each baseline.
                    await ghBlogJson([
                        'api',
                        `repos/RentnerProxy/platform/commits/${encodeURIComponent(`refs/tags/${benchmark.baseline.tag}`)}`,
                    ]),
                )
                if (baseline.sha !== benchmark.baseline.revision)
                    throw new Error(
                        'Benchmark baseline tag does not resolve to the claimed revision',
                    )
            }
        },
        async listAssets() {
            const values = await ghBlogJson([
                'api',
                `${endpoint}/assets?per_page=100`,
                '--paginate',
                '--slurp',
            ])
            if (!Array.isArray(values) || !values.every(Array.isArray))
                throw new Error('Invalid blog asset listing')
            const candidates: unknown[] = values.flat()
            const relevant = candidates.filter((value) => {
                const record = blogRecord(value)
                return (
                    record.name === markerName ||
                    (typeof record.name === 'string' &&
                        /^rentnerproxy-blog-post-r[1-9][0-9]*\.json$/.test(record.name))
                )
            })
            const assets = relevant.map(parseBlogAsset)
            if (new Set(assets.map((value) => value.name)).size !== assets.length)
                throw new Error('Duplicate blog assets')
            return assets
        },
        readAsset(value) {
            return ghBlogCommand([
                'api',
                `repos/RentnerProxy/platform/releases/assets/${value.id}`,
                '-H',
                'Accept: application/octet-stream',
            ])
        },
        async uploadAsset(name, bytes) {
            const path = resolve(directory, name)
            await writeFile(path, bytes)
            await ghBlogCommand([
                'release',
                'upload',
                post.tag,
                path,
                '--repo',
                'RentnerProxy/platform',
            ])
            const uploaded = (await client.listAssets()).find((value) => value.name === name)
            if (!uploaded || uploaded.state !== 'uploaded' || !uploaded.size)
                throw new Error('Uploaded blog asset is unavailable or incomplete')
            return uploaded
        },
        async deleteAsset(value) {
            await ghBlogCommand([
                'api',
                '--method',
                'DELETE',
                `repos/RentnerProxy/platform/releases/assets/${value.id}`,
            ])
        },
    }
    return client
}

async function main(): Promise<void> {
    const env = process.env
    if (!env.RUNNER_TEMP || env.GITHUB_REPOSITORY !== 'RentnerProxy/platform')
        throw new Error('Official release runner is required')
    const directory = resolve(env.RUNNER_TEMP, 'rentnerproxy-release')
    const post = await prepareReleaseBlog(resolve('source'), directory, env.RELEASE_TAG ?? '')
    if (!post) {
        process.stdout.write('No authored release blog content; blog publication skipped.\n')
        return
    }
    if (
        String(post.releaseId) !== env.RELEASE_ID ||
        post.revision !== env.RELEASE_REVISION ||
        post.publishedAt !== env.RELEASE_PUBLISHED_AT ||
        post.channel !== env.RELEASE_CHANNEL ||
        post.image.repository !== env.RELEASE_IMAGE ||
        post.image.digest !== env.RELEASE_DIGEST ||
        post.image.platforms.join(',') !== env.RELEASE_PLATFORMS
    )
        throw new Error('Blog readiness differs from the release pipeline identity')
    if (env.RELEASE_PRERELEASE !== 'true' && env.RELEASE_PRERELEASE !== 'false')
        throw new Error('Original release prerelease state is required')
    const client = createGitHubBlogClient(post, directory, env.RELEASE_PRERELEASE === 'true')
    await publishReleaseBlog(post, client)
}

if (import.meta.main) {
    try {
        await main()
    } catch (error) {
        process.stderr.write(
            `Release blog publication failed: ${error instanceof Error ? error.message : 'unknown error'}\n`,
        )
        process.exitCode = 1
    }
}
