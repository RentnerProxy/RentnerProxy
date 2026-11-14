/* oxlint-disable no-control-regex -- Reject control characters in published text and URLs. */
/* oxlint-disable no-map-spread -- Construct fresh validated records without retaining source fields. */
import { createHash } from 'node:crypto'
import { lstat, readFile, writeFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { parseReleaseReadinessInput } from './generate-release-readiness.ts'
import { deriveReleaseChannel, validateReleaseTag } from '../lib/release-notes.ts'
import type {
    BlogBenchmark,
    BlogLocale,
    ReleaseBlogPost,
    ReleaseBlogReady,
} from './Types/release-blog.types.ts'

export function blogRecord(value: unknown): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value))
        throw new Error('Expected a blog object')
    return value as Record<string, unknown>
}

function text(value: unknown, maximum = 20_000): string {
    if (
        typeof value !== 'string' ||
        !value.trim() ||
        value.length > maximum ||
        /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(value)
    )
        throw new Error('Invalid blog text')
    return value
}

function identifier(value: unknown): string {
    const result = text(value, 64)
    if (!/^[a-z][a-z0-9-]{0,63}$/.test(result)) throw new Error('Invalid blog identifier')
    return result
}

export function blogTimestamp(value: unknown): string {
    const result = text(value, 32)
    if (
        !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(result) ||
        !Number.isFinite(Date.parse(result)) ||
        new Date(result).toISOString() !== result.replace(/(?<!\.\d{3})Z$/, '.000Z')
    )
        throw new Error('Invalid blog timestamp')
    return result
}

function url(value: unknown): string {
    const result = text(value, 2048)
    if (/[\s\u0000-\u001f\u007f]/.test(result)) throw new Error('Invalid blog URL')
    const parsed = new URL(result)
    if (parsed.protocol !== 'https:' || parsed.username || parsed.password)
        throw new Error('Blog links require HTTPS without credentials')
    return result
}

function array(value: unknown, maximum: number, minimum = 1): unknown[] {
    if (!Array.isArray(value) || value.length < minimum || value.length > maximum)
        throw new Error('Invalid blog collection')
    return value
}

export function blogRevision(value: unknown): number {
    if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 1)
        throw new Error('Invalid blog content revision')
    return value
}

function sourceVersion(input: unknown): { tag: string; revision: string; values: number[] } {
    const record = blogRecord(input)
    const tag = text(record.tag, 128)
    validateReleaseTag(tag, deriveReleaseChannel(tag) !== 'stable')
    if (
        !/^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-(?:alpha|beta)(?:\.[0-9A-Za-z-]+)*)?$/.test(
            tag,
        )
    )
        throw new Error('Invalid benchmark version')
    const revision = text(record.revision, 40)
    if (!/^[a-f0-9]{40}$/.test(revision)) throw new Error('Invalid benchmark revision')
    const values = array(record.values, 1000, 5).map((value) => {
        if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0 || value > 1e15)
            throw new Error('Benchmark samples must be positive finite bounded measurements')
        return value
    })
    return { tag, revision, values }
}

function benchmark(value: unknown, tag: string, revision: string): BlogBenchmark {
    const record = blogRecord(value)
    const baseline = sourceVersion(record.baseline)
    const candidate = sourceVersion(record.candidate)
    if (
        candidate.tag !== tag ||
        candidate.revision !== revision ||
        baseline.tag === tag ||
        baseline.revision === revision
    )
        throw new Error('Benchmarks require a distinct real baseline and exact release candidate')
    if (record.direction !== 'lower' && record.direction !== 'higher')
        throw new Error('Invalid benchmark direction')
    const unit = text(record.unit, 32)
    if (!/^[A-Za-z0-9%/ ._-]+$/.test(unit)) throw new Error('Invalid benchmark unit')
    return {
        id: identifier(record.id),
        unit,
        direction: record.direction,
        baseline,
        candidate,
        measuredAt: blogTimestamp(record.measuredAt),
        environment: text(record.environment, 4000),
        methodology: text(record.methodology, 8000),
        runUrl: url(record.runUrl),
    }
}

function locale(input: unknown, benchmarkIds: string[]): BlogLocale {
    const record = blogRecord(input)
    const sections = array(record.sections, 30).map((sectionInput) => {
        const section = blogRecord(sectionInput)
        const code = section.code === undefined ? undefined : blogRecord(section.code)
        const language = code === undefined ? undefined : text(code.language, 32)
        if (
            language &&
            ![
                'plaintext',
                'bash',
                'sh',
                'json',
                'yaml',
                'toml',
                'typescript',
                'rust',
                'dockerfile',
                'js',
                'javascript',
                'ts',
                'css',
                'html',
            ].includes(language)
        )
            throw new Error('Unsupported blog code language')
        return {
            id: identifier(section.id),
            title: text(section.title, 300),
            paragraphs: array(section.paragraphs, 30).map((value) => text(value)),
            ...(code && language ? { code: { language, value: text(code.value, 30_000) } } : {}),
            ...(section.links === undefined
                ? {}
                : {
                      links: array(section.links, 30, 0).map((value) => {
                          const link = blogRecord(value)
                          return { label: text(link.label, 300), url: url(link.url) }
                      }),
                  }),
        }
    })
    if (new Set(sections.map((section) => section.id)).size !== sections.length)
        throw new Error('Duplicate blog section IDs')
    let benchmarks: BlogLocale['benchmarks']
    if (record.benchmarks !== undefined) {
        const labels = blogRecord(record.benchmarks)
        if (
            Object.keys(labels).length !== benchmarkIds.length ||
            benchmarkIds.some((id) => !Object.hasOwn(labels, id))
        )
            throw new Error('Localized benchmark labels must match all measurements')
        benchmarks = Object.fromEntries(
            benchmarkIds.map((id) => {
                const label = blogRecord(labels[id])
                return [
                    id,
                    { title: text(label.title, 300), description: text(label.description, 4000) },
                ]
            }),
        )
    } else if (benchmarkIds.length) throw new Error('All locales need benchmark labels')
    return {
        title: text(record.title, 300),
        summary: text(record.summary, 2000),
        sections,
        ...(benchmarks === undefined ? {} : { benchmarks }),
    }
}

function validateBlogReadiness(readinessValue: unknown) {
    const readiness = blogRecord(readinessValue)
    const image = blogRecord(readiness.image)
    const validated = parseReleaseReadinessInput({
        repository: 'RentnerProxy/platform',
        releaseId: String(readiness.releaseId),
        tag: String(readiness.tag),
        channel: String(readiness.channel),
        prerelease: readiness.channel === 'stable' ? 'false' : 'true',
        publishedAt: String(readiness.publishedAt),
        revision: String(readiness.revision),
        image: String(image.repository),
        digest: String(image.digest),
        platforms: Array.isArray(image.platforms) ? image.platforms.join(',') : '',
    })
    if (readiness.schemaVersion !== 1) throw new Error('Unsupported readiness schema')
    return { validated, image }
}

export function buildReleaseBlog(readinessValue: unknown, sourceValue: unknown): ReleaseBlogPost {
    const { validated, image } = validateBlogReadiness(readinessValue)
    const source = blogRecord(sourceValue)
    const benchmarks =
        source.benchmarks === undefined
            ? undefined
            : array(source.benchmarks, 30).map((value) =>
                  benchmark(value, validated.tag, validated.revision),
              )
    const ids = benchmarks?.map((value) => value.id) ?? []
    if (new Set(ids).size !== ids.length) throw new Error('Duplicate benchmark IDs')
    const locales = blogRecord(source.locales)
    if (Object.keys(locales).toSorted().join(',') !== 'de,en,es,zh')
        throw new Error('Blog content requires exactly de, en, es, zh')
    return {
        schemaVersion: 1,
        releaseId: validated.releaseId,
        tag: validated.tag,
        revision: validated.revision,
        publishedAt: validated.publishedAt,
        channel: validated.channel,
        image: {
            repository: String(image.repository),
            digest: validated.digest,
            platforms: validated.platforms,
        },
        contentRevision: blogRevision(source.contentRevision),
        locales: {
            de: locale(locales.de, ids),
            en: locale(locales.en, ids),
            es: locale(locales.es, ids),
            zh: locale(locales.zh, ids),
        },
        ...(benchmarks === undefined ? {} : { benchmarks }),
    }
}

export function blogBytes(post: ReleaseBlogPost): Uint8Array {
    const bytes = Buffer.from(JSON.stringify(post, null, 4) + '\n')
    if (bytes.length > 1_048_576) throw new Error('Blog post exceeds one MiB')
    return bytes
}

export function blogReady(
    post: ReleaseBlogPost,
    bytes: Uint8Array,
    readyAt: string,
): ReleaseBlogReady {
    return {
        schemaVersion: 1,
        releaseId: post.releaseId,
        tag: post.tag,
        revision: post.revision,
        publishedAt: post.publishedAt,
        contentRevision: post.contentRevision,
        postAsset: `rentnerproxy-blog-post-r${post.contentRevision}.json`,
        postSha256: createHash('sha256').update(bytes).digest('hex'),
        readyAt: blogTimestamp(readyAt),
    }
}

export async function prepareReleaseBlog(
    sourceDirectory: string,
    artifactDirectory: string,
    tag: string,
): Promise<ReleaseBlogPost | undefined> {
    if (
        !/^v[0-9]+\.[0-9]+\.[0-9]+(?:-(?:alpha|beta)(?:\.[0-9A-Za-z-]+)*)?$/.test(tag) ||
        tag.length > 128
    )
        throw new Error('Invalid blog source tag')
    let source: string
    try {
        source = await readFile(
            resolve(sourceDirectory, '.github/release-content', `${tag}.json`),
            'utf8',
        )
    } catch (error) {
        if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return undefined
        throw error
    }
    if (Buffer.byteLength(source) > 1_048_576) throw new Error('Blog source exceeds one MiB')
    const readiness: unknown = JSON.parse(
        await readFile(resolve(artifactDirectory, 'rentnerproxy-update.json'), 'utf8'),
    )
    const { validated } = validateBlogReadiness(readiness)
    const authored = blogRecord(JSON.parse(source) as unknown)
    const benchmarkPath = resolve(artifactDirectory, 'release-blog-benchmarks.json')
    let artifact: Record<string, unknown> | undefined
    const info = await lstat(benchmarkPath).catch((error: unknown) => {
        if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return undefined
        throw error
    })
    if (info) {
        if (!info.isFile() || info.nlink !== 1 || info.size > 1_048_576)
            throw new Error('Benchmark artifact must be a regular bounded data file')
        const bytes = await readFile(benchmarkPath)
        if (bytes.length > 1_048_576) throw new Error('Benchmark artifact exceeds one MiB')
        artifact = blogRecord(JSON.parse(bytes.toString('utf8')) as unknown)
        if (
            artifact.schemaVersion !== 1 ||
            artifact.releaseId !== validated.releaseId ||
            artifact.tag !== validated.tag ||
            artifact.revision !== validated.revision ||
            artifact.imageDigest !== validated.digest
        )
            throw new Error('Benchmark artifact differs from the assessed release identity')
        if (authored.benchmarks !== undefined)
            throw new Error('Benchmark measurements cannot be supplied by both source and artifact')
        // The shared builder validates every sample and all four localized measurement labels.
        authored.benchmarks = array(artifact.benchmarks, 30)
    } else if (authored.benchmarks === undefined) {
        // Editorial labels may be prepared before optional measurements exist.
        for (const localized of Object.values(blogRecord(authored.locales))) {
            delete blogRecord(localized).benchmarks
        }
    }
    const post = buildReleaseBlog(readiness, authored)
    if (post.tag !== tag) throw new Error('Blog source and readiness tags differ')
    await writeFile(
        resolve(artifactDirectory, `rentnerproxy-blog-post-r${post.contentRevision}.json`),
        blogBytes(post),
    )
    return post
}
