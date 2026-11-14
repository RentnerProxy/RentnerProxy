export interface BlogBenchmark {
    id: string
    unit: string
    direction: 'lower' | 'higher'
    baseline: { tag: string; revision: string; values: number[] }
    candidate: { tag: string; revision: string; values: number[] }
    measuredAt: string
    environment: string
    methodology: string
    runUrl: string
}

export interface BlogLocale {
    title: string
    summary: string
    sections: {
        id: string
        title: string
        paragraphs: string[]
        code?: { language: string; value: string }
        links?: { label: string; url: string }[]
    }[]
    benchmarks?: Record<string, { title: string; description: string }>
}

export interface ReleaseBlogPost {
    schemaVersion: 1
    releaseId: number
    tag: string
    revision: string
    publishedAt: string
    channel: 'alpha' | 'beta' | 'stable'
    image: { repository: string; digest: string; platforms: readonly string[] }
    contentRevision: number
    locales: Record<'de' | 'en' | 'es' | 'zh', BlogLocale>
    benchmarks?: BlogBenchmark[]
}

export interface ReleaseBlogReady {
    schemaVersion: 1
    releaseId: number
    tag: string
    revision: string
    publishedAt: string
    contentRevision: number
    postAsset: string
    postSha256: string
    readyAt: string
}

export interface BlogAsset {
    id: number
    name: string
    state?: 'starter' | 'uploading' | 'uploaded'
    size?: number
}

export interface BlogPublicationClient {
    verifyIdentity(): Promise<void>
    listAssets(): Promise<BlogAsset[]>
    readAsset(asset: BlogAsset): Promise<Uint8Array>
    uploadAsset(name: string, bytes: Uint8Array): Promise<BlogAsset>
    deleteAsset(asset: BlogAsset): Promise<void>
}
