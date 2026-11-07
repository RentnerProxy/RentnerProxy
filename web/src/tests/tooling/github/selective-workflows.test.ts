import { readFile } from 'node:fs/promises'
import { expect, test } from 'bun:test'

type Step = {
    name: string
    if?: string
    run?: string
    uses?: string
    env?: Record<string, string>
    with?: Record<string, unknown>
}

type Job = {
    name?: string
    needs?: string
    if?: string
    uses?: string
    env?: Record<string, string>
    steps?: Step[]
    services?: Record<string, { image: string }>
    strategy?: { matrix: { include: { language: string; scope: string }[] } }
}

async function workflow(name: string): Promise<{ jobs: Record<string, Job> }> {
    return Bun.YAML.parse(
        await readFile(
            new URL(`../../../../../.github/workflows/${name}.yml`, import.meta.url),
            'utf8',
        ),
    ) as { jobs: Record<string, Job> }
}

test.each([
    ['beta2-integration', 'beta2', 'runtime'],
    ['arm64-appliance', 'native-arm64', 'runtime'],
    ['workflow-lint', 'workflow-lint', 'actions'],
    ['codeql', 'analyze', '[matrix.scope]'],
])(
    '%s preserves its status and rejects failed detection before omitting work',
    async (name, id, scope) => {
        const { jobs } = await workflow(name!)
        expect(jobs.changes?.uses).toBe('./.github/workflows/detect-changes.yml')
        const job = jobs[id!]!
        expect(job.needs).toBe('changes')
        expect(job.if).toBe('${{ !cancelled() }}')
        expect(job.env?.RUN_CHECK).toContain(scope!)
        const [guard, unaffected, ...expensive] = job.steps!
        expect(guard?.if).toBeUndefined()
        expect(guard?.env?.CHANGE_RESULT).toBe('${{ needs.changes.result }}')
        expect(guard?.run).toBe('test "$CHANGE_RESULT" = success')
        expect(unaffected?.if).toBe("env.RUN_CHECK != 'true'")
        expect(unaffected?.run).toContain('No relevant inputs changed')
        expect(expensive.length).toBeGreaterThan(0)
        for (const step of expensive)
            expect(step.if, step.name).toContain("env.RUN_CHECK == 'true'")
    },
)

test.each([
    ['beta2-integration', 'beta2', "needs.changes.outputs.runtime != 'false'"],
    ['ci', 'web-tests', "needs.changes.outputs.web == 'true'"],
])(
    '%s omits database and cache service containers when unaffected',
    async (name, id, selection) => {
        const { jobs } = await workflow(name!)
        for (const service of Object.values(jobs[id!]!.services!)) {
            expect(service.image).toContain(selection!)
            expect(service.image).toContain("|| ''")
            expect(service.image).toContain('@sha256:')
        }
    },
)

test('Beta journeys cache the exact Caddy fixture without giving PRs cache writes', async () => {
    const { jobs } = await workflow('beta2-integration')
    const build = jobs.beta2!.steps!.find((step) =>
        step.uses?.startsWith('docker/build-push-action@'),
    )!
    expect(build.with?.target).toBe('caddy-build')
    expect(build.with?.load).toBe(true)
    expect(build.with?.push).toBe(false)
    expect(build.with?.tags).toBe('rentnerproxy-beta2-caddy:${{ github.sha }}')
    expect(build.with?.['cache-from']).toContain('scope=production-smokes-proxy-runtime-v1')
    expect(build.with?.['cache-to']).toContain("github.event_name != 'pull_request'")
    const extract = jobs.beta2!.steps!.find((step) => step.env?.CADDY_FIXTURE_IMAGE)!
    expect(extract.env?.CADDY_FIXTURE_IMAGE).toBe('rentnerproxy-beta2-caddy:${{ github.sha }}')
    expect(extract.run).toContain('docker cp "$container:/usr/bin/caddy"')
    expect(extract.run).not.toContain('docker build ')
})

test('CodeQL selects each language independently while retaining all check names', async () => {
    const { jobs } = await workflow('codeql')
    expect(jobs.analyze!.name).toBe('CodeQL (${{ matrix.display-name }})')
    expect(
        jobs.analyze!.strategy!.matrix.include.map(({ language, scope }) => [language, scope]),
    ).toEqual([
        ['javascript-typescript', 'web'],
        ['rust', 'rust'],
        ['actions', 'actions'],
    ])
    expect(jobs.analyze!.env?.RUN_CHECK).toBe(
        "${{ needs.changes.outputs[matrix.scope] != 'false' }}",
    )
})

test('release builds require real full verification before preparing any publication', async () => {
    const { jobs } = await workflow('release-pipeline')
    const steps = jobs.prepare!.steps!
    const source = steps.findIndex((step) => step.run?.includes('/prepare-release-source.sh'))
    const verification = steps.findIndex((step) =>
        step.run?.includes('/verify-exact-revision-ci.ts'),
    )
    const assets = steps.findIndex((step) => step.uses?.startsWith('actions/upload-artifact@'))
    expect(source).toBeGreaterThanOrEqual(0)
    expect(verification).toBeGreaterThan(source)
    expect(assets).toBeGreaterThan(verification)
    expect(steps[verification]?.env?.RELEASE_REVISION).toBe('${{ steps.release.outputs.revision }}')
    expect(steps[verification]?.run).toContain('exit 1')
    expect(steps[verification]?.run).toContain('Run CI and Production Smokes manually')
    expect(jobs.build?.needs).toBe('prepare')
})
