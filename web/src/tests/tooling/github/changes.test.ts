import { mkdir, mkdtemp, rename, rm, writeFile, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, test } from 'bun:test'
import { selectChecks, detectChanges } from '../../../../../.github/scripts/ci/changes.ts'

function git(cwd: string, ...args: string[]): string {
    const result = Bun.spawnSync(['git', ...args], {
        cwd,
        env: {
            ...process.env,
            GIT_CONFIG_NOSYSTEM: '1',
            GIT_CONFIG_GLOBAL: process.platform === 'win32' ? 'NUL' : '/dev/null',
            GIT_AUTHOR_NAME: 'CI Fixture',
            GIT_AUTHOR_EMAIL: 'fixture@example.invalid',
            GIT_COMMITTER_NAME: 'CI Fixture',
            GIT_COMMITTER_EMAIL: 'fixture@example.invalid',
        },
        stdout: 'pipe',
        stderr: 'pipe',
    })
    if (result.exitCode !== 0) throw new Error(result.stderr.toString())
    return result.stdout.toString().trim()
}

async function fixture(run: (directory: string, base: string) => Promise<void>) {
    const directory = await mkdtemp(join(tmpdir(), 'rentnerproxy-caddy-changes-'))
    try {
        git(directory, 'init', '--quiet')
        await mkdir(join(directory, 'core/caddy-modules/responsecache'), { recursive: true })
        await writeFile(join(directory, 'README.md'), 'fixture documentation')
        await writeFile(
            join(directory, 'core/caddy-modules/responsecache/cache.go'),
            'fixture module',
        )
        git(directory, 'add', '.')
        git(directory, 'commit', '--quiet', '-m', 'fixture base')
        await run(directory, git(directory, 'rev-parse', 'HEAD'))
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
}

function commit(directory: string): string {
    git(directory, 'add', '--all')
    git(directory, 'commit', '--quiet', '-m', 'fixture change')
    return git(directory, 'rev-parse', 'HEAD')
}

describe('affected CI selection', () => {
    test('documentation avoids heavy checks and unknown ownership runs the full suite', () => {
        expect(Object.values(selectChecks(['README.md', 'docs/architecture.md']))).toEqual(
            Array(11).fill(false),
        )
        expect(Object.values(selectChecks(['new-area/input']))).toEqual(Array(11).fill(true))
        expect(selectChecks(['package.json']).full).toBe(true)
        expect(selectChecks(['tsconfig.scripts.json']).full).toBe(true)
    })
    test('client presentation selects web without native and runtime tests', () => {
        const checks = selectChecks(['web/src/features/Hosts/Components/HostCard.tsx'])
        expect(checks.web).toBe(true)
        expect(checks.build_web).toBe(true)
        for (const name of ['rust', 'go', 'caddy', 'runtime'] as const)
            expect(checks[name]).toBe(false)
    })
    test.each([
        'web/src/features/Hosts/middleware.ts',
        'web/src/routes/api/session.ts',
        'web/src/features/Hosts/validation.ts',
        'web/src/shared/contract.ts',
        'web/src/db/schema.ts',
    ])('%s retains runtime checks', (path) => {
        expect(selectChecks([path]).runtime).toBe(true)
    })
    test('language and automation ownership remain independent', () => {
        expect(selectChecks(['core/src/main.rs'])).toMatchObject({
            rust: true,
            build_core: true,
            runtime: true,
            web: false,
            go: false,
        })
        expect(selectChecks(['core/caddy-modules/responsecache/cache.go'])).toMatchObject({
            go: true,
            caddy: true,
            runtime: true,
            rust: false,
        })
        expect(selectChecks(['.github/workflows/codeql.yml'])).toMatchObject({
            actions: true,
            web: true,
            scripts: true,
            rust: true,
        })
        expect(selectChecks(['.github/scripts/ci/go-coverage-check.go']).go).toBe(true)
        expect(selectChecks(['docker/tooling/buildinfo/verify.go']).go).toBe(true)
        expect(selectChecks(['.github/scripts/ci/changes.ts']).full).toBe(true)
    })
    test.each([
        'scripts/assets/prepare-country-flags.ts',
        'scripts/assets/download-geoip.ts',
        'scripts/assets/Types/download-geoip.types.ts',
    ])('%s retains production build and integration verification', (path) => {
        expect(selectChecks([path])).toMatchObject({ build_web: true, runtime: true })
    })
    test.each([
        '.github/scripts/security/scan-image.sh',
        '.github/scripts/security/extract-image-input.sh',
        '.github/scripts/security/install-dependency-tools.sh',
    ])('%s retains source and production image assessment', (path) => {
        expect(selectChecks([path])).toMatchObject({ dependencies: true, runtime: true })
    })
    test.each([
        'scripts/coverage/coverage.config.ts',
        'scripts/coverage/summary.ts',
        'scripts/coverage/report.ts',
        'scripts/coverage/Types/report.types.ts',
    ])('%s verifies the shared coverage contract against every producer', (path) => {
        expect(selectChecks([path])).toMatchObject({ web: true, go: true, rust: true })
    })
    test('manual, invalid and absent revisions select full verification', () => {
        expect(detectChanges('workflow_dispatch', '', '').full).toBe(true)
        expect(detectChanges('pull_request', '', '').full).toBe(true)
        expect(detectChanges('push', '0'.repeat(40), 'a'.repeat(40)).full).toBe(true)
    })
    test('PR uses merge base while push includes changes from the previous tip', async () => {
        await fixture(async (directory, base) => {
            await writeFile(join(directory, 'README.md'), 'PR documentation')
            const head = commit(directory)
            git(directory, 'checkout', '--detach', base)
            await writeFile(join(directory, 'core/base-only.rs'), 'base change')
            const advanced = commit(directory)
            expect(detectChanges('pull_request', advanced, head, directory).full).toBe(false)
            expect(detectChanges('pull_request', advanced, head, directory).rust).toBe(false)
            expect(detectChanges('push', advanced, head, directory).rust).toBe(true)
        })
    })
    test('deletion and rename retain old runtime input, including unusual path characters', async () => {
        await fixture(async (directory, base) => {
            await mkdir(join(directory, 'docs'))
            await rename(
                join(directory, 'core/caddy-modules/responsecache/cache.go'),
                join(directory, 'docs/cache name.md'),
            )
            expect(detectChanges('push', base, commit(directory), directory).caddy).toBe(true)
            await writeFile(join(directory, 'core/new input.rs'), 'new')
            const added = commit(directory)
            await rm(join(directory, 'core/new input.rs'))
            expect(detectChanges('push', added, commit(directory), directory).rust).toBe(true)
        })
    })
    test('unresolvable comparison fails closed', async () => {
        await fixture(async (directory, base) => {
            expect(() => detectChanges('push', 'a'.repeat(40), base, directory)).toThrow(
                'Cannot resolve',
            )
        })
    })
})

test('required CI jobs guard detector failures and preserve successful unaffected statuses', async () => {
    const workflow = Bun.YAML.parse(
        await readFile(new URL('../../../../../.github/workflows/ci.yml', import.meta.url), 'utf8'),
    ) as {
        jobs: Record<
            string,
            {
                name: string
                if?: string
                needs?: string | string[]
                steps?: { name: string; run?: string; env?: Record<string, string>; if?: string }[]
            }
        >
    }
    for (const id of ['lint', 'typecheck', 'web-tests', 'rust', 'build', 'caddy-response-cache']) {
        const job = workflow.jobs[id]!
        expect(job.needs).toBe('changes')
        expect(job.if).toBe('${{ !cancelled() }}')
        expect(job.steps?.[0]?.env?.DETECTION_RESULT).toBe('${{ needs.changes.result }}')
        expect(job.steps?.[0]?.run).toContain('test "$DETECTION_RESULT" = success')
        expect(job.steps?.[1]?.name).toBe('Explain unaffected check')
    }
    expect(workflow.jobs.fuzz?.steps).toHaveLength(1)
    expect(workflow.jobs.fuzz?.steps?.[0]?.run).not.toContain('test:fuzz')
    expect(workflow.jobs['full-verification']?.if).toContain("needs.changes.outputs.full == 'true'")
    expect(workflow.jobs.coverage?.steps?.some((step) => step.run?.includes('--producers'))).toBe(
        true,
    )
})
