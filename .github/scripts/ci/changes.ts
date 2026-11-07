import { appendFile } from 'node:fs/promises'

const checkNames = [
    'web',
    'scripts',
    'rust',
    'go',
    'caddy',
    'build_web',
    'build_core',
    'runtime',
    'dependencies',
    'actions',
    'full',
] as const
type CheckSelection = Record<(typeof checkNames)[number], boolean>

function selection(value: boolean): CheckSelection {
    return Object.fromEntries(checkNames.map((name) => [name, value])) as CheckSelection
}

export function selectChecks(paths: readonly string[]): CheckSelection {
    const selected = selection(false)
    const enable = (...names: (keyof CheckSelection)[]) => {
        for (const name of names) selected[name] = true
    }
    for (const path of paths) {
        if (/^(?:docs\/|README\.md$|SECURITY\.md$|LICENSE$|AGENTS\.md$)/u.test(path)) continue
        if (/^\.github\/.*\.md$/u.test(path)) continue
        if (/^\.github\/(?:ISSUE_TEMPLATE\/|assets\/|CODEOWNERS$|release-notes\.json$)/u.test(path))
            continue
        if (/^\.github\/(?:workflows\/|actions\/)/u.test(path)) {
            enable('actions', 'web', 'scripts')
            if (
                /^\.github\/(?:actions\/(?:runtime-images|load-runtime-images)\/|workflows\/(?:production-smokes|arm64-appliance|beta2-integration|release-compatibility|runtime-reliability)\.yml$)/u.test(
                    path,
                )
            )
                enable('runtime')
            if (path === '.github/workflows/dependency-security.yml')
                enable('dependencies', 'runtime')
            if (path === '.github/workflows/codeql.yml') enable('rust')
            if (
                path === '.github/workflows/detect-changes.yml' ||
                path === '.github/workflows/ci.yml'
            )
                return selection(true)
            continue
        }
        if (path.startsWith('.github/scripts/')) {
            enable('web', 'scripts', 'actions')
            if (path.includes('/security/')) enable('dependencies', 'runtime')
            if (path.includes('/ci/') || path.includes('/lib/')) enable('runtime')
            if (path.endsWith('/rust-coverage.sh')) enable('rust')
            if (path.includes('/caddy-')) enable('go', 'caddy', 'runtime')
            if (path.endsWith('.go') || path.endsWith('/go-coverage.sh')) enable('go')
            if (path.endsWith('/changes.ts') || path.includes('/Types/changes'))
                return selection(true)
            continue
        }
        if (path.startsWith('core/caddy-modules/')) {
            enable('go', 'caddy', 'runtime')
            continue
        }
        if (/^core\/(?:src\/|tests\/|build\.rs$)/u.test(path)) {
            enable('rust', 'build_core', 'runtime')
            continue
        }
        if (
            /^core\/(?:Cargo\.(?:toml|lock)$|\.cargo\/)/u.test(path) ||
            /^(?:rust-toolchain|\.cargo\/)/u.test(path)
        ) {
            enable('rust', 'build_core', 'runtime', 'dependencies')
            continue
        }
        if (path.startsWith('web/src/tests/')) {
            enable('web')
            if (path.startsWith('web/src/tests/tooling/github/caddy-'))
                enable('go', 'caddy', 'runtime')
            if (path.startsWith('web/src/tests/tooling/docker/')) enable('runtime')
            continue
        }
        if (
            path.startsWith('web/src/features/') &&
            (path.endsWith('/middleware.ts') ||
                path.includes('.server.') ||
                (!path.endsWith('.tsx') && !/\/(?:Components|Hooks|Types)\//u.test(path)))
        ) {
            enable('web', 'scripts', 'build_web', 'runtime')
            continue
        }
        if (
            /^web\/(?:public\/|src\/(?:features|layouts|styles)\/)/u.test(path) ||
            /^web\/src\/.*\.css$/u.test(path)
        ) {
            enable('web', 'build_web')
            continue
        }
        if (path.startsWith('web/')) {
            enable('web', 'scripts', 'build_web', 'runtime')
            continue
        }
        if (path.startsWith('scripts/')) {
            enable('web', 'scripts')
            if (
                /^scripts\/coverage\/(?:coverage\.config\.ts|report\.ts|summary\.ts|Types\/report\.types\.ts)$/u.test(
                    path,
                )
            )
                enable('go', 'rust')
            if (!/^scripts\/(?:coverage\/|test-web\.ts$)/u.test(path))
                enable('build_web', 'build_core', 'runtime')
            continue
        }
        if (
            path.startsWith('docker/') ||
            path.startsWith('packaging/') ||
            path === 'docker-compose.yml' ||
            path === '.dockerignore'
        ) {
            enable('web', 'scripts', 'runtime', 'build_web', 'build_core')
            if (path.startsWith('docker/tooling/buildinfo/')) enable('go')
            if (path.startsWith('docker/proxy-runtime/') || path.startsWith('docker/caddy/'))
                enable('caddy', 'go')
            continue
        }
        if (
            /^(?:package\.json|bun\.lock|bunfig\.toml|tsconfig.*\.json|vite\.config\.ts|drizzle\.config\.ts|knip\.json|\.oxlintrc.*\.json)$/u.test(
                path,
            )
        )
            return selection(true)
        return selection(true)
    }
    selected.full = checkNames.filter((name) => name !== 'full').every((name) => selected[name])
    return selected
}

export function detectChanges(
    event: string,
    base: string,
    head: string,
    cwd = process.cwd(),
): CheckSelection {
    if (!['pull_request', 'push'].includes(event)) return selection(true)
    for (const revision of [base, head]) {
        if (!/^[a-f0-9]{40,64}$/u.test(revision) || /^0+$/u.test(revision)) return selection(true)
    }
    const comparison = event === 'pull_request' ? `${base}...${head}` : `${base}..${head}`
    const result = Bun.spawnSync(
        ['git', 'diff', '--no-renames', '--name-only', '-z', comparison, '--'],
        {
            cwd,
            stdout: 'pipe',
            stderr: 'pipe',
        },
    )
    if (result.exitCode !== 0) throw new Error('Cannot resolve the CI change comparison')
    return selectChecks(result.stdout.toString().split('\0').filter(Boolean))
}

if (import.meta.main) {
    const selected = detectChanges(
        process.env.GITHUB_EVENT_NAME ?? '',
        process.env.CI_BASE_SHA ?? '',
        process.env.CI_HEAD_SHA ?? '',
    )
    if (!process.env.GITHUB_OUTPUT) throw new Error('GITHUB_OUTPUT is required')
    await appendFile(
        process.env.GITHUB_OUTPUT,
        checkNames.map((name) => `${name}=${String(selected[name])}\n`).join(''),
    )
    console.log(JSON.stringify(selected, null, 2))
}
