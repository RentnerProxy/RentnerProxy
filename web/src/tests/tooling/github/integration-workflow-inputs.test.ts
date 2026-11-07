import { readFile } from 'node:fs/promises'
import { expect, test } from 'bun:test'

type Trigger = { paths?: string[]; 'paths-ignore'?: string[] } | null
type Workflow = { on: Record<string, Trigger> }

async function workflow(name: string): Promise<Workflow> {
    return Bun.YAML.parse(
        await readFile(
            new URL(`../../../../../.github/workflows/${name}.yml`, import.meta.url),
            'utf8',
        ),
    ) as Workflow
}

test.each(['release-compatibility', 'runtime-reliability'])(
    '%s retains manual diagnostics while central main and PR checks cover runtime inputs',
    async (name) => {
        const triggers = (await workflow(name)).on
        expect(triggers).not.toHaveProperty('pull_request')
        expect(triggers).not.toHaveProperty('push')
        expect(triggers).toHaveProperty('workflow_dispatch')
        const central = (await workflow('production-smokes')).on
        for (const event of ['pull_request', 'push']) {
            expect(central).toHaveProperty(event)
            expect(central[event]?.paths).toBeUndefined()
            expect(central[event]?.['paths-ignore']).toBeUndefined()
        }
    },
)

test.each([
    'ci',
    'workflow-lint',
    'gitleaks',
    'codeql',
    'dependency-review',
    'dependency-security',
    'production-smokes',
    'pr-preview-source',
])('%s keeps required PR checks independent of path filters', async (name) => {
    const triggers = (await workflow(name)).on
    expect(triggers).toHaveProperty('pull_request')
    expect(triggers.pull_request?.paths).toBeUndefined()
    expect(triggers.pull_request?.['paths-ignore']).toBeUndefined()
})
