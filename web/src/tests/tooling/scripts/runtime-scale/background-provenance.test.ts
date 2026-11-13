import { expect, test } from 'bun:test'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import {
    backgroundAutomationManifest,
    recheckBackgroundAutomationManifest,
    backgroundSourceManifest,
    backgroundFixtureSourceManifest,
} from '../../../../../../scripts/runtime-scale/background-provenance.ts'
import { BACKGROUND_FIXTURE_SOURCE_PATHS } from '../../../../../../scripts/runtime-scale/background-source.config.ts'
import { RELIABILITY_FIXTURE_SOURCE_PATHS } from '../../../../../../scripts/runtime-reliability/fixture-source.config.ts'

async function fixture(root: string) {
    const sources = {
        'scripts/runtime-scale/background-run.ts':
            "import '../smoke/process.ts'; await import('../smoke/wait.ts'); import {helper} from '@/lib/helper.ts'; import type {Unused} from './Types/not-present.ts'; import 'external-package'; console.log(helper)",
        'scripts/runtime-reliability/harness.ts': "export * from '../smoke/process.ts'",
        'scripts/smoke/process.ts': "import './nested/child.ts'; export const process = 1",
        'scripts/smoke/wait.ts': 'export const wait = 1',
        'scripts/smoke/nested/child.ts': 'export const child = 1',
        'web/src/lib/helper.ts': 'export const helper = 1',
        'package.json': '{}',
        'bun.lock': '# fixture',
        'tsconfig.json': '{}',
        'tsconfig.scripts.json': '{}',
    }
    await Promise.all(
        Object.entries(sources).map(async ([path, text]) => {
            await mkdir(dirname(join(root, path)), { recursive: true })
            await writeFile(join(root, path), text)
        }),
    )
    return Object.keys(sources).toSorted()
}

test('expected fixture manifest matches both source staging owners and archive inputs, and excludes unrelated files', async () => {
    const root = await mkdtemp(join(tmpdir(), 'background-fixture-inputs-'))
    try {
        const paths = [
            ...BACKGROUND_FIXTURE_SOURCE_PATHS,
            ...RELIABILITY_FIXTURE_SOURCE_PATHS,
            'web/src/fixture.ts',
            'package.json',
            'bun.lock',
            'tsconfig.json',
        ]
        await Promise.all(
            [...new Set(paths)].map(async (path) => {
                await mkdir(dirname(join(root, path)), { recursive: true })
                await writeFile(join(root, path), path)
            }),
        )
        const staged = await backgroundSourceManifest(root)
        await writeFile(join(root, 'unrelated.txt'), 'not a fixture input')
        expect(await backgroundFixtureSourceManifest(root)).toEqual(staged)
        await writeFile(join(root, 'scripts/lib/request-context.ts'), 'changed staging input')
        expect((await backgroundFixtureSourceManifest(root)).sha256).not.toBe(staged.sha256)
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test('the actual runner closure includes both entry points and shared smoke process/wait execution', async () => {
    const manifest = await backgroundAutomationManifest(
        resolve(import.meta.dir, '../../../../../..'),
    )
    const paths = new Set(manifest.files.map((file) => file.path))
    for (const path of [
        'scripts/runtime-scale/background-run.ts',
        'scripts/runtime-reliability/harness.ts',
        'scripts/smoke/process.ts',
        'scripts/smoke/wait.ts',
    ])
        expect(paths.has(path)).toBe(true)
    expect([...paths].some((path) => path.includes('node_modules') || path.endsWith('.env'))).toBe(
        false,
    )
})

test('automation manifest follows the complete executable local closure, including dynamic smoke imports and aliases', async () => {
    const root = await mkdtemp(join(tmpdir(), 'background-provenance-'))
    try {
        const paths = await fixture(root)
        const before = await backgroundAutomationManifest(root)
        expect(before.files.map((file) => file.path).toSorted()).toEqual(paths)
        await writeFile(join(root, 'scripts/smoke/nested/child.ts'), 'export const child = 2')
        expect((await backgroundAutomationManifest(root)).sha256).not.toBe(before.sha256)
        expect(await recheckBackgroundAutomationManifest(root, before.sha256)).toMatchObject({
            unchanged: false,
            status: 'changed',
        })
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test('unreadable final input returns unavailable provenance without throwing away report generation or inventing a digest', async () => {
    const root = await mkdtemp(join(tmpdir(), 'background-provenance-'))
    try {
        await fixture(root)
        const before = await backgroundAutomationManifest(root)
        await rm(join(root, 'package.json'))
        const result = await recheckBackgroundAutomationManifest(root, before.sha256)
        expect(result).toEqual({ unchanged: false, sha256: null, status: 'unavailable' })
        expect(JSON.stringify(result)).not.toContain('ENOENT')
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test('local executable imports cannot cause environment or out-of-root reads', async () => {
    const root = await mkdtemp(join(tmpdir(), 'background-provenance-'))
    try {
        await fixture(root)
        await writeFile(
            join(root, 'scripts/runtime-scale/background-run.ts'),
            "import '../../.env'",
        )
        await expect(backgroundAutomationManifest(root)).rejects.toThrow(
            'outside the source boundary',
        )
        await writeFile(
            join(root, 'scripts/runtime-scale/background-run.ts'),
            "import '../../../../outside.ts'",
        )
        await expect(backgroundAutomationManifest(root)).rejects.toThrow(
            'outside the source boundary',
        )
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})
