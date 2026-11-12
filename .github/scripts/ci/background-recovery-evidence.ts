import { constants } from 'node:fs'
import { open, mkdir, writeFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import {
    backgroundRecoveryEvidence,
    backgroundRecoveryMatrix,
} from '../../../scripts/runtime-scale/background-evidence.ts'
import {
    backgroundAutomationManifest,
    backgroundFixtureSourceManifest,
} from '../../../scripts/runtime-scale/background-provenance.ts'

async function readReport(path: string): Promise<unknown> {
    const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW)
    try {
        const before = await file.stat()
        if (!before.isFile() || before.size > 8 * 1024 * 1024)
            throw new Error('Invalid report file')
        const text = await file.readFile({ encoding: 'utf8' })
        const after = await file.stat()
        if (before.size !== after.size || before.mtimeMs !== after.mtimeMs)
            throw new Error('Report changed while reading')
        return JSON.parse(text) as unknown
    } finally {
        await file.close()
    }
}

async function main() {
    const [source, amd64, arm64, output, ...extra] = process.argv.slice(2)
    if (!source || !/^[a-f0-9]{40}$/u.test(source) || !amd64 || !arm64 || !output || extra.length)
        throw new Error(
            'Expected source SHA, native AMD64 report, native ARM64 report and output path',
        )
    const root = resolve(import.meta.dir, '../../..')
    const [automation, fixture, amd64Report, arm64Report] = await Promise.all([
        backgroundAutomationManifest(root),
        backgroundFixtureSourceManifest(root),
        readReport(amd64),
        readReport(arm64),
    ])
    const expected = {
        targetSha: source,
        automationSha256: automation.sha256,
        fixtureSha256: fixture.sha256,
    }
    const matrix = backgroundRecoveryMatrix([
        backgroundRecoveryEvidence(amd64Report, { ...expected, imagePlatform: 'linux/amd64' }),
        backgroundRecoveryEvidence(arm64Report, { ...expected, imagePlatform: 'linux/arm64' }),
    ])
    await mkdir(dirname(resolve(output)), { recursive: true })
    await writeFile(output, JSON.stringify(matrix, null, 2) + '\n', { mode: 0o600 })
    console.log(
        'Native background recovery evidence: both declared platforms verified; sustained capacity remains insufficient',
    )
}

if (import.meta.main) {
    try {
        await main()
    } catch {
        console.error(
            'Native background recovery evidence is missing, invalid or outside its declared scope',
        )
        process.exitCode = 1
    }
}
