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

export async function readBackgroundReportFile(path: string): Promise<unknown> {
    const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK)
    try {
        const before = await file.stat()
        if (!before.isFile() || before.size > 8 * 1024 * 1024)
            throw new Error('Invalid report file')
        const bytes = Buffer.alloc(before.size + 1)
        let count = 0
        while (count < bytes.length) {
            // oxlint-disable-next-line no-await-in-loop -- Each bounded read advances this one immutable artifact handle.
            const result = await file.read(bytes, count, bytes.length - count, count)
            if (result.bytesRead === 0) break
            count += result.bytesRead
        }
        const after = await file.stat()
        if (count !== before.size || before.size !== after.size || before.mtimeMs !== after.mtimeMs)
            throw new Error('Report changed while reading')
        return JSON.parse(bytes.subarray(0, count).toString('utf8')) as unknown
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
        readBackgroundReportFile(amd64),
        readBackgroundReportFile(arm64),
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
