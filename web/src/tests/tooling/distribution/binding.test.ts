// oxlint-disable no-await-in-loop -- Mutate each immutable proof independently to check rejection.
import { expect, test } from 'bun:test'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { bindDistribution } from '../../../../../.github/scripts/distribution/bind.ts'
import {
    validateDistributionCandidate,
    verifyCandidateDistribution,
    verifyBoundDistribution,
    validateDistributionReports,
} from '../../../../../.github/scripts/distribution/candidate.ts'
import {
    bundleFiles,
    verifyDistributionBundle,
    writeDistributionBundle,
} from '../../../../../.github/scripts/distribution/bundle.ts'
import {
    validateNativeDistributionProofs,
    signDistribution,
} from '../../../../../.github/scripts/distribution/publication.ts'
import { assembleCandidateImage } from '../../../../../.github/scripts/release/multiarch-image.ts'
import { identity, platformFixture, workflow } from '../github/fixtures/oci.ts'

async function fixture(root: string) {
    const amd = await platformFixture(join(root, 'amd64'), 'amd64'),
        arm = await platformFixture(join(root, 'arm64'), 'arm64')
    const layout = join(root, 'layout')
    const image = await assembleCandidateImage({
        directory: root,
        output: layout,
        identity,
        expected: { amd64: amd.expected, arm64: arm.expected },
    })
    const expected = { ...identity, ...workflow, digest: image.digest }
    const candidate = join(root, 'candidate'),
        reports = join(root, 'reports'),
        output = join(root, 'final')
    await mkdir(candidate)
    await mkdir(reports)
    await mkdir(join(candidate, 'packages/amd64'), { recursive: true })
    await writeFile(join(candidate, 'packages/amd64/fixture.deb'), Buffer.from([0, 255, 1, 99]))
    const archiveSha256 = 'b'.repeat(64)
    await writeFile(
        join(candidate, 'image-candidate.json'),
        JSON.stringify({ schemaVersion: 1, ...expected, archiveSha256, status: 'unassessed' }),
    )
    const candidateHash = await writeDistributionBundle(candidate, expected)
    for (const target of ['debian', 'fedora'])
        for (const architecture of ['amd64', 'arm64'])
            await writeFile(
                join(reports, `native-${target}-${architecture}.json`),
                JSON.stringify({
                    schemaVersion: 1,
                    ...expected,
                    target,
                    architecture,
                    passed: true,
                    candidateBundleSha256: candidateHash,
                }),
            )
    return {
        candidate,
        reports,
        output,
        layout,
        candidateHash,
        archiveSha256,
        identity: expected,
        evidence: {
            schemaVersion: 1,
            ...expected,
            native: [amd.evidence, arm.evidence],
            portability: {
                schemaVersion: 1,
                ...identity,
                sourceManifest: amd.runtime.digest,
                targetManifest: arm.runtime.digest,
                backupVersion: 4,
                checks: 'amd64-backup-to-native-arm64',
                nativeUpgradeSources: [],
            },
        },
    }
}

test('unassessed candidates validate exact workflow identity without making an approval claim', () => {
    const expected = { ...identity, ...workflow, digest: 'sha256:' + 'a'.repeat(64) },
        archive = 'b'.repeat(64)
    const metadata = { schemaVersion: 1, ...expected, status: 'unassessed' }
    expect(
        validateDistributionCandidate(metadata, { ...expected, runAttempt: 3 }, archive).status,
    ).toBe('unassessed')
    for (const change of [
        { status: 'assessed' },
        { revision: 'b'.repeat(40) },
        { runId: '999' },
        { runAttempt: 0 },
        { runAttempt: 3 },
        { digest: 'sha256:' + 'c'.repeat(64) },
    ])
        expect(() =>
            validateDistributionCandidate({ ...metadata, ...change }, expected, archive),
        ).toThrow()
})

test('late binding preserves every candidate byte and creates a distinct closed assessed bundle', async () => {
    const root = await mkdtemp(join(tmpdir(), 'rp-late-binding-'))
    try {
        const options = await fixture(root),
            before = await readFile(join(options.candidate, 'bundle.json'))
        await verifyCandidateDistribution(
            options.candidate,
            options.candidateHash,
            options.identity,
        )
        await expect(
            validateNativeDistributionProofs(options.candidate, options.reports, options.identity),
        ).rejects.toThrow()
        const finalHash = await bindDistribution(options)
        expect(finalHash).not.toBe(options.candidateHash)
        await verifyDistributionBundle(options.output, finalHash, options.identity)
        await validateNativeDistributionProofs(options.output, options.reports, options.identity)
        expect(
            await verifyBoundDistribution(options.output, { ...options.identity, runAttempt: 3 }),
        ).toBe(options.candidateHash)
        for (const path of await bundleFiles(options.candidate))
            expect(
                await readFile(
                    join(options.output, path === 'bundle.json' ? 'candidate-bundle.json' : path),
                ),
            ).toEqual(await readFile(join(options.candidate, path)))
        expect(await readFile(join(options.candidate, 'bundle.json'))).toEqual(before)
        await writeFile(join(options.output, 'packages/amd64/fixture.deb'), 'substitution')
        await expect(verifyBoundDistribution(options.output, options.identity)).rejects.toThrow(
            'changed',
        )
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test('every native report binds exact early bundle bytes, including safe attempts and cancelled gates', async () => {
    const root = await mkdtemp(join(tmpdir(), 'rp-native-binding-'))
    try {
        const options = await fixture(root)
        for (const target of ['debian', 'fedora'])
            for (const architecture of ['amd64', 'arm64']) {
                const path = join(options.reports, `native-${target}-${architecture}.json`),
                    original = await readFile(path, 'utf8')
                for (const change of [
                    { candidateBundleSha256: 'c'.repeat(64) },
                    { candidateBundleSha256: undefined },
                    { passed: false },
                    { runAttempt: 3 },
                    { runId: '999' },
                ]) {
                    await writeFile(path, JSON.stringify({ ...JSON.parse(original), ...change }))
                    await expect(bindDistribution(options)).rejects.toThrow('gate')
                }
                await writeFile(path, JSON.stringify({ ...JSON.parse(original), runAttempt: 1 }))
                await validateDistributionReports(
                    options.reports,
                    options.identity,
                    options.candidateHash,
                )
                await rm(path)
                await expect(bindDistribution(options)).rejects.toThrow()
                await writeFile(path, original)
            }
        await writeFile(join(options.reports, 'native-extra.json'), '{}')
        await expect(bindDistribution(options)).rejects.toThrow('Unexpected')
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test('late binding rejects altered image proof, archive identity and unsealed or extra packages', async () => {
    const root = await mkdtemp(join(tmpdir(), 'rp-rejected-binding-'))
    try {
        const options = await fixture(root)
        for (const evidence of [
            { ...options.evidence, digest: 'sha256:' + 'd'.repeat(64) },
            { ...options.evidence, runId: '999' },
            { ...options.evidence, native: [] },
            {
                ...options.evidence,
                native: options.evidence.native.map((entry) => ({ ...entry, runAttempt: 99 })),
            },
        ])
            await expect(bindDistribution({ ...options, evidence })).rejects.toThrow()
        await expect(
            bindDistribution({ ...options, archiveSha256: 'c'.repeat(64) }),
        ).rejects.toThrow('archive')
        await writeFile(join(options.candidate, 'extra.rpm'), 'unexpected')
        await expect(bindDistribution(options)).rejects.toThrow('Unexpected')
        await rm(join(options.candidate, 'extra.rpm'))
        await writeFile(join(options.candidate, 'packages/amd64/fixture.deb'), 'changed')
        await expect(bindDistribution(options)).rejects.toThrow('substituted')
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test('repackaging the same identity cannot remove candidate markers and downgrade signing', async () => {
    const root = await mkdtemp(join(tmpdir(), 'rp-no-sign-downgrade-'))
    try {
        const options = await fixture(root)
        await bindDistribution(options)
        const sign = async (bundleHash: string) =>
            signDistribution({
                bundle: options.output,
                reports: options.reports,
                identity: options.identity,
                bundleHash,
                source: root,
                publicRoot: join(root, 'public'),
                assets: join(root, 'assets'),
                key: new Uint8Array(),
                fingerprint: 'A'.repeat(40),
                previousFingerprints: [],
                baseUrl: 'https://fixture.invalid',
                publishedAt: '2026-11-08T00:00:00.000Z',
            })
        for (const marker of ['image-candidate.json', 'binding.json', 'candidate-bundle.json']) {
            const path = join(options.output, marker),
                bytes = await readFile(path)
            await rm(path)
            await rm(join(options.output, 'bundle.json'))
            const repackagedHash = await writeDistributionBundle(options.output, options.identity)
            await verifyDistributionBundle(options.output, repackagedHash, options.identity)
            await expect(sign(repackagedHash)).rejects.toThrow()
            await writeFile(path, bytes)
        }
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})
