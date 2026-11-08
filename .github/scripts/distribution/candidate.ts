// oxlint-disable no-await-in-loop -- Authenticate each bounded candidate file and native gate.
import { lstat, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { ociRecord } from '../lib/oci-layout.ts'
import { bundleFiles, distributionFileHash, verifyDistributionBundle } from './bundle.ts'
import type { distributionWorkflowIdentity } from './prepare.ts'

type Identity = ReturnType<typeof distributionWorkflowIdentity>

export function validateDistributionCandidate(
    raw: unknown,
    identity: Identity,
    archiveSha256: string,
) {
    const candidate = ociRecord(raw)
    if (
        !/^[a-f0-9]{64}$/u.test(archiveSha256) ||
        candidate.schemaVersion !== 1 ||
        candidate.status !== 'unassessed' ||
        Object.entries(identity).some(
            ([key, value]) => key !== 'runAttempt' && candidate[key] !== value,
        ) ||
        typeof candidate.runAttempt !== 'number' ||
        !Number.isSafeInteger(candidate.runAttempt) ||
        candidate.runAttempt < 1 ||
        candidate.runAttempt > identity.runAttempt
    )
        throw new Error('Unassessed distribution candidate identity mismatch')
    return {
        schemaVersion: 1,
        ...identity,
        runAttempt: candidate.runAttempt,
        status: 'unassessed',
        archiveSha256,
    }
}

export async function verifyCandidateDistribution(root: string, hash: string, identity: Identity) {
    await verifyDistributionBundle(root, hash, identity)
    const files = await bundleFiles(root)
    if (
        files.some((file) =>
            ['image-evidence.json', 'binding.json', 'candidate-bundle.json'].includes(file),
        )
    )
        throw new Error('Candidate bundle must not claim image assessment')
    const raw = ociRecord(JSON.parse(await readFile(join(root, 'image-candidate.json'), 'utf8')))
    return validateDistributionCandidate(raw, identity, String(raw.archiveSha256))
}

export async function validateDistributionReports(
    reports: string,
    identity: Identity,
    candidateBundleSha256: string,
) {
    if (!/^[a-f0-9]{64}$/u.test(candidateBundleSha256))
        throw new Error('Invalid candidate bundle checksum')
    const expected: string[] = []
    for (const architecture of ['amd64', 'arm64'])
        for (const target of ['debian', 'fedora']) {
            const name = `native-${target}-${architecture}.json`
            expected.push(name)
            const path = join(reports, name),
                info = await lstat(path)
            if (!info.isFile() || info.nlink !== 1 || info.size > 4096)
                throw new Error('Invalid native distribution report')
            const report = ociRecord(JSON.parse(await readFile(path, 'utf8')))
            if (
                report.schemaVersion !== 1 ||
                report.architecture !== architecture ||
                report.target !== target ||
                report.passed !== true ||
                report.candidateBundleSha256 !== candidateBundleSha256 ||
                Object.entries(identity).some(
                    ([key, value]) => key !== 'runAttempt' && report[key] !== value,
                ) ||
                typeof report.runAttempt !== 'number' ||
                !Number.isSafeInteger(report.runAttempt) ||
                report.runAttempt < 1 ||
                report.runAttempt > identity.runAttempt
            )
                throw new Error('Missing or mismatched native distribution gate')
        }
    if (JSON.stringify(await bundleFiles(reports)) !== JSON.stringify(expected.toSorted()))
        throw new Error('Unexpected native distribution reports')
}

export async function verifyBoundDistribution(root: string, identity: Identity) {
    await distributionFileHash(join(root, 'binding.json'))
    const binding = ociRecord(JSON.parse(await readFile(join(root, 'binding.json'), 'utf8')))
    if (
        binding.schemaVersion !== 1 ||
        binding.status !== 'assessed' ||
        typeof binding.candidateBundleSha256 !== 'string' ||
        !/^[a-f0-9]{64}$/u.test(binding.candidateBundleSha256) ||
        (await distributionFileHash(join(root, 'candidate-bundle.json'))) !==
            binding.candidateBundleSha256 ||
        (await distributionFileHash(join(root, 'image-evidence.json'))) !==
            binding.imageEvidenceSha256
    )
        throw new Error('Invalid assessed distribution binding')
    const manifest = ociRecord(
        JSON.parse(await readFile(join(root, 'candidate-bundle.json'), 'utf8')),
    )
    if (
        manifest.schemaVersion !== 1 ||
        !Array.isArray(manifest.files) ||
        Object.entries(identity).some(
            ([key, value]) => key !== 'runAttempt' && manifest[key] !== value,
        ) ||
        typeof manifest.runAttempt !== 'number' ||
        !Number.isSafeInteger(manifest.runAttempt) ||
        manifest.runAttempt < 1 ||
        manifest.runAttempt > identity.runAttempt
    )
        throw new Error('Distribution candidate proof identity mismatch')
    const files = (await bundleFiles(root)).filter(
        (file) =>
            ![
                'bundle.json',
                'candidate-bundle.json',
                'image-evidence.json',
                'binding.json',
            ].includes(file),
    )
    if (files.length !== manifest.files.length)
        throw new Error('Unexpected bound distribution files')
    for (const [index, path] of files.entries()) {
        const entry = ociRecord(manifest.files[index])
        if (entry.file !== path || entry.sha256 !== (await distributionFileHash(join(root, path))))
            throw new Error('Bound package bytes changed')
    }
    const raw = ociRecord(JSON.parse(await readFile(join(root, 'image-candidate.json'), 'utf8')))
    validateDistributionCandidate(raw, identity, String(raw.archiveSha256))
    return binding.candidateBundleSha256
}
