// oxlint-disable no-await-in-loop -- Copy only files authenticated by the immutable candidate manifest.
import { appendFile, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path'
import { tmpdir } from 'node:os'
import { hashOciFile, ociText, readOciJson } from '../lib/oci-layout.ts'
import { verifyMultiarchImage } from '../release/multiarch-image.ts'
import { bundleFiles, distributionFileHash, writeDistributionBundle } from './bundle.ts'
import {
    verifyBoundDistribution,
    verifyCandidateDistribution,
    validateDistributionReports,
} from './candidate.ts'
import { distributionWorkflowIdentity } from './prepare.ts'
import { distributionCommand } from './process.ts'

export async function bindDistribution(options: {
    candidate: string
    candidateHash: string
    output: string
    reports: string
    layout: string
    evidence: unknown
    archiveSha256: string
    identity: ReturnType<typeof distributionWorkflowIdentity>
}) {
    for (const [parent, child] of [
        [options.candidate, options.output],
        [options.output, options.candidate],
    ] as const) {
        const path = relative(resolve(parent), resolve(child))
        if (!path || (!isAbsolute(path) && path !== '..' && !path.startsWith('..' + sep)))
            throw new Error('Overlapping distribution binding directories')
    }
    const candidate = await verifyCandidateDistribution(
        options.candidate,
        options.candidateHash,
        options.identity,
    )
    if (candidate.archiveSha256 !== options.archiveSha256)
        throw new Error('Candidate image archive checksum mismatch')
    const evidence = await verifyMultiarchImage(
        options.layout,
        options.evidence,
        options.identity,
        options.identity.runId,
        options.identity.runAttempt,
    )
    if (evidence.digest !== options.identity.digest)
        throw new Error('Distribution image root mismatch')
    await validateDistributionReports(options.reports, options.identity, options.candidateHash)
    await mkdir(options.output)
    for (const path of await bundleFiles(options.candidate)) {
        const destination = join(
            options.output,
            path === 'bundle.json' ? 'candidate-bundle.json' : path,
        )
        await mkdir(dirname(destination), { recursive: true })
        await writeFile(destination, await readFile(join(options.candidate, path)), {
            flag: 'wx',
            mode: 0o644,
        })
    }
    await writeFile(join(options.output, 'image-evidence.json'), JSON.stringify(evidence) + '\n', {
        flag: 'wx',
    })
    await writeFile(
        join(options.output, 'binding.json'),
        JSON.stringify({
            schemaVersion: 1,
            status: 'assessed',
            candidateBundleSha256: options.candidateHash,
            imageEvidenceSha256: await distributionFileHash(
                join(options.output, 'image-evidence.json'),
            ),
        }) + '\n',
        { flag: 'wx' },
    )
    await verifyBoundDistribution(options.output, options.identity)
    return writeDistributionBundle(options.output, options.identity)
}

export async function bindDistributionCommand(env: NodeJS.ProcessEnv = process.env) {
    const candidate = resolve(ociText(env.CANDIDATE_DIRECTORY))
    if (
        (await hashOciFile(join(candidate, 'candidate-image.tar'))).slice(7) !==
            env.ARCHIVE_SHA256 ||
        (await hashOciFile(join(candidate, 'evidence.json'))).slice(7) !== env.EVIDENCE_SHA256
    )
        throw new Error('Assessed image artifact checksum mismatch')
    const temporary = await mkdtemp(join(tmpdir(), 'rentnerproxy-distribution-bind-'))
    try {
        const layout = join(temporary, 'layout')
        await distributionCommand(
            [
                'skopeo',
                'copy',
                '--all',
                '--preserve-digests',
                'oci-archive:' + join(candidate, 'candidate-image.tar'),
                'oci:' + layout + ':candidate',
            ],
            { timeoutMs: 360_000 },
        )
        const hash = await bindDistribution({
            candidate: resolve(ociText(env.CANDIDATE_BUNDLE_DIRECTORY)),
            candidateHash: ociText(env.CANDIDATE_BUNDLE_SHA256),
            output: resolve(ociText(env.DISTRIBUTION_DIRECTORY)),
            reports: resolve(ociText(env.NATIVE_REPORT_DIRECTORY)),
            layout,
            evidence: await readOciJson(join(candidate, 'evidence.json')),
            archiveSha256: ociText(env.ARCHIVE_SHA256),
            identity: distributionWorkflowIdentity(env),
        })
        if (env.GITHUB_OUTPUT) await appendFile(env.GITHUB_OUTPUT, 'bundle_sha256=' + hash + '\n')
    } finally {
        await rm(temporary, { recursive: true, force: true })
    }
}

if (import.meta.main) await bindDistributionCommand()
