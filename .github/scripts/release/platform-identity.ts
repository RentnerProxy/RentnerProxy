import { appendFile } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { hashOciFile, inspectOciLayout, ociDigest, ociText } from '../lib/oci-layout.ts'
import { candidateCommand } from './candidate-runtime.ts'

if (import.meta.main) {
    try {
        const env = process.env
        const architecture = env.IMAGE_ARCHITECTURE
        if (architecture !== 'amd64' && architecture !== 'arm64')
            throw new Error('Unsupported candidate architecture')
        const directory = resolve(ociText(env.CANDIDATE_DIRECTORY))
        const archive = join(directory, 'candidate-image.tar')
        const archiveSha256 = (await hashOciFile(archive)).slice(7)
        if (env.ARCHIVE_SHA256 !== undefined && archiveSha256 !== env.ARCHIVE_SHA256)
            throw new Error('Producer archive checksum mismatch')
        const layout = join(directory, 'layout')
        await candidateCommand(
            [
                'skopeo',
                'copy',
                '--all',
                '--preserve-digests',
                'oci-archive:' + archive,
                'oci:' + layout + ':candidate',
            ],
            directory,
            env,
        )
        const image = await inspectOciLayout(
            layout,
            { revision: ociText(env.REVISION), version: ociText(env.VERSION) },
            [architecture],
        )
        if (image.digest !== ociDigest(env.EXPECTED_DIGEST))
            throw new Error('Producer OCI identity mismatch')
        if (env.GITHUB_OUTPUT)
            await appendFile(
                env.GITHUB_OUTPUT,
                `digest=${image.digest}\narchive_sha256=${archiveSha256}\n`,
            )
    } catch {
        console.error('Immutable platform identity validation failed.')
        process.exitCode = 1
    }
}
