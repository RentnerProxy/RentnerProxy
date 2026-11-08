// oxlint-disable no-await-in-loop -- Exercise each native architecture against the same candidate bytes.
import { expect, test } from 'bun:test'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { verifyNativeCandidateImage } from '../../../../../.github/scripts/ci/distribution/native-probe.ts'
import { identity, workflow } from '../github/fixtures/oci.ts'

test('native lifecycle identity uses unassessed candidate images and rejects image or revision substitution', async () => {
    const root = await mkdtemp(join(tmpdir(), 'rp-native-candidate-'))
    const images = [
        { architecture: 'amd64', imageId: 'sha256:' + 'b'.repeat(64) },
        { architecture: 'arm64', imageId: 'sha256:' + 'c'.repeat(64) },
    ]
    try {
        await writeFile(
            join(root, 'image-candidate.json'),
            JSON.stringify({
                schemaVersion: 1,
                ...identity,
                ...workflow,
                digest: 'sha256:' + 'd'.repeat(64),
                archiveSha256: 'e'.repeat(64),
                status: 'unassessed',
                images,
            }),
        )
        for (const image of images) {
            await verifyNativeCandidateImage(
                root,
                image.architecture,
                image.imageId,
                identity.revision,
            )
            await verifyNativeCandidateImage(
                root,
                image.architecture,
                image.imageId.slice(7),
                identity.revision,
            )
            await expect(
                verifyNativeCandidateImage(
                    root,
                    image.architecture,
                    'sha256:' + 'f'.repeat(64),
                    identity.revision,
                ),
            ).rejects.toThrow()
            await expect(
                verifyNativeCandidateImage(root, image.architecture, image.imageId, 'f'.repeat(40)),
            ).rejects.toThrow()
        }
        await expect(
            verifyNativeCandidateImage(root, 's390x', images[0]!.imageId, identity.revision),
        ).rejects.toThrow()
        expect(await Bun.file(join(root, 'image-evidence.json')).exists()).toBe(false)
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})
