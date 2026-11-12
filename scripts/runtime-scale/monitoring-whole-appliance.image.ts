// oxlint-disable no-await-in-loop -- Baseline verification, bundling and image publication are ordered provenance boundaries.
import { createHash } from 'node:crypto'
import { copyFile, mkdir, readFile, symlink, writeFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { z } from 'zod'
import type { ReliabilityContext } from '../runtime-reliability/Types/harness.types.ts'
import type { MonitoringApplianceOptions } from './Types/monitoring-whole-appliance.types.ts'

const root = resolve(import.meta.dir, '../..')
const application = '/opt/rentnerproxy/web'
const digest = z.string().regex(/^sha256:[a-f0-9]{64}$/u)
const hash = z.string().regex(/^[a-f0-9]{64}$/u)
const sealSchema = z.strictObject({
    prototype: z.literal(true),
    baseRevision: z.string().regex(/^[a-f0-9]{40}$/u),
    baseImageIdentity: digest,
    experimentSha256: hash,
    nativeBinarySha256: hash,
})

/** Describe the private overlay and preserve original compiled assets/native hashes with a scoped fixture UID. */
export function monitoringPrototypeDockerfile(
    seal: z.infer<typeof sealSchema>,
    localBaseTag: string,
    originalServeSha256: string,
) {
    sealSchema.parse(seal)
    hash.parse(originalServeSha256)
    if (!/^rentnerproxy-b2-base:[a-f0-9]{12}$/u.test(localBaseTag))
        throw new Error('Invalid owned B2 base tag')
    return `FROM ${localBaseTag}
USER root
WORKDIR ${application}
RUN test -f web/dist/server/server.js && test -d web/dist/client && printf '%s  %s\\n' '${originalServeSha256}' 'docker/web/serve.ts' | sha256sum -c - && cp docker/web/serve.ts docker/web/serve.production.ts && sha256sum /usr/local/bin/rentnerproxy-controller /usr/bin/caddy /usr/local/bin/bun /usr/local/bin/rentnerproxy-production /usr/bin/tini > monitoring-b2-original-native.sha256 && find web/dist -type f -exec sha256sum '{}' + | LC_ALL=C sort > monitoring-b2-original-assets.sha256 && test -s monitoring-b2-original-assets.sha256 && find web/dist -type l -printf '%p %l\\n' | LC_ALL=C sort > monitoring-b2-original-symlinks.txt
COPY monitoring-b2-appliance.js monitoring-b2-web.js monitoring-b2-web-replay.js monitoring-b2-capabilities.js monitoring-b2 monitoring-b2-build-seal.json ${application}/
COPY monitoring-b2-prototype-entrypoint /usr/local/bin/rentnerproxy-b2-prototype
COPY serve.ts ${application}/docker/web/serve.ts
RUN groupadd --gid 20004 monitoring-b2 && useradd --uid 20004 --gid monitoring-b2 --no-create-home --shell /usr/sbin/nologin monitoring-b2 && usermod --append --groups monitoring-b2 rentnerproxy-web && chmod 755 monitoring-b2 /usr/local/bin/rentnerproxy-b2-prototype && chmod 644 monitoring-b2-*.js monitoring-b2-build-seal.json docker/web/serve.ts && mkdir -p /tmp/monitoring-b2/state && chown 0:0 /tmp/monitoring-b2 && chmod 755 /tmp/monitoring-b2 && chown 0:20004 /tmp/monitoring-b2/state && chmod 2770 /tmp/monitoring-b2/state && find web/dist -type f -exec sha256sum '{}' + | LC_ALL=C sort > /tmp/observed-assets.sha256 && cmp monitoring-b2-original-assets.sha256 /tmp/observed-assets.sha256 && find web/dist -type l -printf '%p %l\\n' | LC_ALL=C sort > /tmp/observed-symlinks.txt && cmp monitoring-b2-original-symlinks.txt /tmp/observed-symlinks.txt && sha256sum -c monitoring-b2-original-native.sha256 && sha256sum docker/web/serve.production.ts monitoring-b2-original-assets.sha256 monitoring-b2-original-symlinks.txt monitoring-b2-original-native.sha256 > monitoring-b2-image-proof.sha256
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/rentnerproxy-b2-prototype"]
LABEL org.opencontainers.image.revision="${seal.baseRevision}" org.opencontainers.image.title="Private monitoring B2 experiment overlay" org.opencontainers.image.description="Test-only wrapper and native prototype over byte-identical compiled baseline assets; not an unmodified production image" io.rentnerproxy.experiment="monitoring-b2" io.rentnerproxy.experiment.base-image="${seal.baseImageIdentity}" io.rentnerproxy.experiment.base-revision="${seal.baseRevision}" io.rentnerproxy.experiment.source="${seal.experimentSha256}" io.rentnerproxy.experiment.native="${seal.nativeBinarySha256}"
`
}

/** Seal the exact baseline image, add only owned experiment inputs, then verify layer, asset and native proofs. */
export async function buildMonitoringPrototypeImage(
    context: ReliabilityContext,
    options: MonitoringApplianceOptions,
    overlay: { paths: readonly string[]; sha256: string },
    nativeBinarySha256: string,
    image: string,
) {
    if (!/^rentnerproxy-b2-prototype:[a-f0-9]{12}$/u.test(image))
        throw new Error('Invalid owned prototype image')
    const inspect = async (name: string, format: string) =>
        context.docker(['image', 'inspect', '--format', format, name])
    const baseImageIdentity = digest.parse(await inspect(options.image, '{{.Id}}'))
    if (baseImageIdentity !== options.baselineImageIdentity)
        throw new Error('B2 immutable base image identity mismatch')
    if (
        (await inspect(
            options.image,
            '{{index .Config.Labels "org.opencontainers.image.revision"}}',
        )) !== options.baselineSha
    )
        throw new Error('B2 immutable base revision mismatch')
    const baseLayers = z
        .array(digest)
        .parse(JSON.parse(await inspect(baseImageIdentity, '{{json .RootFS.Layers}}')))
    const source = join(context.temp, 'prototype-source'),
        build = join(context.temp, 'prototype-build')
    await mkdir(source, { recursive: true })
    await mkdir(build, { recursive: true })
    const archive = join(context.temp, 'prototype-source.tar')
    await context.command([
        'git',
        '-C',
        options.baselineRoot,
        'archive',
        '--format=tar',
        '--output=' + archive,
        options.baselineSha,
    ])
    await context.command(['tar', '-xf', archive, '-C', source])
    await symlink(
        join(options.baselineRoot, 'node_modules'),
        join(source, 'node_modules'),
        process.platform === 'win32' ? 'junction' : 'dir',
    )
    const stagedHash = createHash('sha256')
    for (const path of overlay.paths) {
        const target = join(source, path)
        await mkdir(dirname(target), { recursive: true })
        await copyFile(join(root, path), target)
        stagedHash.update(path)
        stagedHash.update(await readFile(target))
    }
    if (stagedHash.digest('hex') !== overlay.sha256)
        throw new Error('B2 experiment source changed while staging image')
    const bundles: Record<string, string> = {}
    for (const entry of [
        'monitoring-b2-appliance',
        'monitoring-b2-web',
        'monitoring-b2-web-replay',
        'monitoring-b2-capabilities',
    ]) {
        const output = join(build, entry + '.js')
        await context.command([
            process.execPath,
            '--no-env-file',
            'build',
            join(source, 'scripts/runtime-scale/' + entry + '.ts'),
            '--tsconfig-override=' + join(source, 'tsconfig.json'),
            '--target=bun',
            '--packages=external',
            '--outfile=' + output,
        ])
        bundles[entry] = createHash('sha256')
            .update(await readFile(output))
            .digest('hex')
    }
    await copyFile(options.nativeBinary, join(build, 'monitoring-b2'))
    if (
        createHash('sha256')
            .update(await readFile(join(build, 'monitoring-b2')))
            .digest('hex') !== nativeBinarySha256
    )
        throw new Error('B2 native binary changed while staging image')
    const seal = sealSchema.parse({
        prototype: true,
        baseRevision: options.baselineSha,
        baseImageIdentity,
        experimentSha256: overlay.sha256,
        nativeBinarySha256,
    })
    await writeFile(join(build, 'monitoring-b2-build-seal.json'), JSON.stringify(seal) + '\n')
    const bootstrap =
        '#!/bin/sh\nset -eu\n/usr/local/bin/bun --no-env-file /opt/rentnerproxy/web/monitoring-b2-capabilities.js\nexec /usr/local/bin/rentnerproxy-production "$@"\n'
    await writeFile(join(build, 'monitoring-b2-prototype-entrypoint'), bootstrap)
    await writeFile(
        join(build, 'serve.ts'),
        `import { runMonitoringB2Web } from '${application}/monitoring-b2-web.js'\nawait runMonitoringB2Web()\n`,
    )
    const originalServeSha256 = createHash('sha256')
        .update(await readFile(join(source, 'docker/web/serve.ts')))
        .digest('hex')
    const baseTag = 'rentnerproxy-b2-base:' + context.runId
    await context.docker(['tag', baseImageIdentity, baseTag])
    try {
        await writeFile(
            join(build, 'Dockerfile'),
            monitoringPrototypeDockerfile(seal, baseTag, originalServeSha256),
        )
        await context.docker(['build', '--pull=false', '--network=none', '--tag', image, build], {
            timeoutMs: 120000,
        })
    } finally {
        await context.docker(['image', 'rm', baseTag])
    }
    const overlayImageIdentity = digest.parse(await inspect(image, '{{.Id}}'))
    const layers = z
        .array(digest)
        .parse(JSON.parse(await inspect(image, '{{json .RootFS.Layers}}')))
    if (
        layers.length <= baseLayers.length ||
        baseLayers.some((layer, index) => layers[index] !== layer)
    )
        throw new Error('B2 image does not preserve the immutable baseline layers')
    for (const [label, expected] of Object.entries({
        'io.rentnerproxy.experiment': 'monitoring-b2',
        'io.rentnerproxy.experiment.base-image': baseImageIdentity,
        'io.rentnerproxy.experiment.base-revision': options.baselineSha,
        'io.rentnerproxy.experiment.source': overlay.sha256,
        'io.rentnerproxy.experiment.native': nativeBinarySha256,
    }))
        if ((await inspect(image, '{{index .Config.Labels "' + label + '"}}')) !== expected)
            throw new Error('B2 prototype image label mismatch')
    const proofContainer = 'rentnerproxy-b2-image-proof-' + context.runId
    let proof: string
    let nativeProof: string
    await context.docker([
        'create',
        '--name',
        proofContainer,
        '--entrypoint',
        '/bin/true',
        overlayImageIdentity,
    ])
    try {
        const proofFile = join(context.temp, 'prototype-image-proof.sha256')
        await context.docker([
            'cp',
            proofContainer + ':' + application + '/monitoring-b2-image-proof.sha256',
            proofFile,
        ])
        proof = await readFile(proofFile, 'utf8')
        if (!proof.startsWith(originalServeSha256 + '  docker/web/serve.production.ts\n'))
            throw new Error('B2 preserved entrypoint proof mismatch')
        const nativeProofFile = join(context.temp, 'prototype-native-proof.sha256')
        await context.docker([
            'cp',
            proofContainer + ':' + application + '/monitoring-b2-original-native.sha256',
            nativeProofFile,
        ])
        nativeProof = await readFile(nativeProofFile, 'utf8')
        if (
            !/^[a-f0-9]{64}  \/usr\/local\/bin\/rentnerproxy-controller\n[a-f0-9]{64}  \/usr\/bin\/caddy\n[a-f0-9]{64}  \/usr\/local\/bin\/bun\n[a-f0-9]{64}  \/usr\/local\/bin\/rentnerproxy-production\n[a-f0-9]{64}  \/usr\/bin\/tini\n$/u.test(
                nativeProof,
            )
        )
            throw new Error('B2 preserved native runtime proof mismatch')
    } finally {
        await context.docker(['rm', '--volumes', proofContainer])
    }
    return {
        image,
        overlayImageIdentity,
        baseImageIdentity,
        baseRevision: options.baselineSha,
        experimentSha256: overlay.sha256,
        nativeBinarySha256,
        originalServeSha256,
        bundles,
        bootstrapSha256: createHash('sha256').update(bootstrap).digest('hex'),
        originalAssetsProof: proof,
        originalNativeProof: nativeProof,
        originalAssetsVerified: true,
        productionImageUnmodified: false,
        fixtureCompiler: Bun.version,
    }
}
