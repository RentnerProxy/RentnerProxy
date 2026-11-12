import { createHash, randomUUID } from 'node:crypto'
import { lstat, mkdir, readFile, readdir, writeFile } from 'node:fs/promises'
import { isAbsolute, join, relative, resolve } from 'node:path'

const repository = resolve(import.meta.dir, '../..'),
    crate = join(repository, 'scripts/runtime-scale/monitoring-b2')

/** Build inputs and exported artifact hashes are separate from the unchanged baseline image revision. */
export async function monitoringB2RustSourceHash() {
    const paths = ['Cargo.toml', 'Cargo.lock', '../monitoring-b2-build.ts']
    /** Hash only ordinary Rust source files; a link or unrelated source entry fails the build identity. */
    async function collect(directory: string) {
        await Promise.all(
            (await readdir(join(crate, directory), { withFileTypes: true })).map(async (entry) => {
                const path = directory + '/' + entry.name
                if (entry.isDirectory()) await collect(path)
                else if (entry.isFile() && entry.name.endsWith('.rs')) paths.push(path)
                else throw new Error('B2 Rust build source must contain ordinary Rust files')
            }),
        )
    }
    await collect('src')
    const hash = createHash('sha256')
    const files = await Promise.all(
        paths
            .toSorted()
            .map(async (path) => ({ path, contents: await readFile(join(crate, path)) })),
    )
    for (const { path, contents } of files) {
        hash.update(path)
        hash.update(contents)
    }
    return hash.digest('hex')
}

/** Bound Docker output and host-client time; callers separately own cleanup of any named container. */
async function boundedDocker(args: string[], maxOutput = 8192, timeoutMs = 15000) {
    const child = Bun.spawn(['docker', ...args], { stdout: 'pipe', stderr: 'pipe' })
    const collect = async (stream: ReadableStream<Uint8Array>, maximum: number) => {
        const chunks: Uint8Array[] = []
        let bytes = 0
        for await (const chunk of stream) {
            bytes += chunk.byteLength
            if (bytes > maximum) {
                child.kill('SIGKILL')
                throw new Error('B2 offline builder output bound exceeded')
            }
            chunks.push(chunk)
        }
        return Buffer.concat(chunks)
    }
    const timer = setTimeout(() => child.kill('SIGKILL'), timeoutMs)
    try {
        const results = await Promise.allSettled([
            child.exited,
            collect(child.stdout, maxOutput),
            collect(child.stderr, 1024 * 1024),
        ])
        if (results.some((result) => result.status !== 'fulfilled'))
            throw new Error('B2 offline builder output failed')
        const exit = results[0] as PromiseFulfilledResult<number>,
            stdout = results[1] as PromiseFulfilledResult<Buffer>
        if (exit.value !== 0)
            throw new Error(
                'B2 offline build failed; required tools and dependencies must be cached',
            )
        return stdout.value
    } finally {
        clearTimeout(timer)
    }
}

/** Require an explicit public dependency cache; callers cannot enable fetching or container networking. */
export function parseMonitoringB2BuildOptions(args: string[]) {
    const values = new Map<string, string>()
    for (let index = 0; index < args.length; index += 2) {
        const name = args[index]!,
            value = args[index + 1]
        if (
            !['--output', '--registry-cache', '--builder-image'].includes(name) ||
            values.has(name) ||
            !value ||
            value.startsWith('--')
        )
            throw new Error('Invalid B2 offline build option')
        values.set(name, value)
    }
    const registryCache = values.get('--registry-cache')
    if (!registryCache) throw new Error('B2 requires an explicit local Cargo registry cache')
    return {
        output: values.get('--output') ?? join(repository, 'tmp/b2-linux'),
        registryCache,
        builderImage: values.get('--builder-image') ?? 'rentnerproxy-backup-progress:rust-checks',
    }
}

/** Compile only in a bounded, offline container with explicit read-only local cache inputs. */
export async function buildMonitoringB2Rust(
    output: string,
    registryCache: string,
    builderReference = 'rentnerproxy-backup-progress:rust-checks',
) {
    const destination = resolve(output),
        owned = relative(repository, destination).replaceAll('\\', '/')
    if (isAbsolute(owned) || !/^tmp\/b2-[a-zA-Z0-9_-]+$/u.test(owned))
        throw new Error('B2 build output must remain inside an owned repository tmp/b2- directory')
    if (!/^[a-zA-Z0-9][a-zA-Z0-9._/:@-]{0,255}$/u.test(builderReference))
        throw new Error('B2 local builder reference invalid')
    const registry = resolve(registryCache)
    await Promise.all(
        [registry, join(registry, 'src'), join(registry, 'cache'), join(registry, 'index')].map(
            async (path) => {
                const metadata = await lstat(path)
                if (!metadata.isDirectory() || metadata.isSymbolicLink())
                    throw new Error(
                        'B2 requires ordinary read-only Cargo registry cache directories',
                    )
            },
        ),
    )
    const builder = JSON.parse(
        (
            await boundedDocker([
                'image',
                'inspect',
                '--format',
                '{"id":{{json .Id}},"os":{{json .Os}},"architecture":{{json .Architecture}}}',
                builderReference,
            ])
        ).toString(),
    ) as { id?: unknown; os?: unknown; architecture?: unknown }
    if (
        typeof builder.id !== 'string' ||
        !/^sha256:[a-f0-9]{64}$/u.test(builder.id) ||
        builder.os !== 'linux' ||
        builder.architecture !== 'amd64'
    )
        throw new Error('B2 cached builder must identify an immutable Linux amd64 image')
    const toolchain = '/usr/local/rustup/toolchains/1.99.1-x86_64-unknown-linux-gnu/bin'
    const compilerDetails = (
        await boundedDocker([
            'run',
            '--rm',
            '--pull=never',
            '--network=none',
            '--read-only',
            '--cpus=1',
            '--memory=128m',
            '--pids-limit=32',
            '--cap-drop=ALL',
            '--security-opt=no-new-privileges',
            '--entrypoint',
            toolchain + '/rustc',
            builder.id,
            '-vV',
        ])
    )
        .toString()
        .trim()
    const compiler = compilerDetails.split('\n')[0]!
    if (
        !/^rustc 1\.99\.1 \([a-f0-9]+ 2026-10-13\)$/u.test(compiler) ||
        !compilerDetails.includes('\nhost: x86_64-unknown-linux-gnu\n')
    )
        throw new Error('B2 cached compiler differs from the pinned target/toolchain')
    const sourceHash = await monitoringB2RustSourceHash()
    const lockHash = createHash('sha256')
        .update(await readFile(join(crate, 'Cargo.lock')))
        .digest('hex')
    const manifestHash = createHash('sha256')
        .update(await readFile(join(crate, 'Cargo.toml')))
        .digest('hex')
    const container = 'rentnerproxy-b2-build-' + randomUUID().replaceAll('-', '').slice(0, 12)
    let binary: Buffer
    try {
        binary = await boundedDocker(
            [
                'run',
                '--rm',
                '--pull=never',
                '--name',
                container,
                '--network=none',
                '--read-only',
                '--cpus=2',
                '--memory=2g',
                '--pids-limit=128',
                '--user=65534:65534',
                '--workdir=/scratch',
                '--ulimit=nofile=512:512',
                '--ulimit=fsize=268435456:268435456',
                '--cap-drop=ALL',
                '--security-opt=no-new-privileges',
                '--tmpfs=/scratch:rw,exec,nosuid,nodev,size=2g,mode=1777',
                '--mount',
                'type=bind,source=' + crate + ',target=/source,readonly',
                '--mount',
                'type=bind,source=' + registry + ',target=/cached-registry,readonly',
                '--entrypoint=/usr/bin/env',
                builder.id,
                '-i',
                'PATH=' + toolchain + ':/usr/local/bin:/usr/bin:/bin',
                'HOME=/scratch/home',
                'TMPDIR=/scratch/tmp',
                'CARGO_HOME=/scratch/cargo',
                'CARGO_TARGET_DIR=/scratch/target',
                'CARGO_NET_OFFLINE=true',
                'SOURCE_DATE_EPOCH=0',
                'RUSTFLAGS=--remap-path-prefix=/source=/experiment --remap-path-prefix=/cached-registry=/registry --remap-path-prefix=/scratch=/build',
                'timeout',
                '-k',
                '3s',
                '240s',
                '/bin/sh',
                '-c',
                'set -eu; mkdir -p /scratch/home /scratch/tmp /scratch/cargo /scratch/export; ln -s /cached-registry /scratch/cargo/registry; cargo build --frozen --release --manifest-path /source/Cargo.toml >&2; binary=/scratch/target/release/rentnerproxy-monitoring-b2-experiment; test -f "$binary"; test ! -L "$binary"; cp -- "$binary" /scratch/export/monitoring-b2; test "$(stat -c %h /scratch/export/monitoring-b2)" = 1; cat /scratch/export/monitoring-b2',
            ],
            64 * 1024 * 1024,
            250000,
        )
    } finally {
        await boundedDocker(['rm', '--force', container]).catch(() => {})
    }
    if (
        sourceHash !== (await monitoringB2RustSourceHash()) ||
        binary.length < 20 ||
        binary.subarray(0, 4).toString('hex') !== '7f454c46' ||
        binary[4] !== 2 ||
        binary[5] !== 1 ||
        binary.readUInt16LE(18) !== 62
    )
        throw new Error('B2 source changed or offline artifact target is invalid')
    const manifest = {
        rustSourceSha256: sourceHash,
        binarySha256: createHash('sha256').update(binary).digest('hex'),
        target: 'x86_64-unknown-linux-gnu',
        compiler,
        compilerVerboseSha256: createHash('sha256').update(compilerDetails).digest('hex'),
        builderImage: builder.id,
        builderReference,
        cargoLockSha256: lockHash,
        cargoManifestSha256: manifestHash,
        profile: 'release',
        offline: true,
        network: 'none',
        registry: 'read-only-local-cargo-cache',
    }
    const temporaryRoot = join(repository, 'tmp')
    await mkdir(temporaryRoot, { recursive: true })
    const metadata = await lstat(temporaryRoot)
    if (!metadata.isDirectory() || metadata.isSymbolicLink())
        throw new Error('B2 build output parent is not an ordinary owned directory')
    await mkdir(destination)
    await writeFile(join(destination, 'monitoring-b2'), binary, { flag: 'wx' })
    await writeFile(join(destination, 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n', {
        flag: 'wx',
    })
    return manifest
}

if (import.meta.main) {
    const options = parseMonitoringB2BuildOptions(process.argv.slice(2))
    console.log(
        JSON.stringify(
            await buildMonitoringB2Rust(
                options.output,
                options.registryCache,
                options.builderImage,
            ),
        ),
    )
}
