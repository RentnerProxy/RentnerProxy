import { expect, test } from 'bun:test'
import * as fs from 'node:fs/promises'
import type { ResourceProcFilesystem } from '../../../../../../scripts/runtime-reliability/Types/resource-probe.types.ts'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
    readRuntimeResourceMetrics,
    readMemoryObservation,
    memoryResourceEvidence,
    runtimeResourceProbeSource,
} from '../../../../../../scripts/runtime-reliability/resource-probe.ts'

const stat = (pid: number, ticks = 123) =>
    `${pid} (serving (owner)) S ${Array(18).fill('0').join(' ')} ${ticks} 0`
const status = (uid: number, rss = '24 kB') =>
    `Uid:\t${uid}\t${uid}\t${uid}\t${uid}\nVmRSS:\t${rss}\n`

/** Model serving owners separately from same-UID one-shot fixtures using only readable proc metadata. */
async function fixture(run: (root: string, files: ResourceProcFilesystem) => Promise<void>) {
    const root = await fs.mkdtemp(join(tmpdir(), 'rentnerproxy-rss-'))
    const owners = [
        {
            pid: 10,
            uid: 10002,
            command: 'bun\0/opt/rentnerproxy/web/docker/web/serve.ts\0',
            comm: 'bun',
        },
        {
            pid: 20,
            uid: 10001,
            command: '/usr/local/bin/rentnerproxy-controller\0',
            comm: 'rentnerproxy-co',
        },
        {
            pid: 30,
            uid: 10001,
            command: 'caddy\0run\0--config\0/private/config\0',
            comm: 'caddy',
        },
    ]
    try {
        await Promise.all(
            owners.map(async (owner) => {
                const directory = join(root, String(owner.pid))
                await fs.mkdir(join(directory, 'fd'), { recursive: true })
                await Promise.all([
                    fs.writeFile(join(directory, 'status'), status(owner.uid)),
                    fs.writeFile(join(directory, 'stat'), stat(owner.pid)),
                    fs.writeFile(join(directory, 'cmdline'), owner.command),
                    fs.writeFile(join(directory, 'comm'), owner.comm + '\n'),
                    fs.writeFile(join(directory, 'fd', '0'), ''),
                ])
            }),
        )
        const files: ResourceProcFilesystem = { ...fs }
        await run(root, files)
    } finally {
        await fs.rm(root, { recursive: true, force: true })
    }
}

test('VmRSS is converted from kB independently for exact serving owners and serializes only numeric evidence', async () => {
    await fixture(async (root, files) => {
        expect(await readRuntimeResourceMetrics(root, files)).toEqual({
            webRssBytes: 24576,
            controllerRssBytes: 24576,
            caddyRssBytes: 24576,
            webFds: 1,
            controllerFds: 1,
            caddyFds: 1,
        })
        expect(JSON.stringify(await readRuntimeResourceMetrics(root, files))).not.toContain(
            'private',
        )
        expect(runtimeResourceProbeSource()).toContain('readRuntimeProcesses')
    })
})

test('fixtures, wrong credentials, malformed RSS, and ambiguous serving identities remain unknown', async () => {
    await Promise.all(
        [
            'fixture',
            'mixed-uid',
            'rss-unit',
            'missing-rss',
            'zero-rss',
            'overflow',
            'duplicate',
            'executable',
            'zombie',
        ].map(async (mutation) => {
            await fixture(async (root, files) => {
                const directory = join(root, '10')
                if (mutation === 'fixture')
                    await fs.writeFile(
                        join(directory, 'cmdline'),
                        'bun\0/opt/rentnerproxy/web/reliability-fixture.js\0',
                    )
                if (mutation === 'mixed-uid')
                    await fs.writeFile(
                        join(directory, 'status'),
                        'Uid:\t10002\t0\t10002\t10002\nVmRSS:\t24 kB\n',
                    )
                if (mutation === 'rss-unit')
                    await fs.writeFile(join(directory, 'status'), status(10002, '24 MB'))
                if (mutation === 'missing-rss')
                    await fs.writeFile(
                        join(directory, 'status'),
                        'Uid:\t10002\t10002\t10002\t10002\n',
                    )
                if (mutation === 'zero-rss')
                    await fs.writeFile(join(directory, 'status'), status(10002, '0 kB'))
                if (mutation === 'overflow')
                    await fs.writeFile(
                        join(directory, 'status'),
                        status(10002, '9007199254740992 kB'),
                    )
                if (mutation === 'duplicate')
                    await fs.cp(directory, join(root, '11'), { recursive: true })
                if (mutation === 'executable')
                    await fs.writeFile(join(directory, 'comm'), 'unrelated\n')
                if (mutation === 'zombie')
                    await fs.writeFile(join(directory, 'stat'), stat(10).replace(') S ', ') Z '))
                const result = await readRuntimeResourceMetrics(root, files)
                expect(result.webRssBytes).toBeNull()
                expect(result.webFds).toBeNull()
            })
        }),
    )
})

test('PID reuse during descriptor collection never attributes RSS to the replacement', async () => {
    await fixture(async (root, files) => {
        const readdir = files.readdir
        files.readdir = (async (...args: Parameters<ResourceProcFilesystem['readdir']>) => {
            if (String(args[0]).endsWith('/10/fd'))
                await fs.writeFile(join(root, '10', 'stat'), stat(10, 999))
            return readdir(...args)
        }) as ResourceProcFilesystem['readdir']
        expect((await readRuntimeResourceMetrics(root, files)).webRssBytes).toBeNull()
    })
})

test('memory evidence strips private extras and preserves missing or invalid RSS without summing it', () => {
    const input = {
        memoryBytes: 100,
        cgroupMemoryCurrentBytes: 200,
        webRssBytes: 300,
        controllerRssBytes: 400,
        caddyRssBytes: 500,
        command: 'private secret',
    }
    expect(readMemoryObservation(input)).not.toHaveProperty('command')
    expect(memoryResourceEvidence([input])).toMatchObject({
        complete: true,
        maxima: { cgroupMemoryCurrentBytes: 200, webRssBytes: 300 },
    })
    expect(memoryResourceEvidence([{ memoryBytes: 100 }])).toMatchObject({
        complete: false,
        maxima: { webRssBytes: null },
    })
    for (const webRssBytes of [0, -1, NaN, Infinity])
        expect(readMemoryObservation({ ...input, webRssBytes })).toBeNull()
})

test('serialized resource probe executes independently with no imported host closure', async () => {
    await fixture(async (root) => {
        const child = Bun.spawn(
            [
                process.execPath,
                '--no-env-file',
                '-e',
                runtimeResourceProbeSource() +
                    'process.stdout.write(JSON.stringify(processResources))',
                root,
            ],
            { stdout: 'pipe', stderr: 'pipe' },
        )
        const [code, stdout, stderr] = await Promise.all([
            child.exited,
            new Response(child.stdout).text(),
            new Response(child.stderr).text(),
        ])
        expect({ code, stderr }).toEqual({ code: 0, stderr: '' })
        expect(JSON.parse(stdout)).toEqual({
            webFds: 1,
            controllerFds: 1,
            caddyFds: 1,
            webRssBytes: 24576,
            controllerRssBytes: 24576,
            caddyRssBytes: 24576,
        })
    })
})
