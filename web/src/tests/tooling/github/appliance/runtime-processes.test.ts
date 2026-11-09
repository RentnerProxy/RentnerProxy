import { expect, test } from 'bun:test'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
    readRuntimeProcesses,
    runtimeFdCountsProbe,
    runtimeProcessesProbe,
} from '../../../../../../scripts/smoke/appliance-processes.ts'

test('runtime census uses start ticks, exact executable names, and every web UID process', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'runtime-processes-'))
    try {
        await Promise.all(
            [
                { pid: 10, uid: 10002, executable: '/usr/local/bin/bun', ticks: '100', state: 'S' },
                { pid: 11, uid: 10002, executable: '/bin/worker', ticks: '101', state: 'Z' },
                {
                    pid: 12,
                    uid: 10001,
                    executable: '/usr/local/bin/rentnerproxy-controller',
                    ticks: '102',
                    state: 'S',
                },
                { pid: 13, uid: 10001, executable: '/usr/bin/caddy', ticks: '103', state: 'S' },
                { pid: 14, uid: 0, executable: '/usr/bin/bun', ticks: '104', state: 'S' },
                { pid: 15, uid: 0, executable: '/tmp/caddy-helper', ticks: '105', state: 'S' },
            ].map(async ({ pid, uid, executable, ticks, state }) => {
                const root = join(directory, String(pid))
                await mkdir(root)
                await mkdir(join(root, 'fd'))
                await Promise.all([
                    writeFile(join(root, 'fd', '0'), 'private descriptor content'),
                    writeFile(join(root, 'fd', '1'), 'private descriptor content'),
                    writeFile(
                        join(root, 'status'),
                        'Uid:\t' + uid + '\t' + uid + '\t' + uid + '\t' + uid + '\n',
                    ),
                    writeFile(join(root, 'cmdline'), executable + '\0argument\0'),
                    writeFile(
                        join(root, 'stat'),
                        pid +
                            ' (a complicated ) comm) ' +
                            [state, ...Array<string>(18).fill('0'), ticks, '0'].join(' '),
                    ),
                ])
            }),
        )
        // A process disappearing between enumeration and reads is expected.
        await mkdir(join(directory, '16'))
        const rows = await readRuntimeProcesses(directory)
        expect(rows.map((row) => [row.pid, row.startTicks, row.role, row.state])).toEqual([
            [10, '100', 'web', 'S'],
            [11, '101', 'web', 'Z'],
            [12, '102', 'controller', 'S'],
            [13, '103', 'caddy', 'S'],
        ])
        const child = Bun.spawn(
            [process.execPath, '--no-env-file', '-e', runtimeProcessesProbe(directory)],
            { stdout: 'pipe', stderr: 'pipe' },
        )
        const [exit, output, errors] = await Promise.all([
            child.exited,
            new Response(child.stdout).text(),
            new Response(child.stderr).text(),
        ])
        expect({ exit, errors }).toEqual({ exit: 0, errors: '' })
        expect(JSON.parse(output)).toEqual(rows)
        const descriptors = Bun.spawn(
            [process.execPath, '--no-env-file', '-e', runtimeFdCountsProbe(directory)],
            { stdout: 'pipe', stderr: 'pipe' },
        )
        const [descriptorExit, descriptorOutput, descriptorErrors] = await Promise.all([
            descriptors.exited,
            new Response(descriptors.stdout).text(),
            new Response(descriptors.stderr).text(),
        ])
        expect({ exit: descriptorExit, errors: descriptorErrors }).toEqual({ exit: 0, errors: '' })
        expect(JSON.parse(descriptorOutput)).toEqual(
            rows.map(({ role, pid, startTicks }) => ({ role, pid, startTicks, openFds: 2 })),
        )
        expect(descriptorOutput).not.toContain('private descriptor content')
        // Recycling a PID must produce a different identity.
        await writeFile(
            join(directory, '10/stat'),
            '10 (bun) ' + ['S', ...Array<string>(18).fill('0'), '999', '0'].join(' '),
        )
        expect(
            (await readRuntimeProcesses(directory)).find((row) => row.pid === 10)?.startTicks,
        ).toBe('999')
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})

test('overlapping health and version clients are excluded while duplicate serving owners and every web UID remain visible', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'runtime-processes-probes-'))
    const controller = '/usr/local/bin/rentnerproxy-controller'
    const caddy = '/usr/bin/caddy'
    try {
        await Promise.all(
            [
                { pid: 20, uid: 10001, args: [controller] },
                { pid: 21, uid: 10001, args: [controller, '--healthcheck', 'ready'] },
                { pid: 22, uid: 10001, args: [controller, '--healthcheck', 'progress'] },
                { pid: 23, uid: 10001, args: [controller, '--healthcheck'] },
                // Only the dedicated first argument selects healthcheck mode in the Rust entrypoint.
                { pid: 24, uid: 10001, args: [controller, '--serve', '--healthcheck'] },
                { pid: 25, uid: 10001, args: [caddy, 'run', '--config', '-'] },
                { pid: 26, uid: 10001, args: [caddy, 'version'] },
                { pid: 27, uid: 10001, args: [caddy, 'run', '--config', '/tmp/duplicate.json'] },
                { pid: 28, uid: 10002, args: [controller, '--healthcheck', 'ready'] },
            ].map(async ({ pid, uid, args }) => {
                const root = join(directory, String(pid))
                await mkdir(root)
                await Promise.all([
                    writeFile(join(root, 'status'), `Uid:\t${uid}\t${uid}\t${uid}\t${uid}\n`),
                    writeFile(join(root, 'cmdline'), args.join('\0') + '\0'),
                    writeFile(
                        join(root, 'stat'),
                        `${pid} (controller) ` +
                            ['S', ...Array<string>(18).fill('0'), String(pid * 10), '0'].join(' '),
                    ),
                ])
            }),
        )
        const rows = await readRuntimeProcesses(directory)
        expect(rows.map(({ pid, role }) => [pid, role])).toEqual([
            [20, 'controller'],
            [24, 'controller'],
            [25, 'caddy'],
            [27, 'caddy'],
            [28, 'web'],
        ])
        const child = Bun.spawn(
            [process.execPath, '--no-env-file', '-e', runtimeProcessesProbe(directory)],
            { stdout: 'pipe', stderr: 'pipe' },
        )
        const [exit, output, errors] = await Promise.all([
            child.exited,
            new Response(child.stdout).text(),
            new Response(child.stderr).text(),
        ])
        expect({ exit, errors }).toEqual({ exit: 0, errors: '' })
        expect(JSON.parse(output)).toEqual(rows)
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})

test('incomplete identities fail instead of claiming web descendants were reaped', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'runtime-processes-invalid-'))
    try {
        await mkdir(join(directory, '10'))
        await Promise.all([
            writeFile(join(directory, '10/status'), 'Uid:\t10002\t10002\t10002\t10002\n'),
            writeFile(join(directory, '10/cmdline'), '/usr/bin/bun\0'),
            writeFile(join(directory, '10/stat'), '10 (bun) S'),
        ])
        await expect(readRuntimeProcesses(directory)).rejects.toThrow('identity is incomplete')
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
})
