import { expect, test } from 'bun:test'
import { backgroundCaptureReadCommand } from '../../../../../../scripts/runtime-scale/background-capture-read.ts'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test('capture-only helper imports and runs without SQL credentials or fixture context', async () => {
    const output = await runBunScript(
        `
        delete process.env.DATABASE_URL
        delete process.env.DATABASE_URL_FILE
        process.env.RENTNERPROXY_BACKGROUND_CAPTURE_RUN = '012345abcdef'
        process.env.RENTNERPROXY_BACKGROUND_CAPTURE_SOURCE_SHA = 'a'.repeat(40)
        const { runBackgroundCaptureRead } = await import('./scripts/runtime-scale/background-capture-read.ts')
        const result = await runBackgroundCaptureRead(['012345abcdef'])
        let rejected = false
        try { await runBackgroundCaptureRead(['012345abcdef', '/arbitrary/path']) } catch { rejected = true }
        console.log(JSON.stringify({ missing: result.capture === null, rejected }))
    `,
        { cwd: new URL('../../../../../../', import.meta.url) },
    )
    expect(JSON.parse(output)).toEqual({ missing: true, rejected: true })
})

test('fixed readonly command has independent nonzero remote and client bounds and refuses insufficient time', () => {
    const command = backgroundCaptureReadCommand('owned-fixture', '012345abcdef', 2000)
    expect(command).toMatchObject({ clientTimeoutMs: 2000, remoteTimeoutMs: 1000 })
    expect(command.args.slice(4)).toEqual([
        '/usr/bin/timeout',
        '--signal=KILL',
        '1.000s',
        'bun',
        '--no-env-file',
        '/opt/rentnerproxy/web/background-capture-read.js',
        '012345abcdef',
    ])
    expect(backgroundCaptureReadCommand('owned-fixture', '012345abcdef', 251).args).toContain(
        '0.001s',
    )
    for (const remaining of [0, 1, 250, NaN, Infinity])
        expect(() =>
            backgroundCaptureReadCommand('owned-fixture', '012345abcdef', remaining),
        ).toThrow()
})

test.skipIf(process.platform !== 'linux')(
    'remote timeout kills a hanging readonly Bun owner independently of the Docker client',
    async () => {
        const command = backgroundCaptureReadCommand('owned-fixture', '012345abcdef', 500)
        const started = performance.now()
        const child = Bun.spawn(
            [
                ...command.args.slice(4, 7),
                process.execPath,
                '--no-env-file',
                '-e',
                'await Bun.sleep(10000)',
            ],
            { stdout: 'pipe', stderr: 'pipe' },
        )
        const [exitCode] = await Promise.all([
            child.exited,
            new Response(child.stdout).text(),
            new Response(child.stderr).text(),
        ])
        expect(exitCode).toBe(137)
        expect(performance.now() - started).toBeLessThan(2000)
    },
)
