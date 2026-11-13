import { expect, test } from 'bun:test'
import { mkdir, mkdtemp, open, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { readBackgroundReportFile } from '../../../../../.github/scripts/ci/background-recovery-evidence.ts'
import { runSmokeProcess } from '../../../../../scripts/smoke/process.ts'

test('background evidence reads one bounded regular JSON report and rejects directories and oversized files', async () => {
    const root = await mkdtemp(join(tmpdir(), 'background-report-reader-'))
    try {
        const report = join(root, 'report.json')
        await writeFile(report, JSON.stringify({ source: 'fixture', passed: true }))
        expect(await readBackgroundReportFile(report)).toEqual({ source: 'fixture', passed: true })
        await mkdir(join(root, 'directory'))
        await expect(readBackgroundReportFile(join(root, 'directory'))).rejects.toThrow()
        const huge = await open(join(root, 'huge.json'), 'wx')
        try {
            await huge.truncate(8 * 1024 * 1024 + 1)
        } finally {
            await huge.close()
        }
        await expect(readBackgroundReportFile(join(root, 'huge.json'))).rejects.toThrow(
            'Invalid report file',
        )
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test.skipIf(process.platform !== 'linux')(
    'background evidence refuses report symlinks',
    async () => {
        const root = await mkdtemp(join(tmpdir(), 'background-report-link-'))
        try {
            await writeFile(join(root, 'payload'), '{}')
            await symlink(join(root, 'payload'), join(root, 'report.json'))
            await expect(readBackgroundReportFile(join(root, 'report.json'))).rejects.toThrow()
        } finally {
            await rm(root, { recursive: true, force: true })
        }
    },
)

test.skipIf(process.platform !== 'linux' || !Bun.which('mkfifo'))(
    'a non-regular report cannot block artifact qualification on a FIFO',
    async () => {
        const root = await mkdtemp(join(tmpdir(), 'background-report-fifo-'))
        try {
            const fifo = join(root, 'report.json')
            expect(Bun.spawnSync(['mkfifo', fifo]).exitCode).toBe(0)
            const repository = resolve(import.meta.dir, '../../../../..')
            const module = pathToFileURL(
                join(repository, '.github/scripts/ci/background-recovery-evidence.ts'),
            ).href
            const result = await runSmokeProcess(
                [
                    process.execPath,
                    '--no-env-file',
                    '-e',
                    'const {readBackgroundReportFile}=await import(process.argv[1]);try{await readBackgroundReportFile(process.argv[2]);process.exitCode=3}catch{console.log("rejected")}',
                    module,
                    fifo,
                ],
                { cwd: repository, timeoutMs: 1000 },
            )
            expect(result.timedOut).toBe(false)
            expect(result.exitCode).toBe(0)
            expect(result.stdout.trim()).toBe('rejected')
        } finally {
            await rm(root, { recursive: true, force: true })
        }
    },
)
