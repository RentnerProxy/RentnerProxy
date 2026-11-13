import { afterEach, describe, expect, test } from 'bun:test'
import { mkdtemp, mkdir, readFile, realpath, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

import { coverageFixtureSource } from '@/tests/Helpers/coverageFixture.ts'

const prefix = join(tmpdir(), 'rentnerproxy-migration-cli-')
const directories: string[] = []

afterEach(async () => {
    await Promise.all(
        directories.splice(0).map(async (directory) => {
            if (!resolve(directory).startsWith(resolve(prefix)))
                throw new Error('Unsafe migration CLI fixture cleanup')
            await rm(directory, { recursive: true, force: true })
        }),
    )
})

const kitFixture = `
    const fs = require('node:fs')
    const path = require('node:path')
    const args = process.argv.slice(2)
    fs.appendFileSync('calls.jsonl', JSON.stringify({ args, cwd: process.cwd() }) + '\\n')
    if (process.env.FIXTURE_KIT_FAIL === '1') {
        console.error('synthetic Kit failure')
        process.exit(17)
    }
    const output = path.join(process.cwd(), 'web/drizzle')
    const meta = path.join(output, 'meta')
    fs.mkdirSync(meta, { recursive: true })
    const journalPath = path.join(meta, '_journal.json')
    const journal = fs.existsSync(journalPath)
        ? JSON.parse(fs.readFileSync(journalPath, 'utf8')) : { entries: [] }
    const custom = args.includes('--custom')
    if (custom || journal.entries.length === 0) {
        const name = custom ? args.find((value) => value.startsWith('--name=')).slice(7) : 'base_schema'
        const idx = journal.entries.length
        const prefix = String(idx).padStart(4, '0')
        const tag = prefix + '_' + name
        fs.writeFileSync(path.join(meta, prefix + '_snapshot.json'), JSON.stringify({ id: tag }))
        journal.entries.push({ idx, tag })
        fs.writeFileSync(journalPath, JSON.stringify(journal))
        fs.writeFileSync(path.join(output, tag + '.sql'), custom
            ? '-- Custom SQL migration file, put your code below! --' : 'select 0;')
    }
`

async function fixture() {
    const directory = await mkdtemp(prefix)
    directories.push(directory)
    await Promise.all([
        mkdir(join(directory, 'scripts/database'), { recursive: true }),
        mkdir(join(directory, 'node_modules/drizzle-kit'), { recursive: true }),
        mkdir(join(directory, 'web/src/db/CustomMigrations'), { recursive: true }),
    ])
    await Promise.all([
        ...['generate.ts', 'custom-migrations.ts'].map(async (filename) => {
            await writeFile(
                join(directory, 'scripts/database', filename),
                await coverageFixtureSource('scripts/database/' + filename),
            )
        }),
        writeFile(join(directory, 'node_modules/drizzle-kit/bin.cjs'), kitFixture),
        writeFile(join(directory, 'web/src/db/CustomMigrations/audit_v1.sql'), 'select 17;\n'),
    ])
    const run = async (args: readonly string[], fail = false) => {
        const child = Bun.spawn(
            [
                process.execPath,
                '--no-env-file',
                join(directory, 'scripts/database/generate.ts'),
                ...args,
            ],
            {
                cwd: tmpdir(),
                env: { ...process.env, FIXTURE_KIT_FAIL: fail ? '1' : '0' },
                stdout: 'pipe',
                stderr: 'pipe',
            },
        )
        const [status, stdout, stderr] = await Promise.all([
            child.exited,
            new Response(child.stdout).text(),
            new Response(child.stderr).text(),
        ])
        return { status, output: stdout + stderr }
    }
    const calls = async () =>
        (await readFile(join(directory, 'calls.jsonl'), 'utf8'))
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { args: string[]; cwd: string })
    return { directory, cwd: await realpath(directory), run, calls }
}

describe('owned migration generation CLI', () => {
    test('forwards arguments, generates schema before custom SQL and reruns without duplicating history', async () => {
        const context = await fixture()
        const args = ['--name=cli_fixture', '--prefix=index']
        const first = await context.run(args)
        expect(first.status, first.output).toBe(0)
        expect(await context.calls()).toEqual([
            { args: ['generate', '--config=drizzle.config.ts', ...args], cwd: context.cwd },
            {
                args: ['generate', '--config=drizzle.config.ts', '--custom', '--name=audit_v1'],
                cwd: context.cwd,
            },
        ])
        const sqlPath = join(context.directory, 'web/drizzle/0001_audit_v1.sql')
        const journalPath = join(context.directory, 'web/drizzle/meta/_journal.json')
        const sql = await readFile(sqlPath, 'utf8')
        const journal = await readFile(journalPath, 'utf8')
        expect(sql).toContain('-- Generated from CustomMigrations/audit_v1.sql; sha256:')
        expect(sql).toEndWith('select 17;\n')
        expect(JSON.parse(journal)).toEqual({
            entries: [
                { idx: 0, tag: '0000_base_schema' },
                { idx: 1, tag: '0001_audit_v1' },
            ],
        })
        const second = await context.run(args)
        expect(second.status, second.output).toBe(0)
        expect(await context.calls()).toHaveLength(3)
        expect(await readFile(sqlPath, 'utf8')).toBe(sql)
        expect(await readFile(journalPath, 'utf8')).toBe(journal)
    }, 15_000)

    test('propagates a failed Kit process and stops before custom allocation', async () => {
        const context = await fixture()
        const result = await context.run(['--name=failed_schema'], true)
        expect(result.status).not.toBe(0)
        expect(result.output).toContain('synthetic Kit failure')
        expect(result.output).toContain('Drizzle migration generation failed')
        expect(await context.calls()).toEqual([
            {
                args: ['generate', '--config=drizzle.config.ts', '--name=failed_schema'],
                cwd: context.cwd,
            },
        ])
        await expect(
            readFile(join(context.directory, 'web/drizzle/meta/_journal.json'), 'utf8'),
        ).rejects.toThrow()
    }, 15_000)

    test('rejects incomplete history before starting Kit or changing existing SQL', async () => {
        const context = await fixture()
        await mkdir(join(context.directory, 'web/drizzle'))
        const sqlPath = join(context.directory, 'web/drizzle/0000_unregistered.sql')
        await writeFile(sqlPath, 'select 41;\n')
        const result = await context.run([])
        expect(result.status).not.toBe(0)
        expect(result.output).toContain('metadata is missing for existing SQL outputs')
        await expect(context.calls()).rejects.toThrow()
        expect(await readFile(sqlPath, 'utf8')).toBe('select 41;\n')
    }, 15_000)
})
