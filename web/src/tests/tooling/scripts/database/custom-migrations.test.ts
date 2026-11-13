import { describe, expect, test } from 'bun:test'
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

import { generateCustomMigrations } from '../../../../../../scripts/database/custom-migrations.ts'

async function fixture(
    operation: (
        options: Parameters<typeof generateCustomMigrations>[0],
        calls: string[],
    ) => Promise<void>,
) {
    const prefix = join(tmpdir(), 'rentnerproxy-custom-migrations-')
    const directory = await mkdtemp(prefix)
    if (!resolve(directory).startsWith(resolve(prefix)))
        throw new Error('Unexpected fixture directory')
    const sourceDirectory = join(directory, 'sources')
    const outputDirectory = join(directory, 'output')
    const calls: string[] = []
    try {
        await mkdir(sourceDirectory)
        await mkdir(outputDirectory)
        await writeFile(join(sourceDirectory, 'audit_v1.sql'), 'select 1;\n')
        await operation(
            {
                sourceDirectory,
                outputDirectory,
                generate: async (name) => {
                    calls.push(name)
                    await writeFile(
                        join(outputDirectory, `0001_${name}.sql`),
                        '-- Custom SQL migration file, put your code below! --',
                    )
                },
            },
            calls,
        )
    } finally {
        await rm(directory, { recursive: true, force: true })
    }
}

describe('custom migration generation', () => {
    test('emits a source only into the newly kit-created file and is idempotent', async () => {
        await fixture(async (options, calls) => {
            await writeFile(join(options.outputDirectory, '0000_existing.sql'), 'select 0;')
            await generateCustomMigrations(options)
            const output = await readFile(
                join(options.outputDirectory, '0001_audit_v1.sql'),
                'utf8',
            )
            expect(output).toContain('-- Generated from CustomMigrations/audit_v1.sql; sha256:')
            expect(output).toEndWith('select 1;\n')
            await generateCustomMigrations(options)
            expect(calls).toEqual(['audit_v1'])
            expect(await readdir(options.outputDirectory)).toHaveLength(2)
            expect(await readFile(join(options.outputDirectory, '0000_existing.sql'), 'utf8')).toBe(
                'select 0;',
            )
        })
    })

    test('rejects changed source and output without replacing history or generating a new migration', async () => {
        await fixture(async (options, calls) => {
            await generateCustomMigrations(options)
            const path = join(options.outputDirectory, '0001_audit_v1.sql')
            const original = await readFile(path, 'utf8')
            await writeFile(join(options.sourceDirectory, 'audit_v1.sql'), 'select 2;\n')
            await expect(generateCustomMigrations(options)).rejects.toThrow('immutable')
            expect(await readFile(path, 'utf8')).toBe(original)
            await writeFile(join(options.sourceDirectory, 'audit_v1.sql'), 'select 1;\n')
            await writeFile(path, original + 'select 3;\n')
            await expect(generateCustomMigrations(options)).rejects.toThrow('immutable')
            expect(await readFile(path, 'utf8')).toBe(original + 'select 3;\n')
            expect(calls).toEqual(['audit_v1'])
        })
    })

    test('refuses an output not created by this invocation or a nonempty kit output', async () => {
        await fixture(async (options) => {
            await expect(
                generateCustomMigrations({ ...options, generate: async () => {} }),
            ).rejects.toThrow('exactly one')
            await expect(
                generateCustomMigrations({
                    ...options,
                    generate: async (name) => {
                        await writeFile(
                            join(options.outputDirectory, `0001_${name}.sql`),
                            'select 99;',
                        )
                    },
                }),
            ).rejects.toThrow('nonempty')
            expect(await readFile(join(options.outputDirectory, '0001_audit_v1.sql'), 'utf8')).toBe(
                'select 99;',
            )
        })
    })

    test('recovers only the exact Kit placeholder after interrupted generation', async () => {
        await fixture(async (options, calls) => {
            const path = join(options.outputDirectory, '0001_audit_v1.sql')
            await writeFile(path, '-- Custom SQL migration file, put your code below! --\n')
            await generateCustomMigrations(options)
            expect(await readFile(path, 'utf8')).toEndWith('select 1;\n')
            expect(calls).toHaveLength(0)
            expect(await readdir(options.outputDirectory)).toEqual(['0001_audit_v1.sql'])
            await writeFile(path, '-- A real historical migration may contain only a comment.\n')
            await expect(generateCustomMigrations(options)).rejects.toThrow('immutable')
            expect(await readFile(path, 'utf8')).toBe(
                '-- A real historical migration may contain only a comment.\n',
            )
        })
    })
})
