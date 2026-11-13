import { describe, expect, test } from 'bun:test'
import { mkdtemp, mkdir, readFile, readdir, rename, rm, rmdir, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

import {
    assertMigrationHistoryComplete,
    generateCustomMigrations,
} from '../../../../../../scripts/database/custom-migrations.ts'

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
                    const tag = `${String(calls.length).padStart(4, '0')}_${name}`
                    let tags: string[] = []
                    try {
                        const journal = JSON.parse(
                            await readFile(join(outputDirectory, 'meta/_journal.json'), 'utf8'),
                        ) as { entries: { tag: string }[] }
                        tags = journal.entries.map((entry) => entry.tag)
                    } catch (error) {
                        if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT'))
                            throw error
                    }
                    await writeHistory(outputDirectory, [...tags, tag])
                    await writeFile(
                        join(outputDirectory, `${tag}.sql`),
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

async function writeHistory(outputDirectory: string, tags: readonly string[]) {
    const metadataDirectory = join(outputDirectory, 'meta')
    await mkdir(metadataDirectory, { recursive: true })
    await writeFile(
        join(metadataDirectory, '_journal.json'),
        JSON.stringify({
            entries: tags.map((tag, idx) => ({ idx, tag })),
        }),
    )
    await Promise.all(
        tags.map((tag) =>
            writeFile(
                join(metadataDirectory, `${tag.split('_')[0]}_snapshot.json`),
                JSON.stringify({ id: tag }),
            ),
        ),
    )
}

describe('custom migration generation', () => {
    test('rejects an unsupported source name before starting Kit', async () => {
        await fixture(async (options, calls) => {
            await rm(join(options.sourceDirectory, 'audit_v1.sql'))
            const path = join(options.sourceDirectory, 'invalid-name.sql')
            await writeFile(path, 'select 9;\n')
            await expect(generateCustomMigrations(options)).rejects.toThrow(
                'Invalid custom migration source name',
            )
            expect(calls).toHaveLength(0)
            expect(await readFile(path, 'utf8')).toBe('select 9;\n')
        })
    })

    test.each([
        { value: null, error: 'Invalid Drizzle migration journal' },
        { value: { entries: 1 }, error: 'Invalid Drizzle migration journal' },
        { value: { entries: [null] }, error: 'Invalid Drizzle migration journal entry' },
        { value: { entries: [{ tag: 7 }] }, error: 'Invalid Drizzle migration journal entry' },
        {
            value: { entries: [{ tag: '../outside' }] },
            error: 'Invalid or duplicate Drizzle migration journal tag',
        },
    ])(
        'rejects malformed journal metadata without allocating a migration: %j',
        async ({ value, error }) => {
            await fixture(async (options, calls) => {
                await mkdir(join(options.outputDirectory, 'meta'))
                const path = join(options.outputDirectory, 'meta/_journal.json')
                const original = JSON.stringify(value)
                await writeFile(path, original)
                await expect(generateCustomMigrations(options)).rejects.toThrow(error)
                expect(calls).toHaveLength(0)
                expect(await readFile(path, 'utf8')).toBe(original)
            })
        },
    )

    test('rejects an existing metadata directory without its journal', async () => {
        await fixture(async (options, calls) => {
            await mkdir(join(options.outputDirectory, 'meta'))
            await expect(generateCustomMigrations(options)).rejects.toThrow(
                'metadata journal is missing',
            )
            expect(calls).toHaveLength(0)
            expect(await readdir(join(options.outputDirectory, 'meta'))).toEqual([])
        })
    })

    test('propagates an unusable metadata path without treating it as a fresh history', async () => {
        await fixture(async (options, calls) => {
            const path = join(options.outputDirectory, 'meta')
            await writeFile(path, 'not a directory')
            await expect(generateCustomMigrations(options)).rejects.toThrow()
            expect(calls).toHaveLength(0)
            expect(await readFile(path, 'utf8')).toBe('not a directory')
        })
    })

    test.each(['custom-name', 'journal-tag'] as const)(
        'rejects a true duplicate %s without changing completed SQL',
        async (duplicate) => {
            await fixture(async (options, calls) => {
                await generateCustomMigrations(options)
                const path = join(options.outputDirectory, '0001_audit_v1.sql')
                const original = await readFile(path, 'utf8')
                if (duplicate === 'custom-name') {
                    await writeFile(join(options.outputDirectory, '0002_audit_v1.sql'), original)
                    await writeHistory(options.outputDirectory, ['0001_audit_v1', '0002_audit_v1'])
                } else {
                    await writeFile(
                        join(options.outputDirectory, 'meta/_journal.json'),
                        JSON.stringify({
                            entries: [{ tag: '0001_audit_v1' }, { tag: '0001_audit_v1' }],
                        }),
                    )
                }
                await expect(generateCustomMigrations(options)).rejects.toThrow(
                    duplicate === 'custom-name'
                        ? 'Duplicate custom migration'
                        : 'Invalid or duplicate Drizzle migration journal tag',
                )
                expect(calls).toEqual(['audit_v1'])
                expect(await readFile(path, 'utf8')).toBe(original)
            })
        },
    )

    test('emits a source only into the newly kit-created file and is idempotent', async () => {
        await fixture(async (options, calls) => {
            await writeFile(join(options.outputDirectory, '0000_existing.sql'), 'select 0;')
            await writeHistory(options.outputDirectory, ['0000_existing'])
            await generateCustomMigrations(options)
            const output = await readFile(
                join(options.outputDirectory, '0001_audit_v1.sql'),
                'utf8',
            )
            expect(output).toContain('-- Generated from CustomMigrations/audit_v1.sql; sha256:')
            expect(output).toEndWith('select 1;\n')
            await generateCustomMigrations(options)
            expect(calls).toEqual(['audit_v1'])
            expect(await readdir(options.outputDirectory)).toHaveLength(3)
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
            await writeHistory(options.outputDirectory, ['0001_audit_v1'])
            // Keep compatibility with the published full-tag snapshot naming convention.
            await rename(
                join(options.outputDirectory, 'meta/0001_snapshot.json'),
                join(options.outputDirectory, 'meta/0001_audit_v1_snapshot.json'),
            )
            await generateCustomMigrations(options)
            expect(await readFile(path, 'utf8')).toEndWith('select 1;\n')
            expect(calls).toHaveLength(0)
            expect((await readdir(options.outputDirectory)).toSorted()).toEqual([
                '0001_audit_v1.sql',
                'meta',
            ])
            await writeFile(path, '-- A real historical migration may contain only a comment.\n')
            await expect(generateCustomMigrations(options)).rejects.toThrow('immutable')
            expect(await readFile(path, 'utf8')).toBe(
                '-- A real historical migration may contain only a comment.\n',
            )
        })
    })

    test('keeps similarly suffixed versioned sources distinct on an idempotent rerun', async () => {
        await fixture(async (options, calls) => {
            await writeFile(join(options.sourceDirectory, 'repair_audit_v1.sql'), 'select 2;\n')
            await generateCustomMigrations(options)
            await writeHistory(options.outputDirectory, ['0001_audit_v1', '0002_repair_audit_v1'])
            const paths = ['0001_audit_v1.sql', '0002_repair_audit_v1.sql']
            const before = await Promise.all(
                paths.map((path) => readFile(join(options.outputDirectory, path), 'utf8')),
            )
            await generateCustomMigrations(options)
            expect(calls).toEqual(['audit_v1', 'repair_audit_v1'])
            expect(
                await Promise.all(
                    paths.map((path) => readFile(join(options.outputDirectory, path), 'utf8')),
                ),
            ).toEqual(before)
            expect(before[0]).toEndWith('select 1;\n')
            expect(before[1]).toEndWith('select 2;\n')
        })
    })

    test('rejects a newly created file whose full migration name belongs to another source', async () => {
        await fixture(async (options) => {
            await expect(
                generateCustomMigrations({
                    ...options,
                    generate: async () => {
                        await writeFile(
                            join(options.outputDirectory, '0001_repair_audit_v1.sql'),
                            '-- Custom SQL migration file, put your code below! --',
                        )
                    },
                }),
            ).rejects.toThrow('exactly one')
            expect(
                await readFile(join(options.outputDirectory, '0001_repair_audit_v1.sql'), 'utf8'),
            ).toBe('-- Custom SQL migration file, put your code below! --')
        })
    })

    test('fails before allocating another entry when Kit registered a snapshot and journal without SQL', async () => {
        await fixture(async (options, calls) => {
            await writeHistory(options.outputDirectory, ['0001_audit_v1'])
            const journalPath = join(options.outputDirectory, 'meta/_journal.json')
            const snapshotPath = join(options.outputDirectory, 'meta/0001_snapshot.json')
            const before = await Promise.all([
                readFile(journalPath, 'utf8'),
                readFile(snapshotPath, 'utf8'),
            ])
            await expect(generateCustomMigrations(options)).rejects.toThrow(
                'Incomplete Drizzle migration history for 0001_audit_v1',
            )
            expect(calls).toHaveLength(0)
            expect(
                await Promise.all([readFile(journalPath, 'utf8'), readFile(snapshotPath, 'utf8')]),
            ).toEqual(before)
            expect(await readdir(options.outputDirectory)).toEqual(['meta'])
        })
    })

    test('fails before allocation for an orphan snapshot or registered SQL without a snapshot', async () => {
        await fixture(async (options, calls) => {
            await writeHistory(options.outputDirectory, [])
            const snapshotPath = join(options.outputDirectory, 'meta/0001_snapshot.json')
            await writeFile(snapshotPath, '{}')
            await expect(generateCustomMigrations(options)).rejects.toThrow('unregistered snapshot')
            await rm(snapshotPath)
            await writeHistory(options.outputDirectory, ['0001_audit_v1'])
            await rm(snapshotPath)
            await writeFile(
                join(options.outputDirectory, '0001_audit_v1.sql'),
                '-- Custom SQL migration file, put your code below! --',
            )
            await expect(generateCustomMigrations(options)).rejects.toThrow(
                'SQL or snapshot is missing',
            )
            expect(calls).toHaveLength(0)
        })
    })

    test('rejects missing metadata over existing SQL without allocating or changing that SQL', async () => {
        await fixture(async (options, calls) => {
            const output = join(options.outputDirectory, '0000_existing.sql')
            await writeFile(output, 'select 123;\n')
            await expect(generateCustomMigrations(options)).rejects.toThrow(
                'metadata is missing for existing SQL outputs',
            )
            expect(calls).toHaveLength(0)
            expect(await readFile(output, 'utf8')).toBe('select 123;\n')
            expect(await readdir(options.outputDirectory)).toEqual(['0000_existing.sql'])
        })
    })

    test('rejects exact generated SQL when its journal entry and snapshot have been lost', async () => {
        await fixture(async (options, calls) => {
            await generateCustomMigrations(options)
            const output = join(options.outputDirectory, '0001_audit_v1.sql')
            const original = await readFile(output, 'utf8')
            await writeHistory(options.outputDirectory, [])
            await rm(join(options.outputDirectory, 'meta/0001_snapshot.json'))
            const journal = await readFile(
                join(options.outputDirectory, 'meta/_journal.json'),
                'utf8',
            )
            await expect(generateCustomMigrations(options)).rejects.toThrow(
                'unregistered SQL output',
            )
            expect(calls).toEqual(['audit_v1'])
            expect(await readFile(output, 'utf8')).toBe(original)
            expect(
                await readFile(join(options.outputDirectory, 'meta/_journal.json'), 'utf8'),
            ).toBe(journal)
            expect(await readdir(join(options.outputDirectory, 'meta'))).toEqual(['_journal.json'])
            expect((await readdir(options.outputDirectory)).toSorted()).toEqual([
                '0001_audit_v1.sql',
                'meta',
            ])
        })
    })

    test('rejects unrelated orphan SQL alongside otherwise complete migration history', async () => {
        await fixture(async (options, calls) => {
            await generateCustomMigrations(options)
            const output = join(options.outputDirectory, '0002_unrelated.sql')
            await writeFile(output, 'select 456;\n')
            await expect(generateCustomMigrations(options)).rejects.toThrow(
                'unregistered SQL output',
            )
            expect(calls).toEqual(['audit_v1'])
            expect(await readFile(output, 'utf8')).toBe('select 456;\n')
        })
    })

    test('allows genuinely empty and absent output directories before first Kit generation', async () => {
        await fixture(async (options, calls) => {
            await expect(
                assertMigrationHistoryComplete(options.outputDirectory),
            ).resolves.toBeUndefined()
            await rmdir(options.outputDirectory)
            await expect(
                assertMigrationHistoryComplete(options.outputDirectory),
            ).resolves.toBeUndefined()
            expect(calls).toHaveLength(0)
        })
    })
})
