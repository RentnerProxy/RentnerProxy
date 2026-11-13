import { createHash, randomUUID } from 'node:crypto'
import { readFile, readdir, rename, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

/** Kit owns journal/snapshot creation; emit immutable SQL sources only into its new custom outputs. */
export async function generateCustomMigrations(options: {
    sourceDirectory: string
    outputDirectory: string
    generate: (name: string) => Promise<void>
}): Promise<void> {
    await assertMigrationHistoryComplete(options.outputDirectory)
    const sources = (await readdir(options.sourceDirectory))
        .filter((name) => name.endsWith('.sql'))
        .toSorted()
    for (const source of sources) {
        // oxlint-disable-next-line no-await-in-loop -- Each generated migration must follow its predecessor's snapshot.
        await generateSource(options, source)
    }
}

async function generateSource(
    options: Parameters<typeof generateCustomMigrations>[0],
    source: string,
): Promise<void> {
    const name = source.slice(0, -4)
    if (!/^[a-z][a-z0-9_]*$/u.test(name)) throw new Error('Invalid custom migration source name')
    const content = (await readFile(join(options.sourceDirectory, source), 'utf8')).replaceAll(
        '\r\n',
        '\n',
    )
    const digest = createHash('sha256').update(content).digest('hex')
    const generated = `-- Generated from CustomMigrations/${source}; sha256:${digest}\n${content}`
    const before = await readdir(options.outputDirectory)
    const matchesSource = (file: string) => /^\d+_(.+)\.sql$/u.exec(file)?.[1] === name
    const existing = before.filter(matchesSource)
    if (existing.length > 1) throw new Error(`Duplicate custom migration: ${name}`)
    if (existing[0]) {
        const output = join(options.outputDirectory, existing[0])
        const emitted = (await readFile(output, 'utf8')).replaceAll('\r\n', '\n')
        if (emitted === generated) return
        // Recover interruption between Kit's journal/snapshot creation and our atomic emission.
        if (isKitPlaceholder(emitted)) await emitMigration(output, generated)
        else throw new Error(`Custom migration ${name} is immutable; add a new versioned source`)
        return
    }
    await options.generate(name)
    const after = await readdir(options.outputDirectory)
    const created = after.filter((file) => !before.includes(file) && matchesSource(file))
    if (created.length !== 1)
        throw new Error(`Kit did not generate exactly one custom migration: ${name}`)
    const output = join(options.outputDirectory, created[0]!)
    const placeholder = await readFile(output, 'utf8')
    if (!isKitPlaceholder(placeholder))
        throw new Error(`Refusing to replace nonempty custom migration: ${name}`)
    await emitMigration(output, generated)
}

/** Kit writes snapshot/journal before SQL; never allocate another entry over interrupted history. */
export async function assertMigrationHistoryComplete(outputDirectory: string): Promise<void> {
    const metadataDirectory = join(outputDirectory, 'meta')
    let metadata: string[]
    try {
        metadata = await readdir(metadataDirectory)
    } catch (error) {
        if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error
        let outputs: string[]
        try {
            outputs = await readdir(outputDirectory)
        } catch (outputError) {
            if (
                outputError instanceof Error &&
                'code' in outputError &&
                outputError.code === 'ENOENT'
            )
                return
            throw outputError
        }
        if (outputs.some((name) => name.endsWith('.sql')))
            throw new Error(
                'Incomplete Drizzle migration history: metadata is missing for existing SQL outputs',
                { cause: error },
            )
        return
    }
    const snapshots = new Set(
        metadata.filter((name) => /^\d+(?:_[A-Za-z0-9_-]+)?_snapshot\.json$/u.test(name)),
    )
    if (!metadata.includes('_journal.json'))
        throw new Error('Incomplete Drizzle migration history: metadata journal is missing')
    const journal: unknown = JSON.parse(
        await readFile(join(metadataDirectory, '_journal.json'), 'utf8'),
    )
    if (
        typeof journal !== 'object' ||
        journal === null ||
        !('entries' in journal) ||
        !Array.isArray(journal.entries)
    )
        throw new Error('Invalid Drizzle migration journal')
    const sqlFiles = new Set(await readdir(outputDirectory))
    const tags = new Set<string>()
    for (const entry of journal.entries) {
        if (
            typeof entry !== 'object' ||
            entry === null ||
            !('tag' in entry) ||
            typeof entry.tag !== 'string'
        )
            throw new Error('Invalid Drizzle migration journal entry')
        const prefix = /^(\d+)_[A-Za-z0-9_-]+$/u.exec(entry.tag)?.[1]
        if (!prefix || tags.has(entry.tag))
            throw new Error('Invalid or duplicate Drizzle migration journal tag')
        // Published history also contains a full-tag snapshot from an older custom migration.
        const snapshot = snapshots.has(`${prefix}_snapshot.json`)
            ? `${prefix}_snapshot.json`
            : `${entry.tag}_snapshot.json`
        if (!sqlFiles.has(`${entry.tag}.sql`) || !snapshots.has(snapshot))
            throw new Error(
                `Incomplete Drizzle migration history for ${entry.tag}: SQL or snapshot is missing; repair interrupted Kit output before generating`,
            )
        tags.add(entry.tag)
        snapshots.delete(snapshot)
    }
    if (snapshots.size > 0)
        throw new Error(
            'Incomplete Drizzle migration history: unregistered snapshot; repair interrupted Kit output before generating',
        )
}

function isKitPlaceholder(content: string): boolean {
    return content.trim() === '-- Custom SQL migration file, put your code below! --'
}

async function emitMigration(output: string, generated: string): Promise<void> {
    const temporary = `${output}.${randomUUID()}.tmp`
    try {
        await writeFile(temporary, generated, { flag: 'wx' })
        await rename(temporary, output)
    } finally {
        await rm(temporary, { force: true })
    }
}
