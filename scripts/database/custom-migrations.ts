import { createHash, randomUUID } from 'node:crypto'
import { readFile, readdir, rename, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

/** Kit owns journal/snapshot creation; emit immutable SQL sources only into its new custom outputs. */
export async function generateCustomMigrations(options: {
    sourceDirectory: string
    outputDirectory: string
    generate: (name: string) => Promise<void>
}): Promise<void> {
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
    const existing = before.filter((file) => file.endsWith(`_${name}.sql`))
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
    const created = after.filter((file) => !before.includes(file) && file.endsWith(`_${name}.sql`))
    if (created.length !== 1)
        throw new Error(`Kit did not generate exactly one custom migration: ${name}`)
    const output = join(options.outputDirectory, created[0]!)
    const placeholder = await readFile(output, 'utf8')
    if (!isKitPlaceholder(placeholder))
        throw new Error(`Refusing to replace nonempty custom migration: ${name}`)
    await emitMigration(output, generated)
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
