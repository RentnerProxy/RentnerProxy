import { fileURLToPath } from 'node:url'

import { generateCustomMigrations } from './custom-migrations.ts'

const repositoryRoot = fileURLToPath(new URL('../..', import.meta.url))

async function generate(argumentsList: readonly string[]): Promise<void> {
    const command = Bun.spawn(
        [
            process.execPath,
            '--no-env-file',
            'node_modules/drizzle-kit/bin.cjs',
            'generate',
            '--config=drizzle.config.ts',
            ...argumentsList,
        ],
        { cwd: repositoryRoot, stdin: 'inherit', stdout: 'inherit', stderr: 'inherit' },
    )
    if ((await command.exited) !== 0) throw new Error('Drizzle migration generation failed')
}

await generate(process.argv.slice(2))
await generateCustomMigrations({
    sourceDirectory: fileURLToPath(new URL('../../web/src/db/CustomMigrations', import.meta.url)),
    outputDirectory: fileURLToPath(new URL('../../web/drizzle', import.meta.url)),
    generate: (name) => generate(['--custom', `--name=${name}`]),
})
