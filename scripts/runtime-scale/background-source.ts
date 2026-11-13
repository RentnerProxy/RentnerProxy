import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { BACKGROUND_FIXTURE_SOURCE_PATHS } from './background-source.config.ts'

/** Stage the complete background automation graph independently of the archived application source. */
export async function stageBackgroundFixtureSources(directory: string) {
    await Promise.all(
        BACKGROUND_FIXTURE_SOURCE_PATHS.map(async (relative) => {
            const destination = join(directory, relative)
            await mkdir(dirname(destination), { recursive: true })
            await writeFile(
                destination,
                await readFile(resolve(import.meta.dir, '../..', relative)),
            )
        }),
    )
}
