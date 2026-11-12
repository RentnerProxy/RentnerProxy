import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'

/** Stage the complete background automation graph independently of the archived application source. */
export async function stageBackgroundFixtureSources(directory: string) {
    await Promise.all(
        [
            'scripts/runtime-scale/background-fixture.ts',
            'scripts/runtime-scale/background-capture.ts',
            'scripts/runtime-scale/background-capture-read.ts',
            'scripts/runtime-scale/background-notifications.ts',
            'scripts/runtime-scale/background-notification-identities.ts',
            'scripts/runtime-scale/background-receivers.ts',
            'scripts/runtime-scale/background-network.ts',
            'scripts/runtime-scale/background-discovery.ts',
            'scripts/runtime-scale/background-recovery.ts',
            'scripts/runtime-scale/background-contention-fixture.ts',
            'scripts/runtime-scale/background-contention-receiver.ts',
            'scripts/runtime-scale/background-contention-web.ts',
            'scripts/runtime-scale/background.ts',
            'scripts/runtime-scale/background.validation.ts',
            'scripts/runtime-reliability/control.ts',
            'scripts/runtime-reliability/control.config.ts',
            'scripts/runtime-reliability/resource-probe.ts',
            'scripts/runtime-reliability/Types/resource-probe.types.ts',
            'scripts/smoke/appliance-processes.ts',
        ].map(async (relative) => {
            const destination = join(directory, relative)
            await mkdir(dirname(destination), { recursive: true })
            await writeFile(
                destination,
                await readFile(resolve(import.meta.dir, '../..', relative)),
            )
        }),
    )
}
