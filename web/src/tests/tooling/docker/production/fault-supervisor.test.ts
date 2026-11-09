import { expect, test } from 'bun:test'
import { fileURLToPath } from 'node:url'
import { runMaintenanceProcess } from '../../../../../../scripts/backup/maintenance-process.ts'

test.skipIf(process.platform !== 'linux')(
    'fault supervisor isolates web failures and preserves process, maintenance and retry fences',
    async () => {
        const root = fileURLToPath(new URL('../../../../../..', import.meta.url))
        const result = await runMaintenanceProcess(
            ['bash', 'docker/production/tests/fault-supervisor.sh'],
            {
                cwd: root,
                timeoutMs: 15_000,
                maximumOutputBytes: 65_536,
            },
        )
        expect(Buffer.from(result.stderr).toString('utf8')).not.toContain('FAIL:')
        expect(result.failed).toBe(false)
        expect(result.stopped).toBe(true)
        expect(Buffer.from(result.stdout).toString('utf8')).toContain('PASS:')
    },
    20_000,
)
