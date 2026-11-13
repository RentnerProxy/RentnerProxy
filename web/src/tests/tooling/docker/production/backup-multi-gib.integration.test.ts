import { expect, test } from 'bun:test'
import { fileURLToPath } from 'node:url'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

const enabled = process.env.RENTNERPROXY_LARGE_BACKUP_INTEGRATION === '1'
const repositoryRoot = fileURLToPath(new URL('../../../../../../', import.meta.url))

test.skipIf(!enabled)(
    'a real 3 GiB controller survives validated export, encryption, streamed download and managed restore with bounded memory',
    async () => {
        // CI explicitly opts in inside a disposable root production container.
        // An enabled run must fail, rather than silently skip missing tools.
        expect(process.platform).toBe('linux')
        expect(process.getuid?.()).toBe(0)
        for (const executable of ['initdb', 'gosu', 'pg_dump', 'pg_restore', 'tar'])
            expect(Bun.which(executable), executable).not.toBeNull()
        const output = await runBunScript(
            `
            import { runBackupOrchestrationFixture } from './web/src/tests/Helpers/backupOrchestrationFixture.ts'
            const result = await runBackupOrchestrationFixture('agent-multi-gib')
            console.log('MULTI_GIB_RESULT=' + JSON.stringify(result))
            `,
            {
                cwd: repositoryRoot,
                env: {
                    ...process.env,
                    PUBLIC_ORIGIN: 'https://fixture.example.test',
                    PROXY_TRUSTED_PROXY_CIDRS: '',
                    RENTNERPROXY_DATABASE_HOST: '127.0.0.1',
                },
            },
        )
        const result = JSON.parse(
            output
                .split('\n')
                .find((line) => line.startsWith('MULTI_GIB_RESULT='))!
                .slice('MULTI_GIB_RESULT='.length),
        )
        expect(result).toMatchObject({
            exportState: 'succeeded',
            restoreState: 'succeeded',
            restoreError: null,
            phase: 'restarting',
            payloadBytes: 3 * 1024 ** 3 + 65_537,
            payloadVerified: true,
            encryptedRoundtrip: true,
            cipherHashVerified: true,
            supervisorIdentityVerified: true,
            downloadVerified: true,
            hashesUnchanged: true,
            inventoryUnchanged: true,
            databaseValue: '1',
            sessions: '00000000-0000-4000-8000-000000000003',
            controller: 'old controller state',
            journalExists: false,
            snapshotExists: false,
            keyMatchesBackup: true,
            pendingKeyJournal: false,
            runtimeConfigExcluded: true,
            ddnsPaused: true,
            discoveryPaused: true,
            crowdsecKey: 'a'.repeat(64),
        })
        expect(result.controllerArchiveBytes).toBeGreaterThan(result.payloadBytes)
        expect(result.encryptedBytes).toBeGreaterThan(result.controllerArchiveBytes)
        expect(result.peakRssBytes).toBeGreaterThan(0)
        expect(result.peakRssBytes).toBeLessThan(512 * 1024 ** 2)
        console.log(
            `Multi-GiB backup payloadBytes=${result.payloadBytes} peakRssBytes=${result.peakRssBytes}`,
        )
    },
    600_000,
)
