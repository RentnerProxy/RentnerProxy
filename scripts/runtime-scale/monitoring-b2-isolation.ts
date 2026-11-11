import { lstat, readFile } from 'node:fs/promises'
import { assertIsolatedMonitoringDatabase } from './monitoring.ts'

/** Ordinary test DBs use the existing strict guard; appliance fixtures additionally require the root-owned run seal. */
export async function monitoringB2DatabaseUrl(): Promise<string> {
    if (process.env.RENTNERPROXY_MONITORING_B2 !== '1')
        throw new Error('B2 fixture opt-in required')
    if (process.env.RENTNERPROXY_DATABASE_INTEGRATION === '1') {
        assertIsolatedMonitoringDatabase(
            process.env.RENTNERPROXY_DATABASE_INTEGRATION,
            process.env.DATABASE_URL,
        )
        return process.env.DATABASE_URL!
    }
    const runId = process.env.RENTNERPROXY_RELIABILITY_ISOLATED
    if (
        process.platform !== 'linux' ||
        !runId ||
        !/^[a-f0-9]{12}$/u.test(runId) ||
        process.env.B2_APPLIANCE_FIXTURE !== runId ||
        process.env.DATABASE_URL_FILE !== '/run/rentnerproxy/database-url/value'
    )
        throw new Error('B2 isolated appliance seal required')
    const directory = await lstat('/tmp/monitoring-b2')
    if (!directory.isDirectory() || directory.uid !== 0 || (directory.mode & 0o022) !== 0)
        throw new Error('B2 appliance seal directory mismatch')
    const path = '/tmp/monitoring-b2/mode.json'
    const metadata = await lstat(path)
    if (
        !metadata.isFile() ||
        metadata.uid !== 0 ||
        (metadata.mode & 0o022) !== 0 ||
        metadata.size > 4096
    )
        throw new Error('B2 appliance seal ownership mismatch')
    const state: unknown = JSON.parse(await readFile(path, 'utf8'))
    if (!state || typeof state !== 'object' || !('runId' in state) || state.runId !== runId)
        throw new Error('B2 appliance seal identity mismatch')
    const { getDatabaseUrl } = await import('../../web/src/server/env.server.ts')
    const value = getDatabaseUrl()
    if (!value) throw new Error('B2 synthetic appliance database missing')
    const url = new URL(value)
    if (
        !['postgres:', 'postgresql:'].includes(url.protocol) ||
        url.hostname !== '127.0.0.1' ||
        url.pathname !== '/rentnerproxy'
    )
        throw new Error('B2 appliance database outside synthetic fixture')
    return value
}
