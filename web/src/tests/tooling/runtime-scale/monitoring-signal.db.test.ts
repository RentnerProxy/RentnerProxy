import { describe, expect, test } from 'bun:test'
import { fileURLToPath } from 'node:url'
import { assertIsolatedMonitoringDatabase } from '../../../../../scripts/runtime-scale/monitoring.ts'

let isolated = false
try {
    assertIsolatedMonitoringDatabase(
        process.env.RENTNERPROXY_DATABASE_INTEGRATION,
        process.env.DATABASE_URL,
    )
    isolated = true
} catch {
    // Ordinary unit runs never connect to a database.
}
// Windows terminates Bun children forcibly instead of delivering POSIX signals.
const integrationTest = isolated && process.platform !== 'win32' ? test : test.skip

describe('monitoring capacity signal cleanup', () => {
    integrationTest(
        'SIGTERM stops a seeded CLI run and removes owned fixtures before nonzero exit',
        async () => {
            const [{ db }, { hostCheckPolicies, proxyHosts, redirectHosts }, { sql }] =
                await Promise.all([
                    import('@/db/index.ts'),
                    import('@/db/schema.ts'),
                    import('drizzle-orm'),
                ])
            async function counts() {
                const result = await db.execute(
                    sql`select (select count(*)::int from ${hostCheckPolicies}) as policies, (select count(*)::int from ${proxyHosts}) as hosts, (select count(*)::int from ${redirectHosts}) as redirects`,
                )
                return result[0]!
            }
            expect(await counts()).toEqual({ policies: 0, hosts: 0, redirects: 0 })
            const repositoryRoot = fileURLToPath(new URL('../../../../../', import.meta.url))
            const child = Bun.spawn(
                [
                    process.execPath,
                    '--no-env-file',
                    'scripts/runtime-scale/monitoring.ts',
                    '--hosts',
                    '100',
                    '--periods',
                    '2',
                    '--profile',
                    'mixed',
                ],
                {
                    cwd: repositoryRoot,
                    env: process.env,
                    stdout: 'pipe',
                    stderr: 'pipe',
                },
            )
            const stdout = new Response(child.stdout).text()
            const stderr = new Response(child.stderr).text()
            try {
                const seedDeadline = Date.now() + 5000
                while (Date.now() < seedDeadline) {
                    // oxlint-disable-next-line no-await-in-loop -- Wait for the child to commit its owned fixture.
                    if (Number((await counts()).policies) === 100) {
                        // oxlint-disable-next-line no-await-in-loop -- An active lease proves the child's worker has started before signaling it.
                        const running = await db.execute(
                            sql`select count(*)::int as count from ${hostCheckPolicies} where lease_token is not null`,
                        )
                        if (Number(running[0]?.count) > 0) break
                    }
                    // oxlint-disable-next-line no-await-in-loop -- Bound polling without overlapping database queries.
                    await Bun.sleep(25)
                }
                expect(Number((await counts()).policies)).toBe(100)
                child.kill('SIGTERM')
                const timer = setTimeout(() => child.kill('SIGKILL'), 5000)
                let exitCode: number
                try {
                    exitCode = await child.exited
                } finally {
                    clearTimeout(timer)
                }
                expect(exitCode).not.toBe(0)
                expect(await counts()).toEqual({ policies: 0, hosts: 0, redirects: 0 })
                const report: unknown = JSON.parse((await stdout).trim().split('\n').at(-1)!)
                expect(report).toMatchObject({
                    passed: false,
                    failures: expect.arrayContaining([
                        'Monitoring capacity interrupted by SIGTERM',
                    ]),
                })
                expect(await stderr).toBe('')
            } finally {
                if (child.exitCode === null) child.kill('SIGKILL')
                await child.exited
            }
        },
        15000,
    )
})
