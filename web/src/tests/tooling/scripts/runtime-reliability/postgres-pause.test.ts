import { expect, test } from 'bun:test'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import {
    probeWithPausedPostgres,
    signalPostgresIdentities,
} from '../../../../../../scripts/runtime-reliability/postgres-pause.ts'
import type { PostgresPauseDependencies } from '../../../../../../scripts/runtime-reliability/Types/postgres-pause.types.ts'

/** Record ordering so tests distinguish guaranteed cleanup from an assumed successful probe. */
function fixture() {
    const events: string[] = []
    const dependencies: PostgresPauseDependencies = {
        guard: async () => {
            events.push('arm')
            return async () => {
                events.push('cancel')
            }
        },
        pause: async () => {
            events.push('pause')
        },
        resume: async () => {
            events.push('resume')
        },
        probeReady: async () => {
            events.push('ready')
            return false
        },
        probeProxy: async () => {
            events.push('proxy')
            return 200
        },
    }
    return { events, dependencies }
}

test('PostgreSQL guard is armed before STOP and cancelled only after CONT', async () => {
    const f = fixture()
    expect(await probeWithPausedPostgres(f.dependencies)).toEqual({
        notReady: true,
        proxyStatus: 200,
        resumed: true,
    })
    expect(f.events).toEqual(['arm', 'pause', 'ready', 'proxy', 'resume', 'cancel'])
})

test.each(['pause', 'probeReady', 'probeProxy'] as const)(
    'PostgreSQL resumes when %s fails',
    async (phase) => {
        const f = fixture()
        const failure = new Error('probe failure')
        f.dependencies[phase] = async () => {
            throw failure
        }
        await expect(probeWithPausedPostgres(f.dependencies)).rejects.toBe(failure)
        expect(f.events.slice(-2)).toEqual(['resume', 'cancel'])
    },
)

test('independent PostgreSQL guard stays armed if direct CONT fails', async () => {
    const f = fixture()
    f.dependencies.resume = async () => {
        throw new Error('resume failure')
    }
    await expect(probeWithPausedPostgres(f.dependencies)).rejects.toThrow('resume failure')
    expect(f.events).not.toContain('cancel')
})

test('PostgreSQL signals refuse recycled PIDs, changed owners, and unrelated executables', async () => {
    const root = await mkdtemp(join(tmpdir(), 'postgres-identities-'))
    try {
        await Promise.all(
            [10, 11, 12, 13].map(async (pid) => {
                await mkdir(join(root, String(pid)))
                await Promise.all([
                    writeFile(
                        join(root, String(pid), 'stat'),
                        pid +
                            ' (postgres) ' +
                            [
                                'S',
                                ...Array<string>(18).fill('0'),
                                pid === 11 ? 'recycled' : '123',
                                '0',
                            ].join(' '),
                    ),
                    writeFile(
                        join(root, String(pid), 'status'),
                        'Uid:\t' + (pid === 12 ? '10002' : '999') + '\t999\t999\t999\n',
                    ),
                    writeFile(
                        join(root, String(pid), 'comm'),
                        pid === 13 ? 'other\n' : 'postgres\n',
                    ),
                ])
            }),
        )
        const targets = [10, 11, 12, 13, 14].map((pid) => ({ pid, startTicks: '123', uid: 999 }))
        const signals: [number, string][] = []
        expect(
            await signalPostgresIdentities(targets, 'SIGSTOP', root, (pid, signal) =>
                signals.push([pid, signal]),
            ),
        ).toBe(1)
        expect(
            await signalPostgresIdentities(targets, 'SIGCONT', root, (pid, signal) =>
                signals.push([pid, signal]),
            ),
        ).toBe(1)
        expect(signals).toEqual([
            [10, 'SIGSTOP'],
            [10, 'SIGCONT'],
        ])
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})

test('a guard expiring between process reads refuses the next STOP and resumes captured identities', async () => {
    const root = await mkdtemp(join(tmpdir(), 'postgres-expired-guard-'))
    try {
        await Promise.all(
            [10, 11].map(async (pid) => {
                await mkdir(join(root, String(pid)))
                await Promise.all([
                    writeFile(
                        join(root, String(pid), 'stat'),
                        pid +
                            ' (postgres) ' +
                            ['S', ...Array<string>(18).fill('0'), '123', '0'].join(' '),
                    ),
                    writeFile(join(root, String(pid), 'status'), 'Uid:\t999\t999\t999\t999\n'),
                    writeFile(join(root, String(pid), 'comm'), 'postgres\n'),
                ])
            }),
        )
        const targets = [10, 11].map((pid) => ({ pid, startTicks: '123', uid: 999 }))
        const f = fixture()
        const signals: [number, string][] = []
        f.dependencies.pause = async () => {
            await signalPostgresIdentities(targets, 'SIGSTOP', root, (pid, signal) => {
                if (pid === 11) throw new Error('guard expired')
                signals.push([pid, signal])
            })
        }
        f.dependencies.resume = async () => {
            await signalPostgresIdentities(targets, 'SIGCONT', root, (pid, signal) =>
                signals.push([pid, signal]),
            )
            f.events.push('resume')
        }
        await expect(probeWithPausedPostgres(f.dependencies)).rejects.toThrow('guard expired')
        expect(signals).toEqual([
            [10, 'SIGSTOP'],
            [10, 'SIGCONT'],
            [11, 'SIGCONT'],
        ])
        expect(f.events).toEqual(['arm', 'resume', 'cancel'])
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})
