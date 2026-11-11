import { expect, test } from 'bun:test'
import { randomUUID } from 'node:crypto'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { basename, dirname, join, resolve } from 'node:path'
import { prepareMonitoringB2Web } from '../../../../../scripts/runtime-scale/monitoring-b2-web.ts'

for (const design of ['A', 'B2'] as const)
    test(`original ${design} scheduler is intercepted before an asynchronous publisher import can start it`, async () => {
        const directory = await mkdtemp(join(tmpdir(), 'rentnerproxy-b2-startup-'))
        if (
            dirname(resolve(directory)) !== resolve(tmpdir()) ||
            !basename(directory).startsWith('rentnerproxy-b2-startup-')
        )
            throw new Error('B2 startup test ownership mismatch')
        const key = 'b2fixture' + randomUUID().replaceAll('-', ''),
            state = {
                originalStarts: 0,
                wakes: 0,
                worker: undefined as undefined | { start: () => void },
            }
        const globals = globalThis as unknown as Record<string, unknown>
        globals[key] = state
        try {
            // A thenable module deterministically invokes the startup boundary at the added dynamic-import await.
            // Static importing its publisher does not run this hook; no timer/scheduler timing assumption is needed.
            await writeFile(
                join(directory, 'application-change.js'),
                `export function p(){globalThis[${JSON.stringify(key)}].wakes++}
export function then(resolve){globalThis[${JSON.stringify(key)}].worker.start();resolve({p})}`,
            )
            await writeFile(
                join(directory, 'host-monitoring.worker.server-Fixture.js'),
                `import { p as publishApplicationChange } from "./application-change.js";
export const hostMonitoringWorker={start(){globalThis[${JSON.stringify(key)}].originalStarts++},async stop(){}};
globalThis[${JSON.stringify(key)}].worker=hostMonitoringWorker;
void publishApplicationChange;`,
            )
            const prepared = await prepareMonitoringB2Web(design, directory)
            expect(state.originalStarts).toBe(design === 'A' ? 1 : 0)
            expect(prepared.proof()).toMatchObject({
                design,
                originalStartCalls: design === 'A' ? 1 : 0,
                suppressedStartCalls: design === 'B2' ? 1 : 0,
            })
            prepared.wake()
            expect(state.wakes).toBe(1)
            expect(prepared.changeChunkSha256).toMatch(/^[a-f0-9]{64}$/u)
        } finally {
            delete globals[key]
            await rm(directory, { recursive: true, force: true })
        }
    })
