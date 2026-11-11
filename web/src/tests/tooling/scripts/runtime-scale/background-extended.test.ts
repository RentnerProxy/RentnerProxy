import { expect, test } from 'bun:test'
import { backgroundSnapshotSchema } from '../../../../../../scripts/runtime-scale/background.validation.ts'
import { waitForMonitoringDrain } from '../../../../../../scripts/runtime-scale/background-extended.ts'

const snapshot = (observations: number) =>
    backgroundSnapshotSchema.parse({
        origin: 0,
        policies: [],
        observations: Array.from({ length: observations }, () => ({
            policyId: '00000000-0000-4000-8000-000000000001',
            policyVersion: '00000000-0000-4000-8000-000000000002',
            checkedAt: 1,
            status: 'healthy',
            code: 'expected_status',
        })),
        managementReadMs: 0,
    })

const boundedWait = async (check: () => Promise<boolean>, message: string) => {
    for (let index = 0; index < 8; index++) if (await check()) return
    throw new Error(message)
}

test('lease-free stable fixture reads do not pass while the actual web monitoring worker is running', async () => {
    await expect(
        waitForMonitoringDrain(
            async () => 1,
            async () => snapshot(1),
            boundedWait,
        ),
    ).rejects.toThrow('Actual web monitoring operations drain')
})

test('monitoring drain requires worker completion and three stable durable reads after the last write', async () => {
    const running = [1, 0, 0, 0, 0, 0]
    const observationCounts = [0, 1, 1, 1, 1]
    let read = 0
    await expect(
        waitForMonitoringDrain(
            async () => running[read] ?? 0,
            async () => snapshot(observationCounts[read++] ?? 1),
            boundedWait,
        ),
    ).resolves.toBeUndefined()
    expect(read).toBe(5)
})
