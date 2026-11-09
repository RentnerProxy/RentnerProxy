import { expect, spyOn, test } from 'bun:test'
import { verifyApplianceRuntimeFaults } from '../../../../../../.github/scripts/ci/appliance/runtime-faults.ts'
import type { ApplianceRuntimeFaultContext } from '../../../../../../.github/scripts/ci/appliance/Types/runtime-faults.types.ts'

test('initial native progress waits for a completed tick before taking the fault baseline', async () => {
    const log = spyOn(console, 'log').mockImplementation(() => {})
    const errorLog = spyOn(console, 'error').mockImplementation(() => {})
    const resourceFailure = new Error('isolated resource command failure')
    let statusCalls = 0
    const context: ApplianceRuntimeFaultContext = {
        recreatedId: 'fixture',
        httpPort: 80,
        httpsPort: 443,
        managementPort: 81,
        hostDomain: 'fixture.test',
        trafficMarker: 'fixture',
        passed: () => {},
        inspect: async () => '0',
        command: async (args) => {
            if (!args.at(-1)?.includes('/internal/v1/proxy/status')) throw resourceFailure
            statusCalls += 1
            return JSON.stringify({
                status: 200,
                body: JSON.stringify({
                    recovery: {
                        workerRunning: true,
                        circuitOpen: false,
                        completedTicks: statusCalls === 1 ? 0 : 1,
                        lastCompletedAgeSeconds: statusCalls === 1 ? null : 0,
                    },
                }),
            })
        },
    }
    try {
        await expect(verifyApplianceRuntimeFaults(context)).rejects.toBe(resourceFailure)
        expect(statusCalls).toBe(2)
        expect(errorLog).toHaveBeenCalledWith('Runtime web failure stage=resource-stats')
        expect(log).toHaveBeenCalledWith(
            'Runtime web native workerRunning=true circuitOpen=false completedTicks=0 lastCompletedAgeSeconds=null',
        )
    } finally {
        log.mockRestore()
        errorLog.mockRestore()
    }
})
