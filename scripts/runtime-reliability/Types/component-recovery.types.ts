import type { readRuntimeProcesses } from '../../smoke/appliance-processes.ts'

export type ApplianceProcess = Awaited<ReturnType<typeof readRuntimeProcesses>>[number]
export type ComponentRecoverySnapshot = {
    running: boolean
    startedAt: string
    processes: ApplianceProcess[]
}
export type ComponentRecoveryDependencies = {
    snapshot: () => Promise<ComponentRecoverySnapshot | undefined>
    signal: (target: ApplianceProcess) => Promise<void>
    start: () => Promise<void>
    ready: () => Promise<boolean>
    timeoutError: (label: string) => Error
    now?: () => number
    sleep?: (milliseconds: number) => Promise<unknown>
}
