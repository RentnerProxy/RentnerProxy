export type PostgresCpuProcess = {
    pid: number
    startTicks: string
    cpuNanoseconds: string
    runQueueNanoseconds: string
}

export type PostgresCpuIdentity = {
    pid: number
    backendStartEpoch: number
    bootEpoch: number
    clockTicks: number
}
