export type PostgresProcessIdentity = { pid: number; startTicks: string; uid: number }
export type PostgresPauseDependencies = {
    guard: () => Promise<() => Promise<void>>
    pause: () => Promise<void>
    resume: () => Promise<void>
    probeReady: () => Promise<boolean>
    probeProxy: () => Promise<number | null>
}
