/** Exact root-sealed selectors for the private finite measurement workload. */
export interface MonitoringB2WindowCapScope {
    enabled?: boolean
    runId: string
    windowId: string
    design: 'A' | 'B2'
    policyIds: string[]
}

/** Queried installed scope and horizon; no per-check admission receipt is added. */
export interface MonitoringB2WindowCapProof {
    runId: string
    windowId: string
    policySetSha256: string
    policies: number
    installed: boolean
    originMs: number | null
    endMs: number | null
    deferredMs: number | null
}
