export type MonitoringB2RecoveryScenario = 'replay' | 'fences' | 'lease-expiry' | 'pressure' | 'gap'

export interface MonitoringB2RecoveryResult {
    scenario: MonitoringB2RecoveryScenario
    passed: boolean
    evidence: Record<string, number | boolean | string | null>
    failures: string[]
}

export interface MonitoringB2ReplayEvidence {
    rawDurableRecords: number
    distinctRawPolicies: number
    nativeHeads: number
    distinctNativeHosts: number
    replayedResults: number
    observations: number
    replayMs: number
    pending: number
    rejectedApplied: number
    staleCommits: number | null
}
