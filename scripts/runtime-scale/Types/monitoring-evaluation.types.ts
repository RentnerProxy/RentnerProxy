export interface MonitoringEvaluationFence {
    readonly storeId: string
    readonly controllerEpoch: string
    readonly policyId: string
    readonly policyVersion: number
    readonly grantVersion: number
    readonly revision: string
    readonly hostProof: string
    readonly leaseToken: string
    readonly leaseExpiresAtMs: number
    readonly enabled: boolean
    readonly paused: boolean
}

export interface MonitoringEvaluationResult extends MonitoringEvaluationFence {
    readonly attemptId: string
    readonly checkedAtMs: number
    readonly outcome: 'healthy' | 'unhealthy' | 'unknown'
}

export interface MonitoringEvaluationSample {
    readonly sourceMatched: boolean
    readonly wholeAppliance: boolean
    readonly durationMs: number
    readonly policies: number
    readonly intervalSeconds: number
    readonly exactPolicyCoverage: boolean
    readonly dueToStartP99Ms: number
    readonly dueToCommitMaxMs: number
    readonly ssrP99Ms: number
    readonly proxyP99Ms: number
    readonly proxyErrors: number
    readonly backlogGrowth: number
    readonly baselineCpu: number
    readonly candidateCpu: number
    readonly baselineRssMiB: number
    readonly candidateRssMiB: number
    readonly extraDatabaseConnections: number
}

export interface MonitoringEvaluationEvidence {
    readonly samples: readonly MonitoringEvaluationSample[]
    readonly profile: 'mixed' | 'all-3s-timeout'
    readonly confirmedBunStopMs: number
    readonly distinctDurableStartsAfterStop: number
    readonly retainedNativeIdentity: boolean
    readonly replayedResults: number
    readonly replayMs: number
    readonly securityAndRestorePassed: boolean
    readonly freshAuthorityPassed: boolean
    readonly staleCommits: number
    readonly duplicateObservations: number
    readonly duplicateIncidents: number
    readonly outboxPressureVisible: boolean
    readonly measuredRelativeVariation: number
}
