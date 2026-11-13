export type BackgroundEvidenceExpectation = {
    targetSha: string
    imagePlatform: 'linux/amd64' | 'linux/arm64'
    automationSha256: string
    fixtureSha256: string
}

export type BackgroundRecoveryEvidence = {
    targetSha: string
    imagePlatform: 'linux/amd64' | 'linux/arm64'
    imageIdentity: string
    runId: string
    reportSha256: string
    elapsedMs: number
    schedulerAdmissions: number
    deferredIdentities: number
    faults: readonly string[]
    capacity: 'insufficient'
}
