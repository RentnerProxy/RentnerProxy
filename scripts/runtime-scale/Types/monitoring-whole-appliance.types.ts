export type MonitoringApplianceDesign = 'A' | 'B2'
export type MonitoringApplianceProfile = 'mixed' | 'all-3s-timeout'

export interface MonitoringApplianceWindow {
    profile: MonitoringApplianceProfile
    pair: number
    design: MonitoringApplianceDesign
    windowId: string
}

export interface MonitoringApplianceOptions {
    baselineRoot: string
    baselineSha: string
    baselineImageIdentity: string
    image: string
    nativeBinary: string
    reportPath: string
    topologyOnly: boolean
    safetyOnly: boolean
    diagnosticOnly: boolean
}

export interface MonitoringApplianceFaultSession {
    context: import('../../runtime-reliability/Types/harness.types.ts').ReliabilityContext
    request: (path: string, body?: unknown) => Promise<unknown>
    report: () => Promise<MonitoringApplianceReport>
    start: () => Promise<void>
    recover: (role: 'web' | 'controller', signal?: 'SIGTERM' | 'SIGKILL') => Promise<void>
    startControl: () => Promise<void>
    backupRestore: () => Promise<void>
    prepareAfterRebootstrap: () => Promise<void>
    continuity: () => Promise<{
        failures: string[]
        confirmedBunStopMs: number
        distinctDurableStartsAfterStop: number
        actualNativeHeads: number
        retainedNativeIdentity: boolean
    }>
}

export interface MonitoringApplianceFaultRecord {
    name: string
    state: 'passed' | 'failed' | 'incomplete'
    evidence: unknown
    failures: string[]
}

export interface MonitoringApplianceReport {
    windowId: string
    originMs: number
    endMs: number
    design: MonitoringApplianceDesign
    profile: MonitoringApplianceProfile
    active: number
    wakeProof: {
        design: MonitoringApplianceDesign
        runId: string
        windowId: string
        originMs: number
        publishedAtMs: number
        webIdentity: { pid: number; startTicks: string }
        originalChangeChunkSha256: string
        wakeCount: number
    } | null
    policies: {
        policyId: string
        hostId: string
        dueAtMs: number
        expectedStatus: string
        expectedCode: string
    }[]
    heads: { hostId: string; atMs: number; durationMs: number }[]
    starts: {
        policyId: string
        dueAtMs: number
        atMs: number
        admittedAtMs: number
        durableAtMs: number
        attemptId: string
        durable: boolean
    }[]
    observations: {
        policyId: string
        checkedAtMs: number
        committedAtMs: number
        commitTimeSource: 'first-visible-observation-upper-bound'
        status: string
        code: string
    }[]
    backlog: number
    admissionStore: {
        records: number
        admitted: number
        oldestAtMs: number | null
        maxRecords: number
        retentionMs: number
        pressure: boolean
    } | null
    outbox: { records: number; bytes: number; oldestAtMs: number | null }
    nativeIdentity: { pid: number; startTicks: string } | null
    controllerIdentity: { pid: number; startTicks: string }
}

export interface MonitoringApplianceResource {
    atMs: number
    monotonicMs: number
    cpuMicroseconds: number
    rssBytes: number
    workingSetBytes: number
    memoryBytes: number
    pids: number
    fds: number
    postgresConnections: number
    memoryLimitBytes: number
    pidLimit: number
    cpuQuotaCores: number
    postgresConnectionLimit: number
    oomKills: number
    roles: Record<string, { processes: number; rssBytes: number; fds: number }>
    database: {
        commits: number
        rollbacks: number
        blocksRead: number
        blocksHit: number
        inserted: number
        updated: number
        deleted: number
    }
}

export interface MonitoringApplianceTraffic {
    kind: 'ssr' | 'proxy'
    atMs: number
    elapsedMs: number
    status: number
    validBody: boolean
}

export interface MonitoringAppliancePairInput extends MonitoringApplianceWindow {
    cpuPercent: number | null
    rssMiB: number | null
    peakDatabaseConnections: number | null
    failures: readonly string[]
}

/** Exact process identity and state observed inside the appliance. */
export interface MonitoringApplianceStoppedIdentity {
    pid: number
    startTicks: string
    state: string
    atMs: number
}

/** One local STOP interval, its two reports and the actual same-owner resume outcome. */
export interface MonitoringApplianceContinuityCapture {
    before: MonitoringApplianceReport
    after: MonitoringApplianceReport
    stopped: MonitoringApplianceStoppedIdentity
    confirmed: MonitoringApplianceStoppedIdentity
    afterReport: MonitoringApplianceStoppedIdentity
    stoppedMonotonicMs: number
    confirmedMonotonicMs: number
    afterReportMonotonicMs: number
    resumed: {
        delivered: true
        before: MonitoringApplianceStoppedIdentity
        after: MonitoringApplianceStoppedIdentity | null
        outcome: 'resumed' | 'exited'
    }
}

/** Local collector I/O; host transport must not occur while the owner is stopped. */
export interface MonitoringApplianceContinuityDependencies {
    owner: { pid: number; startTicks: string }
    report: () => Promise<MonitoringApplianceReport>
    readOwner: () => Promise<MonitoringApplianceStoppedIdentity | null>
    signal: (signal: 'SIGSTOP' | 'SIGCONT') => Promise<void>
    sleep: (milliseconds: number) => Promise<unknown>
    monotonic: () => number
}
