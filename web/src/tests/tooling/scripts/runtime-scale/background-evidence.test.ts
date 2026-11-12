import { expect, test } from 'bun:test'
import {
    backgroundRecoveryEvidence,
    backgroundRecoveryMatrix,
} from '../../../../../../scripts/runtime-scale/background-evidence.ts'
import { backgroundReportBinding } from '../../../../../../scripts/runtime-scale/background-provenance.ts'
import { backgroundFaultSelection } from '../../../../../../scripts/runtime-scale/background.ts'

const targetSha = '1'.repeat(40)
const files = [{ path: 'fixture.ts', sha256: 'a'.repeat(64) }]
const manifest = { files, sha256: backgroundReportBinding(files) }
const expected = {
    targetSha,
    imagePlatform: 'linux/amd64' as const,
    automationSha256: manifest.sha256,
    fixtureSha256: manifest.sha256,
}

function fixture(platform: 'linux/amd64' | 'linux/arm64' = 'linux/amd64') {
    const imageIdentity = 'sha256:' + (platform === 'linux/amd64' ? 'a' : 'b').repeat(64)
    const scenario = {
        runId: 'fixture-' + platform,
        seed: 70,
        policyCount: 100,
        pattern: 'simultaneous',
        profile: 'mixed',
        durationSeconds: 120,
        contention: false,
        extendedRecovery: true,
        faults: backgroundFaultSelection(true),
    }
    const installed = {
        sourceManifest: structuredClone(manifest),
        artifacts: [
            'reliability-fixture.js',
            'background-contention-receiver.js',
            'background-contention-web.js',
            'background-capture-read.js',
        ].map((name) => ({ name, sha256: 'c'.repeat(64) })),
        installedArtifactsVerified: true,
    }
    const report = {
        version: 1,
        targetSha,
        imageIdentity,
        imagePlatform: platform,
        seed: 70,
        policyCount: 100,
        duePattern: 'simultaneous',
        latencyProfile: 'mixed',
        extendedRecoverySelected: true,
        provenance: {
            runId: scenario.runId,
            scenario,
            bindingSha256: backgroundReportBinding({
                targetSha,
                imageIdentity,
                imagePlatform: platform,
                scenario,
                fixture: installed,
            }),
            fixture: installed,
            automationSources: structuredClone(manifest),
            automationSourcesUnchanged: true,
            automationSourceRecheck: {
                unchanged: true,
                status: 'matched',
                sha256: manifest.sha256,
            },
        },
        monitoring: {
            status: 'met',
            attempted: 100,
            schedulerAdmitted: 100,
            committed: 100,
            networkAccepted: 100,
            schedulerCapture: {
                status: 'complete',
                expectedAttempts: 100,
                capturedAdmissions: 100,
                matchedStartedAttempts: 100,
                identity: { runId: scenario.runId, sourceSha: targetSha },
                flags: { overflowed: false, expired: false, clockRegressed: false },
            },
        },
        notifications: {
            initiallyDeferred: 1,
            initiallyDeferredIdentities: [
                {
                    incidentId: 'incident',
                    channelId: 'channel',
                    channelRevision: 1,
                    type: 'firing',
                },
            ],
            deferredAdmissions: [
                {
                    incidentId: 'incident',
                    channelId: 'channel',
                    channelRevision: 1,
                    type: 'firing',
                    deliveryId: 'delivery',
                },
            ],
            admittedIds: ['delivery'],
        },
        recovery: [
            { component: 'web', elapsedMs: 1000, passed: true },
            { component: 'controller', elapsedMs: 1000, passed: true },
            { component: 'postgres-unavailable', elapsedMs: 1000, passed: true },
        ],
        extendedRecovery: {
            status: 'met',
            elapsedMs: 5000,
            interruptedCertificateJobs: true,
            activeRestore: {
                jobId: '00000000-0000-4000-8000-000000000001',
                finalStage: 'applied',
                strandedMonitoringLeases: 0,
                noLateMonitoringWrites: true,
            },
            audit: {
                status: 'met',
                finalCursor: 'epoch:10',
                receiptIds: ['event-10'],
                auditedEventCount: 1,
                duplicateAuditEffects: 0,
                monitoringLeases: 0,
            },
        },
        summary: { passed: true, capacity: 'insufficient' },
        reportSha256: '',
    }
    const { reportSha256: _digest, ...payload } = report
    report.reportSha256 = backgroundReportBinding(payload)
    return report
}

function reseal(report: ReturnType<typeof fixture>) {
    const { reportSha256: _digest, ...payload } = report
    report.reportSha256 = backgroundReportBinding(payload)
}

test('native recovery evidence requires completed selected faults and keeps delivery and capacity claims separate', () => {
    const amd64 = backgroundRecoveryEvidence(fixture(), expected)
    const arm64 = backgroundRecoveryEvidence(fixture('linux/arm64'), {
        ...expected,
        imagePlatform: 'linux/arm64',
    })
    const matrix = backgroundRecoveryMatrix([arm64, amd64])
    expect(matrix.status).toBe('met')
    expect(matrix.application.map((item) => item.imagePlatform)).toEqual([
        'linux/amd64',
        'linux/arm64',
    ])
    expect(matrix.application[0]?.faults).toHaveLength(18)
    expect(matrix.capacity).toBe('insufficient')
    expect(matrix.packaging).toContain('separate')
    expect(backgroundFaultSelection(false)).toHaveLength(14)
})

const invalid: {
    name: string
    mutate: (report: ReturnType<typeof fixture>) => void
}[] = [
    {
        name: 'foreign source',
        mutate: (report) => {
            report.targetSha = '2'.repeat(40)
        },
    },
    {
        name: 'wrong native platform',
        mutate: (report) => {
            report.imagePlatform = 'linux/arm64'
        },
    },
    {
        name: 'long workload',
        mutate: (report) => {
            report.provenance.scenario.durationSeconds = 1800
        },
    },
    {
        name: 'unselected recovery',
        mutate: (report) => {
            report.extendedRecoverySelected = false
        },
    },
    {
        name: 'declared faults without extended completion',
        mutate: (report) => {
            report.extendedRecovery.status = 'insufficient'
        },
    },
    {
        name: 'omitted extended fault selection',
        mutate: (report) => {
            report.provenance.scenario.faults = backgroundFaultSelection(false)
        },
    },
    {
        name: 'different owner run',
        mutate: (report) => {
            report.provenance.scenario.runId = 'foreign'
        },
    },
    {
        name: 'different seed',
        mutate: (report) => {
            report.seed = 71
        },
    },
    {
        name: 'expired scheduler capture',
        mutate: (report) => {
            report.monitoring.schedulerCapture.flags.expired = true
        },
    },
    {
        name: 'foreign capture owner',
        mutate: (report) => {
            report.monitoring.schedulerCapture.identity.runId = 'foreign'
        },
    },
    {
        name: 'lost scheduler start',
        mutate: (report) => {
            report.monitoring.schedulerCapture.matchedStartedAttempts = 99
        },
    },
    {
        name: 'missing deferred identities',
        mutate: (report) => {
            report.notifications.initiallyDeferredIdentities = []
        },
    },
    {
        name: 'unmatched deferred delivery',
        mutate: (report) => {
            report.notifications.deferredAdmissions[0]!.deliveryId = 'foreign'
        },
    },
    {
        name: 'unmatched incident identity',
        mutate: (report) => {
            report.notifications.deferredAdmissions[0]!.incidentId = 'foreign'
        },
    },
    {
        name: 'stale source recheck',
        mutate: (report) => {
            report.provenance.automationSourcesUnchanged = false
        },
    },
    {
        name: 'incomplete installed bundle verification',
        mutate: (report) => {
            report.provenance.fixture.installedArtifactsVerified = false
        },
    },
    {
        name: 'changed source manifest',
        mutate: (report) => {
            report.provenance.fixture.sourceManifest.files[0]!.sha256 = 'd'.repeat(64)
        },
    },
    {
        name: 'missing audit effects',
        mutate: (report) => {
            report.extendedRecovery.audit.auditedEventCount = 0
        },
    },
    {
        name: 'duplicate replay effects',
        mutate: (report) => {
            report.extendedRecovery.audit.duplicateAuditEffects = 1
        },
    },
    {
        name: 'stranded monitoring lease',
        mutate: (report) => {
            report.extendedRecovery.activeRestore.strandedMonitoringLeases = 1
        },
    },
    {
        name: 'late post-restore write',
        mutate: (report) => {
            report.extendedRecovery.activeRestore.noLateMonitoringWrites = false
        },
    },
    {
        name: 'missing component recovery',
        mutate: (report) => {
            report.recovery.pop()
        },
    },
    {
        name: 'failed component recovery',
        mutate: (report) => {
            report.recovery[0]!.passed = false
        },
    },
    {
        name: 'duplicate component recovery',
        mutate: (report) => {
            report.recovery[1]!.component = 'web'
        },
    },
    {
        name: 'unknown component recovery',
        mutate: (report) => {
            report.recovery[1]!.component = 'unknown'
        },
    },
    {
        name: 'negative recovery duration',
        mutate: (report) => {
            report.recovery[0]!.elapsedMs = -1
        },
    },
    {
        name: 'capacity claim from bounded evidence',
        mutate: (report) => {
            report.summary.capacity = 'met'
        },
    },
]
test.each(invalid)('rejects $name even when the report digest is recomputed', ({ mutate }) => {
    const report = fixture()
    mutate(report)
    reseal(report)
    expect(() => backgroundRecoveryEvidence(report, expected)).toThrow()
})

test('rejects report tampering before qualifying any observation', () => {
    const report = fixture()
    report.extendedRecovery.elapsedMs++
    expect(() => backgroundRecoveryEvidence(report, expected)).toThrow()
})

test('a platform matrix cannot reuse one report or omit an architecture', () => {
    const report = backgroundRecoveryEvidence(fixture(), expected)
    expect(() => backgroundRecoveryMatrix([report])).toThrow()
    expect(() => backgroundRecoveryMatrix([report, report])).toThrow()
})
