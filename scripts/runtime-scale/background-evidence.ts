import { z } from 'zod'
import { backgroundReportBinding } from './background-provenance.ts'
import { backgroundFaultSelection } from './background.ts'
import { backgroundExtendedRecoverySchema } from './background.validation.ts'
import type {
    BackgroundEvidenceExpectation,
    BackgroundRecoveryEvidence,
} from './Types/background-evidence.types.ts'

const digest = z.string().regex(/^[a-f0-9]{64}$/u)
const manifest = z.object({
    sha256: digest,
    files: z
        .array(z.object({ path: z.string().min(1), sha256: digest }))
        .min(1)
        .max(10_000),
})
const notificationIdentity = z.object({
    incidentId: z.string().min(1),
    channelId: z.string().min(1),
    channelRevision: z.number().int().positive(),
    type: z.string().min(1),
})
const schema = z.object({
    version: z.literal(1),
    targetSha: z.string().regex(/^[a-f0-9]{40}$/u),
    imageIdentity: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
    imagePlatform: z.enum(['linux/amd64', 'linux/arm64']),
    seed: z.number().int().nonnegative().max(0xffffffff),
    policyCount: z.literal(100),
    duePattern: z.literal('simultaneous'),
    latencyProfile: z.literal('mixed'),
    extendedRecoverySelected: z.literal(true),
    provenance: z.object({
        runId: z.string().min(1),
        scenario: z
            .object({
                runId: z.string().min(1),
                seed: z.number().int().nonnegative(),
                policyCount: z.literal(100),
                pattern: z.literal('simultaneous'),
                profile: z.literal('mixed'),
                durationSeconds: z.literal(120),
                contention: z.literal(false),
                extendedRecovery: z.literal(true),
                faults: z.array(z.string()).max(32),
            })
            .passthrough(),
        bindingSha256: digest,
        fixture: z
            .object({
                sourceManifest: manifest,
                artifacts: z.array(z.object({ name: z.string(), sha256: digest })).length(4),
                installedArtifactsVerified: z.literal(true),
            })
            .passthrough(),
        automationSources: manifest,
        automationSourcesUnchanged: z.literal(true),
        automationSourceRecheck: z.object({
            unchanged: z.literal(true),
            status: z.literal('matched'),
            sha256: digest,
        }),
    }),
    monitoring: z.object({
        status: z.literal('met'),
        attempted: z.literal(100),
        schedulerAdmitted: z.literal(100),
        committed: z.literal(100),
        networkAccepted: z.literal(100),
        schedulerCapture: z.object({
            status: z.literal('complete'),
            expectedAttempts: z.literal(100),
            capturedAdmissions: z.literal(100),
            matchedStartedAttempts: z.literal(100),
            identity: z.object({ runId: z.string().min(1), sourceSha: z.string() }),
            flags: z.object({
                overflowed: z.literal(false),
                expired: z.literal(false),
                clockRegressed: z.literal(false),
            }),
        }),
    }),
    notifications: z.object({
        initiallyDeferred: z.number().int().positive().max(1024),
        initiallyDeferredIdentities: z.array(notificationIdentity).max(1024),
        deferredAdmissions: z
            .array(notificationIdentity.extend({ deliveryId: z.string().min(1) }))
            .max(1024),
        admittedIds: z.array(z.string()).max(1024),
    }),
    recovery: z
        .array(
            z.object({
                component: z.enum(['web', 'controller', 'postgres-unavailable']),
                elapsedMs: z.number().finite().nonnegative(),
                passed: z.literal(true),
            }),
        )
        .length(3),
    extendedRecovery: backgroundExtendedRecoverySchema,
    summary: z.object({ passed: z.literal(true), capacity: z.literal('insufficient') }),
    reportSha256: digest,
})

function requireEvidence(condition: boolean): asserts condition {
    if (!condition)
        throw new Error('Background recovery evidence does not match its declared scope')
}

/** Qualify completed observations and exact source/image bindings; selected faults or a green summary alone never prove coverage. */
export function backgroundRecoveryEvidence(
    input: unknown,
    expected: BackgroundEvidenceExpectation,
): BackgroundRecoveryEvidence {
    const raw = z.record(z.string(), z.unknown()).parse(input)
    const { reportSha256, ...payload } = raw
    requireEvidence(backgroundReportBinding(payload) === reportSha256)
    const report = schema.parse(input)
    const provenance = report.provenance
    const rawProvenance = z.record(z.string(), z.unknown()).parse(raw.provenance)
    requireEvidence(
        report.targetSha === expected.targetSha && report.imagePlatform === expected.imagePlatform,
    )
    requireEvidence(!/^0+$/u.test(report.targetSha) && !/^sha256:0+$/u.test(report.imageIdentity))
    requireEvidence(provenance.runId === provenance.scenario.runId)
    requireEvidence(report.seed === provenance.scenario.seed)
    requireEvidence(report.monitoring.schedulerCapture.identity.runId === provenance.runId)
    requireEvidence(report.monitoring.schedulerCapture.identity.sourceSha === expected.targetSha)
    requireEvidence(provenance.automationSources.sha256 === expected.automationSha256)
    requireEvidence(provenance.automationSourceRecheck.sha256 === expected.automationSha256)
    requireEvidence(
        backgroundReportBinding(provenance.automationSources.files) === expected.automationSha256,
    )
    requireEvidence(provenance.fixture.sourceManifest.sha256 === expected.fixtureSha256)
    requireEvidence(
        backgroundReportBinding(provenance.fixture.sourceManifest.files) === expected.fixtureSha256,
    )
    const artifactNames = provenance.fixture.artifacts.map((item) => item.name).toSorted()
    requireEvidence(
        JSON.stringify(artifactNames) ===
            JSON.stringify([
                'background-capture-read.js',
                'background-contention-receiver.js',
                'background-contention-web.js',
                'reliability-fixture.js',
            ]),
    )
    requireEvidence(
        provenance.bindingSha256 ===
            backgroundReportBinding({
                targetSha: report.targetSha,
                imageIdentity: report.imageIdentity,
                imagePlatform: report.imagePlatform,
                scenario: rawProvenance.scenario,
                fixture: rawProvenance.fixture,
            }),
    )
    requireEvidence(
        JSON.stringify(provenance.scenario.faults) ===
            JSON.stringify(backgroundFaultSelection(true)),
    )
    requireEvidence(new Set(report.recovery.map((item) => item.component)).size === 3)
    const notifications = report.notifications
    requireEvidence(
        notifications.initiallyDeferredIdentities.length === notifications.initiallyDeferred,
    )
    requireEvidence(notifications.deferredAdmissions.length === notifications.initiallyDeferred)
    const deliveryIds = notifications.deferredAdmissions.map((item) => item.deliveryId)
    requireEvidence(new Set(deliveryIds).size === deliveryIds.length)
    requireEvidence(deliveryIds.every((id) => notifications.admittedIds.includes(id)))
    const identities = notifications.initiallyDeferredIdentities.map((item) => JSON.stringify(item))
    requireEvidence(new Set(identities).size === identities.length)
    requireEvidence(
        notifications.initiallyDeferredIdentities.every(
            (identity) =>
                notifications.deferredAdmissions.filter(
                    (item) =>
                        item.incidentId === identity.incidentId &&
                        item.channelId === identity.channelId &&
                        item.channelRevision === identity.channelRevision &&
                        item.type === identity.type,
                ).length === 1,
        ),
    )
    requireEvidence(!('failure' in raw) && !('cleanupFailures' in raw))
    return {
        targetSha: report.targetSha,
        imagePlatform: report.imagePlatform,
        imageIdentity: report.imageIdentity,
        runId: provenance.runId,
        reportSha256: report.reportSha256,
        elapsedMs: report.extendedRecovery.elapsedMs,
        schedulerAdmissions: report.monitoring.schedulerAdmitted,
        deferredIdentities: notifications.initiallyDeferred,
        faults: provenance.scenario.faults,
        capacity: report.summary.capacity,
    }
}

/** Two native reports qualify only their declared Docker application cases; package/Podman and sustained-capacity claims stay separate. */
export function backgroundRecoveryMatrix(reports: readonly BackgroundRecoveryEvidence[]) {
    requireEvidence(reports.length === 2)
    requireEvidence(new Set(reports.map((item) => item.imagePlatform)).size === 2)
    requireEvidence(new Set(reports.map((item) => item.runId)).size === 2)
    requireEvidence(new Set(reports.map((item) => item.imageIdentity)).size === 2)
    requireEvidence(new Set(reports.map((item) => item.targetSha)).size === 1)
    const first = reports.at(0)
    requireEvidence(first !== undefined)
    return {
        status: 'met' as const,
        targetSha: first.targetSha,
        application: reports.toSorted((left, right) =>
            left.imagePlatform.localeCompare(right.imagePlatform),
        ),
        packaging:
            'Use the separate Debian/Fedora and Podman/Quadlet native lifecycle reports; these application reports do not qualify those delivery combinations',
        capacity: 'insufficient' as const,
    }
}
