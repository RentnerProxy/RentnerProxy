import { expect, test } from 'bun:test'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
    backgroundMonitoringReport,
    backgroundPolicies,
    parseBackgroundOptions,
} from '../../../../../../scripts/runtime-scale/background.ts'
import { backgroundSnapshotSchema } from '../../../../../../scripts/runtime-scale/background.validation.ts'
import { hostMonitoringCaptureSchema } from '../../../../server/HostMonitoring/host-monitoring.capture.ts'
import {
    backgroundSourceManifest,
    backgroundReportBinding,
} from '../../../../../../scripts/runtime-scale/background-provenance.ts'
import { backgroundCaptureMatchesOwner } from '../../../../../../scripts/runtime-scale/background-capture.ts'

const identity = { runId: '012345abcdef', sourceSha: 'a'.repeat(40) }
function fixture() {
    const options = parseBackgroundOptions(['--profile', 'healthy'])
    const policies = backgroundPolicies(options)
    const snapshot = backgroundSnapshotSchema.parse({
        origin: 1000,
        managementReadMs: 1,
        policies: policies.map((item) => ({
            id: item.id,
            version: item.id,
            index: item.index,
            enabled: true,
            leased: false,
            dueAt: 1000,
        })),
        observations: policies.map((item) => ({
            policyId: item.id,
            policyVersion: item.id,
            checkedAt: 2000,
            status: 'healthy',
            code: 'expected_status',
        })),
        schedulerCapture: {
            version: 2,
            publication: { generation: 1, fd: 123 },
            ...identity,
            pid: 42,
            processStartTicks: '123',
            epoch: policies[0]!.id,
            createdAt: 900,
            expiresAt: 600900,
            overflowed: false,
            expired: false,
            clockRegressed: false,
            events: policies.flatMap((item) =>
                ['admitted', 'started'].map((kind, index) => ({
                    kind,
                    policyId: item.id,
                    policyVersion: item.id,
                    attemptHash: item.id.replaceAll('-', '').padEnd(64, '0'),
                    dueAt: 1000,
                    at: 1050 + index * 3,
                    monotonicMs: 10 + index * 3,
                })),
            ),
        },
    })
    const receipts = policies.map((item) => ({
        index: item.index,
        latency: item.latency,
        startedAt: 1100,
        finishedAt: 1200,
    }))
    return { options, snapshot, receipts }
}

test('real claim-return and adapter-start series remain separate from independently received network starts', () => {
    const f = fixture()
    const report = backgroundMonitoringReport(f.options, f.snapshot, f.receipts, 35000, identity)
    expect(report).toMatchObject({
        status: 'met',
        schedulerAdmitted: 100,
        queueLag: { count: 100, p99: 50 },
        schedulerStartLag: { count: 100, p99: 3 },
        startLag: { count: 100, p99: 100 },
        schedulerCapture: {
            status: 'complete',
            expectedAttempts: 100,
            capturedAdmissions: 100,
            matchedStartedAttempts: 100,
        },
    })
    expect(
        backgroundMonitoringReport(
            f.options,
            { ...f.snapshot, schedulerCapture: undefined },
            f.receipts,
            35000,
            identity,
        ),
    ).toMatchObject({
        status: 'insufficient',
        schedulerAdmitted: null,
        queueLag: null,
        schedulerStartLag: null,
    })
})

test('mismatched source/run, overflow and regressed clocks never qualify as scheduler evidence', () => {
    const f = fixture()
    for (const change of [
        { sourceSha: 'b'.repeat(40) },
        { runId: 'abcdef012345' },
        { overflowed: true },
        { expired: true },
        { clockRegressed: true },
        { publication: { generation: 0, fd: null } },
    ]) {
        const schedulerCapture = hostMonitoringCaptureSchema.parse({
            ...f.snapshot.schedulerCapture,
            ...change,
        })
        expect(
            backgroundMonitoringReport(
                f.options,
                { ...f.snapshot, schedulerCapture },
                f.receipts,
                35000,
                identity,
            ),
        ).toMatchObject({
            status: 'insufficient',
            schedulerAdmitted: null,
            schedulerCapture: { rejected: true },
        })
    }
})

test('unleased legacy snapshots cannot satisfy the current publication contract', () => {
    const capture = fixture().snapshot.schedulerCapture!
    const { publication: _publication, ...legacy } = capture
    expect(hostMonitoringCaptureSchema.safeParse({ ...legacy, version: 1 }).success).toBe(false)
})

test('capture belongs to the same live serving owner on both census brackets, not a fixture child or reused PID', () => {
    const capture = fixture().snapshot.schedulerCapture!
    const owner = [{ pid: capture.pid, startTicks: capture.processStartTicks, state: 'S' }]
    expect(backgroundCaptureMatchesOwner(capture, identity, owner, owner, 2000)).toBe(true)
    for (const after of [
        [],
        [...owner, owner[0]!],
        [{ ...owner[0]!, pid: 43 }],
        [{ ...owner[0]!, startTicks: '124' }],
        [{ ...owner[0]!, state: 'Z' }],
    ])
        expect(backgroundCaptureMatchesOwner(capture, identity, owner, after, 2000)).toBe(false)
    expect(
        backgroundCaptureMatchesOwner(
            capture,
            { ...identity, sourceSha: undefined },
            owner,
            owner,
            2000,
        ),
    ).toBe(false)
    expect(
        backgroundCaptureMatchesOwner(capture, identity, owner, owner, capture.expiresAt + 1),
    ).toBe(false)
})

test('cancelled late claims and duplicated or cross-attempt starts cannot fabricate a complete wait series', () => {
    const f = fixture(),
        original = f.snapshot.schedulerCapture!
    for (const events of [
        original.events.map((event, index) =>
            index === 1 ? { ...event, kind: 'cancelled' as const } : event,
        ),
        [...original.events, original.events[0]!],
        original.events.map((event, index) =>
            index === 1 ? { ...event, attemptHash: 'f'.repeat(64) } : event,
        ),
        [...original.events, { ...original.events[1]!, attemptHash: 'f'.repeat(64) }],
    ]) {
        const report = backgroundMonitoringReport(
            f.options,
            { ...f.snapshot, schedulerCapture: { ...original, events } },
            f.receipts,
            35000,
            identity,
        )
        expect(report.status).toBe('insufficient')
        expect(report.schedulerStartLag!.count).toBe(99)
    }
})

test('source and report binding change for actual bytes, image/profile and fault selection', async () => {
    const root = await mkdtemp(join(tmpdir(), 'background-source-'))
    try {
        await writeFile(join(root, 'fixture.ts'), 'export const value = 1')
        const first = await backgroundSourceManifest(root)
        await writeFile(join(root, 'fixture.ts'), 'export const value = 2')
        expect((await backgroundSourceManifest(root)).sha256).not.toBe(first.sha256)
        expect(first.files[0]!.path).toBe('fixture.ts')
        const input = {
            image: 'sha256:' + 'a'.repeat(64),
            profile: 'healthy',
            faults: ['web-stop'],
            source: first.sha256,
        }
        const hash = backgroundReportBinding(input)
        for (const change of [
            { image: 'different' },
            { profile: 'timeout' },
            { faults: ['controller-stop'] },
            { source: 'different' },
        ])
            expect(backgroundReportBinding({ ...input, ...change })).not.toBe(hash)
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})
