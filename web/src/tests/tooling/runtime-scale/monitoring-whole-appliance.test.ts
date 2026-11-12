import { describe, expect, test } from 'bun:test'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { tmpdir } from 'node:os'
import { monitoringEvaluationContract as contract } from '../../../../../scripts/runtime-scale/monitoring-evaluation.ts'
import { monitoringPrototypeDockerfile } from '../../../../../scripts/runtime-scale/monitoring-whole-appliance.image.ts'
import {
    assessMonitoringApplianceRecovery,
    monitoringApplianceFaultCases,
} from '../../../../../scripts/runtime-scale/monitoring-whole-appliance.faults.ts'
import {
    monitoringApplianceResourceProbe,
    monitoringApplianceResourceSchema,
    summarizeMonitoringApplianceProgress,
    summarizeMonitoringApplianceResources,
    assessMonitoringAppliancePairs,
    assessMonitoringApplianceWake,
} from '../../../../../scripts/runtime-scale/monitoring-whole-appliance.metrics.ts'
import {
    monitoringApplianceWindows,
    parseMonitoringApplianceOptions,
    monitoringApplianceTrafficProbe,
    proveMonitoringApplianceContinuity,
    collectMonitoringApplianceContinuity,
    monitoringApplianceOverlay,
    validateMonitoringApplianceNativeManifest,
    runMonitoringAppliance,
} from '../../../../../scripts/runtime-scale/monitoring-whole-appliance.ts'
import type {
    MonitoringApplianceReport,
    MonitoringApplianceResource,
    MonitoringApplianceTraffic,
    MonitoringAppliancePairInput,
} from '../../../../../scripts/runtime-scale/Types/monitoring-whole-appliance.types.ts'

function progress(): MonitoringApplianceReport {
    const report: MonitoringApplianceReport = {
        windowId: 'test-window',
        originMs: 1_000_000,
        endMs: 1_120_000,
        design: 'B2',
        profile: 'mixed',
        active: 0,
        wakeProof: {
            design: 'B2',
            runId: '012345abcdef',
            windowId: 'test-window',
            originMs: 1_000_000,
            publishedAtMs: 999_500,
            webIdentity: { pid: 300, startTicks: '124' },
            originalChangeChunkSha256: 'a'.repeat(64),
            wakeCount: 1,
        },
        policies: [],
        heads: [],
        starts: [],
        observations: [],
        backlog: 0,
        admissionStore: {
            records: 2000,
            admitted: 2000,
            oldestAtMs: 1_000_000,
            maxRecords: 4096,
            retentionMs: 120000,
            pressure: false,
        },
        outbox: { records: 0, bytes: 0, oldestAtMs: null },
        nativeIdentity: { pid: 200, startTicks: '123' },
        controllerIdentity: { pid: 100, startTicks: '122' },
    }
    for (let index = 0; index < contract.policies; index++) {
        const expectedStatus = index % 4 === 3 ? 'unhealthy' : 'healthy'
        const expectedCode = index % 4 === 3 ? 'upstream_timeout' : 'expected_status'
        const policyId = 'policy-' + index,
            hostId = 'host-' + index
        report.policies.push({
            policyId,
            hostId,
            dueAtMs: report.originMs,
            expectedStatus,
            expectedCode,
        })
        for (let period = 0; period < 2; period++) {
            const dueAtMs = report.originMs + period * 60_000
            report.heads.push({ hostId, atMs: dueAtMs + 50, durationMs: 1000 })
            report.starts.push({
                policyId,
                dueAtMs,
                atMs: dueAtMs + 30,
                admittedAtMs: dueAtMs + 20,
                durableAtMs: dueAtMs + 25,
                attemptId: policyId + '-' + period,
                durable: true,
            })
            report.observations.push({
                policyId,
                checkedAtMs: dueAtMs + 1050,
                committedAtMs: dueAtMs + 1500,
                commitTimeSource: 'first-visible-observation-upper-bound',
                status: expectedStatus,
                code: expectedCode,
            })
        }
    }
    return report
}

function traffic(): MonitoringApplianceTraffic[] {
    return Array.from({ length: 200 }, (_, index) => ({
        kind: index % 2 ? 'ssr' : 'proxy',
        atMs: 1_000_000 + index * 500,
        elapsedMs: 20,
        status: 200,
        validBody: true,
    }))
}

function resources(): MonitoringApplianceResource[] {
    return Array.from({ length: 121 }, (_, index) => ({
        atMs: 1_000_000 + index * 1000,
        monotonicMs: 100 + index * 1000,
        cpuMicroseconds: 1_000_000 + index * 10_000,
        rssBytes: 400 * 1024 ** 2,
        workingSetBytes: 420 * 1024 ** 2,
        memoryBytes: 450 * 1024 ** 2,
        pids: 35,
        fds: 300,
        postgresConnections: 12,
        memoryLimitBytes: 1024 ** 3,
        pidLimit: 512,
        cpuQuotaCores: 2,
        postgresConnectionLimit: 100,
        oomKills: 0,
        roles: {
            web: { processes: 1, rssBytes: 100, fds: 20 },
            controller: { processes: 1, rssBytes: 100, fds: 20 },
            caddy: { processes: 1, rssBytes: 100, fds: 20 },
        },
        database: {
            commits: index,
            rollbacks: 0,
            blocksRead: 10,
            blocksHit: 10 + index,
            inserted: index * 2,
            updated: index * 3,
            deleted: index,
        },
    }))
}

/** Model a supervisor deadline and process identity without real sleeps or subprocesses. */
function continuityFixture() {
    const original = progress(),
        after = progress(),
        signals: string[] = []
    let now = original.originMs,
        monotonic = 0,
        state = 'S',
        ticks = '124',
        alive = true,
        reads = 0
    let failReport = false,
        reuseAfterReport = false,
        exitOnContinue = false
    original.endMs = now
    original.starts = []
    original.heads = []
    const dependencies = {
        owner: { pid: 300, startTicks: '124' },
        report: () => {
            if (reads++ === 0) return Promise.resolve(original)
            if (failReport) return Promise.reject(new Error('stopped report failed'))
            after.endMs = now
            if (reuseAfterReport) ticks = 'reused'
            return Promise.resolve(after)
        },
        readOwner: () =>
            Promise.resolve(alive ? { pid: 300, startTicks: ticks, state, atMs: now } : null),
        signal: (signal: 'SIGSTOP' | 'SIGCONT') => {
            signals.push(signal)
            state = signal === 'SIGSTOP' ? 'T' : 'S'
            if (signal === 'SIGSTOP') {
                const shift = now - after.originMs
                for (const start of after.starts) {
                    start.atMs += shift
                    start.admittedAtMs += shift
                    start.durableAtMs += shift
                }
                for (const head of after.heads) head.atMs += shift
            }
            if (signal === 'SIGCONT' && exitOnContinue) alive = false
            return Promise.resolve()
        },
        sleep: (milliseconds: number) => {
            now += milliseconds
            monotonic += milliseconds
            return Promise.resolve()
        },
        monotonic: () => monotonic,
    }
    return {
        dependencies,
        original,
        after,
        signals,
        advance: (ms: number) => {
            now += ms
            monotonic += ms
            if (state === 'T' && monotonic >= 33000) alive = false
        },
        failReport: () => {
            failReport = true
        },
        reuseAfterReport: () => {
            reuseAfterReport = true
        },
        exitOnContinue: () => {
            exitOnContinue = true
        },
    }
}

describe('whole-appliance monitoring reference evidence', () => {
    test('rejects an old baseline harness before it can create an externally reachable fixture', async () => {
        const root = await mkdtemp(join(tmpdir(), 'monitoring-b2-harness-test-'))
        try {
            await mkdir(join(root, 'scripts/runtime-reliability'), { recursive: true })
            await writeFile(
                join(root, 'scripts/runtime-reliability/harness.ts'),
                'export async function createHarness(){throw new Error("fixture must not start")}',
            )
            await expect(
                runMonitoringAppliance({
                    baselineRoot: root,
                    baselineSha: 'a'.repeat(40),
                    baselineImageIdentity: 'sha256:' + 'b'.repeat(64),
                    image: 'unused',
                    nativeBinary: 'unused',
                    reportPath: 'unused',
                    topologyOnly: true,
                    safetyOnly: false,
                    diagnosticOnly: false,
                }),
            ).rejects.toThrow('baseline harness must support verified internal network isolation')
        } finally {
            expect(dirname(root)).toBe(tmpdir())
            await rm(root, { recursive: true, force: true })
        }
    })
    test('cannot qualify native isolation from endpoint success or an incorrectly privileged Unix role', () => {
        const keys = [
            'nativeStopped',
            'freshAuthorityAdmitted',
            'foreignStoreRejected',
            'foreignEpochRejected',
            'nullIdentityClaimRejected',
            'unauthorizedWriteRejected',
            'withdrawnSqlGrantRejected',
            'restoredSqlGrantAdmitted',
            'committedConfigRejected',
            'unpublishedConfigClaimRejected',
            'oldGenerationRejectedAfterPublish',
            'freshGenerationAdmitted',
            'oldStoreEpochRejected',
            'resetRequiresPublication',
            'oldStoreEpochRejectedAfterPublish',
            'committedApplicationGrantRejected',
            'unpublishedApplicationGrantClaimRejected',
            'freshApplicationGrantAdmitted',
            'managementCapabilityRejected',
            'appSecretReadRejected',
            'controllerSecretReadRejected',
            'rootControlRejected',
            'webControlRejected',
            'probeTokenRejectedByController',
            'nativeGroupsRestricted',
        ]
        const evidence = {
            ...Object.fromEntries(keys.map((key) => [key, true])),
            unauthorizedWrites: 0,
            nativeHeads: 0,
            nativeUID: 20004,
            nativeGID: 20004,
            nativeGroups: '20004',
            brokerManagementDenials: 5,
            directManagementDenials: 5,
            rootControlStatus: 403,
            webControlStatus: 403,
            appSecretErrno: 'EACCES',
            controllerSecretErrno: 'EACCES',
            rootCapErrno: 'EPERM',
            webCapErrno: 'EACCES',
        }
        const value = { scenario: 'fences', passed: true, evidence, failures: [] }
        expect(assessMonitoringApplianceRecovery(value, 'fences').state).toBe('passed')
        expect(
            assessMonitoringApplianceRecovery(
                { ...value, evidence: { ...evidence, nativeGID: 10002 } },
                'fences',
            ).state,
        ).toBe('failed')
        expect(
            assessMonitoringApplianceRecovery(
                { ...value, evidence: { ...evidence, nativeGroups: '10002 20004' } },
                'fences',
            ).state,
        ).toBe('failed')
        expect(
            assessMonitoringApplianceRecovery(
                { ...value, evidence: { ...evidence, appSecretErrno: 'ENOENT' } },
                'fences',
            ).state,
        ).toBe('failed')
        const { controllerSecretErrno: omitted, ...incomplete } = evidence
        expect(omitted).toBe('EACCES')
        expect(
            assessMonitoringApplianceRecovery({ ...value, evidence: incomplete }, 'fences').state,
        ).toBe('incomplete')
    })
    test('never upgrades endpoint success or missing replay timing/fence evidence into a safety pass', () => {
        expect(monitoringApplianceFaultCases).toHaveLength(12)
        expect(
            assessMonitoringApplianceRecovery(
                { scenario: 'replay', passed: true, failures: [], evidence: {} },
                'replay',
            ).state,
        ).toBe('incomplete')
        const evidence = {
            nativeStopped: true,
            records: 1000,
            rawDurableRecords: 1000,
            distinctRawPolicies: 1000,
            nativeHeads: 1000,
            distinctNativeHosts: 1000,
            replayedResults: 1000,
            observations: 1000,
            pending: 0,
            rejectedApplied: 0,
            staleCommits: 0,
            duplicateObservations: 0,
            duplicateIncidents: 0,
            duplicateRecords: 16,
            duplicateReceipts: 16,
            noWakeReplayPassed: true,
            duplicateReplayPassed: true,
            conflictingReplayRejected: true,
            replayMs: 29999.5,
        }
        const result = { scenario: 'replay', passed: true, failures: [], evidence }
        expect(assessMonitoringApplianceRecovery(result, 'replay').state).toBe('passed')
        expect(
            assessMonitoringApplianceRecovery(
                { ...result, evidence: { ...evidence, staleCommits: null } },
                'replay',
            ).state,
        ).toBe('incomplete')
        expect(
            assessMonitoringApplianceRecovery(
                { ...result, evidence: { ...evidence, replayMs: 30000.1 } },
                'replay',
            ).state,
        ).toBe('failed')
        expect(
            assessMonitoringApplianceRecovery(
                { ...result, evidence: { ...evidence, rejectedApplied: 1 } },
                'replay',
            ).state,
        ).toBe('failed')
        expect(assessMonitoringApplianceRecovery(result, 'gap').state).toBe('incomplete')
    })
    test('marks the private overlay separately and verifies preserved original assets without a baked runtime owner', () => {
        const seal = {
            prototype: true as const,
            baseRevision: 'a'.repeat(40),
            baseImageIdentity: 'sha256:' + 'b'.repeat(64),
            experimentSha256: 'c'.repeat(64),
            nativeBinarySha256: 'd'.repeat(64),
        }
        const dockerfile = monitoringPrototypeDockerfile(
            seal,
            'rentnerproxy-b2-base:012345abcdef',
            'e'.repeat(64),
        )
        expect(dockerfile).toContain('Test-only wrapper and native prototype')
        expect(dockerfile).toContain(
            'io.rentnerproxy.experiment.base-image="' + seal.baseImageIdentity + '"',
        )
        expect(dockerfile).toContain('cmp monitoring-b2-original-assets.sha256')
        expect(dockerfile).toContain('cmp monitoring-b2-original-symlinks.txt')
        expect(dockerfile).toContain(
            'sha256sum /usr/local/bin/rentnerproxy-controller /usr/bin/caddy /usr/local/bin/bun',
        )
        expect(dockerfile).toContain('sha256sum -c monitoring-b2-original-native.sha256')
        expect(dockerfile).toContain(
            '/usr/local/bin/rentnerproxy-production /usr/bin/tini > monitoring-b2-original-native.sha256',
        )
        expect(dockerfile).toContain(
            'COPY monitoring-b2-prototype-entrypoint /usr/local/bin/rentnerproxy-b2-prototype',
        )
        expect(dockerfile).toContain(
            'ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/rentnerproxy-b2-prototype"]',
        )
        expect(dockerfile).toContain('groupadd --gid 20004 monitoring-b2')
        expect(dockerfile).toContain('useradd --uid 20004 --gid monitoring-b2')
        expect(dockerfile).toContain('usermod --append --groups monitoring-b2 rentnerproxy-web')
        expect(dockerfile).toContain('chown 0:20004 /tmp/monitoring-b2/state')
        expect(dockerfile).not.toContain('chown 0:10002 /tmp/monitoring-b2/state')
        expect(dockerfile).not.toContain('mode.json')
        expect(() =>
            monitoringPrototypeDockerfile(seal, 'unowned:latest', 'e'.repeat(64)),
        ).toThrow()
        expect(() =>
            monitoringPrototypeDockerfile(
                { ...seal, nativeBinarySha256: 'bad' },
                'rentnerproxy-b2-base:012345abcdef',
                'e'.repeat(64),
            ),
        ).toThrow()
    })
    test('rejects stale native build manifests instead of assigning new source identity to an old binary', () => {
        const binary = 'a'.repeat(64),
            source = 'b'.repeat(64),
            lock = 'e'.repeat(64),
            cargo = 'f'.repeat(64)
        const manifest = {
            binarySha256: binary,
            rustSourceSha256: source,
            compiler: 'rustc 1.99.1 (abc123 2026-10-13)',
            target: 'x86_64-unknown-linux-gnu' as const,
            builderImage: 'sha256:' + 'c'.repeat(64),
            builderReference: 'rentnerproxy-backup-progress:rust-checks',
            compilerVerboseSha256: '1'.repeat(64),
            cargoLockSha256: lock,
            cargoManifestSha256: cargo,
            profile: 'release',
            offline: true,
            network: 'none',
            registry: 'read-only-local-cargo-cache',
        } as const
        expect(
            validateMonitoringApplianceNativeManifest(manifest, binary, source, lock, cargo),
        ).toEqual(manifest)
        expect(() =>
            validateMonitoringApplianceNativeManifest(
                manifest,
                'd'.repeat(64),
                source,
                lock,
                cargo,
            ),
        ).toThrow('manifest mismatch')
        expect(() =>
            validateMonitoringApplianceNativeManifest(
                manifest,
                binary,
                'd'.repeat(64),
                lock,
                cargo,
            ),
        ).toThrow('manifest mismatch')
        expect(() =>
            validateMonitoringApplianceNativeManifest(
                { ...manifest, target: 'windows' },
                binary,
                source,
                lock,
                cargo,
            ),
        ).toThrow()
        for (const changed of [
            { cargoLockSha256: '0'.repeat(64) },
            { cargoManifestSha256: '0'.repeat(64) },
            { profile: 'debug' },
            { offline: false },
            { network: 'default' },
            { builderImage: 'rentnerproxy-backup-progress:rust-checks' },
        ])
            expect(() =>
                validateMonitoringApplianceNativeManifest(
                    { ...manifest, ...changed },
                    binary,
                    source,
                    lock,
                    cargo,
                ),
            ).toThrow()
    })
    test('hashes the explicit experiment source separately and excludes Rust build products', async () => {
        const root = await mkdtemp(join(tmpdir(), 'monitoring-b2-source-test-'))
        try {
            const source = await monitoringApplianceOverlay()
            await Promise.all(
                source.paths.map(async (path) => {
                    await mkdir(dirname(join(root, path)), { recursive: true })
                    await writeFile(join(root, path), 'source:' + path)
                }),
            )
            const first = await monitoringApplianceOverlay(root)
            await mkdir(join(root, 'scripts/runtime-scale/monitoring-b2/target'), {
                recursive: true,
            })
            await writeFile(
                join(root, 'scripts/runtime-scale/monitoring-b2/target/generated'),
                'not source',
            )
            expect((await monitoringApplianceOverlay(root)).sha256).toBe(first.sha256)
            await writeFile(
                join(root, 'scripts/runtime-scale/monitoring-b2-web.ts'),
                'changed source',
            )
            expect((await monitoringApplianceOverlay(root)).sha256).not.toBe(first.sha256)
            expect(first.sha256).toMatch(/^[a-f0-9]{64}$/u)
        } finally {
            expect(dirname(root)).toBe(tmpdir())
            await rm(root, { recursive: true, force: true })
        }
    })
    test('compares every matched pair to fixed CPU, RSS and connection budgets before considering benefit', () => {
        const windows: MonitoringAppliancePairInput[] = monitoringApplianceWindows().map((window) =>
            Object.assign({}, window, {
                cpuPercent: window.design === 'A' ? 20 : 15,
                rssMiB: 400,
                peakDatabaseConnections: 10,
                failures: [],
            }),
        )
        const valid = assessMonitoringAppliancePairs(windows)
        expect(valid.failures).toEqual([])
        expect(valid.efficiencyJustified).toBe(true)
        expect(valid.minimumCpuBenefit).toBe(0.25)
        const boundary = windows.map((window) =>
            Object.assign(
                {},
                window,
                window.design === 'B2'
                    ? { cpuPercent: 22, rssMiB: 432, peakDatabaseConnections: 12 }
                    : {},
            ),
        )
        expect(assessMonitoringAppliancePairs(boundary).failures).toEqual([])
        const exactBenefit = windows.map((window) =>
            Object.assign({}, window, window.design === 'B2' ? { cpuPercent: 16 } : {}),
        )
        expect(assessMonitoringAppliancePairs(exactBenefit).efficiencyJustified).toBe(true)
        expect(assessMonitoringAppliancePairs(windows.slice(1)).failures).toContain(
            'missing-or-duplicate-matched-pair',
        )
        const expensive = windows.map((window) => ({
            ...window,
            ...(window.design === 'B2'
                ? { cpuPercent: 23, rssMiB: 433, peakDatabaseConnections: 13 }
                : {}),
        }))
        expect(assessMonitoringAppliancePairs(expensive).failures).toContain(
            'added-resource-budget-exceeded',
        )
        const noisy = windows.map((window) => ({
            ...window,
            ...(window.pair === 3 ? { cpuPercent: window.cpuPercent! * 1.5 } : {}),
        }))
        expect(assessMonitoringAppliancePairs(noisy).efficiencyJustified).toBe(false)
        const absent = windows.map((window) => Object.assign({}, window, { cpuPercent: null }))
        expect(assessMonitoringAppliancePairs(absent).failures).toContain(
            'invalid-or-missing-resource-evidence',
        )
    })
    test('keeps host latency outside the halted interval and preserves every fresh-start gate', async () => {
        const fixture = continuityFixture()
        // Docker starts once before STOP; its return happens after local CONT.
        fixture.advance(3100)
        const capture = await collectMonitoringApplianceContinuity(
            fixture.dependencies,
            contract.confirmedBunStopMs,
        )
        fixture.advance(3100)
        const result = proveMonitoringApplianceContinuity(capture)
        expect(result.failures).toEqual([])
        expect(result.confirmedBunStopMs).toBe(25000)
        expect(result.distinctDurableStartsAfterStop).toBe(1000)
        expect(result.actualNativeHeads).toBe(1000)
        expect(fixture.signals).toEqual(['SIGSTOP', 'SIGCONT'])
        expect(capture.resumed.outcome).toBe('resumed')
        for (const start of capture.after.starts) start.admittedAtMs = capture.stopped.atMs - 1
        expect(proveMonitoringApplianceContinuity(capture).failures).toContain(
            'fresh-admission-and-durable-native-start-threshold',
        )
    })
    test('always delivers CONT to the same owner when the stopped report fails', async () => {
        const fixture = continuityFixture()
        fixture.failReport()
        await expect(
            collectMonitoringApplianceContinuity(fixture.dependencies, 25000),
        ).rejects.toThrow('stopped report failed')
        expect(fixture.signals).toEqual(['SIGSTOP', 'SIGCONT'])
    })
    test('still resumes the exact owner after a lost STOP acknowledgement', async () => {
        const fixture = continuityFixture()
        const signal = fixture.dependencies.signal
        fixture.dependencies.signal = async (value) => {
            await signal(value)
            if (value === 'SIGSTOP') throw new Error('lost STOP response')
        }
        await expect(
            collectMonitoringApplianceContinuity(fixture.dependencies, 25000),
        ).rejects.toThrow('lost STOP response')
        expect(fixture.signals).toEqual(['SIGSTOP', 'SIGCONT'])
    })
    test('never signals a reused PID and never presents a vanished owner as resumed', async () => {
        const reused = continuityFixture()
        reused.reuseAfterReport()
        await expect(
            collectMonitoringApplianceContinuity(reused.dependencies, 25000),
        ).rejects.toThrow('B2 continuity web identity changed')
        expect(reused.signals).toEqual(['SIGSTOP'])
        const exited = continuityFixture()
        exited.exitOnContinue()
        const capture = await collectMonitoringApplianceContinuity(exited.dependencies, 25000)
        expect(capture.resumed.delivered).toBe(true)
        expect(capture.resumed.outcome).toBe('exited')
        expect(capture.resumed.after).toBeNull()
        expect(proveMonitoringApplianceContinuity(capture).failures).toEqual([])
    })
    test('host rejects absent, short or unhalted report evidence and changed window identity', async () => {
        const fixture = continuityFixture()
        const capture = await collectMonitoringApplianceContinuity(fixture.dependencies, 25000)
        expect(() =>
            proveMonitoringApplianceContinuity({ ...capture, resumed: undefined }),
        ).toThrow()
        const short = await collectMonitoringApplianceContinuity(
            continuityFixture().dependencies,
            24999,
        )
        expect(proveMonitoringApplianceContinuity(short).failures).toContain(
            'bun-stop-duration-or-identity-unproven',
        )
        expect(
            proveMonitoringApplianceContinuity({ ...capture, confirmedMonotonicMs: 24999 })
                .failures,
        ).toContain('bun-stop-duration-or-identity-unproven')
        expect(
            proveMonitoringApplianceContinuity({
                ...capture,
                afterReport: { ...capture.afterReport, state: 'S' },
            }).failures,
        ).toContain('bun-stop-duration-or-identity-unproven')
        expect(
            proveMonitoringApplianceContinuity({
                ...capture,
                after: { ...capture.after, windowId: 'other' },
            }).failures,
        ).toContain('native-controller-or-window-identity-changed')
        expect(
            proveMonitoringApplianceContinuity({
                ...capture,
                after: { ...capture.after, endMs: capture.afterReport.atMs + 1 },
            }).failures,
        ).toContain('bun-stop-duration-or-identity-unproven')
    })
    test('requires an explicit measurement window and refuses workload or duration overrides', () => {
        const args = [
            '--baseline-root',
            '.',
            '--baseline-sha',
            'a'.repeat(40),
            '--baseline-image-id',
            'sha256:' + 'b'.repeat(64),
            '--image',
            'fixture:exact',
            '--native-binary',
            'fixture-binary',
            '--report',
            'tmp/report.json',
            '--measurement-window',
            'approved',
        ]
        expect(parseMonitoringApplianceOptions(args).topologyOnly).toBe(false)
        expect(parseMonitoringApplianceOptions(args).baselineSha).toBe('a'.repeat(40))
        expect(parseMonitoringApplianceOptions(args).baselineImageIdentity).toBe(
            'sha256:' + 'b'.repeat(64),
        )
        for (const [flag, invalid] of [
            ['--baseline-sha', 'main'],
            ['--baseline-image-id', 'fixture:mutable'],
        ] as const) {
            const invalidArgs = [...args]
            invalidArgs[invalidArgs.indexOf(flag) + 1] = invalid
            expect(() => parseMonitoringApplianceOptions(invalidArgs)).toThrow()
            const missingArgs = [...args]
            missingArgs.splice(missingArgs.indexOf(flag), 2)
            expect(() => parseMonitoringApplianceOptions(missingArgs)).toThrow()
        }
        expect(parseMonitoringApplianceOptions(args).diagnosticOnly).toBe(false)
        const diagnostic = parseMonitoringApplianceOptions([
            ...args.slice(0, -1),
            'publication-diagnostic-approved',
        ])
        expect(diagnostic.diagnosticOnly).toBe(true)
        expect(diagnostic.safetyOnly).toBe(true)
        expect(diagnostic.topologyOnly).toBe(false)
        expect(
            parseMonitoringApplianceOptions([...args.slice(0, -1), 'topology-approved'])
                .topologyOnly,
        ).toBe(true)
        for (const extra of [
            ['--duration', '10'],
            ['--policies', '100'],
            ['--image', 'other'],
        ])
            expect(() => parseMonitoringApplianceOptions([...args, ...extra])).toThrow()
        expect(() => parseMonitoringApplianceOptions(args.slice(0, -2))).toThrow()
    })
    test('requires a timely common original wake bound to the run, window and live web owner', () => {
        const report = progress()
        const expected = { runId: '012345abcdef', webIdentity: { pid: 300, startTicks: '124' } }
        expect(assessMonitoringApplianceWake(report, expected)).toEqual([])
        expect(
            assessMonitoringApplianceWake(report, { ...expected, runId: 'abcdef012345' }),
        ).toHaveLength(1)
        expect(
            assessMonitoringApplianceWake(report, {
                ...expected,
                webIdentity: { pid: 300, startTicks: '125' },
            }),
        ).toHaveLength(1)
        for (const delta of [
            { design: 'A' as const },
            { windowId: 'other' },
            { originMs: 999_999 },
            { publishedAtMs: 1_000_000 },
            { publishedAtMs: 994_999 },
            { wakeCount: 0 },
        ]) {
            expect(
                assessMonitoringApplianceWake(
                    { ...report, wakeProof: { ...report.wakeProof!, ...delta } },
                    expected,
                ),
            ).toHaveLength(1)
        }
        expect(
            summarizeMonitoringApplianceProgress({ ...report, wakeProof: null }, traffic())
                .failures,
        ).toContain('common-original-web-wake-unproven')
    })
    test('requires bounded original admission storage evidence instead of hiding the additional SQL journal', () => {
        const report = progress()
        for (const admissionStore of [
            null,
            { ...report.admissionStore!, records: 4097 },
            { ...report.admissionStore!, maxRecords: 5000 },
            { ...report.admissionStore!, admitted: 2001 },
            { ...report.admissionStore!, retentionMs: 180000 },
            { ...report.admissionStore!, pressure: true },
        ])
            expect(
                summarizeMonitoringApplianceProgress({ ...report, admissionStore }, traffic())
                    .failures,
            ).toContain('original-admission-store-bound-unproven')
    })
    test('declares exactly three alternating matched pairs for each full profile', () => {
        const windows = monitoringApplianceWindows()
        expect(windows).toHaveLength(12)
        expect(new Set(windows.map((window) => window.windowId)).size).toBe(12)
        for (const profile of ['mixed', 'all-3s-timeout']) {
            const selected = windows.filter((window) => window.profile === profile)
            expect(selected.map((window) => window.design)).toEqual([
                'A',
                'B2',
                'B2',
                'A',
                'A',
                'B2',
            ])
            expect(selected.map((window) => window.pair)).toEqual([1, 1, 2, 2, 3, 3])
        }
        expect(() => monitoringApplianceResourceProbe(1000)).toThrow()
        expect(monitoringApplianceResourceProbe(120000)).toContain('120000')
        expect(() => monitoringApplianceTrafficProbe(NaN)).toThrow()
        expect(() =>
            new Bun.Transpiler({ loader: 'js' }).scan(monitoringApplianceTrafficProbe(1_000_000)),
        ).not.toThrow()
        expect(() =>
            new Bun.Transpiler({ loader: 'js' }).scan(monitoringApplianceResourceProbe(120000)),
        ).not.toThrow()
    })
    test('accepts two actual ordinal executions and first-visible committed outcomes for every policy', () => {
        const result = summarizeMonitoringApplianceProgress(progress(), traffic())
        expect(result.failures).toEqual([])
        expect(result.coveredSlots).toBe(2000)
        expect(result.dueToStart.p99).toBe(50)
        expect(result.dueToCommit.max).toBe(1500)
    })
    test('does not replace lost, duplicate, unknown or stale evidence with queue or process activity', () => {
        const missing = progress()
        missing.heads.pop()
        expect(summarizeMonitoringApplianceProgress(missing, traffic()).failures).toContain(
            'exact-two-cadence-coverage',
        )
        const duplicate = progress()
        duplicate.heads.push({ ...duplicate.heads[0]! })
        expect(summarizeMonitoringApplianceProgress(duplicate, traffic()).failures).toContain(
            'duplicate-execution-or-observation',
        )
        const unknown = progress()
        unknown.observations[0]!.status = 'unknown'
        expect(summarizeMonitoringApplianceProgress(unknown, traffic()).failures).toContain(
            'unexpected-or-unproven-outcome',
        )
        const wrongCode = progress()
        wrongCode.observations[0]!.code = 'controller_unavailable'
        expect(summarizeMonitoringApplianceProgress(wrongCode, traffic()).failures).toContain(
            'unexpected-or-unproven-outcome',
        )
        const undurable = progress()
        undurable.starts[0]!.durable = false
        expect(summarizeMonitoringApplianceProgress(undurable, traffic()).failures).toContain(
            'execution-authority-coverage',
        )
        const staleAuthority = progress()
        staleAuthority.starts[0]!.durableAtMs = staleAuthority.starts[0]!.admittedAtMs - 1
        expect(summarizeMonitoringApplianceProgress(staleAuthority, traffic()).failures).toContain(
            'execution-authority-coverage',
        )
        const backlog = progress()
        backlog.backlog = 1
        expect(summarizeMonitoringApplianceProgress(backlog, traffic()).failures).toContain(
            'growing-or-undrained-backlog',
        )
    })
    test('rejects late cadence, slow traffic, failed SSR and missing load rather than reporting a passing capacity window', () => {
        const late = progress()
        late.heads[0]!.atMs += 60_000
        late.observations[0]!.committedAtMs += 60_000
        expect(summarizeMonitoringApplianceProgress(late, traffic()).failures).toContain(
            'due-to-commit-gate',
        )
        const slow = traffic().map((item) => Object.assign({}, item, { elapsedMs: 251 }))
        expect(summarizeMonitoringApplianceProgress(progress(), slow).failures).toContain(
            'ssr-or-proxy-gate',
        )
        const errors = traffic()
        errors[0]!.validBody = false
        expect(summarizeMonitoringApplianceProgress(progress(), errors).trafficErrors).toBe(1)
        expect(summarizeMonitoringApplianceProgress(progress(), []).failures).toContain(
            'ssr-or-proxy-gate',
        )
    })
    test('uses complete cgroup CPU deltas and retains the distinct summed RSS, working set, FD, PID and SQL observations', () => {
        const result = summarizeMonitoringApplianceResources(resources())
        expect(result.failures).toEqual([])
        expect(result.cpuPercent).toBe(1)
        const clockStep = resources()
        clockStep.forEach((sample, index) => {
            sample.atMs -= index * 2000
        })
        expect(summarizeMonitoringApplianceResources(clockStep).cpuPercent).toBe(1)
        expect(result.rssMiB).toBe(400)
        expect(result.workingSetMiB).toBe(420)
        expect(result.peakFds).toBe(300)
        expect(result.peakPids).toBe(35)
        expect(result.peakDatabaseConnections).toBe(12)
        expect(result.databaseDelta).toEqual({
            commits: 120,
            rollbacks: 0,
            blocksRead: 0,
            blocksHit: 120,
            inserted: 240,
            updated: 360,
            deleted: 120,
        })
    })
    test('fails closed on relaxed limits, owner loss, missing samples, OOM and invalid counters', () => {
        const relaxed = resources()
        relaxed[0]!.memoryLimitBytes *= 2
        expect(summarizeMonitoringApplianceResources(relaxed).failures).toContain(
            'reference-envelope-mismatch',
        )
        const missing = resources()
        missing[1]!.roles.web!.processes = 2
        expect(summarizeMonitoringApplianceResources(missing).failures).toContain(
            'required-runtime-owner-missing-or-duplicated',
        )
        const oom = resources()
        oom[0]!.oomKills = 1
        expect(summarizeMonitoringApplianceResources(oom).failures).toContain(
            'reference-resource-limit',
        )
        expect(summarizeMonitoringApplianceResources(resources().slice(0, 2)).failures).toContain(
            'incomplete-resource-window',
        )
        expect(() =>
            monitoringApplianceResourceSchema.parse({ ...resources()[0]!, cpuMicroseconds: NaN }),
        ).toThrow()
    })
})
