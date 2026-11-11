import { describe, expect, test } from 'bun:test'
import { resolve } from 'node:path'
import { monitoringB2PublicationFragment } from '../../../../../scripts/runtime-scale/monitoring-b2-publisher.ts'
import { runMonitoringB2PublicationCase } from '../../../../../scripts/runtime-scale/monitoring-b2-publication-case.ts'

const source = `import { n as readProxyRuntimeSnapshot } from "./runtime.js";
import { h as HOST_MONITORING_LIMITS } from "./limits.js";
import { l as lockProxyRuntimeSettings } from "./settings.js";
function contextHash(value) {
 return JSON.stringify(value);
}
function createHostMonitoringContext(host, snapshot) {
 return {host,snapshot};
}
async function readHostMonitoringContextsInTransaction(transaction, inputs) {
 return new Map(inputs.map(input=>[input.hostId,transaction]));
}
async function lockMonitoring(transaction) {
 await transaction.execute(sql\`select 1\`);
}
async function prepareWorkerTransaction(transaction, runtimeSettings = true) {
 if (runtimeSettings) await lockProxyRuntimeSettings(transaction);
 await lockMonitoring(transaction);
}
export { readProxyHostSummariesInTransaction as G, DomainDiagnosticsError as A };`

describe('original canonical B2 publication bridge', () => {
    test('retains exact original declarations with original module bindings and separate source proofs', () => {
        const fragment = monitoringB2PublicationFragment(resolve('fixture/original.js'), source)
        expect(fragment.proof).toMatchObject({ functions: 5, originalSingletonImports: true })
        expect(fragment.proof.originalChunkSha256).toMatch(/^[a-f0-9]{64}$/u)
        expect(fragment.proof.fragmentSha256).toMatch(/^[a-f0-9]{64}$/u)
        expect(fragment.code).toContain('await transaction.execute(sql`select 1`);')
        expect(fragment.code).toContain('G as readProxyHostSummariesInTransaction')
        expect(fragment.code).not.toContain('getAuthDatabase')
        expect(() =>
            new Bun.Transpiler({ target: 'bun' }).transformSync(fragment.code),
        ).not.toThrow()
    })
    test('fails closed when canonical declarations or singleton bindings no longer match the pinned shape', () => {
        expect(() =>
            monitoringB2PublicationFragment(
                resolve('fixture/original.js'),
                source.replace('function contextHash(value)', 'function changed(value)'),
            ),
        ).toThrow('canonical declaration')
        expect(() =>
            monitoringB2PublicationFragment(
                resolve('fixture/original.js'),
                source.replace('./runtime.js', 'https://fixture.invalid/runtime.js'),
            ),
        ).toThrow('original application import')
        expect(() =>
            monitoringB2PublicationFragment(
                resolve('fixture/original.js'),
                source.replace('readProxyHostSummariesInTransaction as G', 'unexpected as G'),
            ),
        ).toThrow('canonical export')
    })
    test('cannot turn an unowned context into an automatic publication proof', async () => {
        const result = await runMonitoringB2PublicationCase({
            state: () => undefined,
            authority: () => undefined,
        } as never)
        expect(result.passed).toBe(false)
        expect(result.evidence).toEqual({ nativeStopped: false })
        expect(result.failures).toEqual(['owned-prepared-B2-required'])
    })
})
