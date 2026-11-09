import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test('context reads are ID-filtered, freshly authorized, bounded read-only transactions and never expose snapshot secrets', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        const id = '0198d98a-0000-7000-8000-000000000001'; const calls = []; let allowed = true; let present = true
        mock.module('./server/Auth/Core/database.server.ts', () => ({ getAuthDatabase: () => ({ transaction: async (work, options) => { calls.push(options); return work({ execute: async () => calls.push('timeout') }) } }) }))
        mock.module('./server/Auth/Access/rbac.service.ts', () => ({ requirePermissionInTransaction: async (_tx, actor, permission) => { calls.push({ actor, permission }); if (!allowed) throw new Error('denied') } }))
        const read = async (_tx, ids) => { calls.push({ ids }); return present ? [{ id, domains: ['app.example.com'], enabled: true, updatedAt: 'PRIVATE', forwardHost: 'PRIVATE-UPSTREAM' }] : [] }
        mock.module('./server/Admin/ProxyHostManagement/proxy-hosts.service.ts', () => ({ readProxyHostSummariesInTransaction: read }))
        mock.module('./server/Admin/RedirectHostManagement/redirect-hosts.service.ts', () => ({ readRedirectHostSummariesInTransaction: read }))
        mock.module('./server/ProxyRuntime/proxy-runtime-data.ts', () => ({ readProxyRuntimeSnapshot: async () => { calls.push('snapshot'); return { revision: 'sha256:' + 'a'.repeat(64), privateKeyPem: 'PRIVATE-PEM' } } }))
        const { readDomainDiagnosticsContext: readContext } = await import('./server/RuntimeDiagnostics/domain-diagnostics-data.ts')
        const input = { hostType: 'proxy', hostId: id, domain: 'app.example.com' }
        const result = await readContext('actor', input)
        const initial = [...calls]; calls.length = 0
        allowed = false; await readContext('actor', input).catch(() => undefined); const denied = [...calls]; calls.length = 0; allowed = true
        const foreign = await readContext('actor', { ...input, domain: 'foreign.example.com' }).catch(e => e.code)
        present = false; const missing = await readContext('actor', input).catch(e => e.code)
        console.log(JSON.stringify({ result, initial, denied, foreign, missing }))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    const result = JSON.parse(output)
    expect(result.initial[0]).toEqual({
        isolationLevel: 'repeatable read',
        accessMode: 'read only',
    })
    expect(result.initial.slice(1)).toEqual([
        'timeout',
        { actor: 'actor', permission: 'proxy_hosts.view' },
        { ids: ['0198d98a-0000-7000-8000-000000000001'] },
        'snapshot',
    ])
    expect(result.denied).toHaveLength(3)
    expect(result.foreign).toBe('invalid_input')
    expect(result.missing).toBe('invalid_input')
    expect(result.result.proof).toMatch(/^sha256:[a-f0-9]{64}$/u)
    expect(JSON.stringify(result.result)).not.toContain('PRIVATE')
    expect(Object.keys(result.result).toSorted()).toEqual(['enabled', 'proof', 'revision'])
})

test('monitoring continuity ignores unrelated host/runtime metadata but includes upstream inputs and the referenced CA', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        const id = '0198d98a-0000-7000-8000-000000000001'
        const caId = '0198d98a-0000-7000-8000-000000000002'
        const baseHost = {
            id, domains: ['app.example.com', 'other.example.com'], enabled: true,
            forwardScheme: 'https', forwardHost: 'PRIVATE-UPSTREAM', forwardPort: 443,
            verifyUpstreamTls: true, upstreamTlsServerName: 'PRIVATE-SNI', trustedCaId: caId,
            certificateId: 'inbound-certificate', forceHttps: false, accessPolicyId: null,
            createdAt: 'initial-created', updatedAt: 'initial-updated',
            discovery: { state: 'managed' }, certificateJob: { stage: 'issuing' },
        }
        const baseSnapshot = {
            revision: 'sha256:' + 'a'.repeat(64),
            trustedCas: [{ id: caId, pem: 'PRIVATE-CA-PEM', fingerprintSha256: 'sha256:' + 'b'.repeat(64) }],
        }
        let host = structuredClone(baseHost); let snapshot = structuredClone(baseSnapshot)
        let hostReads = 0; let snapshotReads = 0
        mock.module('./server/Auth/Core/database.server.ts', () => ({ getAuthDatabase: () => { throw new Error('unexpected transaction') } }))
        mock.module('./server/Auth/Access/rbac.service.ts', () => ({ requirePermissionInTransaction: async () => { throw new Error('unexpected interactive auth') } }))
        mock.module('./server/Admin/ProxyHostManagement/proxy-hosts.service.ts', () => ({
            readProxyHostSummariesInTransaction: async (_tx, ids) => {
                if (ids.length !== 1 || ids[0] !== id) throw new Error('unbounded host read')
                hostReads++; return [host]
            },
        }))
        mock.module('./server/Admin/RedirectHostManagement/redirect-hosts.service.ts', () => ({ readRedirectHostSummariesInTransaction: async () => { throw new Error('unexpected redirect read') } }))
        mock.module('./server/ProxyRuntime/proxy-runtime-data.ts', () => ({ readProxyRuntimeSnapshot: async () => { snapshotReads++; return snapshot } }))
        const { readDomainDiagnosticsContextInTransaction: interactive, readHostMonitoringContextInTransaction: monitoring } = await import('./server/RuntimeDiagnostics/domain-diagnostics-data.ts')
        const input = { hostType: 'proxy', hostId: id, domain: 'app.example.com' }
        const read = () => monitoring({}, input)
        const baseline = await read()
        const interactiveBaseline = await interactive({}, input)
        const same = (a, b) => JSON.stringify(a) === JSON.stringify(b)
        const reset = () => { host = structuredClone(baseHost); snapshot = structuredClone(baseSnapshot) }
        const irrelevant = []
        for (const patch of [
            { discovery: { state: 'orphaned' } }, { certificateJob: { stage: 'applying' } },
            { certificateId: 'other-inbound-certificate' }, { accessPolicyId: 'new-access-policy' },
            { updatedAt: 'changed-updated' }, { responseCache: { enabled: true } },
            { browserCache: { enabled: true } }, { tags: [{ name: 'new-tag' }] },
            { forceHttps: true }, { domains: [...baseHost.domains].reverse() },
        ]) {
            reset(); Object.assign(host, patch)
            const result = await read()
            irrelevant.push({ sameAvailability: same(result.availability, baseline.availability), fullProofChanged: result.proof !== baseline.proof })
        }
        reset(); snapshot.revision = 'sha256:' + 'c'.repeat(64)
        snapshot.proxyHosts = [{ id: 'other-host', forwardHost: 'other-upstream' }]
        snapshot.httpSettings = { responseCache: { enabled: true } }
        const unrelatedRuntime = await read()
        const relevant = []
        for (const patch of [
            { domains: ['app.example.com'] }, { enabled: false }, { forwardScheme: 'http' },
            { forwardHost: 'different-upstream' }, { forwardPort: 8443 },
            { verifyUpstreamTls: false }, { upstreamTlsServerName: 'different-sni' }, { trustedCaId: null },
        ]) {
            reset(); Object.assign(host, patch)
            const result = await read()
            relevant.push({ proofChanged: result.availability.proof !== baseline.availability.proof, revisionChanged: result.availability.revision !== baseline.availability.revision })
        }
        const caChanges = []
        for (const patch of [{ pem: 'ROTATED-PRIVATE-CA' }, { fingerprintSha256: 'sha256:' + 'd'.repeat(64) }]) {
            reset(); Object.assign(snapshot.trustedCas[0], patch)
            const result = await read()
            caChanges.push({ sameProof: result.availability.proof === baseline.availability.proof, revisionChanged: result.availability.revision !== baseline.availability.revision })
        }
        reset(); snapshot.trustedCas.push({ id: 'unreferenced-ca', pem: 'UNRELATED-PRIVATE-CA', fingerprintSha256: 'sha256:' + 'e'.repeat(64) })
        const unrelatedCa = await read()
        const foreign = await monitoring({}, { ...input, domain: 'foreign.example.com' }).catch(error => error.code)
        const redirect = await monitoring({}, { ...input, hostType: 'redirect' }).catch(error => error.code)
        console.log(JSON.stringify({
            baseline, interactiveBaseline, irrelevant, relevant, caChanges,
            unrelatedRuntime: { sameAvailability: same(unrelatedRuntime.availability, baseline.availability), fullRevisionChanged: unrelatedRuntime.revision !== baseline.revision },
            unrelatedCaUnchanged: same(unrelatedCa.availability, baseline.availability), foreign, redirect,
            hostReads, snapshotReads,
        }))
        `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    const result = JSON.parse(output)
    expect(result.interactiveBaseline).toEqual({
        enabled: result.baseline.enabled,
        proof: result.baseline.proof,
        revision: result.baseline.revision,
    })
    expect(result.irrelevant).toHaveLength(10)
    for (const value of result.irrelevant)
        expect(value).toEqual({ sameAvailability: true, fullProofChanged: true })
    expect(result.unrelatedRuntime).toEqual({
        sameAvailability: true,
        fullRevisionChanged: true,
    })
    expect(result.unrelatedCaUnchanged).toBe(true)
    expect(result.relevant).toHaveLength(8)
    for (const value of result.relevant)
        expect(value).toEqual({ proofChanged: true, revisionChanged: true })
    expect(result.caChanges).toHaveLength(2)
    for (const value of result.caChanges)
        expect(value).toEqual({ sameProof: true, revisionChanged: true })
    expect(result.foreign).toBe('invalid_input')
    expect(result.redirect).toBe('invalid_input')
    expect(result.hostReads).toBe(result.snapshotReads + 1)
    expect(JSON.stringify(result.baseline)).not.toContain('PRIVATE')
    expect(result.baseline.availability.proof).toMatch(/^sha256:[a-f0-9]{64}$/u)
    expect(result.baseline.availability.revision).toMatch(/^sha256:[a-f0-9]{64}$/u)
})

test('monitoring batches share transaction-local reads while retaining fresh global and host continuity fences', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        const ids = [1, 2, 3, 4].map(value => '0198d98a-0000-7000-8000-' + String(value).padStart(12, '0'))
        const caId = '0198d98a-0000-7000-8000-000000000005'
        const hosts = [ids[0], ids[1], ids[3]].map((id, index) => ({
            id, domains: [index + '.example.com'], enabled: index !== 1,
            forwardScheme: 'https', forwardHost: 'PRIVATE-UPSTREAM-' + index, forwardPort: 443,
            verifyUpstreamTls: true, upstreamTlsServerName: null, trustedCaId: caId,
            certificateId: null, forceHttps: false, accessPolicyId: null,
        }))
        const snapshot = {
            revision: 'sha256:' + 'a'.repeat(64),
            trustedCas: [{ id: caId, pem: 'PRIVATE-CA', fingerprintSha256: 'sha256:' + 'b'.repeat(64) }],
        }
        const transaction = {}; const calls = []; let failSnapshot = false
        mock.module('./server/Auth/Core/database.server.ts', () => ({ getAuthDatabase: () => { throw new Error('unexpected transaction') } }))
        mock.module('./server/Auth/Access/rbac.service.ts', () => ({ requirePermissionInTransaction: async () => { throw new Error('unexpected interactive auth') } }))
        mock.module('./server/Admin/ProxyHostManagement/proxy-hosts.service.ts', () => ({
            readProxyHostSummariesInTransaction: async (tx, requested) => {
                if (tx !== transaction) throw new Error('wrong transaction')
                calls.push({ ids: requested }); return hosts.filter(host => requested.includes(host.id))
            },
        }))
        mock.module('./server/Admin/RedirectHostManagement/redirect-hosts.service.ts', () => ({ readRedirectHostSummariesInTransaction: async () => { throw new Error('unexpected redirect read') } }))
        mock.module('./server/ProxyRuntime/proxy-runtime-data.ts', () => ({ readProxyRuntimeSnapshot: async tx => {
            if (tx !== transaction) throw new Error('wrong transaction')
            calls.push('snapshot'); if (failSnapshot) throw new Error('snapshot unavailable'); return snapshot
        } }))
        const { readHostMonitoringContextsInTransaction: batch, readHostMonitoringContextInTransaction: single } = await import('./server/RuntimeDiagnostics/domain-diagnostics-data.ts')
        const input = (hostId, domain) => ({ hostType: 'proxy', hostId, domain })
        const inputs = [input(ids[0], '0.example.com'), input(ids[1], '1.example.com'), input(ids[2], 'missing.example.com'), input(ids[3], 'foreign.example.com')]
        const read = async () => Object.fromEntries(await batch(transaction, [...inputs, inputs[0]]))
        const baseline = await read(); const firstCalls = calls.splice(0)
        const singleMatches = JSON.stringify(baseline[ids[0]]) === JSON.stringify(await single(transaction, inputs[0]))
        calls.length = 0
        snapshot.revision = 'sha256:' + 'c'.repeat(64)
        const globalChange = await read(); const globalCalls = calls.splice(0)
        snapshot.trustedCas[0].pem = 'ROTATED-PRIVATE-CA'
        const caChange = await read(); const caCalls = calls.splice(0)
        hosts[0].forwardHost = 'CHANGED-PRIVATE-UPSTREAM'
        const hostChange = await read(); calls.length = 0
        const emptySize = (await batch(transaction, [])).size; const emptyCalls = calls.splice(0)
        const invalid = await batch(transaction, [...inputs, { ...inputs[0], hostType: 'redirect' }]).catch(error => error.code)
        const invalidCalls = calls.splice(0)
        const missing = Object.fromEntries(await batch(transaction, inputs.slice(2))); const missingCalls = calls.splice(0)
        failSnapshot = true; const failure = await batch(transaction, inputs).catch(error => error.message)
        const same = (left, right) => JSON.stringify(left) === JSON.stringify(right)
        console.log(JSON.stringify({
            ids, baseline, firstCalls, singleMatches, globalCalls, caCalls,
            globalChange: ids.slice(0, 2).map(id => ({ fullRevisionChanged: globalChange[id].revision !== baseline[id].revision, sameAvailability: same(globalChange[id].availability, baseline[id].availability) })),
            caChange: ids.slice(0, 2).map(id => ({ fullProofUnchanged: caChange[id].proof === baseline[id].proof, continuityProofUnchanged: caChange[id].availability.proof === baseline[id].availability.proof, continuityRevisionChanged: caChange[id].availability.revision !== baseline[id].availability.revision })),
            hostChange: { fullProofChanged: hostChange[ids[0]].proof !== baseline[ids[0]].proof, continuityChanged: hostChange[ids[0]].availability.proof !== baseline[ids[0]].availability.proof },
            emptySize, emptyCalls, invalid, invalidCalls, missing, missingCalls, failure,
        }))
        `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    const result = JSON.parse(output)
    const [firstId, secondId, missingId, foreignId] = result.ids
    const sharedReads = [{ ids: result.ids }, 'snapshot']
    expect(result.firstCalls).toEqual(sharedReads)
    expect(result.globalCalls).toEqual(sharedReads)
    expect(result.caCalls).toEqual(sharedReads)
    expect(result.singleMatches).toBe(true)
    expect(result.baseline[firstId].enabled).toBe(true)
    expect(result.baseline[secondId].enabled).toBe(false)
    expect(result.baseline[missingId]).toBeNull()
    expect(result.baseline[foreignId]).toBeNull()
    for (const change of result.globalChange)
        expect(change).toEqual({ fullRevisionChanged: true, sameAvailability: true })
    for (const change of result.caChange)
        expect(change).toEqual({
            fullProofUnchanged: true,
            continuityProofUnchanged: true,
            continuityRevisionChanged: true,
        })
    expect(result.hostChange).toEqual({ fullProofChanged: true, continuityChanged: true })
    expect(result.emptySize).toBe(0)
    expect(result.emptyCalls).toEqual([])
    expect(result.invalid).toBe('invalid_input')
    expect(result.invalidCalls).toEqual([])
    expect(result.missing).toEqual({ [missingId]: null, [foreignId]: null })
    expect(result.missingCalls).toEqual([{ ids: [missingId, foreignId] }])
    expect(result.failure).toBe('snapshot unavailable')
    expect(JSON.stringify(result.baseline)).not.toContain('PRIVATE')
})
