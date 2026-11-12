import { expect, test } from 'bun:test'
import { runBunScript } from '@/tests/Helpers/runBunScript.ts'

test('the actual reconciler binds its AbortSignal separately from the critical SQL resource', async () => {
    const output = await runBunScript(
        `
        import { mock } from 'bun:test'
        mock.module('@tanstack/react-start/server-only', () => ({}))
        const resources = []
        let applied = 0
        mock.module('./server/env.server.ts', () => ({ isProxyRuntimeReconciliationEnabled: () => true }))
        mock.module('./server/Auth/Access/authorization.service.ts', () => ({ requirePermissionService: async () => ({ id: 'fixture' }) }))
        mock.module('./server/Auth/Core/database.server.ts', () => ({ getAuthDatabase: () => ({}) }))
        mock.module('./server/Foundation/background-database.server.ts', () => ({
            getBackgroundDatabase: resource => {
                if (resource !== 'critical') throw new Error('Unexpected resource binding')
                resources.push(resource)
                return { transaction: async operation => operation({}) }
            },
        }))
        mock.module('./server/ProxyRuntime/proxy-runtime-data.ts', () => ({
            readProxyRuntimeSnapshot: async () => ({ revision: 'revision-1' }),
        }))
        mock.module('./server/Controller/proxy.server.ts', () => ({
            applyProxyRuntimeConfigurationResult: async snapshot => {
                applied++
                return { status: 'applied', activeRevision: snapshot.revision, lastApplyAt: null }
            },
            getProxyRuntimeStatus: async () => ({ activeRevision: 'revision-1' }),
        }))
        mock.module('./server/Audit/audit.service.ts', () => ({ recordAuditEventBestEffortService: async () => {} }))
        const { reconcileProxyConfigurationService } = await import('./server/ProxyRuntime/proxy-runtime.service.ts')
        await reconcileProxyConfigurationService()
        await reconcileProxyConfigurationService.stop()
        console.log(JSON.stringify({ resources, applied }))
    `,
        { cwd: new URL('../../..', import.meta.url) },
    )
    expect(JSON.parse(output.trim().split('\n').at(-1)!)).toEqual({
        resources: ['critical', 'critical'],
        applied: 1,
    })
})
