import { describe, expect, test } from 'bun:test'
import { monitoringB2OriginalImport } from '../../../../../scripts/runtime-scale/monitoring-b2-bundled.ts'

describe('B2 original compiled application selector', () => {
    test('resolves the actual pinned image aliases without assuming generated export names', () => {
        const worker =
            'import { _ as completeHostMonitoringClaims, x as classifyHostMonitoringOutcome } from "./proxy-hosts.mutations.server-BXPPUeKl.js";\nimport { t as getBackgroundDatabase } from "./background-database.server-Fd1j9aJc.js";'
        expect(monitoringB2OriginalImport(worker, 'completeHostMonitoringClaims')).toEqual({
            path: './proxy-hosts.mutations.server-BXPPUeKl.js',
            exported: '_',
        })
        expect(monitoringB2OriginalImport(worker, 'getBackgroundDatabase')).toEqual({
            path: './background-database.server-Fd1j9aJc.js',
            exported: 't',
        })
        expect(
            monitoringB2OriginalImport(
                'import { f as getAuthDatabase } from "./audit.service-CsYjWpKN.js";',
                'getAuthDatabase',
            ),
        ).toEqual({ path: './audit.service-CsYjWpKN.js', exported: 'f' })
    })
    test('does not substitute a package, external URL or different application owner', () => {
        for (const source of [
            'import { getAuthDatabase } from "https://example.test/a.js"',
            'import { getAuthDatabase } from "some-other-package"',
            'import { getBackgroundDatabase } from "./background.js"',
        ])
            expect(() => monitoringB2OriginalImport(source, 'getAuthDatabase')).toThrow(
                'B2 original application import unavailable',
            )
    })
})
