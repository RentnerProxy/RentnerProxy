import { expect, test } from 'bun:test'
import { parseMonitoringB2BuildOptions } from '../../../../../scripts/runtime-scale/monitoring-b2-build.ts'

test('offline builds require an explicit registry cache and accept only named build inputs', () => {
    expect(() => parseMonitoringB2BuildOptions([])).toThrow('explicit local Cargo registry')
    expect(
        parseMonitoringB2BuildOptions([
            '--registry-cache',
            '/local/registry',
            '--output',
            '/repo/tmp/b2-offline',
            '--builder-image',
            'sha256:' + 'a'.repeat(64),
        ]),
    ).toEqual({
        registryCache: '/local/registry',
        output: '/repo/tmp/b2-offline',
        builderImage: 'sha256:' + 'a'.repeat(64),
    })
    for (const args of [
        ['--registry-cache'],
        ['--registry-cache', '/local/registry', '--registry-cache', '/other'],
        ['--registry-cache', '/local/registry', '--network', 'default'],
    ])
        expect(() => parseMonitoringB2BuildOptions(args)).toThrow('Invalid B2 offline build option')
})
