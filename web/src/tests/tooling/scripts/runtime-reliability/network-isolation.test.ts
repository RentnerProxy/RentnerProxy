import { expect, test } from 'bun:test'
import { reliabilityNetworkArguments } from '../../../../../../scripts/runtime-reliability/harness.ts'
import { smokeDockerArguments } from '../../../../../../scripts/smoke/resources.ts'

test('internal network isolation is explicit and preserves ordinary harness network arguments', () => {
    const network = 'rentnerproxy-reliability-012345abcdef-network'
    expect(reliabilityNetworkArguments(network)).toEqual(['network', 'create', network])
    expect(reliabilityNetworkArguments(network, false)).toEqual(['network', 'create', network])
    const isolated = reliabilityNetworkArguments(network, true)
    expect(isolated).toEqual(['network', 'create', '--internal', network])
    const scoped = smokeDockerArguments(['docker', ...isolated])
    expect(scoped).toContain('--internal')
    expect(scoped.at(-1)).toBe(network)
})
