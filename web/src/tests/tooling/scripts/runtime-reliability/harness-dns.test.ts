import { expect, test } from 'bun:test'
import { createFixtureDnsAddressUpdater } from '../../../../../../scripts/runtime-reliability/harness.ts'
import { dnsFixtureResponse } from '../../../../../../scripts/smoke/certificates/dns-fixture.ts'
import {
    backgroundPolicies,
    parseBackgroundOptions,
} from '../../../../../../scripts/runtime-scale/background.ts'

const domain = 'reliability-012345abcdef.example.com'
const alias = (index: number, latency = 'healthy') => `bg-${index}-${latency}-${domain}`
function query(name: string) {
    const header = Buffer.alloc(12)
    header.writeUInt16BE(1, 4)
    return Buffer.concat([
        header,
        ...name
            .split('.')
            .map((label) => Buffer.concat([Buffer.from([label.length]), Buffer.from(label)])),
        Buffer.from([0, 0, 1, 0, 1]),
    ])
}

test('all boundary host SANs resolve to the owned appliance and remain registered across later restore/IP refreshes', () => {
    for (const profile of ['healthy', 'slow', 'timeout', 'mixed'] as const) {
        const addresses = new Map([['unrelated.invalid', '172.18.0.99']])
        const update = createFixtureDnsAddressUpdater(domain, addresses)
        const inventory = backgroundPolicies(
            parseBackgroundOptions(['--policies', '1000', '--profile', profile, '--seed', '397']),
        )
        const aliases = inventory.slice(-2).map((item) => alias(item.index, item.latency))
        update('172.18.0.2', aliases)
        update('172.18.0.3')
        update('172.18.0.4', aliases)
        for (const name of [domain, ...aliases]) {
            const response = dnsFixtureResponse(query(name), [], addresses)!
            expect(response.readUInt16BE(6)).toBe(1)
            expect([...response.subarray(-4)]).toEqual([172, 18, 0, 4])
        }
        expect(addresses.size).toBe(4)
        expect(addresses.get('unrelated.invalid')).toBe('172.18.0.99')
    }
})

test('DNS alias registration rejects arbitrary domains, indexes, latency labels, overflow and invalid IPs atomically', () => {
    const addresses = new Map<string, string>()
    const update = createFixtureDnsAddressUpdater(domain, addresses)
    update('172.18.0.2')
    for (const aliases of [
        [alias(998), alias(999), alias(999, 'slow')],
        ['arbitrary.example.com'],
        [alias(0)],
        [alias(1000)],
        [alias(998, 'mixed')],
        [alias(998, '*')],
        [alias(998).toUpperCase()],
        [alias(998) + '.unowned.invalid'],
        ['bg-998-healthy.reliability-ffffffffffff.example.com'],
        [alias(998) + '\n'],
        [alias(998), alias(998, 'slow')],
    ]) {
        expect(() => update('172.18.0.3', aliases)).toThrow()
        expect([...addresses]).toEqual([[domain, '172.18.0.2']])
    }
    expect(() => update('256.18.0.2', [alias(998)])).toThrow()
    expect([...addresses]).toEqual([[domain, '172.18.0.2']])
    update('172.18.0.3', [alias(998)])
    update('172.18.0.4', [alias(999)])
    const before = [...addresses]
    expect(() => update('172.18.0.5', [alias(999, 'timeout')])).toThrow()
    expect([...addresses]).toEqual(before)
    expect(() => createFixtureDnsAddressUpdater('unowned.example.com', addresses)).toThrow()
})
