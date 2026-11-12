import { expect, test } from 'bun:test'
import { parsePostgresCpuProcess, postgresCpuDelta } from './postgresCpu.ts'

const identity = { pid: 42, backendStartEpoch: 1100, bootEpoch: 1000, clockTicks: 100 }
const stat =
    '42 (postgres (worker)) ' + ['S', ...Array.from({ length: 18 }, () => '0'), '10000'].join(' ')

test('PostgreSQL CPU identity accepts parenthesized names and nanosecond scheduler counters', () => {
    expect(parsePostgresCpuProcess(stat, '1234567 2000000 3', identity)).toEqual({
        pid: 42,
        startTicks: '10000',
        cpuNanoseconds: '1234567',
        runQueueNanoseconds: '2000000',
    })
})
test.each([
    ['foreign PID', stat.replace('42 (', '43 ('), '1 2 3', identity],
    ['dead process', stat.replace(') S ', ') Z '), '1 2 3', identity],
    ['wrong namespace', stat, '1 2 3', { ...identity, backendStartEpoch: 1200 }],
    ['missing schedstat', stat, '1 2', identity],
    ['negative counter', stat, '-1 2 3', identity],
    ['invalid clock', stat, '1 2 3', { ...identity, clockTicks: 0 }],
] as const)('rejects %s CPU observations', (_name, text, counters, owner) => {
    expect(() => parsePostgresCpuProcess(text, counters, owner)).toThrow()
})
test('backend CPU deltas distinguish completed generations, startup and missing exited counters', () => {
    const first = {
        pid: 42,
        startTicks: '10000',
        cpuNanoseconds: '1000000',
        runQueueNanoseconds: '2000000',
    }
    expect(
        postgresCpuDelta(
            [first],
            [{ ...first, cpuNanoseconds: '3500000', runQueueNanoseconds: '4000000' }],
        ),
    ).toMatchObject({ status: 'complete', processCpuMs: 2.5, runQueueMs: 2, startedBackends: 0 })
    const next = {
        pid: 43,
        startTicks: '10100',
        cpuNanoseconds: '500000',
        runQueueNanoseconds: '0',
    }
    expect(postgresCpuDelta([first], [next])).toMatchObject({
        status: 'insufficient',
        startedBackends: 1,
        exitedBackendsWithoutFinalCounter: 1,
    })
    expect(() => postgresCpuDelta([first], [{ ...first, startTicks: '10100' }])).toThrow()
    expect(() => postgresCpuDelta([first], [{ ...first, cpuNanoseconds: '1' }])).toThrow()
    expect(() => postgresCpuDelta([first], [first, first])).toThrow()
})
