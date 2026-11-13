import { expect, test } from 'bun:test'
import { runWebTestBatches, selectWebTests } from '../../../../../scripts/test-web.ts'

const paths = Array.from({ length: 2_000 }, (_, index) =>
    `./web/src/tests/features/${'long component 😀 '.repeat(5)}${String(index).padStart(4, '0')}.test.tsx`,
)
const command = ['C:\\Program Files\\Bun\\bun.exe', '--no-env-file', 'test', '--max-concurrency=1', '--isolate']

test('runs a large suite once in sorted bounded commands retaining isolation flags', async () => {
    const selected = selectWebTests(paths.toReversed(), '--unit')
    const commands: string[][] = []
    let running = false
    expect(await runWebTestBatches(selected, async (args) => {
        expect(running).toBe(false)
        running = true
        commands.push(args)
        await Promise.resolve()
        running = false
        return 0
    }, command)).toBe(0)
    expect(commands.length).toBeGreaterThan(1)
    expect(commands.flatMap((args) => args.slice(command.length))).toEqual(paths.toSorted())
    for (const args of commands) {
        expect(args.slice(0, command.length)).toEqual(command)
        // Even quoting and escaping every UTF-16 character fits CreateProcess's limit.
        expect(args.map((arg) => `"${arg.replaceAll(/./gu, '\\$&')}"`).join(' ').length + 1)
            .toBeLessThan(32_767)
    }
})

test('returns the first failing batch status and never starts later batches', async () => {
    let calls = 0
    const visited: string[] = []
    const result = await runWebTestBatches(paths, async (args) => {
        calls += 1
        visited.push(...args.slice(command.length))
        return calls === 2 ? 23 : 0
    }, command)
    expect(result).toBe(23)
    expect(calls).toBe(2)
    expect(visited.length).toBeLessThan(paths.length)
    expect(visited).toEqual(paths.slice(0, visited.length))
})

test('keeps unit, database, and fuzz selections disjoint and rejects an empty suite', async () => {
    const mixed = ['b.test.tsx', 'a.test.ts', 'a.db.test.ts', 'a.fuzz.test.ts', 'helper.ts']
    expect(selectWebTests(mixed, '--unit')).toEqual(['a.test.ts', 'b.test.tsx'])
    expect(selectWebTests(mixed, '--database')).toEqual(['a.db.test.ts'])
    expect(selectWebTests(mixed, '--fuzz')).toEqual(['a.fuzz.test.ts'])
    let executed = false
    await expect(runWebTestBatches([], async () => { executed = true; return 0 })).rejects.toThrow('No matching')
    expect(executed).toBe(false)
})

test('rejects an oversized single argument before starting any batch', async () => {
    let executed = false
    await expect(runWebTestBatches([...paths, 'x'.repeat(33_000)], async () => {
        executed = true
        return 0
    }, command)).rejects.toThrow('command-line limit')
    expect(executed).toBe(false)
})
