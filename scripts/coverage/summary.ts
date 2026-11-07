import { appendFile, readFile, writeFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
    coverageThreshold,
    goCoverageGroupNames,
    rustCoverageGroupNames,
    typescriptCoverageGroups,
} from './coverage.config.ts'
import { coverageCount, coverageFailures, formatCoverageResults } from './report.ts'
import { clearPublishedCoverage } from './published.ts'
import type { CoverageProducer, CoverageResult } from './Types/report.types.ts'

export function parseCoverageSummary(value: unknown, producer: CoverageProducer): CoverageResult[] {
    if (value === null || typeof value !== 'object') throw new Error('Missing coverage summary.')
    const summary = value as Record<string, unknown>
    if (
        summary.version !== 1 ||
        summary.minimum !== coverageThreshold ||
        !Array.isArray(summary.groups) ||
        summary.groups.length === 0 ||
        (producer === 'typescript' && summary.testExitCode !== 0)
    )
        throw new Error('Coverage summary is incomplete or tests failed.')
    const expectedNames: readonly string[] =
        producer === 'typescript'
            ? ['TypeScript overall', ...typescriptCoverageGroups.map((group) => group.name)]
            : producer === 'go'
              ? goCoverageGroupNames
              : rustCoverageGroupNames
    const requiredMetrics =
        producer === 'typescript'
            ? (['lines', 'functions'] as const)
            : producer === 'go'
              ? (['statements'] as const)
              : (['lines', 'functions', 'regions'] as const)
    const seen = new Set<string>()
    const results = summary.groups.map((entry: unknown) => {
        if (entry === null || typeof entry !== 'object') throw new Error('Invalid coverage group.')
        const group = entry as Record<string, unknown>
        if (
            typeof group.name !== 'string' ||
            !expectedNames.includes(group.name) ||
            seen.has(group.name)
        )
            throw new Error('Missing or duplicate coverage group.')
        seen.add(group.name)
        const result: CoverageResult = { name: group.name }
        for (const metric of requiredMetrics) {
            if (group[metric] === null || typeof group[metric] !== 'object')
                throw new Error('Invalid coverage metric.')
            const count = group[metric] as Record<string, unknown>
            if (typeof count.covered !== 'number' || typeof count.total !== 'number')
                throw new Error('Missing coverage counts.')
            result[metric] = coverageCount(count.covered, count.total)
            if (result[metric]!.total === 0)
                throw new Error('Required coverage metric has no instrumented code.')
        }
        if (
            ![result.lines, result.functions, result.regions, result.statements].some(
                (count) => count && count.total > 0,
            )
        )
            throw new Error('Coverage group has no instrumented code.')
        return result
    })
    if (seen.size !== expectedNames.length) throw new Error('Required coverage groups are missing.')
    return results
}

export function selectedCoverageProducers(args: readonly string[]): CoverageProducer[] {
    if (args.length === 0) return ['typescript', 'go', 'rust']
    if (args.length !== 2 || args[0] !== '--producers')
        throw new Error('Expected --producers typescript,go,rust.')
    const names = args[1]!.split(',')
    if (
        names.length === 0 ||
        new Set(names).size !== names.length ||
        names.some((name) => !['typescript', 'go', 'rust'].includes(name))
    )
        throw new Error('Invalid selected coverage producers.')
    return names as CoverageProducer[]
}

export function combineSelectedCoverage(
    producers: readonly CoverageProducer[],
    summaries: Partial<Record<CoverageProducer, unknown>>,
): CoverageResult[] {
    if (producers.length === 0 || new Set(producers).size !== producers.length)
        throw new Error('Invalid selected coverage producers.')
    return producers.flatMap((producer) =>
        parseCoverageSummary(summaries[producer], producer).map((group) =>
            producer === 'go'
                ? Object.assign(group, {
                      name: group.name === 'all' ? 'Go overall' : 'Go ' + group.name,
                  })
                : group,
        ),
    )
}

export async function writeCombinedCoverage(args: readonly string[] = []): Promise<number> {
    const root = fileURLToPath(new URL('../..', import.meta.url))
    const output = resolve(root, 'coverage')
    await clearPublishedCoverage(output)
    const producers = selectedCoverageProducers(args)
    const paths: Record<CoverageProducer, string> = {
        typescript: 'coverage/typescript/summary.json',
        go: 'coverage/go/summary.json',
        rust: 'core/target/coverage/groups.json',
    }
    const entries = await Promise.all(
        producers.map(
            async (producer) =>
                [
                    producer,
                    JSON.parse(await readFile(resolve(root, paths[producer]), 'utf8')) as unknown,
                ] as const,
        ),
    )
    const groups = combineSelectedCoverage(producers, Object.fromEntries(entries))
    const report = formatCoverageResults(groups)
    await writeFile(
        resolve(output, 'summary.json'),
        JSON.stringify({ version: 1, minimum: coverageThreshold, producers, groups }, null, 2),
    )
    await writeFile(resolve(output, 'summary.md'), report)
    if (process.env.GITHUB_STEP_SUMMARY) await appendFile(process.env.GITHUB_STEP_SUMMARY, report)
    console.log(report)
    const failures = coverageFailures(groups)
    for (const failure of failures) console.error(failure)
    return failures.length === 0 ? 0 : 1
}

if (import.meta.main) process.exitCode = await writeCombinedCoverage(process.argv.slice(2))
