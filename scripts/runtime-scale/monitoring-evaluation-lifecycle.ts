/** Drain every admitted policy before cleanup; the first failed probe stops further admission. */
export async function runMonitoringEvaluationProbes(
    policies: number,
    probe: (id: number) => Promise<void>,
): Promise<void> {
    let next = 1
    let failed = false
    let failure: unknown
    const worker = async () => {
        while (!failed && next <= policies) {
            const id = next++
            try {
                // oxlint-disable-next-line no-await-in-loop -- A worker owns one bounded policy operation until it settles.
                await probe(id)
            } catch (error) {
                if (!failed) {
                    failed = true
                    failure = error
                }
                throw error
            }
        }
    }
    await Promise.allSettled(Array.from({ length: Math.min(policies, 64) }, worker))
    if (failed) throw failure
}

/** Attempt independent cleanup steps in ownership order without losing earlier failures. */
export async function cleanupMonitoringEvaluation(
    operations: readonly (() => unknown | Promise<unknown>)[],
    executionFailures: readonly unknown[] = [],
): Promise<void> {
    const failures: unknown[] = []
    for (const operation of operations) {
        try {
            // oxlint-disable-next-line no-await-in-loop -- Close users before dropping their schema/role, then remove owned storage.
            await operation()
        } catch (error) {
            failures.push(error)
        }
    }
    if (failures.length)
        throw new AggregateError(
            [...executionFailures, ...failures],
            'Monitoring evaluation cleanup failed',
        )
}
