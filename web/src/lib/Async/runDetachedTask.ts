function reportReporterFailure(): void {
    try {
        console.error('[async] Error reporting failed')
    } catch {
        // Both error sinks failed; keep the terminal handler from creating another rejection.
    }
}

/** Own failures from deliberately detached work, including synchronous failures and a failing error reporter. */
export function runDetachedTask(
    operation: () => unknown,
    onError: (error: unknown) => unknown,
): void {
    const reportFailure = (error: unknown): void => {
        try {
            void Promise.resolve(onError(error)).catch(reportReporterFailure)
        } catch {
            reportReporterFailure()
        }
    }

    try {
        void Promise.resolve(operation()).catch(reportFailure)
    } catch (error) {
        reportFailure(error)
    }
}
