export interface HostMonitoringBatchEntry<Input, Output> {
    readonly input: Input
    readonly resolve: (value: Output) => void
    readonly reject: (reason: unknown) => void
}

export interface HostMonitoringBatchOptions<Input, Output> {
    readonly size: number
    readonly delayMs: number
    readonly run: (inputs: readonly Input[]) => Promise<readonly PromiseSettledResult<Output>[]>
}
