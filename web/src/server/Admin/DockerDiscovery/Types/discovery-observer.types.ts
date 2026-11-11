import type {
    DockerDiscoveryCursor,
    DockerDiscoveryObservation,
    DockerDiscoverySnapshot,
} from '@/lib/DockerDiscovery/Types/docker-discovery.types.ts'

export interface DiscoveryObservation {
    readonly cursor: DockerDiscoveryCursor | null
    readonly snapshot: DockerDiscoverySnapshot
}

export interface DiscoveryObserverDependencies {
    readonly enabled: () => Promise<boolean>
    readonly read: (signal: AbortSignal) => Promise<DiscoveryObservation>
    readonly watch: (
        cursor: DockerDiscoveryCursor,
        signal: AbortSignal,
    ) => Promise<DockerDiscoveryObservation>
    readonly reconcile: (
        snapshot: DockerDiscoverySnapshot,
        signal: AbortSignal,
        assertCurrent: () => void,
        resetBudget: boolean,
    ) => Promise<{
        enabled: boolean
        again: boolean
        /** The applied receipt committed or a durable newer intent superseded it. */
        receiptSettled?: boolean
    }>
    readonly subscribe: (invalidate: () => void) => () => void
    readonly failed: () => Promise<void>
}
