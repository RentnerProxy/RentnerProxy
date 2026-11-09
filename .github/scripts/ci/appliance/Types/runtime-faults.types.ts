export type ApplianceRuntimeFaultContext = {
    readonly recreatedId: string
    readonly httpPort: number
    readonly httpsPort: number
    readonly managementPort: number
    readonly hostDomain: string
    readonly trafficMarker: string
    readonly trafficCaFile?: string
    readonly command: (args: string[], timeoutMs?: number) => Promise<string>
    readonly inspect: (id: string, format: string) => Promise<string>
    readonly passed: (label: string) => void
}
