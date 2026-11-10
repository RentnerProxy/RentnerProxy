import type { NotificationObservation } from '@/lib/Notifications/Types/notifications.types.ts'
import type { CertificateEventPage } from '@/lib/Admin/CertificateManagement/Types/certificates.types.ts'
import type { ControllerCertificateMetadata } from '@/server/Controller/Types/certificates.types.ts'
import type { DdnsSnapshot } from '@/lib/Ddns/Types/ddns.types.ts'
import type { ProxyRuntimeStatus } from '@/lib/ProxyRuntime/Types/proxy-runtime.types.ts'

export interface NotificationCollection {
    readonly observations: readonly NotificationObservation[]
    readonly certificateCursor: string | null
}

export interface NotificationCollectorDependencies {
    readonly certificates: (
        signal?: AbortSignal,
    ) => Promise<readonly ControllerCertificateMetadata[]>
    readonly certificateEvents: (
        after: string | null,
        limit: number,
        signal?: AbortSignal,
    ) => Promise<CertificateEventPage>
    readonly ddns: (signal?: AbortSignal) => Promise<DdnsSnapshot>
    readonly proxySnapshot: () => Promise<{ readonly revision: string }>
    readonly proxyStatus: (signal?: AbortSignal) => Promise<ProxyRuntimeStatus | null>
    readonly now: () => number
}
