import type { AuditEventInput } from '@/lib/Admin/AuditLogs/Types/audit-events.types.ts'

const listeners = new Set<() => void>()

/** Local hints contain no metadata; direct/external database changes use the bounded safety poll. */
export function subscribeToDiscoveryChanges(listener: () => void): () => void {
    listeners.add(listener)
    return () => {
        listeners.delete(listener)
    }
}

export function publishDiscoveryChange(): void {
    for (const listener of listeners) {
        try {
            listener()
        } catch {
            continue
        }
    }
}

export function affectsDockerDiscovery(event: AuditEventInput): boolean {
    if (
        (event.actorKind !== 'system' && (event.actorKind !== 'user' || !event.actorUserId)) ||
        event.result === 'denied'
    )
        return false
    if (event.resource === 'security-settings')
        return Boolean(event.metadata?.changedFields?.includes('dockerDiscovery'))
    if (event.result !== 'success' || event.metadata?.changedFields?.length === 0) return false
    if (event.resource === 'user')
        return (
            ['delete', 'disable', 'enable'].includes(event.action) ||
            event.metadata?.changedFields?.some((field) =>
                ['status', 'roles', 'permissions'].includes(field),
            ) === true
        )
    return [
        'role',
        'proxy-host',
        'redirect-host',
        'access-policy',
        'basic-auth-account',
        'certificate',
        'trusted-ca',
        'proxy-runtime-settings',
        'proxy-host-settings',
        'proxy-runtime',
        'npm-import',
        'config-import',
    ].includes(event.resource)
}
