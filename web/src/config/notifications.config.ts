export const NOTIFICATION_CATEGORIES = [
    'certificate_renewal',
    'ddns',
    'backup',
    'proxy_configuration',
    'host_availability',
] as const
export const NOTIFICATION_SEVERITIES = ['warning', 'error', 'critical'] as const
export const NOTIFICATION_SETTINGS_KEY = 'notification_configuration_v1'
export const NOTIFICATION_STATE_KEY = 'notification_delivery_state_v1'
export const NOTIFICATION_SECRET_CONTEXT = 'rentnerproxy:notifications:v1:'
export const NOTIFICATION_LIMITS = {
    channels: 16,
    totalChannels: 256,
    incidents: 1_024,
    deliveries: 256,
    payloadBytes: 16_384,
    stateBytes: 4 * 1_024 * 1_024,
    attempts: 8,
    retentionMs: 7 * 24 * 60 * 60 * 1_000,
    deliveryLifetimeMs: 24 * 60 * 60 * 1_000,
    leaseMs: 90_000,
    retryInitialMs: 30_000,
    retryMaximumMs: 60 * 60 * 1_000,
    transportTimeoutMs: 15_000,
    dnsTimeoutMs: 5_000,
    pollIntervalMs: 15_000,
    proxyApplyGraceMs: 30_000,
    deliveriesPerTick: 8,
    concurrency: 8,
    channelConcurrency: 1,
    ownerConcurrency: 2,
    transportConcurrency: 4,
    categoryConcurrency: 4,
    rateWindowMs: 1_000,
    globalRate: 16,
    channelRate: 4,
    ownerRate: 8,
    transportRate: 8,
    categoryRate: 8,
    wakeCoalesceMs: 50,
    retryJitterRatio: 0.2,
    collectionTimeoutMs: 35_000,
} as const
