import { DATABASE_CONNECTION_BUDGET } from './database.config.ts'

/** Fixed partitions: idle capacity is deliberately not borrowed from interactive or critical work. */
export const BACKGROUND_DATABASE_POLICY = {
    ceiling: DATABASE_CONNECTION_BUDGET.backgroundTransactions,
    interactiveHeadroom: DATABASE_CONNECTION_BUDGET.interactiveHeadroom,
    classes: {
        monitoring: { running: 2, pending: 8, waitMs: 250, transactionMs: 2_000 },
        notifications: { running: 2, pending: 12, waitMs: 250, transactionMs: 5_000 },
        discovery: { running: 1, pending: 1, waitMs: 250, transactionMs: 20_000 },
        housekeeping: { running: 1, pending: 1, waitMs: 250, transactionMs: 2_000 },
        critical: { running: 2, pending: 8, waitMs: 1_000, transactionMs: 10_000 },
    },
} as const

/** Inventory of independent owners; these limits do not add a second job dispatcher. */
export const BACKGROUND_RESOURCE_ENVELOPE = {
    monitoring: { policies: 1_000, probes: 64, pendingClaims: 64, claimBatch: 16 },
    controller: {
        scheduledDiagnostics: 64,
        interactiveDiagnostics: 2,
        dnsAssistant: 2,
        upstreamHealth: 8,
    },
    notifications: { running: 8, perChannel: 1, perOwner: 2, perTransport: 4, durablePending: 256 },
    discovery: { running: 1, pendingInvalidations: 1, identities: 1_024 },
    certificates: { bindingJobs: 4, eventsPerPass: 500, receiptDeletesPerPass: 1_000 },
    realtime: { running: 4, pending: 32, waitMs: 500, pressureLogIntervalMs: 60_000 },
    // Short mixed-appliance fixture targets, not an unlimited-capacity promise.
    targets: {
        managementP95Ms: 500,
        managementP99Ms: 1_000,
        proxyErrors: 0,
        criticalProgressMs: 15_000,
    },
} as const
