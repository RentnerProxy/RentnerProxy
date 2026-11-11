export const DATABASE_CONNECTION_BUDGET = {
    maxConnections: 16,
    backgroundTransactions: 8,
    interactiveHeadroom: 8,
    backgroundPending: 32,
    backgroundWaitMs: 1_000,
} as const
