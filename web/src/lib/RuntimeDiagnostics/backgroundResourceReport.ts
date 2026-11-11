import { z } from 'zod'
import { BACKGROUND_DATABASE_POLICY } from '@/config/background-resources.config.ts'

const count = z.number().int().min(0).max(Number.MAX_SAFE_INTEGER)
/** Validate only safe nonnegative counters while enforcing the configured class running and pending ceilings. */
function usage(policy: { readonly running: number; readonly pending: number }) {
    return z.object({
        running: count.max(policy.running),
        pending: count.max(policy.pending),
        oldestPendingAgeMs: count,
        busy: count,
        deadline: count,
        cancelled: count,
        admitted: count,
    })
}
const classes = BACKGROUND_DATABASE_POLICY.classes
const resources = z.object({
    sampledAt: count,
    database: z.object({
        monitoring: usage(classes.monitoring),
        notifications: usage(classes.notifications),
        discovery: usage(classes.discovery),
        housekeeping: usage(classes.housekeeping),
        critical: usage(classes.critical),
    }),
})

/** Only fixed numeric resource fields leave the server; unsupported/missing samples remain unavailable. */
export function readBackgroundResourceReport(input: unknown) {
    try {
        const result = resources.safeParse(input)
        return result.success ? result.data : null
    } catch {
        return null
    }
}

export type BackgroundResourceReport = z.infer<typeof resources>
