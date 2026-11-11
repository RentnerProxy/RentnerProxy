import type { z } from 'zod'
import type { monitoringB2ClaimSchema } from '../monitoring-b2.validation.ts'
import type { SQL } from 'bun'
import type { createMonitoringB2Authority } from '../monitoring-b2-authority.ts'

export type MonitoringB2Claim = z.infer<typeof monitoringB2ClaimSchema>
export interface MonitoringB2Runtime {
    readonly namespace: string
    readonly storeId: string
    readonly controllerEpoch: string
    readonly path: string
}
export interface MonitoringB2RoleLifecycle {
    create: (role: string, password: string) => Promise<void>
    drop: (role: string) => Promise<void>
}
export interface MonitoringB2RecoveryContext {
    readonly database: SQL
    state: () =>
        | {
              design: 'A' | 'B2'
              profile: 'mixed' | 'all-3s-timeout'
              windowId: string
              originMs: number
              hosts: readonly { policyId: string; hostId: string; domain: string; index: number }[]
          }
        | undefined
    authority: () => Awaited<ReturnType<typeof createMonitoringB2Authority>> | undefined
    outboxPath: () => string
    heads: () => readonly { hostId: string; atMs: number; durationMs: number }[]
    start: () => Promise<{ originMs: number; design: 'A' | 'B2'; policies: number }>
    stopNative: () => Promise<void>
    restartNative: () => Promise<void>
    controllerAvailable: (enabled: boolean) => Promise<void>
    configureHost?: (
        enabled: boolean,
    ) => Promise<{ hostId: string; changedAtMs: number; revision: string }>
    fastProbes: (enabled: boolean) => void
    quiesce: () => Promise<{
        quiesced: boolean
        nativeStopped: boolean
        activeNativeHeads: number
        quiescedAtMs?: number
    }>
}
export interface MonitoringB2RawRecord {
    readonly sequence: number
    readonly attempt_id: string
    readonly store_id: string
    readonly claim: string
    readonly started_ms: number
    readonly admitted_ms: number | null
    readonly durable_ms: number | null
    readonly completed_ms: number | null
    readonly result: string | null
}
