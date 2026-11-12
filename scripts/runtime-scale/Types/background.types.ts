import type { z } from 'zod'
import type { backgroundSnapshotSchema } from '../background.validation.ts'
import type { ResourceSample } from '../../runtime-reliability/Types/control.types.ts'

export type BackgroundOptions = {
    policies: 100 | 500 | 1000
    seed: number
    pattern: 'simultaneous' | 'staggered'
    profile: 'mixed' | 'healthy' | 'slow' | 'timeout'
    durationSeconds: number
    image?: string
    reportPath?: string
    extendedRecovery?: true
    contention?: true
}

export type BackgroundPolicy = {
    id: string
    index: number
    latency: 'healthy' | 'slow' | 'timeout' | 'failure'
    offsetMs: number
}

export type BackgroundSnapshot = z.output<typeof backgroundSnapshotSchema>
export type BackgroundResourceSample = {
    kind: 'continuous' | 'recovered'
    generation: string
    warmup: boolean
    sample: Omit<ResourceSample, 'phase'> & { phase: 'continuous' | 'recovered' }
}

export type BackgroundReceipt = {
    index: number
    startedAt: number
    finishedAt: number | null
    latency: BackgroundPolicy['latency']
}
