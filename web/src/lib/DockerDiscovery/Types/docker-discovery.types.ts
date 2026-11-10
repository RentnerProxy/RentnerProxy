import type { z } from 'zod'
import type {
    dockerDiscoverySnapshotSchema,
    saveDockerDiscoverySchema,
    dockerDiscoveryErrors,
    dockerDiscoveryCursorSchema,
    dockerDiscoveryObservationSchema,
} from '../validation.ts'

export type DockerDiscoverySnapshot = z.infer<typeof dockerDiscoverySnapshotSchema>
export type SaveDockerDiscovery = z.infer<typeof saveDockerDiscoverySchema>
export type DockerDiscoveryErrorCode = (typeof dockerDiscoveryErrors)[number]
export type DockerDiscoveryCursor = z.infer<typeof dockerDiscoveryCursorSchema>
export type DockerDiscoveryObservation = z.infer<typeof dockerDiscoveryObservationSchema>
