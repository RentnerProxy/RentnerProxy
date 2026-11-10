import '@tanstack/react-start/server-only'
import { z } from 'zod'
import {
    dockerDiscoverySnapshotSchema,
    dockerDiscoveryErrors,
    dockerDiscoveryCursorSchema,
    dockerDiscoveryObservationSchema,
} from '@/lib/DockerDiscovery/validation.ts'
import { DockerDiscoveryError } from '@/server/Admin/DockerDiscovery/docker-discovery.errors.ts'
import type {
    DockerDiscoveryRequest,
    DockerDiscoverySaveRequest,
} from './Types/docker-discovery.types.ts'
import { controllerRequest } from './transport.server.ts'
import type { DockerDiscoveryCursor } from '@/lib/DockerDiscovery/Types/docker-discovery.types.ts'

function decode(value: unknown) {
    const error = z.strictObject({ error: z.enum(dockerDiscoveryErrors) }).safeParse(value)
    if (error.success) throw new DockerDiscoveryError(error.data.error)
    const parsed = dockerDiscoverySnapshotSchema.safeParse(value)
    if (!parsed.success) throw new DockerDiscoveryError('controller_unavailable')
    return parsed.data
}
export function createDockerDiscoveryController(
    request: DockerDiscoveryRequest = controllerRequest,
) {
    const options = {
        privileged: true,
        confidential: true,
        timeoutMs: 10_000,
        responseLimit: 1_048_576,
        acceptNonOkJson: true,
    } as const
    return {
        async inspect(input: { endpoint: string; actorUserId: string }) {
            const value = await request('/internal/v1/discovery/docker/inspect', {
                ...options,
                method: 'POST',
                timeoutMs: 35_000,
                body: JSON.stringify(input),
            })
            const error = z.strictObject({ error: z.enum(dockerDiscoveryErrors) }).safeParse(value)
            if (error.success) throw new DockerDiscoveryError(error.data.error)
            const parsed = z
                .strictObject({ candidates: dockerDiscoverySnapshotSchema.shape.candidates })
                .safeParse(value)
            if (!parsed.success) throw new DockerDiscoveryError('controller_unavailable')
            return parsed.data
        },
        async status(signal?: AbortSignal) {
            return decode(
                await request('/internal/v1/discovery/docker/status', {
                    ...options,
                    ...(signal ? { signal } : {}),
                }),
            )
        },
        async watch(cursor: DockerDiscoveryCursor | null, signal: AbortSignal) {
            const query =
                cursor === null
                    ? null
                    : new URLSearchParams({
                          runtimeId: dockerDiscoveryCursorSchema.parse(cursor).runtimeId,
                          generation: String(cursor.generation),
                      }).toString()
            const value = await request(
                query === null
                    ? '/internal/v1/discovery/docker/watch'
                    : `/internal/v1/discovery/docker/watch?${query}`,
                {
                    ...options,
                    timeoutMs: 30_000,
                    signal,
                },
            )
            const error = z.strictObject({ error: z.enum(dockerDiscoveryErrors) }).safeParse(value)
            if (error.success) throw new DockerDiscoveryError(error.data.error)
            const parsed = dockerDiscoveryObservationSchema.safeParse(value)
            if (!parsed.success) throw new DockerDiscoveryError('controller_unavailable')
            if (
                parsed.data.snapshot === null &&
                (!cursor ||
                    cursor.runtimeId !== parsed.data.cursor.runtimeId ||
                    cursor.generation !== parsed.data.cursor.generation)
            )
                throw new DockerDiscoveryError('controller_unavailable')
            return parsed.data
        },
        async save(input: DockerDiscoverySaveRequest) {
            const value = decode(
                await request('/internal/v1/discovery/docker/config', {
                    ...options,
                    method: 'PUT',
                    body: JSON.stringify(input),
                }),
            )
            if (
                value.configuration.revision !== input.expectedRevision + 1 ||
                value.configuration.settings.enabled !== input.configuration.enabled ||
                value.configuration.settings.endpoint !== input.configuration.endpoint
            )
                throw new DockerDiscoveryError('controller_unavailable')
            return value
        },
    }
}
export const dockerDiscoveryController = createDockerDiscoveryController()
