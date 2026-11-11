import { proxyHostUpstreamHealthSchema } from '@/lib/ProxyRuntime/upstreamHealth.ts'
import type { ProxyHostUpstreamHealth } from '@/lib/ProxyRuntime/Types/upstream-health.types.ts'
import '@tanstack/react-start/server-only'
import { z } from 'zod'

import type {
    ProxyConfigSource,
    ProxyRuntimeStatus,
} from '@/lib/ProxyRuntime/Types/proxy-runtime.types.ts'

import {
    MAX_RUNTIME_PAYLOAD_BYTES,
    PROXY_RUNTIME_REVISION_PATTERN,
} from '@/server/ProxyRuntime/proxy-runtime-snapshot.ts'
import type {
    ProxyRuntimeApplyResponse,
    ProxyRuntimeSnapshot,
} from '@/server/ProxyRuntime/Types/proxy-runtime.types.ts'

import { controllerRequest, controllerRequestResult } from './transport.server.ts'
import type { ProxyRuntimeApplyResult } from '@/server/ProxyRuntime/Types/proxy-runtime.types.ts'
import { CONTROLLER_APPLY_TIMEOUT_MS } from '@/config/controller.config.ts'

const STATUS_TIMEOUT_MS = 2_000

const MAX_CONFIG_RESPONSE_BYTES = 32 * 1_024 * 1_024

const revisionSchema = z.string().regex(PROXY_RUNTIME_REVISION_PATTERN)

const timestampSchema = z
    .string()
    .max(40)
    .refine((value) => /^\d{4}-\d{2}-\d{2}T/u.test(value) && Number.isFinite(Date.parse(value)))
    .nullable()

const statusSchema = z.object({
    available: z.boolean(),
    running: z.boolean(),
    activeRevision: revisionSchema.nullable(),
    lastApplyAt: timestampSchema,
    recovery: z
        .object({
            workerRunning: z.boolean(),
            completedTicks: z.number().int().nonnegative().safe(),
            lastCompletedAgeSeconds: z.number().int().nonnegative().safe().nullable(),
            circuitOpen: z.boolean(),
            restartsInWindow: z.number().int().nonnegative().safe(),
            faults: z.number().int().nonnegative().safe(),
            consecutiveProbeFailures: z.number().int().nonnegative().safe().optional(),
        })
        .optional(),
})

const applySchema = z.object({
    status: z.enum(['applied', 'unchanged']),
    activeRevision: revisionSchema,
    lastApplyAt: timestampSchema,
})

/** Reads privileged runtime observations without duplicating the controller's recovery policy. */
export async function getProxyRuntimeStatus(
    signal?: AbortSignal,
): Promise<ProxyRuntimeStatus | null> {
    const payload = await controllerRequest('/internal/v1/proxy/status', {
        timeoutMs: STATUS_TIMEOUT_MS,
        ...(signal ? { signal } : {}),
        privileged: true,
    })
    const result = statusSchema.safeParse(payload)
    return result.success ? result.data : null
}

export async function applyProxyRuntimeConfiguration(
    snapshot: ProxyRuntimeSnapshot,
    timeoutMs = CONTROLLER_APPLY_TIMEOUT_MS,
): Promise<ProxyRuntimeApplyResponse | null> {
    const result = await applyProxyRuntimeConfigurationResult(snapshot, timeoutMs)
    return result.ok ? result.value : null
}

export async function applyProxyRuntimeConfigurationResult(
    snapshot: ProxyRuntimeSnapshot,
    timeoutMs = CONTROLLER_APPLY_TIMEOUT_MS,
    signal?: AbortSignal,
): Promise<ProxyRuntimeApplyResult> {
    const body = JSON.stringify(snapshot)
    if (Buffer.byteLength(body) > MAX_RUNTIME_PAYLOAD_BYTES)
        return {
            ok: false,
            kind: 'configuration',
            status: null,
            code: 'invalid_configuration',
            retryable: false,
        }
    if (timeoutMs <= 0)
        return { ok: false, kind: 'configuration', status: null, code: null, retryable: true }

    const response = await controllerRequestResult('/internal/v1/proxy/config', {
        timeoutMs: Math.min(timeoutMs, CONTROLLER_APPLY_TIMEOUT_MS),
        ...(signal ? { signal } : {}),
        privileged: true,
        confidential: snapshot.proxyHosts.some(
            (host) => host.accessPolicy?.basicAuth !== undefined,
        ),
        body,
    })
    if (!response.ok) {
        const codes = [
            'busy',
            'runtime_unavailable',
            'apply_failed',
            'invalid_configuration',
            'validation_failed',
            'unauthorized',
            'not_found',
        ]
        const code = response.code !== null && codes.includes(response.code) ? response.code : null
        // Only a rejected desired snapshot is stable across retries. Credentials,
        // controller endpoints and their configuration can recover independently.
        const rejectedSnapshot =
            response.kind === 'http' &&
            (response.status === 400 || response.status === 413 || response.status === 422) &&
            (code === 'invalid_configuration' || code === 'validation_failed')
        return { ...response, code, retryable: !rejectedSnapshot }
    }
    const result = applySchema.safeParse(response.payload)
    return result.success && result.data.activeRevision === snapshot.revision
        ? { ok: true, value: result.data }
        : { ok: false, kind: 'invalid_response', status: 200, code: null, retryable: true }
}

export async function getActiveProxyConfiguration(): Promise<ProxyConfigSource | null> {
    const payload = await controllerRequest('/internal/v1/proxy/config', {
        timeoutMs: CONTROLLER_APPLY_TIMEOUT_MS,
        privileged: true,
        responseLimit: MAX_CONFIG_RESPONSE_BYTES,
    })
    const result = z
        .object({
            config: z.string().max(MAX_CONFIG_RESPONSE_BYTES),
            activeRevision: revisionSchema.nullable(),
        })
        .safeParse(payload)
    return result.success
        ? { config: result.data.config, revision: result.data.activeRevision }
        : null
}

export async function previewProxyConfiguration(
    snapshot: ProxyRuntimeSnapshot,
): Promise<ProxyConfigSource | null> {
    const body = JSON.stringify(snapshot)
    if (Buffer.byteLength(body) > MAX_RUNTIME_PAYLOAD_BYTES) return null
    const payload = await controllerRequest('/internal/v1/proxy/config/preview', {
        timeoutMs: CONTROLLER_APPLY_TIMEOUT_MS,
        method: 'POST',
        privileged: true,
        confidential: snapshot.proxyHosts.some(
            (host) => host.accessPolicy?.basicAuth !== undefined,
        ),
        body,
        responseLimit: MAX_CONFIG_RESPONSE_BYTES,
    })
    const result = z
        .object({
            config: z.string().max(MAX_CONFIG_RESPONSE_BYTES),
            revision: revisionSchema,
        })
        .safeParse(payload)
    return result.success && result.data.revision === snapshot.revision ? result.data : null
}

const MAX_HOST_CONFIG_RESPONSE_BYTES = 512 * 1_024

export async function getActiveProxyHostConfiguration(
    proxyHostId: string,
): Promise<ProxyConfigSource | null> {
    const id = z.uuid().safeParse(proxyHostId)
    if (!id.success) return null
    const payload = await controllerRequest(
        `/internal/v1/proxy/hosts/${id.data.toLowerCase()}/config`,
        {
            timeoutMs: CONTROLLER_APPLY_TIMEOUT_MS,
            privileged: true,
            responseLimit: MAX_HOST_CONFIG_RESPONSE_BYTES,
            allowNotFound: true,
        },
    )
    const result = z
        .object({
            config: z.string().max(MAX_HOST_CONFIG_RESPONSE_BYTES),
            activeRevision: revisionSchema.nullable(),
        })
        .safeParse(payload)
    return result.success
        ? { config: result.data.config, revision: result.data.activeRevision }
        : null
}

export async function previewProxyHostConfiguration(
    proxyHostId: string,
    snapshot: ProxyRuntimeSnapshot,
): Promise<ProxyConfigSource | null> {
    const id = z.uuid().safeParse(proxyHostId)
    if (!id.success) return null
    const body = JSON.stringify(snapshot)
    if (Buffer.byteLength(body) > MAX_RUNTIME_PAYLOAD_BYTES) return null
    const payload = await controllerRequest(
        `/internal/v1/proxy/hosts/${id.data.toLowerCase()}/config/preview`,
        {
            timeoutMs: CONTROLLER_APPLY_TIMEOUT_MS,
            privileged: true,
            method: 'POST',
            confidential: snapshot.proxyHosts.some(
                (host) => host.accessPolicy?.basicAuth !== undefined,
            ),
            body,
            responseLimit: MAX_HOST_CONFIG_RESPONSE_BYTES,
        },
    )
    const result = z
        .object({
            config: z.string().max(MAX_HOST_CONFIG_RESPONSE_BYTES),
            revision: revisionSchema,
        })
        .safeParse(payload)
    return result.success && result.data.revision === snapshot.revision ? result.data : null
}

/**
 * Fetches one UUID-scoped observation through authenticated controller transport
 * with a four-second timeout and 16 KiB response limit. Invalid, missing or
 * mismatched-host responses return null; desired-state freshness is checked by
 * the caller after this wire-shape validation.
 */
export async function getProxyHostUpstreamHealth(
    proxyHostId: string,
): Promise<ProxyHostUpstreamHealth | null> {
    const id = z.uuid().safeParse(proxyHostId)
    if (!id.success) return null
    const payload = await controllerRequest(
        `/internal/v1/proxy/hosts/${id.data.toLowerCase()}/upstreams`,
        {
            timeoutMs: 4_000,
            privileged: true,
            responseLimit: 16 * 1_024,
            allowNotFound: true,
        },
    )
    const parsed = proxyHostUpstreamHealthSchema.safeParse(payload)
    return parsed.success && parsed.data.hostId === id.data.toLowerCase() ? parsed.data : null
}
