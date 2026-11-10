import type {
    ControllerRequestFailure,
    ControllerRequestOptions,
    ControllerRequestResult,
} from './Types/transport.types.ts'
import '@tanstack/react-start/server-only'
import { z } from 'zod'
import { readBoundedStream } from '@/lib/Http/readBoundedStream.ts'

import {
    getControllerBaseUrl,
    getControllerToken,
    isLoopbackControllerUrl,
} from '@/server/env.server.ts'

const MAX_RESPONSE_BYTES = 4_096

async function readBoundedJson(
    response: Response,
    responseLimit = MAX_RESPONSE_BYTES,
    signal?: AbortSignal,
): Promise<unknown> {
    if (!response.body) return null
    const bytes = await readBoundedStream(response.body, responseLimit, signal)
    return JSON.parse(
        Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString('utf8'),
    ) as unknown
}

export async function controllerRequestResult(
    path: Parameters<typeof controllerRequest>[0],
    options: ControllerRequestOptions,
): Promise<ControllerRequestResult> {
    return requestController(path, {
        ...options,
        preserveErrors: true,
    })
}

export async function controllerRequest(
    path:
        | '/health'
        | '/ready'
        | '/internal/v1/backups/status'
        | '/internal/v1/backups/maintenance'
        | `/internal/v1/backups/${string}/delete`
        | `/internal/v1/backups/${string}/restore`
        | '/internal/v1/backups/start'
        | '/internal/v1/backups/config'
        | '/internal/v1/backups/off-host'
        | '/internal/v1/backups/audit/ack'
        | '/internal/v1/diagnostics/domain'
        | '/internal/v1/proxy/status'
        | '/internal/v1/proxy/version'
        | '/internal/v1/crowdsec/status'
        | `/internal/v1/crowdsec/dashboard${string}`
        | '/internal/v1/crowdsec/config'
        | '/internal/v1/crowdsec/test'
        | '/internal/v1/crowdsec/console/enroll'
        | `/internal/v1/proxy/access-logs${string}`
        | '/internal/v1/proxy/config'
        | '/internal/v1/proxy/config/preview'
        | `/internal/v1/proxy/hosts/${string}/upstreams`
        | `/internal/v1/proxy/hosts/${string}/config`
        | `/internal/v1/proxy/hosts/${string}/config/preview`
        | '/internal/v1/certificates'
        | `/internal/v1/certificates/${string}`
        | `/internal/v1/certificates/events${string}`
        | '/internal/v1/trusted-cas/validate'
        | '/internal/v1/dns/connections'
        | '/internal/v1/dns/providers'
        | `/internal/v1/discovery/docker/${'status' | 'config' | 'inspect'}`
        | `/internal/v1/dns/connections/${string}`
        | `/internal/v1/ddns/${'status' | 'config' | 'preview' | 'enable' | 'disable' | 'sync'}`,
    options: ControllerRequestOptions,
): Promise<unknown> {
    return requestController(path, options)
}

type ControllerTransportOptions = ControllerRequestOptions & { readonly preserveErrors?: boolean }

function requestController(
    path: Parameters<typeof controllerRequest>[0],
    options: ControllerTransportOptions & { readonly preserveErrors: true },
): Promise<ControllerRequestResult>
function requestController(
    path: Parameters<typeof controllerRequest>[0],
    options: ControllerRequestOptions,
): Promise<unknown>
async function requestController(
    path: Parameters<typeof controllerRequest>[0],
    options: ControllerTransportOptions,
): Promise<unknown> {
    const failure = (
        kind: ControllerRequestFailure['kind'],
        status: number | null = null,
        code: string | null = null,
    ): ControllerRequestFailure | null =>
        options.preserveErrors ? { ok: false, kind, status, code } : null
    const success = (payload: unknown): unknown =>
        options.preserveErrors ? { ok: true, payload } : payload
    if (options.signal?.aborted) return failure('aborted')
    const baseUrl = getControllerBaseUrl()
    const safePath = path.split('?')[0] ?? path
    if (!baseUrl) return failure('configuration')
    if (!isLoopbackControllerUrl(baseUrl) && !baseUrl.startsWith('https://'))
        return failure('configuration')

    const headers: Record<string, string> = { accept: 'application/json' }
    const isDnsConnectionRequest =
        path === '/internal/v1/dns/providers' ||
        path === '/internal/v1/dns/connections' ||
        path.startsWith('/internal/v1/dns/connections/')
    if (options.privileged || path.startsWith('/internal/')) {
        const token = getControllerToken()
        if (!token) return failure('configuration')
        headers.authorization = 'Bearer ' + token
    }

    if (options.body !== undefined) headers['content-type'] = 'application/json'
    const requestAbort = new AbortController()
    const abortFromCaller = () => requestAbort.abort()
    options.signal?.addEventListener('abort', abortFromCaller, { once: true })
    if (options.signal?.aborted) requestAbort.abort()
    let timedOut = false
    let responseStatus: number | null = null
    const timeout = setTimeout(() => {
        timedOut = true
        requestAbort.abort()
    }, options.timeoutMs)

    try {
        const response = await fetch(baseUrl + path, {
            method: options.method ?? (options.body === undefined ? 'GET' : 'PUT'),
            headers,
            ...(options.body === undefined ? {} : { body: options.body }),
            signal: requestAbort.signal,
            redirect: 'error',
        })
        responseStatus = response.status

        if (
            (!response.ok &&
                !options.preserveErrors &&
                !options.acceptErrorResponse &&
                !options.acceptNonOkJson) ||
            !response.headers.get('content-type')?.includes('application/json')
        ) {
            void response.body?.cancel().catch(() => undefined)
            if (response.status === 404 && options.allowNotFound && !options.preserveErrors)
                return null
            console.warn('[controller] request unavailable', {
                path: safePath,
                status: response.status,
            })
            return failure(response.ok ? 'invalid_response' : 'http', response.status)
        }

        const payload = await readBoundedJson(response, options.responseLimit, requestAbort.signal)
        if (response.ok) return success(payload)
        if (options.preserveErrors) {
            const error = z
                .strictObject({ error: z.string().regex(/^[a-z][a-z0-9_]{0,63}$/u) })
                .safeParse(payload)
            return failure('http', response.status, error.success ? error.data.error : null)
        }
        if (options.acceptNonOkJson) return payload

        const errorSchema = z.object({ error: z.string().max(64) })
        const error = (isDnsConnectionRequest ? errorSchema.strict() : errorSchema).safeParse(
            payload,
        )
        return error.success ? error.data : null
    } catch {
        console.warn('[controller] request unavailable', { path: safePath })
        return failure(
            options.signal?.aborted
                ? 'aborted'
                : timedOut
                  ? 'timeout'
                  : responseStatus === null
                    ? 'network'
                    : 'invalid_response',
            responseStatus,
        )
    } finally {
        options.signal?.removeEventListener('abort', abortFromCaller)
        clearTimeout(timeout)
    }
}
