export interface ControllerRequestOptions {
    readonly timeoutMs: number
    readonly signal?: AbortSignal
    readonly privileged?: boolean
    readonly confidential?: boolean
    readonly body?: string
    readonly method?: 'GET' | 'PUT' | 'POST' | 'DELETE'
    readonly responseLimit?: number
    readonly allowNotFound?: boolean
    readonly acceptErrorResponse?: boolean
    readonly acceptNonOkJson?: boolean
}

export type ControllerRequestFailure = {
    readonly ok: false
    readonly kind: 'configuration' | 'timeout' | 'aborted' | 'network' | 'invalid_response' | 'http'
    readonly status: number | null
    readonly code: string | null
}

export type ControllerRequestResult =
    { readonly ok: true; readonly payload: unknown } | ControllerRequestFailure
