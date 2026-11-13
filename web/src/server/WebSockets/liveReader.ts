import type { LiveReadResult } from './Types/live-reader.types.ts'
import { LIVE_MAX_PAYLOAD_BYTES } from '@/config/realtime.config.ts'
import type { LiveTopic } from '@/lib/Live/Types/realtime.types.ts'
import { readBoundedJson, snapshotUrl } from '@/lib/Live/snapshot.ts'
import type {
    LiveConnectionData,
    LiveTopicSubscription,
} from './Types/realtime-subscriptions.types.ts'
import type { LiveSnapshotReader } from './Types/bun.types.ts'

function handshakeSubscription(connection: LiveConnectionData): LiveTopicSubscription {
    return {
        connection,
        topic: 'app-events',
        query: {},
        queryText: '{}',
        intervalMs: 60_000,
        timer: null,
        abortController: null,
        abortTimer: null,
        inflight: false,
        pending: false,
        lastComparison: null,
        active: true,
    }
}

function abortable<T>(
    signal: AbortSignal,
    onAbort: () => void,
): {
    readonly promise: Promise<T>
    readonly cleanup: () => void
} {
    let handler: (() => void) | null = null
    const promise = new Promise<T>((_, reject) => {
        handler = () => {
            onAbort()
            reject(new DOMException('Snapshot read aborted.', 'AbortError'))
        }
        if (signal.aborted) handler()
        else signal.addEventListener('abort', handler, { once: true })
    })
    return {
        promise,
        cleanup: () => {
            if (handler !== null) signal.removeEventListener('abort', handler)
        },
    }
}

/** Bound response/body observation by cancellation and payload size while tracking underlying promises for truthful permit release and drain. */
export async function readSubscriptionSnapshot(
    subscription: LiveTopicSubscription,
    signal: AbortSignal,
    readSnapshot: (
        request: Request,
        context: { request: Request; topic: LiveTopic; query: unknown },
    ) => Response | Promise<Response>,
    getSnapshotUrl: (subscription: LiveTopicSubscription) => string,
    maxPayloadBytes = LIVE_MAX_PAYLOAD_BYTES,
    trackWork?: (work: Promise<unknown>) => void,
): Promise<LiveReadResult> {
    const headers = new Headers()
    if (subscription.connection.cookie !== null) {
        headers.set('cookie', subscription.connection.cookie)
    }
    headers.set('origin', subscription.connection.origin)
    const request = new Request(getSnapshotUrl(subscription), { method: 'GET', headers, signal })
    let response: Response
    let unclaimedResponse: Response | undefined
    const responseAbort = abortable<Response>(signal, () => undefined)
    try {
        const responsePromise = Promise.resolve()
            .then(() => {
                signal.throwIfAborted()
                return readSnapshot(request, {
                    request,
                    topic: subscription.topic,
                    query: subscription.query,
                })
            })
            .then(async (result) => {
                // Late bodies still own the admitted resource until cancellation actually settles.
                if (signal.aborted) await result.body?.cancel().catch(() => undefined)
                else unclaimedResponse = result
                return result
            })
        trackWork?.(responsePromise)
        response = await Promise.race([responsePromise, responseAbort.promise])
    } catch {
        // Abort can win after the late check but before the response reaches this reader.
        const cancelling = unclaimedResponse?.body?.cancel().catch(() => undefined)
        if (cancelling) trackWork?.(cancelling)
        responseAbort.cleanup()
        return { kind: 'failure', status: 503 }
    }
    responseAbort.cleanup()
    if (!response.ok) {
        const cancelling = response.body?.cancel().catch(() => undefined)
        if (cancelling) trackWork?.(cancelling)
        return { kind: 'failure', status: response.status }
    }
    const bodyAbort = abortable<unknown>(signal, () => {
        const cancelling = response.body?.cancel().catch(() => undefined)
        if (cancelling) trackWork?.(cancelling)
    })
    try {
        const reading = readBoundedJson(response, maxPayloadBytes, signal, trackWork)
        trackWork?.(reading)
        const data = await Promise.race([reading, bodyAbort.promise])
        return { kind: 'success', data }
    } catch {
        void response.body?.cancel().catch(() => undefined)
        return { kind: 'failure', status: signal.aborted ? 503 : 413 }
    } finally {
        bodyAbort.cleanup()
    }
}

/** Authorize through the bounded snapshot reader with a dedicated handshake deadline and expose underlying work to the shared resource owner. */
export async function readLiveHandshake(
    connection: LiveConnectionData,
    snapshotPath: string,
    snapshotTimeoutMs: number,
    maxPayloadBytes: number,
    readSnapshot: LiveSnapshotReader,
    trackWork?: (work: Promise<unknown>) => void,
    onData?: (data: unknown) => void,
): Promise<number> {
    const controller = new AbortController()
    connection.handshakeAbortController = controller
    const timer = setTimeout(() => controller.abort(), snapshotTimeoutMs)
    try {
        const subscription = handshakeSubscription(connection)
        const result = await readSubscriptionSnapshot(
            subscription,
            controller.signal,
            readSnapshot,
            (current) =>
                snapshotUrl(connection.requestUrl, snapshotPath, current.topic, current.queryText),
            maxPayloadBytes,
            trackWork,
        )
        if (result.kind === 'failure') return result.status ?? 503
        onData?.(result.data)
        return 200
    } finally {
        clearTimeout(timer)
        connection.handshakeAbortController = null
    }
}
