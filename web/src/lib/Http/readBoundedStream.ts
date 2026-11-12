export class ResponseBodyLimitError extends Error {
    constructor() {
        super('Response body exceeds limit')
    }
}

/** Bound caller observation while allowing a resource owner to retain actual source-cancellation work. */
export async function readBoundedStream(
    body: ReadableStream<Uint8Array>,
    limit: number,
    signal?: AbortSignal,
    trackWork?: (work: Promise<unknown>) => void,
): Promise<Uint8Array> {
    const reader = body.getReader()
    const chunks: Uint8Array[] = []
    let size = 0
    let cancellation: Promise<void> | undefined
    /** Reuse the original cancellation promise even after the stream has already become closed. */
    const cancel = () => {
        if (!cancellation) {
            cancellation = reader.cancel().catch(() => {})
            trackWork?.(cancellation)
        }
        return cancellation
    }
    const abort = () => {
        void cancel()
    }
    signal?.addEventListener('abort', abort, { once: true })
    try {
        signal?.throwIfAborted()
        while (true) {
            // Streams allow one ordered read at a time and enforce the limit incrementally.
            // eslint-disable-next-line no-await-in-loop
            const chunk = await reader.read()
            signal?.throwIfAborted()
            if (chunk.done) break
            size += chunk.value.byteLength
            if (size > limit) throw new ResponseBodyLimitError()
            chunks.push(chunk.value)
        }
        const result = new Uint8Array(size)
        let offset = 0
        for (const chunk of chunks) {
            result.set(chunk, offset)
            offset += chunk.byteLength
        }
        return result
    } finally {
        signal?.removeEventListener('abort', abort)
        void cancel()
        reader.releaseLock()
    }
}
