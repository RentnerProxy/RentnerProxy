/** A maintenance request owns both an in-progress factory and the adapter it may already have created. */
export function createMonitoringB2AttachmentFence() {
    const controller = new AbortController()
    type Adapter = { drain: () => Promise<void> }
    let attachment: Promise<Adapter | undefined> | undefined
    let draining: Promise<void> | undefined
    return {
        signal: controller.signal,
        attach(factory: (signal: AbortSignal) => Promise<Adapter | undefined>) {
            if (attachment) throw new Error('B2 replay factory already owned')
            if (controller.signal.aborted) return Promise.resolve(undefined)
            let resolve!: (value: Adapter | undefined) => void, reject!: (reason: unknown) => void
            // Publish ownership BEFORE invoking a factory that can synchronously create timer/listeners.
            attachment = new Promise<Adapter | undefined>((yes, no) => {
                resolve = yes
                reject = no
            })
            try {
                Promise.resolve(factory(controller.signal)).then(resolve, reject)
            } catch (error) {
                reject(error)
            }
            return attachment
        },
        drain(neverAttached: () => Promise<void>) {
            controller.abort()
            draining ??= (async () => {
                const adapter = await attachment
                if (adapter) await adapter.drain()
                else await neverAttached()
            })()
            return draining
        },
    }
}
