import { describe, expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import { mkdtemp, open, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { basename, dirname, join, resolve } from 'node:path'
import { createMonitoringB2AttachmentFence } from '../../../../../scripts/runtime-scale/monitoring-b2-web-lifecycle.ts'

describe('B2 startup attachment and maintenance ownership', () => {
    test('blocks the Root ACK through final factory publication, running work, SQLite close and the real drain receipt', async () => {
        const finalPublication = Promise.withResolvers<void>(),
            running = Promise.withResolvers<void>(),
            receipt = Promise.withResolvers<void>()
        const database = new Database(':memory:'),
            fence = createMonitoringB2AttachmentFence()
        const events: string[] = []
        const directory = await mkdtemp(join(tmpdir(), 'rentnerproxy-b2-lifecycle-')),
            receiptPath = join(directory, 'drain.json')
        if (
            dirname(resolve(directory)) !== resolve(tmpdir()) ||
            !basename(directory).startsWith('rentnerproxy-b2-lifecycle-')
        )
            throw new Error('B2 test temp ownership mismatch')
        let neverAttached = false,
            rootAck = false,
            closed = false,
            pending: Promise<void> | undefined
        try {
            const attached = fence.attach(async () => {
                events.push('created')
                await finalPublication.promise
                events.push('factory-published')
                return {
                    drain: async () => {
                        await running.promise
                        events.push('pass-drained')
                        database.close()
                        closed = true
                        events.push('sqlite-closed')
                        await receipt.promise
                        const file = await open(receiptPath, 'w', 0o600)
                        try {
                            await file.writeFile(JSON.stringify({ drained: true }))
                            await file.sync()
                        } finally {
                            await file.close()
                        }
                        events.push('real-receipt-written')
                    },
                }
            })
            pending = fence
                .drain(async () => {
                    neverAttached = true
                })
                .then(() => {
                    rootAck = true
                    events.push('root-ack')
                })
            await Promise.resolve()
            expect(fence.signal.aborted).toBe(true)
            expect(rootAck).toBe(false)
            expect(neverAttached).toBe(false)
            expect(database.query('SELECT 1').get()).toBeDefined()
            finalPublication.resolve()
            await attached
            expect(rootAck).toBe(false)
            expect(database.query('SELECT 1').get()).toBeDefined()
            running.resolve()
            await Bun.sleep(0)
            expect(rootAck).toBe(false)
            expect(() => database.query('SELECT 1').get()).toThrow()
            receipt.resolve()
            await pending
            expect(JSON.parse(await readFile(receiptPath, 'utf8'))).toEqual({ drained: true })
            expect(neverAttached).toBe(false)
            expect(events).toEqual([
                'created',
                'factory-published',
                'pass-drained',
                'sqlite-closed',
                'real-receipt-written',
                'root-ack',
            ])
        } finally {
            finalPublication.resolve()
            running.resolve()
            receipt.resolve()
            await pending?.catch(() => {})
            if (!closed) database.close()
            await rm(directory, { recursive: true, force: true })
        }
    })
    test('an aborted startup cannot create a later adapter after its NeverAttached receipt', async () => {
        const fence = createMonitoringB2AttachmentFence()
        let receipts = 0,
            factories = 0
        await fence.drain(async () => {
            receipts++
        })
        expect(
            await fence.attach(async () => {
                factories++
                return { drain: async () => {} }
            }),
        ).toBeUndefined()
        await fence.drain(async () => {
            receipts++
        })
        expect(factories).toBe(0)
        expect(receipts).toBe(1)
    })
    test('factory or drain rejection never falls back to a NeverAttached proof or a successful ACK', async () => {
        const fence = createMonitoringB2AttachmentFence(),
            gate = Promise.withResolvers<void>()
        let receipts = 0,
            ack = false
        const factory = fence.attach(async () => {
            await gate.promise
            throw new Error('fixed-factory-failure')
        })
        const pending = fence
            .drain(async () => {
                receipts++
            })
            .then(() => {
                ack = true
            })
        gate.resolve()
        const results = await Promise.allSettled([factory, pending])
        expect(results.map((result) => result.status)).toEqual(['rejected', 'rejected'])
        expect(receipts).toBe(0)
        expect(ack).toBe(false)
    })
})
