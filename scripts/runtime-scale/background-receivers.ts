import assert from 'node:assert/strict'
import type { Socket } from 'node:net'
import { createServer } from 'node:tls'
import { createTransport } from 'nodemailer'
import {
    TLS_TEST_CA,
    TLS_TEST_CERTIFICATE,
    TLS_TEST_KEY,
} from '../../web/src/tests/server/Notifications/Types/tls-test.types.ts'
import type { NotificationMailClient } from '../../web/src/server/Mail/Types/notification-mail.types.ts'

/** Own configurable loopback SMTP/webhook receivers, track active transport overlap, and distinguish attempts from accepted identities. */
export async function startBackgroundNotificationReceivers(
    options: { smtpPort?: number; webhookPort?: number; delayMs?: number } = {},
) {
    const accepted = new Set<string>()
    const receiverAttempts = new Map<string, number>()
    const receiverIdentity = new Map<string, string>()
    const slowPayloadIds = new Set<string>()
    const kinds = { email: new Set<string>(), webhook: new Set<string>() }
    let activeWebhooks = 0
    /** Normalize stable payload identity while keeping per-transport unique acceptance separate from repeated receiver attempts. */
    const record = (id: string, kind: keyof typeof kinds) => {
        const stable = receiverIdentity.get(id) ?? id
        accepted.add(stable)
        receiverAttempts.set(stable, (receiverAttempts.get(stable) ?? 0) + 1)
        kinds[kind].add(stable)
    }
    const webhook = Bun.serve({
        hostname: '127.0.0.1',
        port: options.webhookPort ?? 0,
        /** Accept only webhook POSTs with identities, count active delayed sends, and record acceptance before returning success. */
        async fetch(request) {
            if (request.method !== 'POST') return new Response(null, { status: 405 })
            const input: unknown = await request.json()
            assert.ok(
                input && typeof input === 'object' && 'id' in input && typeof input.id === 'string',
            )
            activeWebhooks++
            try {
                await Bun.sleep(options.delayMs ?? (slowPayloadIds.has(input.id) ? 500 : 15))
                record(input.id, 'webhook')
                return new Response(null, { status: 204 })
            } finally {
                activeWebhooks--
            }
        },
    })
    const sockets = new Set<Socket>()
    const smtp = createServer({ key: TLS_TEST_KEY, cert: TLS_TEST_CERTIFICATE }, (socket) => {
        sockets.add(socket)
        socket.on('close', () => sockets.delete(socket))
        socket.on('error', () => undefined)
        socket.write('220 fixture ESMTP\r\n')
        let buffer = '',
            data = false,
            body = '',
            reject = false
        socket.on('data', (bytes) => {
            buffer += bytes.toString()
            let end: number
            while ((end = buffer.indexOf('\r\n')) >= 0) {
                const line = buffer.slice(0, end)
                buffer = buffer.slice(end + 2)
                if (data) {
                    if (line !== '.') {
                        body += line + '\r\n'
                        continue
                    }
                    data = false
                    const messageId =
                        /^Message-ID:[ \t\r\n]*<([a-f0-9]{64})@notifications\.rentnerproxy>/imu.exec(
                            body,
                        )?.[1]
                    const delay =
                        options.delayMs ??
                        (messageId &&
                        slowPayloadIds.has(receiverIdentity.get(messageId) ?? messageId)
                            ? 500
                            : 30)
                    setTimeout(() => {
                        if (!socket.destroyed) {
                            if (messageId) record(messageId, 'email')
                            socket.write('250 accepted\r\n')
                        }
                    }, delay)
                    continue
                }
                if (/^EHLO|^HELO/iu.test(line)) socket.write('250-fixture\r\n250 AUTH PLAIN\r\n')
                else if (/^AUTH PLAIN(?: |$)/iu.test(line)) socket.write('235 authenticated\r\n')
                else if (/^MAIL FROM/iu.test(line)) {
                    reject = false
                    body = ''
                    socket.write('250 sender\r\n')
                } else if (/^RCPT TO/iu.test(line)) {
                    reject = line.includes('permanent@')
                    socket.write(reject ? '550 rejected\r\n' : '250 recipient\r\n')
                } else if (/^DATA$/iu.test(line)) {
                    if (reject) socket.write('554 rejected\r\n')
                    else {
                        data = true
                        socket.write('354 send\r\n')
                    }
                } else if (/^QUIT$/iu.test(line)) {
                    socket.end('221 bye\r\n')
                } else socket.write('250 ok\r\n')
            }
        })
    })
    await new Promise<void>((resolve, reject) => {
        smtp.once('error', reject)
        smtp.listen(options.smtpPort ?? 0, '127.0.0.1', resolve)
    })
    const address = smtp.address()
    assert.ok(address && typeof address === 'object')
    return {
        webhook,
        smtpPort: address.port,
        accepted,
        receiverAttempts,
        receiverIdentity,
        slowPayloadIds,
        stats: () => ({
            accepted: accepted.size,
            emailAccepted: kinds.email.size,
            webhookAccepted: kinds.webhook.size,
            attempts: [...receiverAttempts.values()].reduce((sum, count) => sum + count, 0),
            activeWebhooks,
            smtpConnections: sockets.size,
        }),
        createMailClient(): NotificationMailClient {
            const transport = createTransport({
                host: '127.0.0.1',
                port: address.port,
                secure: true,
                tls: {
                    servername: 'notification-tls.invalid',
                    ca: TLS_TEST_CA,
                    minVersion: 'TLSv1.2',
                },
                connectionTimeout: 1500,
                greetingTimeout: 1500,
                socketTimeout: 1500,
            })
            return {
                from: 'fixture@fixture.invalid',
                sendMail: (message) => transport.sendMail(message),
                close: () => transport.close(),
            }
        },
        async close() {
            webhook.stop(true)
            for (const socket of sockets) socket.destroy()
            await new Promise<void>((resolve) => smtp.close(() => resolve()))
        },
    }
}
