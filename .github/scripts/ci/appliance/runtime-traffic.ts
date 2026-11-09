import type { ApplianceRuntimeFaultContext } from './Types/runtime-faults.types.ts'

export async function samplePublishedRuntimeTraffic(
    context: ApplianceRuntimeFaultContext,
    protocol: 'http' | 'https',
): Promise<boolean> {
    const port = protocol === 'http' ? context.httpPort : context.httpsPort
    try {
        const body = await context.command(
            [
                'curl',
                '--silent',
                '--show-error',
                '--fail',
                '--max-time',
                '2',
                '--noproxy',
                '*',
                ...(context.trafficCaFile ? ['--cacert', context.trafficCaFile] : ['--insecure']),
                '--resolve',
                context.hostDomain + ':' + port + ':127.0.0.1',
                protocol + '://' + context.hostDomain + ':' + port + '/appliance-real-path',
            ],
            3_000,
        )
        return body === context.trafficMarker
    } catch {
        return false
    }
}
