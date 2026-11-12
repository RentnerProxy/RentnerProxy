// oxlint-disable no-await-in-loop -- The child protocol and inode replacement observations must stay serial.
import { expect, test } from 'bun:test'
import { randomBytes } from 'node:crypto'
import { readFile, readdir, rm } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { join } from 'node:path'
import {
    readBoundedBackgroundCapture,
    readLeasedBackgroundCapture,
    backgroundCaptureMatchesOwner,
} from '../../../../../scripts/runtime-scale/background-capture.ts'
import { backgroundCaptureReadCommand } from '../../../../../scripts/runtime-scale/background-capture-read.ts'
import { hostMonitoringCaptureSchema } from '@/server/HostMonitoring/host-monitoring.capture.ts'

const enabled =
    process.platform === 'linux' && process.getuid?.() === 0 && Bun.which('gosu') !== null
const linuxTest = enabled ? test : test.skip
const root = fileURLToPath(new URL('../../../../../', import.meta.url))

/** Foreign-UID runtime fixtures cannot use the root collector's private manifest/cache; parent and ordinary root children stay instrumented. */
function foreignUidEnvironment() {
    const environment = { ...process.env }
    delete environment.RENTNERPROXY_COVERAGE_MANIFEST
    if (environment.BUN_OPTIONS === '--preload=' + join(root, 'scripts/coverage/child-preload.ts'))
        delete environment.BUN_OPTIONS
    return environment
}

test('foreign-UID fixture environment removes only root-private coverage state and preserves parent and unrelated options', () => {
    const manifest = process.env.RENTNERPROXY_COVERAGE_MANIFEST
    const options = process.env.BUN_OPTIONS
    try {
        process.env.RENTNERPROXY_COVERAGE_MANIFEST = '/root-private/manifest.json'
        process.env.BUN_OPTIONS = '--preload=' + join(root, 'scripts/coverage/child-preload.ts')
        const environment = foreignUidEnvironment()
        expect(environment.RENTNERPROXY_COVERAGE_MANIFEST).toBeUndefined()
        expect(environment.BUN_OPTIONS).toBeUndefined()
        expect(process.env.RENTNERPROXY_COVERAGE_MANIFEST).toBe('/root-private/manifest.json')
        expect(process.env.BUN_OPTIONS).toBe(
            '--preload=' + join(root, 'scripts/coverage/child-preload.ts'),
        )
        process.env.BUN_OPTIONS = '--smol'
        expect(foreignUidEnvironment().BUN_OPTIONS).toBe('--smol')
    } finally {
        if (manifest === undefined) delete process.env.RENTNERPROXY_COVERAGE_MANIFEST
        else process.env.RENTNERPROXY_COVERAGE_MANIFEST = manifest
        if (options === undefined) delete process.env.BUN_OPTIONS
        else process.env.BUN_OPTIONS = options
    }
})

const writerSource = `
import { createInterface } from 'node:readline'
import { readFileSync, fstatSync, writeFile } from 'node:fs'
delete process.env.RENTNERPROXY_BACKGROUND_CAPTURE_RUN
const {createHostMonitoringCapturePublisher} = await import('./web/src/server/HostMonitoring/host-monitoring.capture.server.ts')
const {createHostMonitoringCapture} = await import('./web/src/server/HostMonitoring/host-monitoring.capture.ts')
const runId = process.argv[1], path = '/tmp/rentnerproxy-background-capture-' + runId + '.json'
const stat = readFileSync('/proc/self/stat','utf8')
const owner = {pid:process.pid,startTicks:stat.slice(stat.lastIndexOf(')')+2).trim().split(/\\s+/)[19],state:'S'}
let mode='normal', candidateFd, flight, entered, release, writes=0
const publisher = createHostMonitoringCapturePublisher(path, {
  write: async (fd,text) => {
    writes++
    if(mode==='fail') throw new Error('injected later write failure')
    await new Promise((resolve,reject)=>writeFile(fd,text,error=>error?reject(error):resolve()))
    if(mode==='pause'){candidateFd=fd;entered.resolve();await release.promise}
  },
  ...(process.argv[2]==='remove-fails'?{remove: () => {throw new Error('injected namespace cleanup failure')}}:{}),
})
const capture=createHostMonitoringCapture({runId,sourceSha:'a'.repeat(40),pid:process.pid,processStartTicks:owner.startTicks,...publisher})
const claim=token=>({policy:{id:'00000000-0000-4000-8000-000000000001',version:'00000000-0000-4000-8000-000000000002',nextCheckAt:new Date(1000)},leaseToken:token})
const pair=token=>{capture.record('admitted',claim(token));capture.record('started',claim(token))}
const current=()=>JSON.parse(readFileSync(path,'utf8'))
console.log(JSON.stringify({owner}))
for await(const line of createInterface({input:process.stdin})){
  const command=JSON.parse(line)
  let result={}
  if(command==='first'){pair('first-token');await capture.flush();result={capture:current()}}
  if(command==='pending-failure'){mode='fail';pair('second-token')}
  if(command==='flush'){await capture.flush()}
  if(command==='replace'){pair('second-token');await capture.flush();result={capture:current()}}
  if(command==='begin-race'){mode='pause';entered=Promise.withResolvers();release=Promise.withResolvers();pair('second-token');flight=capture.flush();await entered.promise;result={candidateFd}}
  if(command==='more'){capture.record('cancelled',claim('second-token'));result={candidateStillOpen:fstatSync(candidateFd).isFile()}}
  if(command==='release'){mode='normal';release.resolve();await flight;result={capture:current()}}
  console.log(JSON.stringify({...result,writes}))
}
`

async function fixture(asWeb = true, cleanupFails = true) {
    const runId = randomBytes(6).toString('hex')
    const path = '/tmp/rentnerproxy-background-capture-' + runId + '.json'
    const account = (await readFile('/etc/passwd', 'utf8')).includes('rentnerproxy-web:')
        ? 'rentnerproxy-web'
        : '10002:10002'
    const child = Bun.spawn(
        [
            ...(asWeb ? ['gosu', account] : []),
            process.execPath,
            '--no-env-file',
            '-e',
            writerSource,
            runId,
            cleanupFails ? 'remove-fails' : 'remove-ok',
        ],
        {
            cwd: root,
            stdin: 'pipe',
            stdout: 'pipe',
            stderr: 'pipe',
            ...(asWeb ? { env: foreignUidEnvironment() } : {}),
        },
    )
    const stream = child.stdout.getReader()
    const errors = (async () => {
        const reader = child.stderr.getReader()
        const decoder = new TextDecoder()
        let text = ''
        let retainedBytes = 0
        let truncated = false
        for (;;) {
            const chunk = await reader.read()
            if (chunk.done) break
            const remaining = 8192 - retainedBytes
            if (chunk.value.length > remaining) truncated = true
            if (remaining > 0) {
                const retained = chunk.value.subarray(0, remaining)
                retainedBytes += retained.byteLength
                text += decoder.decode(retained)
            }
        }
        return { text, truncated }
    })()
    let buffered = ''
    async function line() {
        const deadline = setTimeout(() => child.kill('SIGKILL'), 2000)
        try {
            while (!buffered.includes('\n')) {
                const chunk = await stream.read()
                if (chunk.done)
                    throw new Error(
                        'Capture writer exited before its acknowledgement: ' +
                            JSON.stringify({ exitCode: await child.exited, stderr: await errors }),
                    )
                buffered += new TextDecoder().decode(chunk.value)
            }
            const end = buffered.indexOf('\n'),
                text = buffered.slice(0, end)
            buffered = buffered.slice(end + 1)
            return JSON.parse(text) as {
                owner: { pid: number; startTicks: string; state: string }
                capture: ReturnType<typeof hostMonitoringCaptureSchema.parse>
                candidateStillOpen: boolean
                writes: number
            }
        } finally {
            clearTimeout(deadline)
        }
    }
    async function close() {
        child.kill('SIGKILL')
        await child.exited
        await errors
        await stream.cancel()
        for (const name of await readdir('/tmp'))
            if (name.startsWith('rentnerproxy-background-capture-' + runId + '.json'))
                await rm('/tmp/' + name, { force: true })
    }
    const ready = await line().catch(async (error: unknown) => {
        await close()
        throw error
    })
    const expected = { runId, sourceSha: 'a'.repeat(40) }
    const owners = async () => [ready.owner]
    return {
        runId,
        path,
        expected,
        owner: ready.owner,
        owners,
        account,
        async request(command: string) {
            child.stdin.write(JSON.stringify(command) + '\n')
            await child.stdin.flush()
            return line()
        },
        close,
    }
}

/** Use the same root timeout -> gosu -> Bun arrangement as the installed capture-only CLI. */
async function readAsWeb(f: Awaited<ReturnType<typeof fixture>>, path?: string) {
    const command = backgroundCaptureReadCommand('owned-fixture', f.runId, 2000)
    const remote = command.args.slice(4)
    remote[4] = f.account
    remote[7] = root + '/scripts/runtime-scale/background-capture-read.ts'
    if (path)
        remote.splice(
            7,
            2,
            '-e',
            "const {readLeasedBackgroundCapture}=await import('./scripts/runtime-scale/background-capture.ts'); console.log(JSON.stringify({capture:await readLeasedBackgroundCapture(process.argv[1],{runId:process.argv[2],sourceSha:process.argv[3]})}));",
            path,
            f.runId,
            f.expected.sourceSha,
        )
    const child = Bun.spawn(remote, {
        cwd: root,
        env: {
            ...foreignUidEnvironment(),
            DATABASE_URL: '',
            DATABASE_URL_FILE: '',
            RENTNERPROXY_BACKGROUND_CAPTURE_RUN: f.runId,
            RENTNERPROXY_BACKGROUND_CAPTURE_SOURCE_SHA: f.expected.sourceSha,
        },
        stdout: 'pipe',
        stderr: 'pipe',
    })
    const [exit, output, error] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
    ])
    expect(exit, error).toBe(0)
    return JSON.parse(output) as {
        capture: ReturnType<typeof hostMonitoringCaptureSchema.parse> | null
    }
}

linuxTest(
    'same-UID capture-only CLI qualifies a live web FD lease and rejects stale publication before and after write plus cleanup failure',
    async () => {
        const f = await fixture()
        try {
            const first = (await f.request('first')).capture
            expect((await readAsWeb(f)).capture?.publication.fd).toBe(first.publication.fd)
            await f.request('pending-failure')
            // Prior JSON is still readable and passes the old identity-only predicate, but no longer carries live authority.
            const stale = hostMonitoringCaptureSchema.parse(
                JSON.parse((await readBoundedBackgroundCapture(f.path))!),
            )
            expect(
                backgroundCaptureMatchesOwner(stale, f.expected, [f.owner], [f.owner], Date.now()),
            ).toBe(true)
            expect(stale.events).toHaveLength(2)
            expect((await readAsWeb(f)).capture).toBeNull()
            await f.request('flush')
            expect(await readBoundedBackgroundCapture(f.path)).not.toBeNull()
            expect((await readAsWeb(f)).capture).toBeNull()
        } finally {
            await f.close()
        }
    },
)

linuxTest(
    'new records during async candidate I/O retain its FD and publish the current generation only after successful stale cleanup',
    async () => {
        const f = await fixture(true, false)
        try {
            const first = (await f.request('first')).capture
            await f.request('begin-race')
            expect((await readAsWeb(f)).capture).toBeNull()
            expect((await f.request('more')).candidateStillOpen).toBe(true)
            const latest = (await f.request('release')).capture
            expect(latest.publication.generation).toBe(5)
            expect(latest.events).toHaveLength(5)
            expect((await readAsWeb(f)).capture?.publication.generation).toBe(5)
            const retiredCandidate = f.path + '.' + first.epoch + '.4.tmp'
            await expect(readBoundedBackgroundCapture(retiredCandidate)).rejects.toMatchObject({
                code: 'ENOENT',
            })
        } finally {
            await f.close()
        }
    },
)

linuxTest(
    'failed stale-candidate cleanup disables capture with at most one leftover candidate and no further writes',
    async () => {
        const f = await fixture()
        try {
            const first = (await f.request('first')).capture
            await f.request('begin-race')
            expect((await f.request('more')).candidateStillOpen).toBe(true)
            const stopped = await f.request('release')
            expect(stopped.capture.publication.generation).toBe(first.publication.generation)
            expect(stopped.writes).toBe(2)
            expect((await readAsWeb(f)).capture).toBeNull()
            const retiredCandidate = f.path + '.' + first.epoch + '.4.tmp'
            expect(await readBoundedBackgroundCapture(retiredCandidate)).not.toBeNull()
            expect((await readAsWeb(f, retiredCandidate)).capture).toBeNull()
            const retried = await f.request('replace')
            expect(retried.writes).toBe(2)
            expect(
                (await readdir('/tmp')).filter(
                    (name) =>
                        name.startsWith('rentnerproxy-background-capture-' + f.runId + '.json') &&
                        name.endsWith('.tmp'),
                ),
            ).toHaveLength(1)
        } finally {
            await f.close()
        }
    },
)

linuxTest(
    'holding the old inode across replacement and FD-number reuse cannot make the old snapshot authoritative',
    async () => {
        const f = await fixture(false)
        try {
            const first = (await f.request('first')).capture
            let replacement: typeof first | undefined
            let census = 0
            const result = await readLeasedBackgroundCapture(f.path, f.expected, async () => {
                if (++census === 2) replacement = (await f.request('replace')).capture
                return [f.owner]
            })
            expect(result).toBeNull()
            // Linux allocates the just-retired low descriptor again, but it now denotes a different inode.
            expect(replacement?.publication.fd).toBe(first.publication.fd)
            expect(
                (await readLeasedBackgroundCapture(f.path, f.expected, f.owners))?.events,
            ).toHaveLength(4)
        } finally {
            await f.close()
        }
    },
)

linuxTest(
    'expiry or backwards time after the final awaited inode checks and close rejects the capture',
    async () => {
        const f = await fixture(false)
        try {
            const first = (await f.request('first')).capture
            for (const finalTime of [first.expiresAt + 1, first.createdAt - 1]) {
                let clocks = 0
                const result = await readLeasedBackgroundCapture(
                    f.path,
                    f.expected,
                    f.owners,
                    () => (++clocks === 1 ? first.createdAt + 1 : finalTime),
                )
                expect(result).toBeNull()
                expect(clocks).toBe(2)
            }
        } finally {
            await f.close()
        }
    },
)
