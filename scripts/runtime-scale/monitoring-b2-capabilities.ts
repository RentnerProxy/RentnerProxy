// oxlint-disable no-await-in-loop -- Each private capability is sealed and verified before initializing the next owner.
import { randomBytes } from 'node:crypto'
import { chmod, chown, lstat, mkdir, open, readFile } from 'node:fs/promises'

const root = '/tmp/monitoring-b2'
export const monitoringB2ControlKeyPath = root + '/control.cap'
export const monitoringB2WebKeyPath = root + '/web.cap'

/** Accept only the expected root-owned ordinary 64-byte capability file and its private owner/mode seal. */
async function read(path: string, gid: number, mode: number) {
    const directory = await lstat(root),
        file = await lstat(path)
    if (
        !directory.isDirectory() ||
        directory.uid !== 0 ||
        (directory.mode & 0o022) !== 0 ||
        !file.isFile() ||
        file.uid !== 0 ||
        file.gid !== gid ||
        (file.mode & 0o777) !== mode ||
        file.nlink !== 1 ||
        file.size !== 64
    )
        throw new Error('B2 private capability seal invalid')
    const value = await readFile(path, 'utf8')
    if (!/^[a-f0-9]{64}$/u.test(value)) throw new Error('B2 private capability format invalid')
    return value
}
/** Root-only fixture management capability; never supplied to the native probe account. */
export async function monitoringB2ControlKey() {
    if (process.platform !== 'linux' || process.getuid?.() !== 0)
        throw new Error('B2 control capability is root-only')
    return read(monitoringB2ControlKeyPath, 0, 0o600)
}
/** Read the separate web-owner capability used for replay control and the narrow maintenance handshake. */
export async function monitoringB2WebKey() {
    if (process.platform !== 'linux' || ![0, 10002].includes(process.getuid?.() ?? -1))
        throw new Error('B2 web capability requires its owner')
    return read(monitoringB2WebKeyPath, 10002, 0o640)
}

/** Prototype entrypoint initializes private per-appliance capabilities before the original web lifecycle. */
export async function initializeMonitoringB2Capabilities() {
    if (process.platform !== 'linux' || process.getuid?.() !== 0)
        throw new Error('B2 capability initialization requires root Linux fixture')
    const seal = await lstat('/opt/rentnerproxy/web/monitoring-b2-build-seal.json')
    if (!seal.isFile() || seal.uid !== 0 || (seal.mode & 0o022) !== 0 || seal.size > 4096)
        throw new Error('B2 prototype capability initialization seal invalid')
    const value: unknown = JSON.parse(
        await readFile('/opt/rentnerproxy/web/monitoring-b2-build-seal.json', 'utf8'),
    )
    if (!value || typeof value !== 'object' || !('prototype' in value) || value.prototype !== true)
        throw new Error('B2 prototype capability initialization opt-in missing')
    await mkdir(root, { recursive: true, mode: 0o755 })
    const owned = await lstat(root)
    if (!owned.isDirectory() || owned.uid !== 0 || (owned.mode & 0o022) !== 0)
        throw new Error('B2 capability root ownership invalid')
    for (const [path, gid, mode] of [
        [monitoringB2ControlKeyPath, 0, 0o600],
        [monitoringB2WebKeyPath, 10002, 0o640],
    ] as const) {
        try {
            const file = await open(path, 'wx', mode)
            try {
                await file.writeFile(randomBytes(32).toString('hex'))
                await file.sync()
            } finally {
                await file.close()
            }
            await chown(path, 0, gid)
            await chmod(path, mode)
        } catch (error) {
            if (!(error && typeof error === 'object' && 'code' in error && error.code === 'EEXIST'))
                throw error
        }
        await read(path, gid, mode)
    }
    await mkdir(root + '/state', { recursive: true, mode: 0o2770 })
    const state = await lstat(root + '/state')
    if (!state.isDirectory() || state.uid !== 0)
        throw new Error('B2 capability state ownership invalid')
    await chown(root + '/state', 0, 20004)
    await chmod(root + '/state', 0o2770)
}

if (import.meta.main) await initializeMonitoringB2Capabilities()
