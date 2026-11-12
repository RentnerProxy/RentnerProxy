import { expect, test } from 'bun:test'
import { mkdtemp, open, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { readBoundedBackgroundCapture } from '../../../../../../scripts/runtime-scale/background-capture.ts'

test('capture reader uses one bounded regular file and rejects oversized source before allocation', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'background-capture-'))
    try {
        const path = join(directory, 'capture.json')
        await writeFile(path, '{"version":1}')
        expect(await readBoundedBackgroundCapture(path)).toBe('{"version":1}')
        const file = await open(path, 'w')
        try { await file.truncate(8 * 1024 * 1024 + 1) } finally { await file.close() }
        expect(await readBoundedBackgroundCapture(path)).toBeNull()
        expect(await readBoundedBackgroundCapture(directory)).toBeNull()
    } finally { await rm(directory, { recursive: true, force: true }) }
})

test.skipIf(process.platform === 'win32')('capture reader refuses a replacement symlink rather than following its target', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'background-capture-'))
    try {
        const target = join(directory, 'target.json'), link = join(directory, 'capture.json')
        await writeFile(target, '{"private":"must not be followed"}')
        await symlink(target, link)
        await expect(readBoundedBackgroundCapture(link)).rejects.toThrow()
    } finally { await rm(directory, { recursive: true, force: true }) }
})
