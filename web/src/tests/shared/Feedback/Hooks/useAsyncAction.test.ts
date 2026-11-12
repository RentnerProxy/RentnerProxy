import { expect, spyOn, test } from 'bun:test'
import { toast } from '@rentnerkev/toasts'
import useAsyncAction from '@/shared/Feedback/Hooks/useAsyncAction.ts'
import renderLogicHook from '@/tests/Helpers/renderLogicHook.tsx'
import { catalogs } from '@/tests/Helpers/withTestLanguage.tsx'

test.each(['throw', 'reject'] as const)('reports an action that fails by %s once with safe translated feedback', async (mode) => {
    const error = spyOn(toast, 'error').mockReturnValue('action-failure')
    const hook = await renderLogicHook(useAsyncAction)
    try {
        await hook.run((runAction) => runAction(() => {
            const failure = new Error('private server response')
            if (mode === 'throw') throw failure
            return Promise.reject(failure)
        }))
        expect(error).toHaveBeenCalledTimes(1)
        expect(error).toHaveBeenCalledWith(catalogs.en.system.error.title, {
            title: catalogs.en.toast.titles.error,
        })
    } finally {
        await hook.dispose()
        error.mockRestore()
    }
})

test('starts a successful native action synchronously without reporting failure', async () => {
    const error = spyOn(toast, 'error').mockReturnValue('unexpected-failure')
    const hook = await renderLogicHook(useAsyncAction)
    let started = false
    try {
        await hook.run((runAction) => {
            runAction(() => { started = true })
            expect(started).toBe(true)
        })
        expect(error).not.toHaveBeenCalled()
    } finally {
        await hook.dispose()
        error.mockRestore()
    }
})
