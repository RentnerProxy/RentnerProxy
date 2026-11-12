import { useCallback } from 'react'
import { toast } from '@rentnerkev/toasts'
import { runDetachedTask } from '@/lib/Async/runDetachedTask.ts'
import useTranslationStore from '@/shared/Language/Hooks/useTranslationStore.ts'

/** Keep native event timing while reporting otherwise unhandled action failures with safe, translated feedback. */
export default function useAsyncAction(): (operation: () => unknown) => void {
    const { t } = useTranslationStore()
    return useCallback(
        (operation: () => unknown) => {
            runDetachedTask(operation, () => {
                toast.error(t('system.error.title'), { title: t('toast.titles.error') })
            })
        },
        [t],
    )
}
