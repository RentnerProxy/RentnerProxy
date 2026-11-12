import useAsyncAction from '@/shared/Feedback/Hooks/useAsyncAction.ts'
import type { ConfirmDialogLogicResult, UseConfirmDialogLogicParams } from '../Types/modal.types.ts'

export default function useConfirmDialogLogic({
    isPending,
    onConfirm,
}: UseConfirmDialogLogicParams): ConfirmDialogLogicResult {
    const runAction = useAsyncAction()
    return {
        handler: {
            handleConfirm: () => {
                if (!isPending) {
                    runAction(() => onConfirm())
                }
            },
        },
    }
}
