import type { MANAGED_BACKUP_SMOKE_STAGES } from '../managed-backup-diagnostics.ts'

export type ManagedBackupSmokeStage = (typeof MANAGED_BACKUP_SMOKE_STAGES)[number]
