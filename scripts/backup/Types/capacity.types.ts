export type BackupCapacityRequirement = Readonly<{ path: string; bytes: number }>

export type BackupCapacityProbe = (
    path: string,
) => Promise<Readonly<{ device: number; availableBytes: number }>>

export type RestoreCapacityEvidence = Readonly<{ databaseBytes: number; sqlBytes: number }>
