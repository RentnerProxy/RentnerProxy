export type StateArchiveAllocationEvidence = Readonly<{ payloadBytes: number; nodes: number }>

export type StateArchiveCapacityEvidence = Readonly<{
    controllerState: StateArchiveAllocationEvidence
    crowdSecState: StateArchiveAllocationEvidence
}>

export type BackupCapacityRequirement = Readonly<{
    path: string
    bytes: number
    archive?: StateArchiveAllocationEvidence
}>

export type BackupCapacityProbe = (
    path: string,
) => Promise<Readonly<{ device: number; availableBytes: number; allocationUnitBytes?: number }>>

export type RestoreCapacityEvidence = Readonly<{ databaseBytes: number; sqlBytes: number }>
