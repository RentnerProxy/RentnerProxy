// Update this only for a reviewed data compatibility change, not for each beta.
// A different contract defaults to manual review until a new wire policy is implemented.
export const CURRENT_COMPATIBILITY_PROFILE = {
    generation: 'appliance-v1',
    major: '1',
    sourceChannels: ['beta', 'stable'],
    targetChannels: ['beta', 'stable'],
    minimumSourceVersion: 'v1.0.0-beta.1',
    minimumTargetVersion: 'v1.0.0-beta.2',
    backup: { writtenVersion: 4, acceptedVersions: [4] },
} as const

export const RELEASE_REPOSITORY = 'RentnerProxy/platform'
export const RELEASE_IMAGE = 'ghcr.io/rentnerproxy/platform'
export const RELEASE_IMAGE_SOURCE = 'https://github.com/RentnerProxy/platform'
export const LEGACY_RELEASE_IMAGE = {
    repository: 'ghcr.io/rentnerkev/rentnerproxy',
    source: 'https://github.com/RentnerKev/RentnerProxy',
    tags: ['v1.0.0-beta.1', 'v1.0.0-beta.2', 'v1.0.0-beta.3'],
} as const
export const READINESS_ASSET = 'rentnerproxy-update.json'
