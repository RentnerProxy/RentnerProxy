import {
    LEGACY_RELEASE_IMAGE,
    RELEASE_IMAGE,
    RELEASE_IMAGE_SOURCE,
} from '../../config/compatibility.config.ts'

export function releaseImageIdentity(repository: string, tag: string) {
    if (repository === RELEASE_IMAGE)
        return { repository: RELEASE_IMAGE, source: RELEASE_IMAGE_SOURCE }
    if (
        repository === LEGACY_RELEASE_IMAGE.repository &&
        LEGACY_RELEASE_IMAGE.tags.some((historical) => historical === tag)
    )
        return {
            repository: LEGACY_RELEASE_IMAGE.repository,
            source: LEGACY_RELEASE_IMAGE.source,
        }
    return null
}
