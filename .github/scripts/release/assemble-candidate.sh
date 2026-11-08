#!/usr/bin/env bash
set -Eeuo pipefail
: "${AUTOMATION_DIRECTORY:?}" "${IMAGE_DIRECTORY:?}" "${IMAGE_OUTPUT:?}" "${GITHUB_OUTPUT:?}"
for architecture in amd64 arm64; do
    prefix=${architecture^^}
    archive_key="${prefix}_ARCHIVE_SHA256"
    archive_sha=${!archive_key}
    [[ "$archive_sha" =~ ^[a-f0-9]{64}$ ]]
    printf '%s  %s\n' "$archive_sha" "$IMAGE_DIRECTORY/$architecture/candidate-image.tar" | sha256sum --check --strict
    skopeo copy --all --preserve-digests "oci-archive:$IMAGE_DIRECTORY/$architecture/candidate-image.tar" "oci:$IMAGE_DIRECTORY/$architecture/layout:candidate"
done
bun --no-env-file "$AUTOMATION_DIRECTORY/.github/scripts/release/multiarch-image.ts" candidate
tar --create --file="$IMAGE_DIRECTORY/candidate-image.tar" --directory="$IMAGE_OUTPUT" oci-layout index.json blobs
expected_digest=$(jq --raw-output .digest "$IMAGE_DIRECTORY/candidate.json")
actual_digest="sha256:$(skopeo inspect --raw "oci-archive:$IMAGE_DIRECTORY/candidate-image.tar" | sha256sum | cut -d ' ' -f1)"
[[ "$actual_digest" == "$expected_digest" ]]
printf 'archive_sha256=%s\n' "$(sha256sum "$IMAGE_DIRECTORY/candidate-image.tar" | cut -d ' ' -f1)" >> "$GITHUB_OUTPUT"
