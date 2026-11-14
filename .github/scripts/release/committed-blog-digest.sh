#!/usr/bin/env bash
set -Eeuo pipefail

# Only local temporary files are written; a committed article must survive a failed rebuild.
marker_id=''
while IFS=$'\t' read -r asset_id asset_name; do
    [[ "$asset_name" == 'rentnerproxy-blog-ready.json' ]] || continue
    [[ -z "$marker_id" && "$asset_id" =~ ^[1-9][0-9]*$ ]] || {
        echo '::error::The committed blog marker has duplicate or invalid asset identifiers.' >&2
        exit 1
    }
    marker_id="$asset_id"
done <<< "$RELEASE_ASSETS"
[[ -n "$marker_id" ]] || exit 0
metadata="$(gh api "repos/$GITHUB_REPOSITORY/releases/assets/$marker_id" \
    --jq '[.id, .name, .state, .size] | @tsv')"
IFS=$'\t' read -r actual_id actual_name state size <<< "$metadata"
size="${size%$'\r'}"
[[ "$actual_id" == "$marker_id" && "$actual_name" == 'rentnerproxy-blog-ready.json' && "$size" =~ ^[0-9]+$ ]] || exit 1
# Interrupted uploads have not committed an article. Leave their cleanup to the final publisher.
if [[ "$state" == starter || ( "$state" == uploaded && "$size" == 0 ) ]]; then exit 0; fi
[[ "$state" == uploaded && "$size" -gt 0 && "$size" -le 16384 ]] || exit 1
[[ "$RELEASE_REVISION" =~ ^[a-f0-9]{40}$ ]] || exit 1
directory="$(mktemp -d "$RUNNER_TEMP/committed-blog.XXXXXX")"
trap 'rm -rf -- "$directory"' EXIT
gh api "repos/$GITHUB_REPOSITORY/releases/assets/$marker_id" \
    -H 'Accept: application/octet-stream' > "$directory/marker.json"
[[ "$(wc -c < "$directory/marker.json")" -le 16384 ]] || exit 1
jq -e --argjson id "$RELEASE_ID" --arg tag "$RELEASE_TAG" \
    --arg revision "$RELEASE_REVISION" --arg published "$RELEASE_PUBLISHED_AT" '
    type == "object" and .schemaVersion == 1 and .releaseId == $id and
    .tag == $tag and .revision == $revision and .publishedAt == $published and
    (.contentRevision | type == "number" and . >= 1 and . <= 9007199254740991 and floor == .) and
    (.postSha256 | type == "string" and test("^[a-f0-9]{64}$")) and
    (.readyAt | type == "string" and test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{3})?Z$") and
        (sub("\\.[0-9]{3}Z$"; "Z") | fromdateiso8601 | . >= 0)) and
    .postAsset == ("rentnerproxy-blog-post-r" + (.contentRevision | tostring) + ".json")
' "$directory/marker.json" > /dev/null
post_name="$(jq -r '.postAsset' "$directory/marker.json" | tr -d '\r')"
post_id=''
while IFS=$'\t' read -r asset_id asset_name; do
    [[ "$asset_name" == "$post_name" ]] || continue
    [[ -z "$post_id" && "$asset_id" =~ ^[1-9][0-9]*$ ]] || exit 1
    post_id="$asset_id"
done <<< "$RELEASE_ASSETS"
[[ -n "$post_id" ]] || exit 1
gh api "repos/$GITHUB_REPOSITORY/releases/assets/$post_id" \
    -H 'Accept: application/octet-stream' > "$directory/post.json"
[[ "$(wc -c < "$directory/post.json")" -le 1048576 ]] || exit 1
post_hash="$(sha256sum < "$directory/post.json" | cut -d ' ' -f 1)"
[[ "$post_hash" == "$(jq -r '.postSha256' "$directory/marker.json" | tr -d '\r')" ]] || exit 1
jq -er --slurpfile marker "$directory/marker.json" --arg channel "$RELEASE_CHANNEL" '
    . as $post | $marker[0] as $marker |
    select(type == "object" and .schemaVersion == 1 and .releaseId == $marker.releaseId and
        .tag == $marker.tag and .revision == $marker.revision and .publishedAt == $marker.publishedAt and
        .contentRevision == $marker.contentRevision and .channel == $channel and
        .image.repository == "ghcr.io/rentnerproxy/platform" and
        (.image.digest | type == "string" and test("^sha256:[a-f0-9]{64}$"))) |
    $post.image.digest
' "$directory/post.json" | tr -d '\r'
