FROM mirror.gcr.io/oven/bun:1.4.4@sha256:1d22548f177f4ff38e0c15baa3c2c46c6c2060ba2d1bc573bb4720b407548f1d AS bun

FROM mirror.gcr.io/library/postgres:18.7@sha256:9f4fd4aa918f52ae3d6f3b687f1de90d8f867cd4d520021545a9a50c120eae3b
COPY --from=bun /usr/local/bin/bun /usr/local/bin/bun

RUN apt-get update \
    && apt-get install -y --no-install-recommends git jq ca-certificates apt-utils createrepo-c gnupg gpgv podman rpm rpm2cpio skopeo \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10002 rentnerproxy \
    && useradd --uid 10002 --gid rentnerproxy --no-create-home rentnerproxy \
    && groupadd --gid 10003 crowdsec \
    && useradd --uid 10003 --gid crowdsec --no-create-home crowdsec \
    && git config --system --add safe.directory /workspace

WORKDIR /workspace
ENV RENTNERPROXY_PACKAGE_TEST=1
ENTRYPOINT ["bun", "--no-env-file"]
