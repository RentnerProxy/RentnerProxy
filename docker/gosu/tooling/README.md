# Patched gosu build

The appliance rebuilds the official gosu 1.20 source at
`29b939aa5bd4dc588af765b8248f6e39150d2565` with Go 1.27.3 and the frozen
`github.com/moby/sys/user` 0.4.1 dependency. That dependency fixes
[CVE-2026-61801](https://github.com/advisories/GHSA-mjcv-p78q-w5fw), which affects
the gosu binary inherited from the PostgreSQL image.

The build checks the source archive checksum, module checksums, readonly locks,
actual binary metadata and user/group switching. It retains the upstream gosu
version and license. Only the patched static binary and license enter the runtime
image. Source lock updates belong in this directory; builds never resolve a
floating version.
