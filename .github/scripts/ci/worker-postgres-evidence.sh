#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"
reports="${2:-/reports}"
case "$mode" in profile|regressions) ;; *) exit 2 ;; esac
test "$(uname -s)" = Linux
test "$(id -u)" = 0
test -f package.json
test -f web/src/db/migrate.ts
test "$(git rev-parse HEAD)" = "$RENTNERPROXY_EVIDENCE_SOURCE_SHA"
mkdir -p "$reports"

owned="$(mktemp -d /tmp/rentnerproxy-worker-sql.XXXXXXXX)"
case "$owned" in /tmp/rentnerproxy-worker-sql.*) ;; *) exit 1 ;; esac
chown postgres:postgres "$owned"
pg=/usr/lib/postgresql/18/bin
started=0
cleanup() {
    status=$?
    trap - EXIT
    if [ "$started" = 1 ]; then
        if ! gosu postgres "$pg/pg_ctl" -D "$owned/data" -m fast -w -t 10 stop; then
            printf '%s\n' 'Owned PostgreSQL fixture cleanup failed' >&2
            status=1
        fi
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
gosu postgres "$pg/initdb" -D "$owned/data" --no-locale --encoding=UTF8 --auth-local=trust --auth-host=trust > "$owned/init.log"
gosu postgres "$pg/pg_ctl" -D "$owned/data" -l "$owned/postgres.log" -w -t 15 -o "-h 127.0.0.1 -p 55476 -k $owned -c max_connections=64 -c shared_preload_libraries=pg_stat_statements -c pg_stat_statements.track=top -c track_io_timing=on" start
started=1
"$pg/psql" --host=127.0.0.1 --port=55476 --username=postgres --dbname=postgres --set=ON_ERROR_STOP=1 --command='create database rentnerproxy_upstream_test'

export DATABASE_URL='postgresql://postgres@127.0.0.1:55476/rentnerproxy_upstream_test?sslmode=disable'
export RENTNERPROXY_DATABASE_INTEGRATION=1
bun --no-env-file run db:migrate > "$reports/migrations-$mode.log" 2>&1
if [ "$mode" = profile ]; then
    export RENTNERPROXY_POSTGRES_PROFILE=1
    export RENTNERPROXY_POSTGRES_PROFILE_CPU=1
    RENTNERPROXY_POSTGRES_CLOCK_TICKS="$(getconf CLK_TCK)"
    export RENTNERPROXY_POSTGRES_CLOCK_TICKS
    export RENTNERPROXY_POSTGRES_PROFILE_SOURCE="$RENTNERPROXY_EVIDENCE_SOURCE_SHA"
    export RENTNERPROXY_POSTGRES_PROFILE_REPORT="$reports/sql-profile.json"
    bun --no-env-file test --max-concurrency=1 --isolate ./web/src/tests/server/Foundation/worker-postgres-profile.db.test.ts 2>&1 | tee "$reports/profile.log"
else
    export RENTNERPROXY_POSTGRES_PROFILE=0
    bun --no-env-file test --max-concurrency=1 --isolate ./web/src/tests/server/Foundation/background-database.db.test.ts ./web/src/tests/server/Foundation/worker-lock-order.db.test.ts ./web/src/tests/server/HostMonitoring/host-monitoring.repository.db.test.ts ./web/src/tests/server/Notifications/notification-repository.db.test.ts 2>&1 | tee "$reports/regressions.log"
fi
