#!/usr/bin/env bash

# Disposable process fixtures; no application, database, or production state.
set -Eeuo pipefail
readonly production_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
readonly supervisor="$production_directory/fault-supervisor.sh"

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
fatal() { printf '%s\n' "$*" >&2; exit 77; }
log() { :; }

setup() {
    scratch=$(mktemp -d "${TMPDIR:-/tmp}/rentnerproxy-fault-supervisor.XXXXXX")
    cleanup_pids=()
    trap cleanup EXIT
    source "$supervisor"
    # source executes inside this setup function; make its declared arrays
    # available to the functions exercised after setup returns.
    declare -ga web_restart_times=()
    declare -gA dependency_health_failures=()
    supervisor_phase=running
    backup_control_directory="$scratch"
    backup_exporter=fixture-exporter
    backup_recovery_job=''
    backup_maintenance_id=''
    fixture_fence=0
    fixture_controller=0
    fixture_live=0
    fixture_dependency_role=''
    fixture_dependency_failure=0
    fixture_valkey_reply=PONG
    valkey_directory=/fixture-valkey
    fixture_request_during_backoff=false
    child_pids=()
    child_names=()
    SECONDS=0
}

cleanup() {
    local pid
    for pid in "${cleanup_pids[@]}"; do
        kill -CONT "$pid" 2>/dev/null || true
        kill -KILL "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    rm -rf -- "$scratch"
}

# Keep the actual /proc, kill and wait implementation. Only deadlines and
# external application probes are accelerated/stubbed.
sleep() {
    case $1 in
        0.1) SECONDS=$((SECONDS + 20)); command sleep 0.02 ;;
        *)
            [[ $1 =~ ^[124]\.[0-9]{3}$ ]] || fail "unexpected backoff: $1"
            ((10#${1#*.} <= 249)) || fail 'jitter exceeds 249ms'
            printf '%s\n' "$1" >> "$scratch/backoffs"
            if $fixture_request_during_backoff; then touch "$scratch/request.json"; fi
            ;;
    esac
}

timeout() {
    [[ $1 == --signal=TERM && $2 == --kill-after=1s && ( $3 == 4s || $3 == 2s ) ]] || fail 'unbounded probe'
    shift 3
    "$@"
}

bun() {
    [[ $1 == --no-env-file && $2 == "$backup_exporter" && $3 == --assert-web-restart-safe ]] \
        || fail 'recovery did not consult the private journal'
    printf 'fence\n' >> "$scratch/events"
    return "$fixture_fence"
}

gosu() {
    case $* in
        'rentnerproxy /usr/local/bin/rentnerproxy-controller --healthcheck progress') return "$fixture_controller" ;;
        'postgres psql --no-psqlrc --no-password --host=/var/run/postgresql --username=rentnerproxy --dbname=rentnerproxy --set=ON_ERROR_STOP=1 --command=SELECT 1')
            [[ $fixture_dependency_role != PostgreSQL ]] || return "$fixture_dependency_failure" ;;
        "rentnerproxy $valkey_directory/bin/valkey-cli -e -h 127.0.0.1 ping")
            if [[ $fixture_dependency_role == Valkey && $fixture_dependency_failure != 0 ]]; then
                return "$fixture_dependency_failure"
            fi
            printf '%s\n' "$fixture_valkey_reply" ;;
        *) fail 'unexpected role or readiness-dependent probe' ;;
    esac
    return 0
}

curl() {
    if [[ ${*: -1} == http://localhost/status ]]; then
        [[ $* == *"--unix-socket $backup_control_directory/agent.sock"* ]] || fail 'backup probe escaped its socket'
        [[ $fixture_dependency_role != backups ]] || return "$fixture_dependency_failure"
        return 0
    fi
    [[ ${*: -1} == http://127.0.0.1:3000/health/live ]] || fail 'web probe depends on readiness'
    return "$fixture_live"
}

spawn_web() {
    local mode=${1:-term} pid attempts
    case $mode in
        term|stopped) command sleep 120 & ;;
        ignore)
            bash -c 'trap "" TERM; touch "$1"; exec sleep 120' fixture "$scratch/ignoring" &
            ;;
        *) fail "unknown fixture mode: $mode" ;;
    esac
    pid=$!
    cleanup_pids+=("$pid")
    child_pids+=("$pid")
    child_names+=(web)
    web_process_start=$(supervisor_process_start "$pid") || fail 'missing real starttime'
    if [[ $mode == ignore ]]; then
        for ((attempts=0; attempts<100; attempts++)); do
            [[ ! -e $scratch/ignoring ]] || break
            command sleep 0.01
        done
        [[ -e $scratch/ignoring ]] || fail 'TERM-ignoring child did not initialize'
    fi
    if [[ $mode == stopped ]]; then kill -STOP "$pid"; fi
}

start_web() {
    printf 'start\n' >> "$scratch/events"
    spawn_web
}

wait_for_web_readiness() { printf 'ready\n' >> "$scratch/events"; }

expect_rejected() {
    local status=0
    (trap - EXIT; "$@") > "$scratch/rejection" 2>&1 || status=$?
    [[ $status == 77 ]] || fail "expected fail-stop, got $status: $(cat "$scratch/rejection")"
}

test_real_termination() (
    local mode old_pid critical_pid
    for mode in term ignore stopped exited; do
        (
            setup
            command sleep 120 &
            critical_pid=$!
            cleanup_pids+=("$critical_pid")
            child_pids+=("$critical_pid")
            child_names+=(controller)
            if [[ $mode == exited ]]; then
                spawn_web
                kill -KILL "${child_pids[1]}"
                wait "${child_pids[1]}" 2>/dev/null || true
            else
                spawn_web "$mode"
            fi
            old_pid=${child_pids[1]}
            recover_web 1 "$mode"
            [[ ! -e /proc/$old_pid ]] || fail "$mode old child was not reaped"
            supervisor_process_live "$critical_pid" || fail 'controller was disturbed'
            [[ ${child_pids[0]} == "$critical_pid" && ${#child_pids[@]} == 2 ]] || fail 'child ownership changed'
            [[ $(cat "$scratch/events") == $'fence\nfence\nfence\nstart\nready' ]] || fail 'admission order changed'
            [[ $web_recovery_active == false && $web_health_failures == 0 ]] || fail 'recovery did not finish'
        )
    done
    printf 'PASS: crashed, TERM, ignored TERM and STOP web children are reaped before isolated admission\n'
)

test_recovery_fences() (
    local variant pid
    for variant in starting maintenance stopping request active request-link recovery backup journal unknown-identity mismatched-identity delayed-request; do
        (
            setup
            spawn_web
            pid=${child_pids[0]}
            case $variant in
                starting|maintenance|stopping) supervisor_phase=$variant ;;
                request) touch "$scratch/request.json" ;;
                active) touch "$scratch/active.json" ;;
                request-link) ln -s "$scratch/missing" "$scratch/request.json" ;;
                recovery) backup_recovery_job=42 ;;
                backup) backup_maintenance_id=fixture ;;
                journal) fixture_fence=1 ;;
                unknown-identity) web_process_start='' ;;
                mismatched-identity) web_process_start=0 ;;
                delayed-request) fixture_request_during_backoff=true ;;
            esac
            expect_rejected recover_web 0 "$variant"
            [[ ! -f $scratch/events ]] || ! command grep -q '^start$' "$scratch/events" || fail "$variant admitted web"
            if [[ $variant != delayed-request ]]; then
                supervisor_process_live "$pid" || fail "$variant signaled an unproven process"
            fi
        )
    done
    printf 'PASS: lifecycle, maintenance, persisted journal and exact process identity fences reject recovery\n'
)

test_restart_budget() (
    local attempt index
    setup
    spawn_web
    for attempt in 1 2 3; do
        index=${!child_pids[*]}
        recover_web "$index" repeated-crash
        [[ ${#web_restart_times[@]} == "$attempt" ]] || fail 'successful start reset the retry budget'
    done
    [[ $(cut -d. -f1 "$scratch/backoffs") == $'1\n2\n4' ]] || fail 'backoff is not exponential'
    index=${!child_pids[*]}
    expect_rejected recover_web "$index" fourth-crash
    [[ $(command grep -c '^start$' "$scratch/events") == 3 ]] || fail 'exhausted budget started web'
    SECONDS=400
    recover_web "$index" expired-window
    [[ ${#web_restart_times[@]} == 1 ]] || fail 'expired budget was not reclaimed'
    printf 'PASS: three successful admissions retain the rolling budget and bounded jitter/backoff\n'
)

test_budget_boundary() (
    setup
    web_restart_times=(100 101 102)
    SECONDS=399
    expect_rejected reserve_web_restart
    SECONDS=400
    reserve_web_restart
    [[ ${web_restart_times[*]} == '101 102 400' ]] || fail 'rolling window expired the wrong attempts'
    printf 'PASS: retry attempts expire individually at the 300-second boundary\n'
)

test_critical_child_failure() (
    setup
    local critical
    command sleep 120 &
    critical=$!
    cleanup_pids+=("$critical")
    child_pids+=("$critical")
    child_names+=(controller)
    spawn_web ignore
    kill -KILL "$critical"
    wait "$critical" 2>/dev/null || true
    expect_rejected recover_web 1 critical-child-exit
    [[ $(cat "$scratch/events") == fence ]] || fail 'critical process loss admitted web'
    printf 'PASS: critical process exit during web termination fails the appliance boundary\n'
)

test_health() (
    setup
    spawn_web
    # Readiness/dependencies may be unavailable: neither probe is allowed to
    # ask for readiness, and healthy liveness must not consume restart budget.
    local attempt
    for attempt in 1 2 3 4; do
        next_health_check=0
        monitor_runtime_health
    done
    [[ ${#web_restart_times[@]} == 0 ]] || fail 'healthy liveness restarted the web'
    fixture_live=1
    for attempt in 1 2 3; do next_health_check=0; monitor_runtime_health; done
    [[ ${#web_restart_times[@]} == 1 ]] || fail 'three liveness misses did not recover web'
    fixture_live=0
    fixture_controller=1
    for attempt in 1 2; do next_health_check=0; monitor_runtime_health; done
    fixture_controller=0
    next_health_check=0
    monitor_runtime_health
    [[ $controller_health_failures == 0 ]] || fail 'controller success did not reset consecutive misses'
    fixture_controller=1
    for attempt in 1 2; do next_health_check=0; monitor_runtime_health; done
    next_health_check=0
    expect_rejected monitor_runtime_health
    [[ ${#web_restart_times[@]} == 1 ]] || fail 'controller failure restarted web'
    printf 'PASS: independent liveness recovers web; three consecutive controller progress misses fail-stop\n'
)

test_dependency_health() (
    local role attempt
    for role in PostgreSQL Valkey backups; do
        (
            setup
            spawn_web
            fixture_dependency_role=$role
            fixture_dependency_failure=1
            for attempt in 1 2; do next_health_check=0; monitor_runtime_health; done
            [[ ${#web_restart_times[@]} == 0 ]] || fail "$role misses restarted web"
            fixture_dependency_failure=0
            next_health_check=0
            monitor_runtime_health
            [[ ${dependency_health_failures[$role]} == 0 ]] || fail "$role success did not reset misses"
            fixture_dependency_failure=1
            for attempt in 1 2; do next_health_check=0; monitor_runtime_health; done
            next_health_check=0
            expect_rejected monitor_runtime_health
            [[ ${#web_restart_times[@]} == 0 ]] || fail "$role failure restarted web"
        )
    done
    (
        setup
        spawn_web
        # A RESP error may have a successful CLI exit code. Only PONG proves
        # this server can execute the cache command used by the application.
        fixture_valkey_reply='BUSY script is running'
        for attempt in 1 2; do next_health_check=0; monitor_runtime_health; done
        next_health_check=0
        expect_rejected monitor_runtime_health
        [[ ${#web_restart_times[@]} == 0 ]] || fail 'RESP error restarted web'
    )
    printf 'PASS: dependency misses reset on success and fail-stop after three misses without restarting web\n'
)

test_monitor_lifecycle() (
    local phase
    setup
    fixture_controller=1
    fixture_live=1
    for phase in starting maintenance stopping; do
        supervisor_phase=$phase
        monitor_runtime_health
    done
    supervisor_phase=running
    web_recovery_active=true
    monitor_runtime_health
    [[ $web_health_failures == 0 && $controller_health_failures == 0 ]] || fail 'monitor ran during lifecycle transition'
    printf 'PASS: runtime health supervision is suppressed during startup, maintenance, shutdown and active recovery\n'
)

test_orphan_writer() (
    setup
    if [[ $(id -u) != 0 ]] || ! command -v setpriv >/dev/null; then
        printf 'SKIP: real UID 10002 orphan fixture requires disposable Linux root and setpriv\n'
        return
    fi
    local orphan attempts
    setpriv --reuid=10002 --regid=10002 --clear-groups sleep 120 &
    orphan=$!
    cleanup_pids+=("$orphan")
    for ((attempts=0; attempts<100; attempts++)); do
        if command grep -q $'^Uid:\t10002\t' "/proc/$orphan/status"; then break; fi
        command sleep 0.01
    done
    command grep -q $'^Uid:\t10002\t' "/proc/$orphan/status" || fail 'orphan fixture did not drop identity'
    spawn_web
    expect_rejected recover_web 0 orphan-writer
    [[ $(cat "$scratch/events") == fence ]] || fail 'orphan writer passed admission'
    supervisor_process_live "$orphan" || fail 'unowned writer was signaled'
    printf 'PASS: full process scan blocks admission for an unowned UID 10002 writer\n'
)

test_process_status_visibility() (
    setup
    local field variant
    # Deterministic equivalent of a /proc glob entry whose process exits before
    # its status file opens. The exact production helper must survive errexit.
    assert_web_process_stopped "$scratch/vanished/status"
    mkdir "$scratch/present"
    expect_rejected assert_web_process_stopped "$scratch/present/status"
    [[ $(cat "$scratch/rejection") == 'web recovery cannot inspect a surviving process identity' ]] \
        || fail 'missing status leaked a raw open error or bypassed the identity fence'
    # Opening a directory succeeds, but reading it fails: exercise the read
    # failure independently of the missing-file/open failure above.
    mkdir "$scratch/present/status"
    expect_rejected assert_web_process_stopped "$scratch/present/status"
    rmdir "$scratch/present/status"
    for variant in missing truncated malformed extra; do
        case $variant in
            missing) printf 'Name:\tfixture\nState:\tS (sleeping)\n' ;;
            truncated) printf 'Uid:\t0\t0\t0\n' ;;
            malformed) printf 'Uid:\t0\tunknown\t0\t0\n' ;;
            extra) printf 'Uid:\t0\t0\t0\t0\t10002\n' ;;
        esac > "$scratch/present/status"
        expect_rejected assert_web_process_stopped "$scratch/present/status"
    done
    printf 'State:\tS (sleeping)\nUid:\t10001\t10001\t10001\t10001\n' > "$scratch/present/status"
    assert_web_process_stopped "$scratch/present/status"
    for field in 0 1 2 3; do
        local -a identities=(0 0 0 0)
        identities[$field]=10002
        printf 'State:\tS (sleeping)\nUid:\t%s\t%s\t%s\t%s\n' "${identities[@]}" > "$scratch/present/status"
        expect_rejected assert_web_process_stopped "$scratch/present/status"
        [[ $(cat "$scratch/rejection") == 'a web writer survived isolated recovery' ]] \
            || fail "web UID field $field was not inspected"
    done
    for variant in Z X; do
        printf 'State:\t%s (exited)\nUid:\t10002\t10002\t10002\t10002\n' "$variant" > "$scratch/present/status"
        assert_web_process_stopped "$scratch/present/status"
    done
    printf 'PASS: vanished processes are tolerated; surviving unreadable or uninspectable identities fail closed in every UID field\n'
)

[[ $(uname -s) == Linux ]] || { printf 'SKIP: fault supervisor fixtures require Linux /proc\n'; exit 77; }
test_real_termination
test_recovery_fences
test_restart_budget
test_budget_boundary
test_critical_child_failure
test_health
test_dependency_health
test_monitor_lifecycle
test_orphan_writer
test_process_status_visibility
