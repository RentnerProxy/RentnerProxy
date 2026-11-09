#!/usr/bin/env bash

# Run only in a disposable Linux sandbox for --filesystem. --environment needs
# Bash and standard text utilities only; neither mode starts appliance services.
set -Eeuo pipefail

readonly source_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
readonly entrypoint="$source_directory/production/entrypoint.sh"
readonly supervisor="$source_directory/crowdsec/runtime/supervisor.sh"

load_function() {
    local file=$1 name=$2 definition
    definition=$(sed -n "/^$name() {$/,/^}$/p" "$file")
    [[ -n $definition ]] || { printf 'missing function: %s\n' "$name" >&2; exit 1; }
    source /dev/stdin <<< "$definition"
}

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
fatal() { exit 1; }

# Verify public-origin validation and child-specific credential exports using extracted production launch functions.
test_environment() (
    local variant role expected
    load_function "$entrypoint" require_public_origin
    for variant in new legacy both empty missing; do
        (
            unset PUBLIC_ORIGIN RENTNERPROXY_PUBLIC_ORIGIN
            case $variant in
                new) export PUBLIC_ORIGIN=https://management.example.invalid ;;
                legacy) export RENTNERPROXY_PUBLIC_ORIGIN=https://legacy.example.invalid ;;
                both) export PUBLIC_ORIGIN=https://management.example.invalid RENTNERPROXY_PUBLIC_ORIGIN=https://legacy.example.invalid ;;
                empty) export PUBLIC_ORIGIN='' RENTNERPROXY_PUBLIC_ORIGIN=https://legacy.example.invalid ;;
            esac
            if [[ $variant == empty || $variant == missing ]]; then
                if (require_public_origin); then fail "$variant public origin accepted"; fi
            else
                require_public_origin || fail "$variant public origin rejected"
            fi
        )
    done
    printf 'PASS: public origin aliases retain required startup validation\n'
    # The extracted preamble includes the real de-export instruction. It stops
    # before any fixed production paths or startup calls can be evaluated.
    load_function "$entrypoint" start_child
    load_function "$entrypoint" run_web
    load_function "$entrypoint" start_web
    source "$source_directory/production/fault-supervisor.sh"
    for variant in configured empty unset; do
        (
            case $variant in
                configured) export SMTP_USER=dummy-user SMTP_PASSWORD=dummy-password ;;
                empty) export SMTP_USER='' SMTP_PASSWORD='' ;;
                unset) unset SMTP_USER SMTP_PASSWORD ;;
            esac
            source /dev/stdin <<< "$(sed '/^readonly app_directory=/,$d' "$entrypoint")"
            export RENTNERPROXY_PROXY_PUBLIC_HTTPS_PORT=1443
            declare -a child_pids=() child_names=()
            probe_non_web() {
                # Probe a fresh interpreter, which sees exported variables only.
                bash -c '[[ ! -v SMTP_USER && ! -v SMTP_PASSWORD && $RENTNERPROXY_PROXY_PUBLIC_HTTPS_PORT == 1443 ]]'
            }
            for role in controller postgres valkey crowdsec-supervisor bootstrap migrations; do
                start_child "$role" probe_non_web
            done
            # Intercept command dispatch and inspect exports in a fresh shell;
            # do not run gosu, Bun or any application code.
            gosu() {
                bash -ec '
                    case $1 in
                        configured) [[ $SMTP_USER == dummy-user && $SMTP_PASSWORD == dummy-password ]] ;;
                        empty) [[ -v SMTP_USER && -v SMTP_PASSWORD && -z $SMTP_USER && -z $SMTP_PASSWORD ]] ;;
                        unset) [[ ! -v SMTP_USER && ! -v SMTP_PASSWORD ]] ;;
                    esac
                    [[ $RENTNERPROXY_PROXY_PUBLIC_HTTPS_PORT == 1443 ]]
                ' fixture "$variant"
            }
            exec() { "$@"; }
            env() {
                local assignment
                while [[ ${1:-} == *=* ]]; do
                    assignment=$1
                    export "$assignment"
                    shift
                done
                "$@"
            }
            local app_key_file=dummy-key database_url_file=dummy-database
            local controller_token_file=dummy-token app_directory=dummy-app
            start_web
            for expected in "${child_pids[@]}"; do
                wait "$expected" || fail "$variant environment isolation"
            done
            probe_non_web || fail 'web export leaked into the supervisor'
        )
    done
    (
        export SMTP_USER=dummy-user SMTP_PASSWORD=dummy-password
        source /dev/stdin <<< "$(sed '/^readonly config_file=/,$d' "$supervisor")"
        bash -c '[[ ! -v SMTP_USER && ! -v SMTP_PASSWORD ]]' \
            || fail 'standalone CrowdSec supervisor retained SMTP credentials'
    )
    printf 'PASS: configured, empty and unset SMTP credentials stay in the web child\n'
)

# Exercise the real controller launch path without allowing proxy readiness to substitute for worker progress.
test_controller_startup_progress() (
    source "$source_directory/production/fault-supervisor.sh"
    load_function "$entrypoint" start_controller
    load_function "$entrypoint" wait_for_command
    local controller_app_key_file=fixture-key controller_token_file=fixture-token
    local proxy_directory=fixture-proxy generation progress_attempts=0 child_checks=0 admissions=0
    local permanent=false budgets='' status probe_cost remaining
    start_child() { [[ $1 == controller ]] || fail 'unexpected controller launch role'; }
    assert_children_running() { child_checks=$((child_checks + 1)); }
    sleep() { [[ $1 == 1 ]] || fail 'unexpected startup sleep'; SECONDS=$((SECONDS + 1)); }
    gosu() {
        case $* in
            'rentnerproxy /usr/local/bin/rentnerproxy-controller --healthcheck ready') return 0 ;;
            'rentnerproxy /usr/local/bin/rentnerproxy-controller --healthcheck progress')
                [[ $controller_health_failures == 2 ]] || fail 'pending controller progress cleared prior misses'
                progress_attempts=$((progress_attempts + 1))
                if $permanent; then return 1; fi
                ((progress_attempts >= 3)) ;;
            *) fail 'unexpected controller startup probe' ;;
        esac
    }
    timeout() {
        [[ $1 == --signal=TERM && $2 == --kill-after=1s && $3 =~ ^[1-4]s$ ]] || fail 'controller progress probe is unbounded'
        remaining=$((60 - SECONDS))
        ((${3%s} <= remaining)) || fail 'controller progress probe exceeded remaining deadline'
        budgets+=" $3"
        shift 3
        status=0
        "$@" || status=$?
        if $permanent; then
            # Vary probe latency so the final attempt has less than the normal four-second budget.
            probe_cost=$((progress_attempts % 2 == 0 ? 4 : 1))
            ((probe_cost <= remaining)) || probe_cost=$remaining
            SECONDS=$((SECONDS + probe_cost))
        fi
        return "$status"
    }
    start_web() {
        [[ $controller_health_failures == 0 && $progress_attempts == 3 ]] || fail 'web admitted before verified controller progress'
        admissions=$((admissions + 1))
    }
    for generation in 1 2; do
        controller_health_failures=2
        next_health_check=0
        progress_attempts=0
        SECONDS=0
        start_controller
        [[ $controller_health_failures == 0 && $progress_attempts == 3 && $next_health_check == 0 ]] \
            || fail 'intentional controller replacement did not verify fresh progress without changing monitor cadence'
        start_web
    done
    [[ $admissions == 2 && $child_checks -gt 0 ]] || fail 'repeated controller startup skipped ownership checks'
    if (
        permanent=true
        progress_attempts=0
        controller_health_failures=2
        SECONDS=0
        budgets=''
        fatal() {
            [[ $* == 'timed out waiting for controller recovery progress within 60 seconds' ]] || exit 88
            [[ $SECONDS == 60 && $controller_health_failures == 2 && $budgets == *' 2s' ]] || exit 89
            exit 77
        }
        start_controller
        start_web
        fail 'permanently stalled controller admitted web'
    ); then
        fail 'permanently stalled controller startup succeeded'
    else
        status=$?
        [[ $status == 77 ]] || fail 'controller startup did not retain the real elapsed deadline and pending failure streak'
    fi
    local boundary
    for boundary in expired-before-probe late-success; do
        if (
            progress_attempts=0
            controller_health_failures=2
            SECONDS=0
            assert_children_running() {
                # Model scheduling delay after the loop condition but before probe admission.
                [[ $boundary != expired-before-probe ]] || SECONDS=60
            }
            timeout() {
                [[ $boundary != expired-before-probe ]] || exit 88
                [[ $3 =~ ^[1-4]s$ ]] || exit 89
                progress_attempts=1
                SECONDS=61
                return 0
            }
            fatal() {
                [[ $* == 'timed out waiting for controller recovery progress within 60 seconds' && $controller_health_failures == 2 ]] || exit 90
                if [[ $boundary == expired-before-probe ]]; then
                    [[ $progress_attempts == 0 && $SECONDS == 60 ]] || exit 91
                else
                    [[ $progress_attempts == 1 && $SECONDS == 61 ]] || exit 92
                fi
                exit 77
            }
            start_controller
            start_web
            exit 93
        ); then
            fail "$boundary admitted controller startup"
        else
            status=$?
            [[ $status == 77 ]] || fail "$boundary bypassed the progress deadline or issued an unbounded probe"
        fi
    done
    (
        SECONDS=0
        controller_health_failures=2
        assert_children_running() { :; }
        timeout() { SECONDS=60; return 0; }
        start_controller
        [[ $controller_health_failures == 0 ]] || fail 'verified progress exactly at the deadline was rejected'
    )
    printf 'PASS: repeated controller starts require fresh worker progress; pending work retains misses and permanent stalls fail closed within 60s\n'
)

test_filesystem() (
    [[ $(uname -s) == Linux && $(id -u) == 0 ]] \
        || { printf 'SKIP: filesystem cases require disposable Linux namespace root\n'; exit 77; }
    command -v setpriv >/dev/null \
        || { printf 'SKIP: filesystem cases require setpriv\n'; exit 77; }
    local scratch
    scratch=$(mktemp -d "${TMPDIR:-/tmp}/rentnerproxy-boundaries.XXXXXX")
    [[ $scratch == "${TMPDIR:-/tmp}"/rentnerproxy-boundaries.* ]] || exit 1
    trap 'rm -rf -- "$scratch"' EXIT
    chmod 0711 "$scratch"

    # Numeric namespace identities avoid depending on or creating host accounts.
    install() {
        local index
        local -a arguments=("$@")
        for ((index = 1; index < ${#arguments[@]}; index++)); do
            case ${arguments[$((index - 1))]} in
                -o | -g)
                    case ${arguments[$index]} in
                        root) arguments[$index]=0 ;;
                        rentnerproxy) arguments[$index]=10001 ;;
                        crowdsec) arguments[$index]=10003 ;;
                    esac ;;
            esac
        done
        command install "${arguments[@]}"
    }
    gosu() {
        local identity=$1
        shift
        case $identity in
            crowdsec) identity=10003 ;;
            rentnerproxy) identity=10001 ;;
            *) fail 'unexpected fixture identity' ;;
        esac
        setpriv --reuid="$identity" --regid="$identity" --clear-groups -- "$@"
    }
    load_function "$entrypoint" initialize_crowdsec_layout
    load_function "$entrypoint" initialize_controller_app_key
    load_function "$supervisor" ensure_online_credentials

    local crowdsec_directory="$scratch/crowdsec" protected="$scratch/protected"
    install -d -m 0700 -o rentnerproxy -g rentnerproxy "$protected"
    local before
    before=$(stat -c '%u:%g:%a' "$protected")
    local child
    for child in credentials data bouncer; do
        rm -rf -- "$crowdsec_directory"
        install -d -m 0711 -o crowdsec -g crowdsec "$crowdsec_directory"
        gosu crowdsec ln -s "$protected" "$crowdsec_directory/$child"
        if (initialize_crowdsec_layout); then fail "accepted linked $child directory"; fi
        [[ $(stat -c '%u:%g:%a' "$protected") == "$before" ]] || fail 'protected metadata changed'
        [[ $(stat -c '%u:%g:%a' "$crowdsec_directory") == 0:0:711 ]] || fail 'parent was not frozen'
    done
    rm -rf -- "$crowdsec_directory"
    initialize_crowdsec_layout
    gosu crowdsec touch "$crowdsec_directory/credentials/dummy"
    gosu crowdsec touch "$crowdsec_directory/data/dummy"
    if gosu crowdsec mv "$crowdsec_directory/data" "$crowdsec_directory/replaced" 2>/dev/null; then
        fail 'CrowdSec could replace a frozen directory entry'
    fi
    initialize_crowdsec_layout
    [[ -f $crowdsec_directory/credentials/dummy && -f $crowdsec_directory/data/dummy ]] \
        || fail 'real persistent state was not preserved'

    local app_key_file="$scratch/dummy-app-key"
    local controller_app_key_file="$scratch/controller-app-key/value"
    printf 'dummy-key\n' > "$app_key_file"
    local selection
    for selection in directory symlink hardlink; do
        rm -rf -- "${controller_app_key_file%/*}"
        install -d -m 0700 -o rentnerproxy -g rentnerproxy "${controller_app_key_file%/*}"
        case $selection in
            directory) gosu rentnerproxy mkdir "$controller_app_key_file" ;;
            symlink) gosu rentnerproxy ln -s "$protected" "$controller_app_key_file" ;;
            hardlink)
                touch "$protected/dummy"
                chown 10001:10001 "$protected/dummy"
                gosu rentnerproxy ln "$protected/dummy" "$controller_app_key_file" ;;
        esac
        if (initialize_controller_app_key); then fail "accepted retained $selection leaf"; fi
        [[ $(stat -c '%u:%g:%a' "$protected") == "$before" ]] || fail 'key selector changed protected metadata'
        [[ ! -e $protected/value ]] || fail 'key was copied through a directory selector'
        rm -f -- "$protected/dummy"
    done
    rm -rf -- "${controller_app_key_file%/*}"
    initialize_controller_app_key
    initialize_controller_app_key
    cmp -s "$app_key_file" "$controller_app_key_file" || fail 'regular key copy failed'
    [[ $(stat -c '%u:%g:%a:%h' "$controller_app_key_file") == 10001:10001:400:1 ]] \
        || fail 'controller key metadata is invalid'
    gosu rentnerproxy cat "$controller_app_key_file" >/dev/null
    if gosu rentnerproxy rm "$controller_app_key_file" 2>/dev/null; then fail 'controller could select its leaf'; fi

    local capi_credentials_file="$crowdsec_directory/credentials/online_api_credentials.yaml"
    local online_config_file=dummy-config
    gosu crowdsec touch "$capi_credentials_file"
    gosu crowdsec chmod 0644 "$capi_credentials_file"
    ensure_online_credentials || fail 'existing credential chmod failed'
    [[ $(stat -c '%a' "$capi_credentials_file") == 600 ]] || fail 'credential mode was not repaired'

    # Registration is a local dummy writer; no cscli or network is invoked.
    timeout() { shift; "$@"; }
    gosu() {
        shift
        if [[ $1 == cscli ]]; then
            setpriv --reuid=10003 --regid=10003 --clear-groups -- touch "$capi_credentials_file"
        else
            setpriv --reuid=10003 --regid=10003 --clear-groups -- "$@"
        fi
    }
    rm -f -- "$capi_credentials_file"
    ensure_online_credentials || fail 'registered credential chmod failed'
    [[ $(stat -c '%a' "$capi_credentials_file") == 600 ]] || fail 'registered credential mode was not repaired'

    # Force replacement precisely between the checks and metadata operation.
    # The protected root-owned target must remain unchanged and chmod must fail.
    local protected_file="$scratch/protected-root-file"
    printf 'dummy-protected\n' > "$protected_file"
    chmod 0644 "$protected_file"
    rm -f -- "$capi_credentials_file"
    ln -s "$protected_file" "$capi_credentials_file"
    if ensure_online_credentials; then fail 'accepted an initially linked credential'; fi
    rm -f -- "$capi_credentials_file"
    touch "$capi_credentials_file"
    if ensure_online_credentials; then fail 'accepted a credential owned by root'; fi
    rm -f -- "$capi_credentials_file"
    setpriv --reuid=10003 --regid=10003 --clear-groups -- touch "$capi_credentials_file"
    gosu() {
        shift
        if [[ $1 == cscli ]]; then
            setpriv --reuid=10003 --regid=10003 --clear-groups -- touch "$capi_credentials_file"
            return
        fi
        rm -f -- "$capi_credentials_file"
        ln -s "$protected_file" "$capi_credentials_file"
        setpriv --reuid=10003 --regid=10003 --clear-groups -- "$@" 2>/dev/null
    }
    if ensure_online_credentials; then fail 'raced credential chmod did not fail closed'; fi
    [[ $(stat -c '%a' "$protected_file") == 644 ]] || fail 'credential chmod crossed its identity boundary'
    rm -f -- "$capi_credentials_file"
    if ensure_online_credentials; then fail 'registered credential race did not fail closed'; fi
    [[ $(stat -c '%a' "$protected_file") == 644 ]] || fail 'registered chmod crossed its identity boundary'
    printf 'PASS: real directories, retained key selectors and lower-identity chmod\n'
)

test_shutdown() (
    local scratch pid name supervisor_pid
    scratch=$(mktemp -d "${TMPDIR:-/tmp}/rentnerproxy-shutdown.XXXXXX")
    trap 'rm -rf -- "$scratch"' EXIT
    load_function "$entrypoint" shutdown_children
    load_function "$entrypoint" on_shutdown_signal
    load_function "$entrypoint" start_child
    log() { :; }
    (
        declare -a child_pids=() child_names=()
        trap shutdown_children EXIT
        trap on_shutdown_signal INT TERM
        for name in postgres valkey crowdsec-supervisor controller web; do
            start_child "$name" bash -c '
                name=$1; scratch=$2
                stopped() {
                    case $name in
                        web|controller)
                            [[ ! -e $scratch/postgres-stopped && ! -e $scratch/valkey-stopped ]] || touch "$scratch/ordering-error"
                            touch "$scratch/$name-stopped"
                            ;;
                        *)
                            [[ -e $scratch/web-stopped && -e $scratch/controller-stopped ]] || touch "$scratch/ordering-error"
                            touch "$scratch/$name-stopped"
                            ;;
                    esac
                    exit 0
                }
                trap stopped TERM
                touch "$scratch/$name-ready"
                while :; do sleep 0.02; done
            ' fixture "$name" "$scratch"
        done
        touch "$scratch/supervisor-ready"
        wait -n "${child_pids[@]}"
    ) &
    supervisor_pid=$!
    for name in supervisor postgres valkey crowdsec-supervisor controller web; do
        local attempt
        for attempt in $(seq 1 100); do
            [[ -e $scratch/$name-ready ]] && break
            sleep 0.02
        done
        [[ -e $scratch/$name-ready ]] || fail "$name fixture did not start"
    done
    kill -TERM "$supervisor_pid"
    wait "$supervisor_pid" || fail 'supervisor signal exit failed'
    [[ ! -e $scratch/ordering-error ]] || fail 'state services stopped before consumers'
    for name in postgres valkey crowdsec-supervisor controller web; do
        [[ -e $scratch/$name-stopped ]] || fail "$name did not receive TERM"
    done
    # Exercise the forced fallback deterministically without waiting 25s or
    # sending any signals to external processes.
    (
        declare -a child_pids=(101 102 103) child_names=(web controller postgres)
        local signals='' states_started=false
        SECONDS=0
        sleep() { SECONDS=$((SECONDS + 1)); }
        kill() {
            [[ $1 != -0 ]] || return 0
            signals+="$1:$2 "
            if [[ $1 == -TERM && $2 == 103 ]]; then
                [[ $signals == *'-KILL:101 '* && $signals == *'-KILL:102 '* ]] || fail 'consumer force stop must precede state TERM'
                ((SECONDS == 23)) || fail 'consumer phase changed the shared deadline'
                states_started=true
            fi
        }
        wait() { :; }
        shutdown_children
        "$states_started" || fail 'state shutdown was skipped'
        ((SECONDS == 25)) || fail 'shutdown phases exceeded total budget'
    )
    printf 'PASS: SIGTERM drains consumers before state services; forced phases share 25s\n'
)

case ${1:-} in
    --environment) test_environment; test_controller_startup_progress ;;
    --filesystem) test_filesystem ;;
    --shutdown) test_shutdown ;;
    *) printf 'usage: bash runtime-boundaries.sh --environment|--filesystem|--shutdown\n' >&2; exit 2 ;;
esac
