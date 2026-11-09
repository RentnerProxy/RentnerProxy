#!/usr/bin/env bash

# Sourced by the root supervisor. Only a completely stopped web identity can
# recover independently; controller/Caddy ownership stays with the appliance.
supervisor_phase=starting
web_process_start=''
web_health_failures=0
controller_health_failures=0
web_recovery_active=false
next_health_check=0
declare -a web_restart_times=()
declare -A dependency_health_failures=()

supervisor_process_start() {
    local value
    IFS= read -r value 2>/dev/null < "/proc/$1/stat" || return 1
    # comm may contain spaces and parentheses. Fields after its final ')' start
    # at state (3); starttime (22) is therefore the twentieth remaining field.
    value=${value##*) }
    local -a fields
    read -r -a fields <<< "$value"
    [[ ${fields[19]:-} =~ ^[0-9]+$ ]] || return 1
    printf '%s\n' "${fields[19]}"
}

supervisor_process_live() {
    local state remainder
    [[ -r /proc/$1/stat ]] || return 1
    IFS= read -r remainder 2>/dev/null < "/proc/$1/stat" || return 1
    remainder=${remainder##*) }
    state=${remainder%% *}
    [[ $state != Z && $state != X ]] && kill -0 "$1" 2>/dev/null
}

assert_critical_children_running() {
    local index
    for index in "${!child_pids[@]}"; do
        [[ ${child_names[$index]} != web ]] || continue
        supervisor_process_live "${child_pids[$index]}" \
            || fatal "${child_names[$index]} stopped unexpectedly"
    done
}

assert_web_restart_allowed() {
    [[ $supervisor_phase == running && -z ${backup_recovery_job:-} && -z ${backup_maintenance_id:-} ]] \
        || fatal 'independent web recovery is unavailable during lifecycle transitions'
    local name
    for name in request.json active.json; do
        [[ ! -e $backup_control_directory/$name && ! -L $backup_control_directory/$name ]] \
            || fatal 'independent web recovery is unavailable during maintenance'
    done
    timeout --signal=TERM --kill-after=1s 4s \
        bun --no-env-file "$backup_exporter" --assert-web-restart-safe \
        || fatal 'web recovery cannot prove the persisted maintenance fence safe'
}

assert_web_writers_stopped() {
    local status_file field real effective saved filesystem remainder
    [[ -r /proc/self/status ]] || fatal 'web recovery requires process identity visibility'
    for status_file in /proc/[0-9]*/status; do
        [[ -r $status_file ]] || continue
        while read -r field real effective saved filesystem remainder; do
            if [[ $field == State: && ( $real == Z || $real == X ) ]]; then break; fi
            [[ $field == Uid: ]] || continue
            case " $real $effective $saved $filesystem " in
                *' 10002 '*) fatal 'a web writer survived isolated recovery' ;;
            esac
            break
        done < "$status_file" 2>/dev/null
    done
}

reserve_web_restart() {
    local started
    local -a recent=()
    for started in "${web_restart_times[@]}"; do
        if ((SECONDS - started < 300)); then recent+=("$started"); fi
    done
    ((${#recent[@]} < 3)) || fatal 'web recovery circuit open: three restarts within 300 seconds'
    web_restart_times=("${recent[@]}" "$SECONDS")
}

recover_web() {
    local index=$1 reason=$2 pid=${child_pids[$1]} deadline current_start backoff status=0
    assert_web_restart_allowed
    reserve_web_restart
    web_recovery_active=true
    log "web degraded ($reason); isolated recovery attempt ${#web_restart_times[@]}/3"
    current_start=$(supervisor_process_start "$pid") || current_start=''
    if supervisor_process_live "$pid"; then
        [[ -n $web_process_start && $current_start == "$web_process_start" ]] \
            || fatal 'web process identity changed before recovery'
        kill -TERM "$pid" 2>/dev/null || true
        deadline=$((SECONDS + 18))
        while supervisor_process_live "$pid" && ((SECONDS < deadline)); do
            assert_critical_children_running
            sleep 0.1
        done
        if supervisor_process_live "$pid"; then
            [[ $(supervisor_process_start "$pid") == "$web_process_start" ]] \
                || fatal 'web process identity changed before forced stop'
            kill -KILL "$pid" 2>/dev/null || true
            deadline=$((SECONDS + 2))
            while supervisor_process_live "$pid" && ((SECONDS < deadline)); do sleep 0.1; done
        fi
    elif [[ -n $current_start && -n $web_process_start && $current_start != "$web_process_start" ]]; then
        fatal 'web process identity changed after exit'
    fi
    supervisor_process_live "$pid" && fatal 'web termination could not be confirmed'
    # Only wait after confirmed exit. A timeout never permits a second writer.
    wait "$pid" 2>/dev/null || status=$?
    unset "child_pids[$index]" "child_names[$index]"
    assert_web_writers_stopped
    assert_critical_children_running
    assert_web_restart_allowed
    # Bounded 1/2/4-second backoff plus up to 249ms jitter; budget survives
    # successful starts so a repeatedly crashing process cannot reset it.
    printf -v backoff '%d.%03d' "$((1 << (${#web_restart_times[@]} - 1)))" "$((RANDOM % 250))"
    sleep "$backoff"
    assert_web_restart_allowed
    start_web
    wait_for_web_readiness
    web_recovery_active=false
    web_health_failures=0
    next_health_check=$((SECONDS + 5))
    log "web isolated recovery completed (previous exit $status); controller and Caddy retained"
}

monitor_runtime_health() {
    [[ $supervisor_phase == running && $web_recovery_active == false ]] || return 0
    ((SECONDS >= next_health_check)) || return 0
    next_health_check=$((SECONDS + 5))
    # Native recovery progress is independent of proxy readiness and the web's
    # database/cache dependencies. An unresponsive controller is never replaced
    # alongside an orphaned Caddy; the container boundary clears both.
    if timeout --signal=TERM --kill-after=1s 4s \
        gosu rentnerproxy /usr/local/bin/rentnerproxy-controller --healthcheck progress; then
        controller_health_failures=0
    else
        controller_health_failures=$((controller_health_failures + 1))
        log "controller recovery progress degraded ($controller_health_failures/3)"
        ((controller_health_failures < 3)) || fatal 'controller recovery progress stopped'
    fi
    check_dependency_health PostgreSQL timeout --signal=TERM --kill-after=1s 2s \
        gosu postgres psql --no-psqlrc --no-password --host=/var/run/postgresql \
        --username=rentnerproxy --dbname=rentnerproxy --set=ON_ERROR_STOP=1 --command='SELECT 1'
    check_dependency_health Valkey probe_valkey_health
    check_dependency_health backups curl --silent --fail --max-time 2 --output /dev/null \
        --unix-socket "$backup_control_directory/agent.sock" http://localhost/status
    if curl --silent --fail --max-time 2 --output /dev/null http://127.0.0.1:3000/health/live; then
        web_health_failures=0
    else
        web_health_failures=$((web_health_failures + 1))
        log "web liveness degraded ($web_health_failures/3)"
        if ((web_health_failures >= 3)); then
            local index
            for index in "${!child_pids[@]}"; do
                if [[ ${child_names[$index]} == web ]]; then recover_web "$index" liveness; return; fi
            done
            fatal 'web recovery has no owned process'
        fi
    fi
}

probe_valkey_health() {
    local response
    response=$(timeout --signal=TERM --kill-after=1s 2s \
        gosu rentnerproxy "$valkey_directory/bin/valkey-cli" -e -h 127.0.0.1 ping) || return 1
    [[ $response == PONG ]]
}

check_dependency_health() {
    local role=$1 failures
    shift
    if "$@" >/dev/null 2>&1; then
        dependency_health_failures[$role]=0
        return
    fi
    failures=$(( ${dependency_health_failures[$role]:-0} + 1 ))
    dependency_health_failures[$role]=$failures
    log "$role functional health degraded ($failures/3); web recovery is not a dependency repair"
    ((failures < 3)) || fatal "$role remained unresponsive"
}
