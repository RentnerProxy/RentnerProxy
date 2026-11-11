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

# Require a completed tick from this controller before startup or maintenance admits steady-state work.
# A new ready process may still be waiting for its first recovery tick; old misses clear only after proof.
wait_for_controller_progress() {
    local deadline=$((SECONDS + 60)) remaining budget
    while ((SECONDS < deadline)); do
        assert_children_running
        remaining=$((deadline - SECONDS))
        ((remaining > 0)) || break
        budget=$remaining
        ((budget <= 4)) || budget=4
        if timeout --signal=TERM --kill-after=1s "${budget}s" \
            gosu rentnerproxy /usr/local/bin/rentnerproxy-controller --healthcheck progress >/dev/null 2>&1; then
            ((SECONDS <= deadline)) || break
            assert_children_running
            controller_health_failures=0
            return
        fi
        assert_children_running
        ((SECONDS < deadline)) || break
        sleep 1
    done
    fatal 'timed out waiting for controller recovery progress within 60 seconds'
}

# Return the kernel start time used with a PID to prove ownership across recovery.
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

# Treat zombies and exited tasks as stopped even when their PID still exists.
supervisor_process_live() {
    local state remainder
    [[ -r /proc/$1/stat ]] || return 1
    IFS= read -r remainder 2>/dev/null < "/proc/$1/stat" || return 1
    remainder=${remainder##*) }
    state=${remainder%% *}
    [[ $state != Z && $state != X ]] && kill -0 "$1" 2>/dev/null
}

# Loss of any owned non-web service requires appliance teardown, including during web drain.
assert_critical_children_running() {
    local index
    for index in "${!child_pids[@]}"; do
        [[ ${child_names[$index]} != web ]] || continue
        supervisor_process_live "${child_pids[$index]}" \
            || fatal "${child_names[$index]} stopped unexpectedly"
    done
}

# Require steady-state ownership and both transient and persisted maintenance fences
# before admitting replacement web writers; the private journal probe is bounded to four seconds.
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

# Prove that a surviving process has no web UID in any credential field.
# Missing status is safe only after the process directory disappears; unreadable live identities fail closed.
assert_web_process_stopped() {
    local status_file=$1 field real effective saved filesystem remainder attempt
    local inspected=false writer=false
    # An opened proc file can lose its task while the numeric path still exists.
    # Reopen briefly for fresh evidence; elapsed time never substitutes for proof.
    for ((attempt=1; attempt<=3; attempt++)); do
        while read -r field real effective saved filesystem remainder; do
            if [[ $field == State: && ( $real == Z || $real == X ) ]]; then
                inspected=true
                break
            fi
            [[ $field == Uid: ]] || continue
            [[ $real =~ ^[0-9]+$ && $effective =~ ^[0-9]+$ && $saved =~ ^[0-9]+$ \
                && $filesystem =~ ^[0-9]+$ && -z $remainder ]] || break
            inspected=true
            case " $real $effective $saved $filesystem " in
                *' 10002 '*) writer=true ;;
            esac
            break
        done 2>/dev/null < "$status_file" || :
        $writer && fatal 'a web writer survived isolated recovery'
        $inspected && return 0
        [[ ! -d ${status_file%/*} ]] && return 0
        if ((attempt < 3)); then sleep 0.1; fi
    done
    fatal 'web recovery cannot inspect a surviving process identity'
}

# Inspect every visible process, including untracked descendants, before a second web writer starts.
assert_web_writers_stopped() {
    local status_file
    [[ -r /proc/self/status ]] || fatal 'web recovery requires process identity visibility'
    for status_file in /proc/[0-9]*/status; do
        assert_web_process_stopped "$status_file"
    done
}

# Spend one of three restart attempts in a rolling 300-second window.
# Successful recoveries retain their attempts so repeated crashes cannot evade the circuit breaker.
reserve_web_restart() {
    local started
    local -a recent=()
    for started in "${web_restart_times[@]}"; do
        if ((SECONDS - started < 300)); then recent+=("$started"); fi
    done
    ((${#recent[@]} < 3)) || fatal 'web recovery circuit open: three restarts within 300 seconds'
    web_restart_times=("${recent[@]}" "$SECONDS")
}

# Stop and reap the exact owned web process with an 18-second TERM and two-second KILL budget.
# Recheck writer and maintenance fences around bounded backoff before readiness admits the replacement.
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

# Sample controller progress, dependency functionality and web liveness every five seconds in steady state.
# Three web misses permit isolated recovery; three controller or dependency misses require appliance teardown.
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

# Require an actual PONG within two seconds; CLI success alone can conceal a RESP server error.
probe_valkey_health() {
    local response
    response=$(timeout --signal=TERM --kill-after=1s 2s \
        gosu rentnerproxy "$valkey_directory/bin/valkey-cli" -e -h 127.0.0.1 ping) || return 1
    [[ $response == PONG ]]
}

# Reset a dependency's consecutive-miss count after success and fail closed on its third failure.
# Restarting Bun cannot repair a broken state service or backup agent.
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
