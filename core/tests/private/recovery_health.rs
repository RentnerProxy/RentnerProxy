use super::*;

#[test]
/// Confirms successful ticks do not replenish the rolling restart budget or clear its open circuit.
fn restart_budget_survives_successes_and_circuit_is_latched() {
    let health = RecoveryHealth::default();
    let now = Instant::now();
    for offset in [0, 30, 60] {
        assert!(health.admit_restart(now + Duration::from_secs(offset)));
        health.completed();
    }
    assert!(!health.admit_restart(now + Duration::from_secs(61)));
    assert!(health.snapshot(now + Duration::from_secs(61)).circuit_open);
    assert!(!health.admit_restart(now + Duration::from_secs(600)));
}

#[test]
/// Checks old restart attempts expire only while the circuit is still closed.
fn restart_window_expires_only_before_circuit_is_opened() {
    let health = RecoveryHealth::default();
    let now = Instant::now();
    assert!(health.admit_restart(now));
    assert!(health.admit_restart(now + Duration::from_secs(300)));
    assert_eq!(
        health
            .snapshot(now + Duration::from_secs(300))
            .restarts_in_window,
        1
    );
}

#[test]
/// Requires live worker progress, a fresh completion, and a closed circuit simultaneously.
fn progress_requires_a_running_worker_recent_completion_and_closed_circuit() {
    let health = RecoveryHealth::default();
    let now = Instant::now();
    assert!(!health.progressing(now));
    health.worker_started();
    assert!(!health.progressing(Instant::now()));
    health.completed();
    let now = Instant::now();
    assert!(health.progressing(now));
    assert!(!health.progressing(now + Duration::from_secs(91)));
    health.worker_stopped();
    assert!(!health.progressing(now));
    health.worker_started();
    for _ in 0..3 {
        assert!(health.admit_restart(now));
    }
    assert!(!health.admit_restart(now));
    assert!(!health.progressing(now));
}
