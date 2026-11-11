use super::*;

#[test]
fn idle_checks_do_not_claim_work_success_and_hangs_stay_running() {
    let registry = Registry::new();
    let _resident = registry.start(Worker::Ddns);
    registry.begin(Worker::Ddns).finish(Outcome::Idle);
    let idle = registry.snapshot().workers[2].clone();
    assert!(matches!(idle.state, State::Idle));
    assert_eq!(idle.attempts, 1);
    assert_eq!(idle.successes, 0);
    assert_eq!(idle.last_success, None);
    let pass = registry.begin(Worker::Ddns);
    for _ in 0..100 {
        let running = registry.snapshot().workers[2].clone();
        assert!(matches!(running.state, State::Running));
        assert_eq!(running.running_count, 1);
        assert_eq!(running.attempts, 2);
        assert_eq!(running.last_finished, idle.last_finished);
    }
    pass.finish(Outcome::Failure);
    assert!(matches!(
        registry.snapshot().workers[2].state,
        State::Degraded
    ));
    registry.begin(Worker::Ddns).finish(Outcome::Success);
    let recovered = registry.snapshot().workers[2].clone();
    assert!(matches!(recovered.state, State::Idle));
    assert_eq!(recovered.failures, 1);
    assert_eq!(recovered.successes, 1);
    assert!(recovered.last_success.is_some());
    assert_eq!(recovered.duration_buckets.iter().sum::<u64>(), 3);
}

#[test]
fn nested_failures_survive_outer_idle_and_shutdown_cannot_be_reversed() {
    let registry = Registry::new();
    let resident = registry.start(Worker::Renewal);
    let outer = registry.begin(Worker::Renewal);
    registry.begin(Worker::Renewal).finish(Outcome::Failure);
    outer.finish(Outcome::Idle);
    assert!(matches!(
        registry.snapshot().workers[1].state,
        State::Degraded
    ));
    let late = registry.begin(Worker::Renewal);
    registry.stopping();
    drop(resident);
    late.finish(Outcome::Success);
    assert!(matches!(
        registry.snapshot().workers[1].state,
        State::Stopped
    ));
    assert_eq!(registry.snapshot().workers[1].running_count, 0);
}

#[test]
fn acme_order_activity_cannot_refresh_a_stalled_renewal_scan() {
    let registry = Registry::new();
    let _resident = registry.start(Worker::Renewal);
    let stalled_scan = registry.begin(Worker::Renewal);
    let before = registry.snapshot().workers[1].clone();
    registry.begin(Worker::Orders).finish(Outcome::Success);
    let snapshot = registry.snapshot();
    assert!(matches!(snapshot.workers[1].state, State::Running));
    assert_eq!(snapshot.workers[1].last_attempt, before.last_attempt);
    assert_eq!(snapshot.workers[1].last_success, None);
    assert_eq!(snapshot.workers[1].attempts, 1);
    assert_eq!(snapshot.workers[4].successes, 1);
    assert!(snapshot.workers[4].last_success.is_some());
    stalled_scan.finish(Outcome::Failure);
}

#[tokio::test]
async fn panic_abort_and_unexpected_resident_exit_are_observed() {
    let registry = Registry::new();
    let worker_registry = Arc::clone(&registry);
    let resident = registry.start(Worker::Docker);
    let task = tokio::spawn(async move {
        let _resident = resident;
        let _pass = worker_registry.begin(Worker::Docker);
        panic!("synthetic failure");
    });
    assert!(task.await.unwrap_err().is_panic());
    assert!(matches!(
        registry.snapshot().workers[3].state,
        State::Failed
    ));
    assert_eq!(registry.snapshot().workers[3].running_count, 0);

    let resident = registry.start(Worker::Ddns);
    let pass = registry.begin(Worker::Ddns);
    let task = tokio::spawn(async move {
        let (_resident, _pass) = (resident, pass);
        std::future::pending::<()>().await;
    });
    registry.stopping();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let snapshot = registry.snapshot();
    assert!(matches!(snapshot.workers[2].state, State::Stopped));
    assert_eq!(snapshot.workers[2].cancellations, 1);
    assert_eq!(snapshot.workers[2].running_count, 0);
}

#[test]
fn fixed_catalog_and_saturated_metrics_cannot_accept_payloads() {
    let registry = Registry::new();
    registry.update(Worker::Recovery, |entry| {
        entry.attempts = MAX;
        entry.successes = MAX;
        entry.duration_buckets[0] = MAX;
    });
    registry.begin(Worker::Recovery).finish(Outcome::Success);
    let snapshot = registry.snapshot();
    let entry = &snapshot.workers[0];
    assert_eq!(entry.attempts, MAX);
    assert_eq!(entry.successes, MAX);
    assert!(entry.overflowed);
    let value = serde_json::to_value(snapshot).unwrap();
    assert_eq!(value["workers"].as_array().unwrap().len(), 5);
    assert_eq!(value["workers"][0]["id"], "caddy_recovery");
    assert!(value.to_string().len() < 4096);
    for forbidden in [
        "token",
        "password",
        "domain",
        "connection",
        "payload",
        "error",
        "label",
    ] {
        assert!(!value.to_string().contains(forbidden));
    }
}

#[tokio::test]
async fn resident_workers_report_real_idle_and_unavailable_passes() {
    use crate::runtime::{ProxyRuntime, RuntimeSettings};
    let path = std::env::temp_dir().join(format!(
        "background-residents-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut settings = RuntimeSettings::new(path, 18080);
    settings.recovery_interval = std::time::Duration::from_millis(5);
    let runtime = ProxyRuntime::new(settings, None);
    runtime.initialize().await;
    runtime.start_recovery_worker().await;
    runtime.start_ddns_scheduler().await;
    runtime.docker_discovery.start().await;
    runtime
        .start_renewal_scheduler(crate::server::challenges::ChallengeStore::new())
        .await;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let snapshot = runtime.background.snapshot();
            if snapshot.workers[0].failures > 0
                && snapshot.workers[1..4]
                    .iter()
                    .all(|entry| matches!(entry.state, State::Idle))
            {
                assert!(
                    snapshot
                        .workers
                        .iter()
                        .all(|entry| entry.last_success.is_none())
                );
                assert!(snapshot.workers[..4].iter().all(|entry| entry.attempts > 0));
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    runtime.shutdown().await;
    assert!(
        runtime
            .background
            .snapshot()
            .workers
            .iter()
            .all(|entry| matches!(entry.state, State::Stopped))
    );
}
