use super::*;

struct IdleEngine;

impl super::super::ProxyEngine for IdleEngine {
    /// Accepts startup without creating a child so the recovery loop can be tested in isolation.
    fn start<'a>(&'a self, _: &'a str, _: &'a str) -> super::super::EngineFuture<'a> {
        Box::pin(async { Ok(()) })
    }
    /// Accepts configuration as a no-op for the idle shutdown fixture.
    fn load<'a>(&'a self, _: &'a str) -> super::super::EngineFuture<'a> {
        Box::pin(async { Ok(()) })
    }
    /// Reports a successful probe for the idle shutdown fixture.
    fn probe<'a>(&'a self, _: &'a str) -> super::super::EngineFuture<'a> {
        Box::pin(async { Ok(()) })
    }
    /// Completes shutdown without a process to reap.
    fn shutdown(&self) -> super::super::EngineFuture<'_> {
        Box::pin(async { Ok(()) })
    }
    /// Confirms no managed child exists in this fixture.
    fn is_running(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
        Box::pin(async { false })
    }
}

#[tokio::test]
/// Exercises worker progress and bounded shutdown while another task holds the apply lock.
async fn apply_contention_does_not_block_independent_recovery_progress_or_shutdown() {
    let mut settings = super::super::RuntimeSettings::new(std::env::temp_dir(), 0);
    settings.lock_wait = Duration::from_millis(5);
    settings.recovery_interval = Duration::from_millis(5);
    let runtime = ProxyRuntime::new(settings, Some(Arc::new(IdleEngine)));
    let held = runtime.apply_lock.lock().await;
    runtime.start_recovery_worker().await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while runtime
            .recovery_health
            .snapshot(Instant::now())
            .completed_ticks
            < 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(runtime.recovery_progressing());
    assert_eq!(runtime.recovery_health.snapshot(Instant::now()).faults, 0);
    runtime
        .shutdown_until(Instant::now() + Duration::from_millis(50))
        .await;
    assert!(!runtime.recovery_progressing());
    assert!(
        !runtime
            .recovery_health
            .snapshot(Instant::now())
            .worker_running
    );
    drop(held);
}

#[tokio::test]
async fn held_apply_lock_and_stalled_ddns_share_the_callers_shutdown_deadline() {
    let runtime = ProxyRuntime::new(
        super::super::RuntimeSettings::new(std::env::temp_dir(), 0),
        None,
    );
    let held_apply = runtime.apply_lock.lock().await;
    let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    struct Exited(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Exited {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let (ready, started) = tokio::sync::oneshot::channel();
    let done = exited.clone();
    *runtime.ddns_task.lock().await = Some(tokio::spawn(async move {
        let _done = Exited(done);
        ready.send(()).unwrap();
        std::future::pending::<()>().await;
    }));
    started.await.unwrap();
    let deadline = Instant::now() + Duration::from_millis(50);
    tokio::time::timeout(Duration::from_millis(500), runtime.shutdown_until(deadline))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert!(exited.load(Ordering::SeqCst));
    assert!(runtime.stopping.load(Ordering::SeqCst));
    drop(held_apply);
}

#[tokio::test]
async fn shutdown_immediately_rejects_manual_renewal_and_candidate_activation() {
    let runtime = ProxyRuntime::new(
        super::super::RuntimeSettings::new(std::env::temp_dir(), 0),
        None,
    );
    runtime.begin_shutdown();
    assert!(matches!(
        runtime
            .start_acme_renewal(
                "not-a-certificate".into(),
                crate::server::challenges::ChallengeStore::new()
            )
            .await,
        Err(super::super::CertificateError::OperationInProgress)
    ));
    assert!(matches!(
        runtime
            .retry_certificate_candidate("not-a-certificate", false)
            .await,
        Err(super::super::CertificateError::OperationInProgress)
    ));
}
