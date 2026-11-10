use super::*;
use std::{
    future::{Future, poll_fn},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::Poll,
    time::Duration,
};
use tokio::{sync::oneshot, time::timeout};

#[tokio::test(flavor = "current_thread")]
async fn cancelled_storage_waiter_keeps_blocking_work_serial_and_executor_responsive() {
    let worker = Arc::new(StorageWorker::new());
    let (started, running) = oneshot::channel();
    let (release, gate) = mpsc::channel();
    let finished = Arc::new(AtomicBool::new(false));
    let first = {
        let worker = Arc::clone(&worker);
        let finished = Arc::clone(&finished);
        tokio::spawn(async move {
            worker
                .run(move || {
                    started.send(()).unwrap();
                    let _ = gate.recv();
                    finished.store(true, Ordering::SeqCst);
                })
                .await
        })
    };

    // A single-thread executor must still deliver this signal while I/O waits.
    timeout(Duration::from_secs(2), running)
        .await
        .unwrap()
        .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert_eq!(worker.admission.available_permits(), 0);

    let mut second = Box::pin(worker.run(move || {
        assert!(finished.load(Ordering::SeqCst));
        42
    }));
    poll_fn(|cx| {
        assert!(second.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    release.send(()).unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), second).await.unwrap(),
        Ok(42)
    );
    assert_eq!(worker.admission.available_permits(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_job_failure_releases_admission_and_keeps_panic_payload_private() {
    let worker = StorageWorker::new();
    assert_eq!(
        worker.run(|| panic!("test-only panic payload")).await,
        Err(RuntimeError::ApplyFailed)
    );
    assert_eq!(worker.run(|| 7).await, Ok(7));
}

#[tokio::test(flavor = "current_thread")]
async fn blocked_storage_keeps_health_responsive_and_cancelled_apply_finishes_transaction() {
    use crate::{
        models::{ProxyConfigRequest, ProxyHttpSettings},
        proxy::{revision_for_configuration, validate_proxy_config},
        runtime::{EngineFuture, ProxyEngine, RuntimeSettings},
        server::{AppState, app_with_state},
    };
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use std::{pin::Pin, sync::atomic::AtomicUsize};
    use tower::ServiceExt;

    struct Engine {
        running: AtomicBool,
        loads: AtomicUsize,
    }
    impl ProxyEngine for Engine {
        fn start<'a>(&'a self, _: &'a str, _: &'a str) -> EngineFuture<'a> {
            Box::pin(async {
                self.running.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
        fn load<'a>(&'a self, _: &'a str) -> EngineFuture<'a> {
            Box::pin(async {
                self.loads.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }
        fn probe<'a>(&'a self, _: &'a str) -> EngineFuture<'a> {
            Box::pin(async { Ok(()) })
        }
        fn shutdown<'a>(&'a self) -> EngineFuture<'a> {
            Box::pin(async {
                self.running.store(false, Ordering::SeqCst);
                Ok(())
            })
        }
        fn is_running<'a>(&'a self) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
            Box::pin(async { self.running.load(Ordering::SeqCst) })
        }
    }

    let directory = std::env::temp_dir().join(format!(
        "rentnerproxy-storage-{}-{}",
        std::process::id(),
        time::OffsetDateTime::now_utc().unix_timestamp_nanos(),
    ));
    let engine = Arc::new(Engine {
        running: AtomicBool::new(false),
        loads: AtomicUsize::new(0),
    });
    let runtime = ProxyRuntime::new(
        RuntimeSettings::new(directory.clone(), 0),
        Some(engine.clone()),
    );
    runtime.initialize().await;
    assert!(runtime.is_ready().await);

    let settings = ProxyHttpSettings::default();
    let revision = revision_for_configuration(&[], &settings);
    let configuration = validate_proxy_config(ProxyConfigRequest {
        version: 7,
        revision: revision.clone(),
        proxy_hosts: vec![],
        redirect_hosts: vec![],
        http_settings: settings,
        trusted_cas: vec![],
        default_site: Default::default(),
    })
    .unwrap();

    let (started, running) = oneshot::channel();
    let (release, gate) = mpsc::channel();
    let blocker = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            runtime
                .storage(move |_| {
                    started.send(()).unwrap();
                    let _ = gate.recv();
                })
                .await
        })
    };
    timeout(Duration::from_secs(2), running)
        .await
        .unwrap()
        .unwrap();
    let caller = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move { runtime.apply(configuration).await })
    };
    timeout(Duration::from_secs(2), async {
        while runtime.apply_sequence.load(Ordering::SeqCst) == 0
            || runtime.apply_lock.try_lock().is_ok()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let response = timeout(
        Duration::from_secs(2),
        app_with_state(AppState::new(Arc::clone(&runtime), None))
            .oneshot(Request::get("/health").body(Body::empty()).unwrap()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(engine.loads.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.state.lock().await.active_revision, None);

    release.send(()).unwrap();
    blocker.await.unwrap().unwrap();
    let guard = timeout(Duration::from_secs(2), runtime.apply_lock.lock())
        .await
        .unwrap();
    assert_eq!(
        runtime.state.lock().await.active_revision.as_deref(),
        Some(revision.as_str())
    );
    assert_eq!(
        runtime
            .restore_active_configuration()
            .await
            .unwrap()
            .revision,
        revision
    );
    assert!(!runtime.configuration_activation_is_fenced());
    assert_eq!(engine.loads.load(Ordering::SeqCst), 1);
    drop(guard);
    runtime.shutdown().await;
    std::fs::remove_dir_all(directory).unwrap();
}
