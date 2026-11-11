use std::{
    future::pending,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use super::{BackgroundJobs, MAX_BACKGROUND_JOBS};
use tokio::{
    sync::{Semaphore, oneshot},
    time::{Instant, timeout},
};

/// Fills tracked admission, closes the gate while preparation is paused, and verifies shutdown
/// waits for the admitted jobs while refusing all subsequent work.
#[tokio::test]
async fn admission_is_bounded_and_closes_before_a_preparing_job_finishes() {
    let jobs = Arc::new(BackgroundJobs::new());
    let gate = Arc::new(Semaphore::new(0));
    let completed = Arc::new(Semaphore::new(0));
    for _ in 0..MAX_BACKGROUND_JOBS {
        let gate = Arc::clone(&gate);
        let completed = Arc::clone(&completed);
        assert!(jobs.spawn("blocked_preparation", async move {
            let _permit = gate.acquire().await.unwrap();
            completed.add_permits(1);
        }));
    }
    assert!(!jobs.spawn("overflow", async {}));
    jobs.stop_accepting();
    assert!(!jobs.spawn("after_close", async {}));
    let draining = Arc::clone(&jobs);
    let mut drain = tokio::spawn(async move {
        draining
            .stop_until(Instant::now() + Duration::from_secs(2))
            .await;
    });
    assert!(
        timeout(Duration::from_millis(10), &mut drain)
            .await
            .is_err()
    );
    gate.add_permits(MAX_BACKGROUND_JOBS);
    drain.await.unwrap();
    assert_eq!(completed.available_permits(), MAX_BACKGROUND_JOBS);
}

struct Dropped(Option<oneshot::Sender<()>>);

#[derive(Clone)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl io::Write for Captured {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for Dropped {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

/// Keeps a job pending past the shutdown deadline and verifies aborting its async future drops
/// its lifetime witness and returns the semaphore permit.
#[tokio::test]
async fn deadline_aborts_owned_work_and_releases_its_permit() {
    let jobs = BackgroundJobs::new();
    let permit = Arc::new(Semaphore::new(1));
    let owned = Arc::clone(&permit).acquire_owned().await.unwrap();
    let (dropped, received) = oneshot::channel();
    let on_drop = Dropped(Some(dropped));
    assert!(jobs.spawn("forced_shutdown", async move {
        let _permit = owned;
        let _on_drop = on_drop;
        pending::<()>().await;
    }));
    jobs.stop_until(Instant::now() + Duration::from_millis(10))
        .await;
    timeout(Duration::from_secs(1), received)
        .await
        .unwrap()
        .unwrap();
    jobs.stop_until(Instant::now() + Duration::from_secs(1))
        .await;
    assert_eq!(permit.available_permits(), 1);
}

/// Panics inside owned work to verify supervision records a sanitized failure while task
/// admission and semaphore permits are released for following work.
#[tokio::test]
async fn a_panicking_job_is_observed_and_releases_admission_and_permits() {
    let output = Captured(Arc::new(Mutex::new(Vec::new())));
    let writer = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(move || writer.clone())
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    let _logging = tracing::dispatcher::set_default(&dispatch);
    let jobs = BackgroundJobs::new();
    let permit = Arc::new(Semaphore::new(1));
    let owned = Arc::clone(&permit).acquire_owned().await.unwrap();
    assert!(jobs.spawn("panicking_acme_job", async move {
        let _permit = owned;
        panic!("private-panic-payload");
    }));
    timeout(Duration::from_secs(1), async {
        loop {
            if permit.available_permits() == 1
                && jobs.state.lock().unwrap().jobs.iter().all(Option::is_none)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let bytes = output.0.lock().unwrap().clone();
    assert!(!String::from_utf8_lossy(&bytes).contains("private-panic-payload"));
    let diagnostic: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(diagnostic["fields"]["operation"], "panicking_acme_job");
    assert_eq!(diagnostic["fields"]["panicked"], true);
    assert!(jobs.spawn("after_panic", async {}));
    jobs.stop_until(Instant::now() + Duration::from_secs(1))
        .await;
}
