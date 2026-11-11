use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use tokio::{
    task::AbortHandle,
    time::{Instant, timeout_at},
};
use tokio_util::task::TaskTracker;

// Four ACME jobs and one permission recovery leave room for local candidate activation.
const MAX_BACKGROUND_JOBS: usize = 8;

struct State {
    closed: bool,
    jobs: [Option<AbortHandle>; MAX_BACKGROUND_JOBS],
}

/// Owns admitted work from its first preparation await through completion.
pub(super) struct BackgroundJobs {
    state: Arc<Mutex<State>>,
    tasks: TaskTracker,
}

impl BackgroundJobs {
    /// Creates an open tracker with bounded admission for background jobs.
    pub(super) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                closed: false,
                jobs: std::array::from_fn(|_| None),
            })),
            tasks: TaskTracker::new(),
        }
    }

    /// Admits a job and tracks it independently of the requesting future.
    ///
    /// Returns false without spawning when shutdown has closed admission or all job
    /// slots are occupied. Registration shares the shutdown lock; slots for finished
    /// jobs are reclaimed on the next admission, while a supervisor reports join failures.
    pub(super) fn spawn<F>(&self, operation: &'static str, future: F) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.closed {
            return false;
        }
        let Some(slot) = state.jobs.iter().position(Option::is_none) else {
            return false;
        };
        // Admission and registration share the shutdown gate. No caller owns
        // preparation after this point, including a disconnected HTTP request.
        let job = tokio::spawn(future);
        state.jobs[slot] = Some(job.abort_handle());
        let shared = Arc::clone(&self.state);
        self.tasks.spawn(async move {
            if let Err(error) = job.await {
                super::task::report_join_failure(operation, &error);
            }
            shared
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .jobs[slot] = None;
        });
        true
    }

    /// Closes admission and the task tracker under the shared shutdown gate.
    ///
    /// Already admitted jobs continue running until completion or a later abort.
    pub(super) fn stop_accepting(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.closed = true;
        self.tasks.close();
    }

    /// Closes admission, waits until the deadline, then aborts remaining async jobs.
    ///
    /// Aborting their futures does not stop blocking I/O that has already started;
    /// this method does not wait for aborted jobs to finish after the deadline.
    pub(super) async fn stop_until(&self, deadline: Instant) {
        self.stop_accepting();
        if timeout_at(deadline, self.tasks.wait()).await.is_ok() {
            return;
        }
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        for job in state.jobs.iter().flatten() {
            job.abort();
        }
        tracing::warn!(
            operation = "acme",
            "Background jobs exceeded shutdown deadline"
        );
    }
}

#[cfg(test)]
#[path = "../../tests/private/runtime_background_jobs.rs"]
mod tests;
