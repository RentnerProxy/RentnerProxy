use std::{future::Future, sync::Arc};

use tokio::sync::Semaphore;

use super::{ProxyRuntime, RuntimeError};

pub(super) struct StorageWorker {
    admission: Arc<Semaphore>,
}

impl StorageWorker {
    pub(super) fn new() -> Self {
        Self {
            admission: Arc::new(Semaphore::new(1)),
        }
    }

    async fn run<T, F>(&self, operation: F) -> Result<T, RuntimeError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let permit = Arc::clone(&self.admission)
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::Unavailable)?;
        tokio::task::spawn_blocking(move || {
            // The job owns admission until the actual I/O ends, including when
            // its waiter is dropped. Never overlap a late write with rollback.
            let _permit = permit;
            operation()
        })
        .await
        .map_err(|error| {
            super::task::report_join_failure("runtime_storage", &error);
            RuntimeError::ApplyFailed
        })
    }
}

impl ProxyRuntime {
    pub(super) async fn storage<T, F>(self: &Arc<Self>, operation: F) -> Result<T, RuntimeError>
    where
        T: Send + 'static,
        F: FnOnce(&Self) -> T + Send + 'static,
    {
        let runtime = Arc::clone(self);
        self.storage_worker.run(move || operation(&runtime)).await
    }

    pub(super) async fn storage_async<T, F, Fut>(
        self: &Arc<Self>,
        operation: F,
    ) -> Result<T, RuntimeError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Self>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let runtime = Arc::clone(self);
        self.storage_worker
            .run(move || tokio::runtime::Handle::current().block_on(operation(runtime)))
            .await
    }
}

#[cfg(test)]
#[path = "../../tests/private/runtime_storage.rs"]
mod tests;
