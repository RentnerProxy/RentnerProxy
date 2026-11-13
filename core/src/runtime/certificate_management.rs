use super::{
    CertificateError, CertificateEventPage, CertificateImportRequest, CertificateMetadata,
    CertificateOperationStage, CertificateStoreReadiness, ProxyRuntime, StagedCertificate,
};
use std::sync::Arc;
use tokio::time::timeout;

impl ProxyRuntime {
    pub(crate) async fn certificates(&self) -> Result<Vec<CertificateMetadata>, CertificateError> {
        self.certificate_store.list().await
    }

    pub(crate) async fn certificate_store_readiness(&self) -> CertificateStoreReadiness {
        self.certificate_store.readiness().await
    }

    pub(crate) async fn certificate_events(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<CertificateEventPage, CertificateError> {
        self.certificate_store.events(after, limit).await
    }

    /// Records cancellation for the specified delegated DNS operation via storage admission.
    ///
    /// Once admitted, the storage worker retains ownership if its waiter is dropped.
    /// Worker failures become `StoreUnavailable`; certificate-store errors propagate.
    pub(crate) async fn cancel_delegated_dns(
        self: &Arc<Self>,
        id: &str,
        action: &super::CertificateCancelRequest,
    ) -> Result<CertificateMetadata, CertificateError> {
        let id = id.to_owned();
        let operation_id = action.operation_id.clone();
        self.storage_async(move |runtime| async move {
            runtime
                .certificate_store
                .request_delegated_cancel(&id, &operation_id)
                .await
        })
        .await
        .map_err(|_| CertificateError::StoreUnavailable)?
    }

    /// Cancels a manual DNS order through the serialized storage worker.
    ///
    /// The owned action survives caller cancellation after admission. Worker failures
    /// become `StoreUnavailable`; order validation errors remain certificate errors.
    pub(crate) async fn cancel_manual_dns(
        self: &Arc<Self>,
        id: &str,
        action: &super::certificates::ManualDnsActionRequest,
    ) -> Result<CertificateMetadata, CertificateError> {
        let id = id.to_owned();
        let action = action.clone();
        self.storage_async(move |runtime| async move {
            runtime
                .certificate_store
                .cancel_manual_dns_order(&id, &action)
                .await
        })
        .await
        .map_err(|_| CertificateError::StoreUnavailable)?
    }

    pub(crate) async fn certificate(
        &self,
        id: &str,
    ) -> Result<CertificateMetadata, CertificateError> {
        self.certificate_store.get(id).await
    }

    pub(crate) async fn import_certificate(
        self: &Arc<Self>,
        id: &str,
        request: CertificateImportRequest,
    ) -> Result<CertificateMetadata, CertificateError> {
        if self.stopping.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(CertificateError::OperationInProgress);
        }
        let runtime = Arc::clone(self);
        let id = id.to_owned();
        tokio::spawn(async move { runtime.import_certificate_inner(&id, request).await })
            .await
            .unwrap_or_else(|error| {
                super::task::report_join_failure("certificate_import", &error);
                Err(CertificateError::StoreUnavailable)
            })
    }

    async fn import_certificate_inner(
        self: &Arc<Self>,
        id: &str,
        request: CertificateImportRequest,
    ) -> Result<CertificateMetadata, CertificateError> {
        let id = id.to_owned();
        let staged = self
            .storage_async(move |runtime| async move {
                runtime.certificate_store.stage_manual(&id, request).await
            })
            .await
            .map_err(|_| CertificateError::StoreUnavailable)??;
        self.commit_or_reapply_staged_certificate(staged).await
    }

    pub(super) async fn commit_or_reapply_staged_certificate(
        self: &Arc<Self>,
        staged: StagedCertificate,
    ) -> Result<CertificateMetadata, CertificateError> {
        let id = staged.id().to_owned();
        let recorded = self
            .storage_async(move |runtime| async move {
                runtime
                    .certificate_store
                    .record_operation_stage(&id, CertificateOperationStage::Applying)
                    .await
            })
            .await
            .and_then(|result| result.map_err(|_| super::RuntimeError::ApplyFailed));
        if recorded.is_err() || self.apply_staged_for_active(staged.clone()).await.is_err() {
            if staged.is_acme() {
                let _ = self
                    .storage_async(move |runtime| async move {
                        runtime
                            .certificate_store
                            .finish_candidate_failed(&staged, CertificateError::RuntimeApplyFailed)
                            .await;
                    })
                    .await;
            } else {
                let _ = self
                    .storage_async(move |runtime| async move {
                        runtime.certificate_store.discard_staged(&staged).await;
                    })
                    .await;
            }
            return Err(CertificateError::RuntimeApplyFailed);
        }
        self.certificate_store.get(staged.id()).await
    }

    /// Admits candidate preparation and activation as one tracked background job.
    ///
    /// Shutdown or exhausted job slots return `OperationInProgress`. After admission,
    /// dropping the caller leaves preparation and publication owned by the job;
    /// loss of its result becomes `StoreUnavailable`.
    pub(crate) async fn retry_certificate_candidate(
        self: &Arc<Self>,
        id: &str,
        scheduled: bool,
    ) -> Result<Option<CertificateMetadata>, CertificateError> {
        if self.stopping.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(CertificateError::OperationInProgress);
        }
        let runtime = Arc::clone(self);
        let id = id.to_owned();
        let (response, result) = tokio::sync::oneshot::channel();
        // Own preparation as well as publication: preparation acquires a lease
        // before waiting for the index, so caller cancellation must not drop it.
        if !self
            .background_jobs
            .spawn("certificate_candidate_retry", async move {
                let outcome = runtime
                    .retry_certificate_candidate_inner(&id, scheduled)
                    .await;
                let _ = response.send(outcome);
            })
        {
            return Err(CertificateError::OperationInProgress);
        }
        result
            .await
            .unwrap_or(Err(CertificateError::StoreUnavailable))
    }

    /// Prepares a candidate under storage admission and applies it when eligible.
    ///
    /// Returns `None` when the store has no candidate ready for this retry. The
    /// caller must retain ownership across preparation and activation; shutdown
    /// rejects preparation and storage-worker failures become `StoreUnavailable`.
    pub(super) async fn retry_certificate_candidate_inner(
        self: &Arc<Self>,
        id: &str,
        scheduled: bool,
    ) -> Result<Option<CertificateMetadata>, CertificateError> {
        if self.stopping.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(CertificateError::OperationInProgress);
        }
        let preparation_guard = timeout(self.settings.lock_wait, self.apply_lock.lock())
            .await
            .map_err(|_| CertificateError::OperationInProgress)?;
        if self.engine.is_some() {
            if !self
                .certificate_store
                .pending_candidate_ids()
                .await?
                .iter()
                .any(|candidate| candidate == id)
            {
                return Ok(None);
            }
            self.ensure_running_locked()
                .await
                .map_err(|_| CertificateError::RuntimeApplyFailed)?;
        }
        let id = id.to_owned();
        let Some(staged) = self
            .storage_async(move |runtime| async move {
                runtime
                    .certificate_store
                    .begin_candidate_activation(&id, scheduled)
                    .await
            })
            .await
            .map_err(|_| CertificateError::StoreUnavailable)??
        else {
            return Ok(None);
        };
        drop(preparation_guard);
        self.commit_or_reapply_staged_certificate(staged)
            .await
            .map(Some)
    }

    /// Runs certificate deletion in an owned task that survives caller cancellation.
    ///
    /// A join failure is reported and returned as `StoreUnavailable`. Usage checks
    /// and deletion stay together inside the task while it holds the apply lock.
    pub(crate) async fn delete_certificate(
        self: &Arc<Self>,
        id: &str,
    ) -> Result<(), CertificateError> {
        let runtime = Arc::clone(self);
        let id = id.to_owned();
        tokio::spawn(async move { runtime.delete_certificate_inner(&id).await })
            .await
            .unwrap_or_else(|error| {
                super::task::report_join_failure("certificate_delete", &error);
                Err(CertificateError::StoreUnavailable)
            })
    }

    /// Holds the apply lock across live and cached usage checks and storage deletion.
    ///
    /// Lock timeout or an unavailable engine conservatively prevents deletion as in-use.
    /// Storage-worker failures become `StoreUnavailable`; store deletion errors propagate.
    async fn delete_certificate_inner(self: &Arc<Self>, id: &str) -> Result<(), CertificateError> {
        let _guard = timeout(self.settings.lock_wait, self.apply_lock.lock())
            .await
            .map_err(|_| CertificateError::InUse)?;
        let state = self.state.lock().await;
        let marker = format!("/certificates/{id}/versions/");
        let in_use =
            !state.engine_available || state.active_json.replace('\\', "/").contains(&marker);
        drop(state);
        let cached_in_use = self
            .restore_active_configuration()
            .await
            .is_some_and(|config| {
                config
                    .proxy_hosts
                    .iter()
                    .any(|host| host.certificate_id.as_deref() == Some(id))
                    || config
                        .redirect_hosts
                        .iter()
                        .any(|host| host.certificate_id.as_deref() == Some(id))
            });
        let id = id.to_owned();
        self.storage_async(move |runtime| async move {
            runtime
                .certificate_store
                .delete_if_unused(&id, in_use || cached_in_use)
                .await
        })
        .await
        .map_err(|_| CertificateError::StoreUnavailable)?
    }
}
