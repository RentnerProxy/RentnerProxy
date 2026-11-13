use super::material_files::{CertificateValidity, validate_candidate_material};
use super::operations;
use super::persistence::{persist_index, report_recovery_result};
use super::recovery::{RetirementContext, persist_candidate_manifest, retire_candidate};
use super::scheduling::{candidate_retry_deadline, next_candidate_retry_delay};
use super::validation::utc_now;
use super::{
    CertificateError, CertificateOperation, CertificateOperationStage, CertificateStatus,
    CertificateStore, OffsetDateTime, Rfc3339, StagedCertificate,
};

#[cfg(test)]
#[path = "../../../tests/private/certificate_candidate_cancellation.rs"]
mod cancellation_tests;

impl CertificateStore {
    pub(crate) async fn pending_candidate_ids(&self) -> Result<Vec<String>, CertificateError> {
        let index = self.index.lock().await;
        Ok(index.pending_candidates.keys().cloned().collect())
    }

    pub(crate) async fn begin_candidate_activation(
        &self,
        id: &str,
        scheduled: bool,
    ) -> Result<Option<StagedCertificate>, CertificateError> {
        self.begin_candidate_activation_at(id, scheduled, OffsetDateTime::now_utc())
            .await
    }

    pub(super) async fn begin_candidate_activation_at(
        &self,
        id: &str,
        scheduled: bool,
        now: OffsetDateTime,
    ) -> Result<Option<StagedCertificate>, CertificateError> {
        self.acquire_lease(id).await?;
        let (candidate, active) = {
            let index = self.index.lock().await;
            let Some(candidate) = index.pending_candidates.get(id).cloned() else {
                drop(index);
                self.release_lease(id).await;
                return Ok(None);
            };
            let Some(active) = index.certificates.get(id).cloned() else {
                drop(index);
                self.release_lease(id).await;
                return Err(CertificateError::StoreUnavailable);
            };
            if active.material_id == candidate.staged.material_id {
                drop(index);
                self.release_lease(id).await;
                return Ok(None);
            }
            if candidate.base_material_id != active.material_id {
                drop(index);
                self.release_lease(id).await;
                return Err(CertificateError::StoreUnavailable);
            }
            if active.metadata.operation != CertificateOperation::Idle {
                drop(index);
                self.release_lease(id).await;
                return Err(CertificateError::OperationInProgress);
            }
            if scheduled
                && candidate
                    .activation
                    .next_attempt_at
                    .as_deref()
                    .and_then(|deadline| OffsetDateTime::parse(deadline, &Rfc3339).ok())
                    .is_some_and(|deadline| deadline > now)
            {
                drop(index);
                self.release_lease(id).await;
                return Ok(None);
            }
            (candidate, active)
        };

        let certificates_dir = match self.certificates_dir() {
            Ok(directory) => directory,
            Err(error) => {
                self.release_lease(id).await;
                return Err(error);
            }
        };
        let validation = validate_candidate_material(&certificates_dir, id, &candidate.staged, now);
        if matches!(validation, Ok(CertificateValidity::NotYetValid)) {
            self.release_lease(id).await;
            return Err(CertificateError::CertificateExpired);
        }
        if let Err(error) = validation {
            if !matches!(
                error,
                CertificateError::InvalidCertificate
                    | CertificateError::KeyMismatch
                    | CertificateError::CertificateExpired
                    | CertificateError::DomainMismatch
            ) {
                self.release_lease(id).await;
                return Err(error);
            }
            let retired = self.retire_invalid_candidate(id, candidate, error).await;
            self.release_lease(id).await;
            retired?;
            return Err(error);
        }

        let now_string = match utc_now() {
            Ok(now) => now,
            Err(error) => {
                self.release_lease(id).await;
                return Err(error);
            }
        };
        let mut attempted_candidate = candidate.clone();
        attempted_candidate.activation.attempt_count = attempted_candidate
            .activation
            .attempt_count
            .saturating_add(1);
        attempted_candidate.activation.last_attempt_at = Some(now_string.clone());
        attempted_candidate.activation.last_error_code = None;
        attempted_candidate.activation.next_attempt_at = None;
        let mut index = self.index.lock().await;
        let Some(current_candidate) = index.pending_candidates.get(id).cloned() else {
            drop(index);
            self.release_lease(id).await;
            return Ok(None);
        };
        let Some(current_active) = index.certificates.get(id).cloned() else {
            drop(index);
            self.release_lease(id).await;
            return Err(CertificateError::StoreUnavailable);
        };
        if current_candidate.staged != candidate.staged
            || current_candidate.base_material_id != active.material_id
            || current_active.material_id != active.material_id
            || current_active.metadata.operation != CertificateOperation::Idle
        {
            drop(index);
            self.release_lease(id).await;
            return Err(CertificateError::OperationInProgress);
        }
        if let Err(error) = persist_candidate_manifest(&certificates_dir, id, &attempted_candidate)
        {
            drop(index);
            self.release_lease(id).await;
            return Err(error);
        }

        index
            .pending_candidates
            .insert(id.to_owned(), attempted_candidate.clone());
        let mut active_attempt = current_active;
        active_attempt.metadata.operation = attempted_candidate.activation_operation;
        active_attempt.metadata.last_error_code = None;
        active_attempt.metadata.updated_at = now_string;
        index.certificates.insert(id.to_owned(), active_attempt);

        report_recovery_result(persist_index(&certificates_dir, &index), "persist_index");
        drop(index);
        Ok(Some(StagedCertificate {
            id: id.to_owned(),
            stored: attempted_candidate.staged,
        }))
    }

    pub(crate) async fn finish_candidate_failed(
        &self,
        staged: &StagedCertificate,
        error: CertificateError,
    ) {
        let mut index = self.index.lock().await;
        let Some(previous_candidate) = index.pending_candidates.get(&staged.id).cloned() else {
            drop(index);
            self.release_lease(&staged.id).await;
            return;
        };
        let mut staged_for_compare = staged.stored.clone();
        staged_for_compare.metadata.current_operation =
            previous_candidate.staged.metadata.current_operation.clone();
        if previous_candidate.staged != staged_for_compare {
            drop(index);
            self.release_lease(&staged.id).await;
            return;
        }
        let now = utc_now().ok();
        let mut candidate = previous_candidate;
        candidate.activation.attempt_count = candidate.activation.attempt_count.max(1);
        let delay = next_candidate_retry_delay(
            &staged.id,
            candidate.activation.attempt_count,
            candidate.activation.retry_delay_seconds,
        );
        candidate.activation.retry_delay_seconds = Some(delay);
        candidate.activation.next_attempt_at = candidate_retry_deadline(delay);
        candidate.activation.last_error_code = Some(error.code().to_owned());
        if candidate.activation.last_attempt_at.is_none() {
            candidate.activation.last_attempt_at = now.clone();
        }
        if let Some(operation) = candidate.staged.metadata.current_operation.as_mut() {
            operation.stage = CertificateOperationStage::RetryScheduled;
            if let Some(now) = now.clone() {
                operation.updated_at = now;
            }
        }
        index
            .pending_candidates
            .insert(staged.id.clone(), candidate.clone());
        if let Some(active) = index.certificates.get_mut(&staged.id) {
            active.metadata.operation = CertificateOperation::Idle;
            active.metadata.status = if active.material_id.is_some() {
                CertificateStatus::Valid
            } else {
                CertificateStatus::Failed
            };
            active.metadata.last_error_code = Some(error.code().to_owned());
            active.metadata.last_error_at = now.clone();
            active.metadata.current_operation = candidate.staged.metadata.current_operation.clone();
            if let Some(now) = now {
                active.metadata.updated_at = now;
            }
        }

        if let Ok(directory) = self.certificates_dir() {
            report_recovery_result(
                persist_candidate_manifest(&directory, &staged.id, &candidate),
                "persist_candidate_manifest",
            );
            if let Some(operation) = candidate.staged.metadata.current_operation.as_ref() {
                report_recovery_result(
                    operations::append_event(
                        &mut index,
                        &operation.id,
                        &staged.id,
                        operations::CertificateEventKind::Failed,
                        CertificateOperationStage::Failed,
                        Some(error.code().to_owned()),
                    ),
                    "append_event",
                );
                report_recovery_result(
                    operations::append_event(
                        &mut index,
                        &operation.id,
                        &staged.id,
                        operations::CertificateEventKind::RetryScheduled,
                        CertificateOperationStage::RetryScheduled,
                        Some(error.code().to_owned()),
                    ),
                    "append_event",
                );
            }
            report_recovery_result(persist_index(&directory, &index), "persist_index");
        }
        drop(index);
        self.release_lease(&staged.id).await;
    }

    async fn retire_invalid_candidate(
        &self,
        id: &str,
        mut candidate: super::StoredCertificateCandidate,
        error: CertificateError,
    ) -> Result<(), CertificateError> {
        let mut index = self.index.lock().await;
        if index.pending_candidates.get(id) != Some(&candidate) {
            return Err(CertificateError::OperationInProgress);
        }
        let operation = candidate
            .staged
            .metadata
            .current_operation
            .as_mut()
            .ok_or(CertificateError::StoreUnavailable)?;
        operation.stage = CertificateOperationStage::Failed;
        operation.updated_at = utc_now()?;
        candidate.retired = true;
        candidate.activation.last_error_code = Some(error.code().to_owned());
        candidate.activation.next_attempt_at = None;
        candidate.activation.retry_delay_seconds = None;
        let mut retired_index = index.clone();
        retire_candidate(&mut retired_index, id, &candidate, RetirementContext::Live)?;
        let directory = self.certificates_dir()?;
        persist_candidate_manifest(&directory, id, &candidate)?;
        persist_index(&directory, &retired_index)?;
        *index = retired_index;
        Ok(())
    }
}
