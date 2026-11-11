use super::acme::acme_store;
use super::background::{Outcome, Worker};
use super::{CertificateMetadata, MAX_CANDIDATE_ACTIVATIONS_PER_TICK, ProxyRuntime, certificates};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::time::interval;

const MAX_ACME_STARTS_PER_TICK: usize = 4;
static PERMISSION_RECOVERY: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

/// Distinguish admitted work from idle/busy admission; only actual certificate errors degrade the scan.
fn certificate_work_outcome<T>(
    result: Result<Option<T>, certificates::CertificateError>,
) -> Outcome {
    match result {
        Ok(Some(_)) => Outcome::Success,
        Ok(None) | Err(certificates::CertificateError::OperationInProgress) => Outcome::Idle,
        Err(_) => Outcome::Failure,
    }
}

impl ProxyRuntime {
    /// Starts the single minute-based certificate recovery and renewal loop.
    ///
    /// Returns without spawning when already started or stopping. The runtime retains
    /// the task handle for shutdown; each tick dispatches bounded ACME work and at most
    /// one concurrent permission-recovery job. Missed ticks are skipped, expiry/scan
    /// failures retain failure evidence, and cancellation stays distinct from idle
    /// or busy admission. Stopping checks prevent further scheduled dispatch.
    pub(crate) async fn start_renewal_scheduler(
        self: &Arc<Self>,
        challenges: crate::server::challenges::ChallengeStore,
    ) {
        let mut task = self.renewal_task.lock().await;
        if task.is_some() || self.stopping.load(Ordering::SeqCst) {
            return;
        }
        let runtime = Arc::clone(self);
        let resident = runtime.background.start(Worker::Renewal);
        *task = Some(tokio::spawn(async move {
            let _resident = resident;
            let mut timer = interval(Duration::from_secs(60));
            let mut cursor = 0_usize;
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                timer.tick().await;
                if runtime.stopping.load(Ordering::SeqCst) {
                    break;
                }
                let pass = runtime.background.begin(Worker::Renewal);
                let candidates = runtime.retry_certificate_candidates().await;
                if runtime.stopping.load(Ordering::SeqCst) {
                    pass.finish(Outcome::Cancelled);
                    break;
                }
                let expiry = acme_store!(runtime; []; store => store.expire_manual_dns_orders());
                let manual = runtime.recover_manual_dns().await;
                let cleanup = runtime.recover_dns_cleanup().await;
                if let Ok(permit) = PERMISSION_RECOVERY.try_acquire() {
                    let checks_runtime = Arc::clone(&runtime);
                    runtime
                        .background_jobs
                        .spawn("dns_permission_recovery", async move {
                            let _permit = permit;
                            let pass = checks_runtime.background.begin(Worker::Orders);
                            let succeeded = checks_runtime
                                .dns_connections
                                .recover_permission_checks()
                                .await;
                            pass.finish(if succeeded {
                                Outcome::Idle
                            } else {
                                Outcome::Failure
                            });
                        });
                }
                let renewal = runtime
                    .renew_due_certificates(challenges.clone(), cursor)
                    .await;
                pass.finish(if runtime.stopping.load(Ordering::SeqCst) {
                    Outcome::Cancelled
                } else if expiry.is_err()
                    || !manual
                    || !cleanup
                    || matches!(candidates, Outcome::Failure)
                    || matches!(renewal, Outcome::Failure)
                {
                    Outcome::Failure
                } else if matches!(candidates, Outcome::Success)
                    || matches!(renewal, Outcome::Success)
                {
                    Outcome::Success
                } else {
                    Outcome::Idle
                });
                cursor = cursor.wrapping_add(1);
            }
        }));
    }

    /// Rotate due certificates fairly and attempt at most four starts per tick; busy admission stays idle and real failures win.
    async fn renew_due_certificates(
        self: &Arc<Self>,
        challenges: crate::server::challenges::ChallengeStore,
        cursor: usize,
    ) -> Outcome {
        if self.stopping.load(Ordering::SeqCst) {
            return Outcome::Cancelled;
        }
        let Ok(mut certificates) = self.certificate_store.list().await else {
            return Outcome::Failure;
        };
        if certificates.is_empty() {
            return Outcome::Idle;
        }
        let offset = cursor % certificates.len();
        certificates.rotate_left(offset);
        let mut starts = 0;
        let mut admitted = 0;
        let mut failed = false;
        for certificate in certificates {
            if self.stopping.load(Ordering::SeqCst) || starts >= MAX_ACME_STARTS_PER_TICK {
                break;
            }
            let retry = Self::acme_retry_is_due_at(&certificate, OffsetDateTime::now_utc());
            if !retry && !Self::renewal_is_due(&certificate) {
                continue;
            }
            if !self
                .certificate_store
                .renewal_is_allowed(&certificate.id)
                .await
            {
                continue;
            }
            starts += 1;
            let result = if retry {
                self.start_scheduled_acme_retry(certificate.id, challenges.clone())
                    .await
            } else {
                self.start_scheduled_acme_renewal(certificate.id, challenges.clone())
                    .await
            };
            match certificate_work_outcome(result.map(Some)) {
                Outcome::Success => admitted += 1,
                Outcome::Failure => failed = true,
                _ => {}
            }
        }
        if self.stopping.load(Ordering::SeqCst) {
            Outcome::Cancelled
        } else if failed {
            Outcome::Failure
        } else if admitted == 0 {
            Outcome::Idle
        } else {
            Outcome::Success
        }
    }

    fn acme_retry_is_due_at(certificate: &CertificateMetadata, now: OffsetDateTime) -> bool {
        certificate.source == certificates::CertificateSource::Acme
            && certificate.operation == certificates::CertificateOperation::Idle
            && certificate.candidate.is_none()
            && certificate
                .current_operation
                .as_ref()
                .is_some_and(|operation| {
                    operation.stage == certificates::CertificateOperationStage::RetryScheduled
                        && matches!(
                            operation.kind,
                            certificates::OperationKind::Issue | certificates::OperationKind::Renew
                        )
                })
            && certificate
                .next_attempt_at
                .as_deref()
                .and_then(|deadline| OffsetDateTime::parse(deadline, &Rfc3339).ok())
                .is_some_and(|deadline| deadline <= now)
    }

    /// Rotate the pending candidate cursor within the activation budget, preserving any failure and reporting shutdown cancellation.
    async fn retry_certificate_candidates(self: &Arc<Self>) -> Outcome {
        if self.stopping.load(Ordering::SeqCst) {
            return Outcome::Cancelled;
        }
        let Ok(mut ids) = self.certificate_store.pending_candidate_ids().await else {
            return Outcome::Failure;
        };
        if ids.is_empty() {
            return Outcome::Idle;
        }
        let offset = self.candidate_cursor.fetch_add(1, Ordering::Relaxed) % ids.len();
        ids.rotate_left(offset);
        let mut outcome = Outcome::Idle;
        for id in ids.into_iter().take(MAX_CANDIDATE_ACTIVATIONS_PER_TICK) {
            if self.stopping.load(Ordering::SeqCst) {
                break;
            }
            match certificate_work_outcome(self.retry_certificate_candidate(&id, true).await) {
                Outcome::Success if !matches!(outcome, Outcome::Failure) => {
                    outcome = Outcome::Success
                }
                Outcome::Failure => outcome = Outcome::Failure,
                _ => {}
            }
        }
        if self.stopping.load(Ordering::SeqCst) {
            Outcome::Cancelled
        } else {
            outcome
        }
    }

    pub(super) fn renewal_is_due(certificate: &CertificateMetadata) -> bool {
        Self::renewal_is_due_at(certificate, OffsetDateTime::now_utc())
    }

    fn renewal_is_due_at(certificate: &CertificateMetadata, now: OffsetDateTime) -> bool {
        if certificate.source != certificates::CertificateSource::Acme
            || certificate.status != certificates::CertificateStatus::Valid
            || certificate.operation != certificates::CertificateOperation::Idle
        {
            return false;
        }
        let Some(issued_at) = certificate.issued_at.as_deref() else {
            return false;
        };
        let Some(expires_at) = certificate.expires_at.as_deref() else {
            return false;
        };
        let Ok(issued_at) = OffsetDateTime::parse(issued_at, &Rfc3339) else {
            return false;
        };
        let Ok(expires_at) = OffsetDateTime::parse(expires_at, &Rfc3339) else {
            return false;
        };
        let Some(renewal_at) = certificates::renewal_timestamp(issued_at, expires_at) else {
            return false;
        };
        now >= renewal_at
    }
}

#[cfg(test)]
#[path = "../../tests/private/renewal.rs"]
mod renewal_tests;
