use super::*;
use crate::runtime::{
    CertificateEnvironment, CertificateIssueRequest, ProxyRuntime, RuntimeSettings,
};
use std::{sync::Arc, time::Duration};

struct TestDirectory(std::path::PathBuf);

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn cancelled_candidate_caller_preserves_preparation_and_releases_lease_after_commit() {
    let root = std::env::temp_dir().join(format!(
        "rp-candidate-cancellation-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _directory = TestDirectory(root.clone());
    std::fs::create_dir_all(&root).unwrap();
    let runtime = ProxyRuntime::new(RuntimeSettings::new(root, 0), None);
    let store = &runtime.certificate_store;
    store.initialize().await.unwrap();
    let id = "0198d98a-0000-7000-8000-000000000010";
    let request = CertificateIssueRequest {
        domains: vec!["example.com".to_owned()],
        environment: CertificateEnvironment::Staging,
        contact_email: None,
        challenge_type: Default::default(),
        dns_mode: Default::default(),
        dns_provider: None,
        accept_terms: true,
    };
    store.begin_issue(id, request.clone(), false).await.unwrap();
    let material = rcgen::generate_simple_self_signed(request.domains.clone()).unwrap();
    let staged = store
        .stage_acme(
            id,
            &request,
            material.cert.pem(),
            material.signing_key.serialize_pem(),
        )
        .await
        .unwrap();
    let expected_fingerprint = staged.stored.metadata.fingerprint.clone();
    store
        .finish_candidate_failed(&staged, CertificateError::RuntimeApplyFailed)
        .await;

    let index = store.index.lock().await;
    let caller = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move { runtime.retry_certificate_candidate(id, false).await })
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        while !store.leases.lock().await.contains(id) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("candidate preparation acquired its lease before waiting for the held index");
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(store.leases.lock().await.contains(id));
    drop(index);

    tokio::time::timeout(Duration::from_secs(2), async {
        while store.leases.lock().await.contains(id) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned retry finishes and releases its lease after caller cancellation");
    let activated = runtime.certificate(id).await.unwrap();
    assert_eq!(activated.fingerprint, expected_fingerprint);
    assert!(activated.candidate.is_none());
    assert_eq!(activated.operation, CertificateOperation::Idle);
    assert!(
        runtime
            .retry_certificate_candidate(id, false)
            .await
            .unwrap()
            .is_none()
    );
    store.acquire_lease(id).await.unwrap();
    store.release_lease(id).await;
}
