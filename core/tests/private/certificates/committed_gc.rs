use super::*;
use crate::runtime::CertificateOperationStage;
use crate::tests::fixtures::{TestDirectory, fail_next_candidate_manifest_removal};

fn marker(state: &Path) -> PathBuf {
    state.join("certificates").join(ID).join("candidate.json")
}

fn index_path(state: &Path) -> PathBuf {
    state.join("certificates/certificate-metadata.json")
}

async fn committed_store(state: &Path) -> (CertificateStore, PathBuf, PathBuf, Vec<u8>, Vec<u8>) {
    let (store, _, old) = active_acme_store(state.to_owned(), ID).await;
    let old_index = std::fs::read(index_path(state)).unwrap();
    let (_, request) = store.begin_renewal(ID).await.unwrap();
    let (material, _) = import_request();
    let staged = store
        .stage_acme(
            ID,
            &request,
            material.certificate_pem,
            material.private_key_pem,
        )
        .await
        .unwrap();
    let candidate = store
        .staged_material(&staged)
        .unwrap()
        .fullchain_path
        .parent()
        .unwrap()
        .to_owned();
    let manifest = std::fs::read(marker(state)).unwrap();
    fail_next_candidate_manifest_removal(marker(state));
    store.commit_staged(&staged).await.unwrap();
    assert!(store.pending_candidate_ids().await.unwrap().is_empty());
    assert_eq!(
        store
            .material(ID)
            .await
            .unwrap()
            .fullchain_path
            .parent()
            .unwrap(),
        candidate
    );
    assert_eq!(std::fs::read(marker(state)).unwrap(), manifest);
    (store, old, candidate, manifest, old_index)
}

async fn collect(store: &CertificateStore) {
    for _ in 0..4 {
        store.collect_garbage().await.unwrap();
    }
}

#[tokio::test]
async fn committed_candidate_gc_retries_failed_commit_unlink_before_releasing_base_material() {
    let directory = TestDirectory(test_state_dir());
    let (store, old, active, manifest, _) = committed_store(&directory.0).await;
    fail_next_candidate_manifest_removal(marker(&directory.0));
    assert_eq!(
        store.collect_garbage().await,
        Err(CertificateError::StoreUnavailable)
    );
    assert!(old.exists() && active.exists());
    assert_eq!(std::fs::read(marker(&directory.0)).unwrap(), manifest);
    collect(&store).await;
    assert!(!marker(&directory.0).exists() && !old.exists());
    assert!(active.join("fullchain.pem").exists());
    drop(store);
    let reopened = CertificateStore::new(directory.0.clone());
    reopened.initialize().await.unwrap();
    assert_eq!(
        reopened
            .material(ID)
            .await
            .unwrap()
            .fullchain_path
            .parent()
            .unwrap(),
        active
    );
}

#[tokio::test]
async fn committed_candidate_gc_retries_leftover_unlink_after_active_metadata_has_expired() {
    let directory = TestDirectory(test_state_dir());
    let (store, old, active, manifest, _) = committed_store(&directory.0).await;
    drop(store);
    let path = index_path(&directory.0);
    let mut index: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    index["certificates"][ID]["expiresAt"] = "2000-01-01T00:00:00Z".into();
    std::fs::write(path, serde_json::to_vec(&index).unwrap()).unwrap();
    std::fs::remove_file(marker(&directory.0)).unwrap();
    let reopened = CertificateStore::new(directory.0.clone());
    reopened.initialize().await.unwrap();
    assert_eq!(
        reopened.get(ID).await.unwrap().status,
        CertificateStatus::Valid
    );
    let domains = ["example.com".to_owned()];
    assert!(!reopened.covers_domains(ID, &domains, true).await.unwrap());
    assert!(reopened.covers_domains(ID, &domains, false).await.unwrap());
    std::fs::write(marker(&directory.0), &manifest).unwrap();
    fail_next_candidate_manifest_removal(marker(&directory.0));
    assert_eq!(
        reopened.collect_garbage().await,
        Err(CertificateError::StoreUnavailable)
    );
    assert!(old.exists() && active.exists());
    assert_eq!(std::fs::read(marker(&directory.0)).unwrap(), manifest);
    collect(&reopened).await;
    assert!(!old.exists() && !marker(&directory.0).exists());
    assert!(active.join("fullchain.pem").exists());
    assert!(!reopened.covers_domains(ID, &domains, true).await.unwrap());
}

#[tokio::test]
async fn committed_candidate_gc_preserves_divergent_live_and_durable_material_pointers() {
    for live_old in [false, true] {
        let directory = TestDirectory(test_state_dir());
        let (store, old, active, manifest, old_index) = committed_store(&directory.0).await;
        let committed_index = std::fs::read(index_path(&directory.0)).unwrap();
        std::fs::write(index_path(&directory.0), old_index).unwrap();
        let other = if live_old {
            std::fs::remove_file(marker(&directory.0)).unwrap();
            let other = CertificateStore::new(directory.0.clone());
            other.initialize().await.unwrap();
            std::fs::write(index_path(&directory.0), &committed_index).unwrap();
            std::fs::write(marker(&directory.0), &manifest).unwrap();
            Some(other)
        } else {
            None
        };
        collect(other.as_ref().unwrap_or(&store)).await;
        assert!(old.exists() && active.exists());
        assert_eq!(std::fs::read(marker(&directory.0)).unwrap(), manifest);
        drop(other);
        std::fs::write(index_path(&directory.0), committed_index).unwrap();
        collect(&store).await;
        assert!(!old.exists() && active.exists() && !marker(&directory.0).exists());
    }
}

#[tokio::test]
async fn committed_candidate_gc_preserves_sidecar_base_for_either_pending_index() {
    for durable_pending in [false, true] {
        let directory = TestDirectory(test_state_dir());
        let (store, old, active, manifest, _) = committed_store(&directory.0).await;
        let committed_index = std::fs::read(index_path(&directory.0)).unwrap();
        let other = if durable_pending {
            let other = CertificateStore::new(directory.0.clone());
            other.initialize().await.unwrap();
            Some(other)
        } else {
            None
        };
        let pending_store = other.as_ref().unwrap_or(&store);
        let candidate = stage_acme_candidate(pending_store, &directory.0, ID).await;
        let ready = pending_store.begin_candidate_activation(ID, false).await;
        assert!(
            matches!(ready, Err(CertificateError::OperationInProgress)),
            "staging lease stays owned"
        );
        drop(ready);
        if durable_pending {
            std::fs::write(marker(&directory.0), &manifest).unwrap();
            collect(&store).await;
        } else {
            drop(store);
            let pending = CertificateStore::new(directory.0.clone());
            pending.initialize().await.unwrap();
            std::fs::write(index_path(&directory.0), &committed_index).unwrap();
            std::fs::write(marker(&directory.0), &manifest).unwrap();
            collect(&pending).await;
            assert_eq!(pending.pending_candidate_ids().await.unwrap(), [ID]);
        }
        assert!(old.exists() && active.exists() && candidate.exists());
        assert_eq!(std::fs::read(marker(&directory.0)).unwrap(), manifest);
    }
}

#[tokio::test]
async fn committed_candidate_gc_keeps_manifest_and_base_without_durable_commit_authority() {
    let directory = TestDirectory(test_state_dir());
    let (store, old, active, manifest, _) = committed_store(&directory.0).await;
    let path = index_path(&directory.0);
    let committed_index = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    collect(&store).await;
    assert!(old.exists() && active.exists());
    assert_eq!(std::fs::read(marker(&directory.0)).unwrap(), manifest);
    std::fs::write(path, committed_index).unwrap();
    collect(&store).await;
    assert!(!old.exists() && active.exists() && !marker(&directory.0).exists());
}

#[tokio::test]
async fn committed_candidate_gc_preserves_leased_and_busy_operations_before_retrying_unlink() {
    let directory = TestDirectory(test_state_dir());
    let (store, old, active, manifest, _) = committed_store(&directory.0).await;
    fail_next_candidate_manifest_removal(marker(&directory.0));
    store.begin_renewal(ID).await.unwrap();
    collect(&store).await;
    assert!(old.exists() && active.exists());
    store.finish_failed(ID, CertificateError::AcmeFailed).await;
    collect(&store).await;
    assert!(old.exists() && active.exists());
    assert_eq!(std::fs::read(marker(&directory.0)).unwrap(), manifest);
    store
        .record_operation_stage(ID, CertificateOperationStage::Applied)
        .await
        .unwrap();
    let mut rejected = false;
    for _ in 0..3 {
        match store.collect_garbage().await {
            Err(CertificateError::StoreUnavailable) => {
                rejected = true;
                break;
            }
            Ok(()) => {}
            other => panic!("unexpected cleanup result: {other:?}"),
        }
        assert!(old.exists() && active.exists());
    }
    assert!(
        rejected,
        "lease and busy stages must preserve the queued unlink fault"
    );
    assert!(old.exists() && active.exists());
    collect(&store).await;
    assert!(!old.exists() && active.exists() && !marker(&directory.0).exists());
}

#[tokio::test]
async fn committed_candidate_gc_accepts_quiescent_legacy_metadata_without_current_operations() {
    let directory = TestDirectory(test_state_dir());
    let (store, old, active, manifest, _) = committed_store(&directory.0).await;
    drop(store);
    let path = index_path(&directory.0);
    let mut index: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    index["certificates"][ID]
        .as_object_mut()
        .unwrap()
        .remove("currentOperation");
    std::fs::write(path, serde_json::to_vec(&index).unwrap()).unwrap();
    std::fs::remove_file(marker(&directory.0)).unwrap();
    let reopened = CertificateStore::new(directory.0.clone());
    reopened.initialize().await.unwrap();
    std::fs::write(marker(&directory.0), manifest).unwrap();
    collect(&reopened).await;
    assert!(!old.exists() && active.exists() && !marker(&directory.0).exists());
}
