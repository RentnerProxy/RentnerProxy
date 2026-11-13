use super::*;
use crate::tests::fixtures::TestDirectory;

async fn pending_store(state: &Path) -> (CertificateStore, PathBuf, PathBuf) {
    let (store, _, active) = active_acme_store(state.to_owned(), ID).await;
    let candidate = stage_candidate(&store).await;
    (store, active, candidate)
}

async fn stage_candidate(store: &CertificateStore) -> PathBuf {
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
    store
        .finish_candidate_failed(&staged, CertificateError::RuntimeApplyFailed)
        .await;
    candidate
}

fn marker(state: &Path) -> PathBuf {
    state.join("certificates").join(ID).join("candidate.json")
}

fn index_path(state: &Path) -> PathBuf {
    state.join("certificates/certificate-metadata.json")
}

async fn retire(store: &CertificateStore, state: &Path, candidate: &Path) -> Vec<u8> {
    std::fs::write(candidate.join("fullchain.pem"), b"invalid candidate").unwrap();
    assert!(matches!(
        store.begin_candidate_activation(ID, false).await,
        Err(CertificateError::InvalidCertificate)
    ));
    let bytes = std::fs::read(marker(state)).unwrap();
    let sidecar: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(sidecar["retired"], true);
    assert!(store.pending_candidate_ids().await.unwrap().is_empty());
    bytes
}

async fn collect(store: &CertificateStore) {
    for _ in 0..4 {
        store.collect_garbage().await.unwrap();
    }
}

fn commit_deleted_index(state: &Path) {
    let path = index_path(state);
    let mut index: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    index["certificates"]
        .as_object_mut()
        .unwrap()
        .remove(ID)
        .unwrap();
    std::fs::write(path, serde_json::to_vec(&index).unwrap()).unwrap();
}

#[tokio::test]
async fn retired_candidate_gc_reclaims_material_after_retirement_and_restart_but_keeps_active() {
    for restart in [false, true] {
        let directory = TestDirectory(test_state_dir());
        let (store, active, candidate) = pending_store(&directory.0).await;
        let bytes = retire(&store, &directory.0, &candidate).await;
        let store = if restart {
            drop(store);
            let reopened = CertificateStore::new(directory.0.clone());
            reopened.initialize().await.unwrap();
            reopened
        } else {
            store
        };
        collect(&store).await;
        assert!(!candidate.exists());
        assert!(active.join("fullchain.pem").exists());
        assert_eq!(std::fs::read(marker(&directory.0)).unwrap(), bytes);
        drop(store);
        let reopened = CertificateStore::new(directory.0.clone());
        reopened.initialize().await.unwrap();
        assert!(reopened.pending_candidate_ids().await.unwrap().is_empty());
        assert!(reopened.material(ID).await.is_ok());
    }
}

#[tokio::test]
async fn retired_candidate_gc_preserves_failed_index_retirement_until_successful_replay() {
    let directory = TestDirectory(test_state_dir());
    let (store, active, candidate) = pending_store(&directory.0).await;
    let path = index_path(&directory.0);
    let pending_index = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    std::fs::write(candidate.join("fullchain.pem"), b"invalid candidate").unwrap();
    assert!(matches!(
        store.begin_candidate_activation(ID, false).await,
        Err(CertificateError::StoreUnavailable)
    ));
    let terminal = std::fs::read(marker(&directory.0)).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&terminal).unwrap()["retired"],
        true
    );
    assert_eq!(
        store.collect_garbage().await,
        Err(CertificateError::StoreUnavailable)
    );
    assert!(active.exists() && candidate.exists());
    assert_eq!(std::fs::read(marker(&directory.0)).unwrap(), terminal);
    std::fs::remove_dir(&path).unwrap();
    std::fs::write(&path, pending_index).unwrap();
    collect(&store).await;
    assert!(
        candidate.exists(),
        "pending retirement must protect staged material"
    );
    let replayed = CertificateStore::new(directory.0.clone());
    replayed.initialize().await.unwrap();
    assert!(replayed.pending_candidate_ids().await.unwrap().is_empty());
    assert_eq!(store.pending_candidate_ids().await.unwrap(), [ID]);
    collect(&store).await;
    assert!(
        candidate.exists(),
        "a live pending ID must protect the terminal marker's material even when durable replay completed"
    );
    assert_eq!(std::fs::read(marker(&directory.0)).unwrap(), terminal);
    drop(store);
    collect(&replayed).await;
    assert!(!candidate.exists());
    assert!(active.join("fullchain.pem").exists());
}

#[tokio::test]
async fn retired_candidate_gc_preserves_durable_pending_references_until_restart_replay() {
    let directory = TestDirectory(test_state_dir());
    let (store, active, candidate) = pending_store(&directory.0).await;
    let pending_index = std::fs::read(index_path(&directory.0)).unwrap();
    let terminal = retire(&store, &directory.0, &candidate).await;
    std::fs::write(index_path(&directory.0), pending_index).unwrap();
    collect(&store).await;
    assert!(
        candidate.exists(),
        "durable pending material remains recoverable"
    );
    assert!(active.exists());
    assert_eq!(std::fs::read(marker(&directory.0)).unwrap(), terminal);
    drop(store);
    let replayed = CertificateStore::new(directory.0.clone());
    replayed.initialize().await.unwrap();
    collect(&replayed).await;
    assert!(!candidate.exists());
    assert!(replayed.material(ID).await.is_ok());
}

#[tokio::test]
async fn retired_candidate_gc_preserves_sidecar_material_for_any_pending_operation_on_same_id() {
    let directory = TestDirectory(test_state_dir());
    let (store, active, old_candidate) = pending_store(&directory.0).await;
    let terminal = retire(&store, &directory.0, &old_candidate).await;
    let new_candidate = stage_candidate(&store).await;
    std::fs::write(marker(&directory.0), &terminal).unwrap();
    collect(&store).await;
    assert!(active.exists() && old_candidate.exists() && new_candidate.exists());
    assert_eq!(std::fs::read(marker(&directory.0)).unwrap(), terminal);
}

#[tokio::test]
async fn retired_candidate_gc_recovers_uncommitted_delete_before_collecting_retired_material() {
    let directory = TestDirectory(test_state_dir());
    let (store, active, candidate) = pending_store(&directory.0).await;
    retire(&store, &directory.0, &candidate).await;
    drop(store);
    let root = directory.0.join("certificates");
    let tombstone = root.join(format!(".deleted-{ID}-123-456"));
    std::fs::rename(root.join(ID), &tombstone).unwrap();
    let reopened = CertificateStore::new(directory.0.clone());
    reopened.initialize().await.unwrap();
    collect(&reopened).await;
    assert!(!tombstone.exists() && !candidate.exists());
    assert!(active.exists() && marker(&directory.0).exists());
    assert!(reopened.material(ID).await.is_ok());
}

#[tokio::test]
async fn retired_candidate_gc_reclaims_committed_deletion_markers_without_removing_unknown_data() {
    for tombstone in [false, true] {
        for material_already_removed in [false, true] {
            for unknown in [false, true] {
                let directory = TestDirectory(test_state_dir());
                let (store, _, candidate) = pending_store(&directory.0).await;
                retire(&store, &directory.0, &candidate).await;
                drop(store);
                commit_deleted_index(&directory.0);
                let root = directory.0.join("certificates");
                let certificate = if tombstone {
                    let target = root.join(format!(".deleted-{ID}-123-456"));
                    std::fs::rename(root.join(ID), &target).unwrap();
                    target
                } else {
                    root.join(ID)
                };
                if material_already_removed {
                    std::fs::remove_dir_all(certificate.join("versions")).unwrap();
                }
                if unknown {
                    std::fs::write(certificate.join("notes.txt"), b"keep").unwrap();
                }
                let reopened = CertificateStore::new(directory.0.clone());
                reopened.initialize().await.unwrap();
                collect(&reopened).await;
                assert!(!certificate.join("candidate.json").exists());
                assert!(!candidate.exists());
                if unknown {
                    assert_eq!(
                        std::fs::read(certificate.join("notes.txt")).unwrap(),
                        b"keep"
                    );
                } else {
                    assert!(!certificate.exists());
                }
                if !unknown {
                    assert!(std::fs::read_dir(&root).unwrap().all(|entry| {
                        !entry
                            .unwrap()
                            .file_name()
                            .to_string_lossy()
                            .starts_with(&format!(".deleted-{ID}-"))
                    }));
                }
            }
        }
    }
}

#[tokio::test]
async fn retired_candidate_gc_keeps_malformed_marker_and_material_for_manual_recovery() {
    let directory = TestDirectory(test_state_dir());
    let (store, active, candidate) = pending_store(&directory.0).await;
    retire(&store, &directory.0, &candidate).await;
    drop(store);
    commit_deleted_index(&directory.0);
    std::fs::write(marker(&directory.0), br#"{"retired":true}"#).unwrap();
    let reopened = CertificateStore::new(directory.0.clone());
    reopened.initialize().await.unwrap();
    assert_eq!(
        reopened.collect_garbage().await,
        Err(CertificateError::StoreUnavailable)
    );
    assert!(active.exists() && candidate.exists() && marker(&directory.0).exists());
}

#[cfg(unix)]
#[tokio::test]
async fn retired_candidate_gc_rejects_linked_markers_without_deleting_recovery_material() {
    for hard_link in [false, true] {
        let directory = TestDirectory(test_state_dir());
        let (store, active, candidate) = pending_store(&directory.0).await;
        let terminal = retire(&store, &directory.0, &candidate).await;
        drop(store);
        commit_deleted_index(&directory.0);
        let path = marker(&directory.0);
        std::fs::remove_file(&path).unwrap();
        let outside = directory.0.join("outside.json");
        std::fs::write(&outside, &terminal).unwrap();
        if hard_link {
            std::fs::hard_link(&outside, &path).unwrap();
        } else {
            std::os::unix::fs::symlink(&outside, &path).unwrap();
        }
        let reopened = CertificateStore::new(directory.0.clone());
        reopened.initialize().await.unwrap();
        assert_eq!(
            reopened.collect_garbage().await,
            Err(CertificateError::StoreUnavailable)
        );
        assert_eq!(std::fs::read(outside).unwrap(), terminal);
        assert!(active.exists() && candidate.exists() && path.exists());
    }
}

#[tokio::test]
async fn retired_candidate_gc_marker_only_roots_respect_removal_budget_and_make_progress() {
    let directory = TestDirectory(test_state_dir());
    let (store, active, candidate) = pending_store(&directory.0).await;
    let terminal = retire(&store, &directory.0, &candidate).await;
    let template: serde_json::Value = serde_json::from_slice(&terminal).unwrap();
    let root = directory.0.join("certificates");
    let mut paths = Vec::new();
    for n in 1000..1130 {
        let id = format!("0198d98a-0000-7000-8000-{n:012x}");
        let path = root.join(&id);
        std::fs::create_dir(&path).unwrap();
        let mut marker = template.clone();
        marker["staged"]["id"] = id.into();
        std::fs::write(
            path.join("candidate.json"),
            serde_json::to_vec(&marker).unwrap(),
        )
        .unwrap();
        paths.push(path);
    }
    let mut previous = paths.len() + 1;
    for _ in 0..6 {
        store.collect_garbage().await.unwrap();
        let remaining =
            paths.iter().filter(|path| path.exists()).count() + usize::from(candidate.exists());
        assert!(previous - remaining <= 64);
        previous = remaining;
    }
    assert_eq!(previous, 0);
    assert!(active.exists() && marker(&directory.0).exists());
}
