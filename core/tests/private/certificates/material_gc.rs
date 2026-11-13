use super::*;

const ID: &str = "0198d98a-0000-7000-8000-000000000070";
const UNKNOWN: &str = "0198d98a-0000-7000-8000-000000000071";

#[path = "terminal_gc.rs"]
mod terminal;

#[path = "committed_gc.rs"]
mod committed;

#[tokio::test]
async fn certificate_gc_recovers_uncommitted_delete_before_reclaiming_committed_tombstone() {
    let state = test_state_dir();
    std::fs::create_dir_all(&state).unwrap();
    let store = CertificateStore::new(state.clone());
    store.initialize().await.unwrap();
    let (request, _) = import_request();
    let staged = store.stage_manual(ID, request).await.unwrap();
    store.commit_staged(&staged).await.unwrap();
    drop(store);
    let root = state.join("certificates");
    let tombstone = root.join(format!(".deleted-{ID}-123-456"));
    std::fs::rename(root.join(ID), &tombstone).unwrap();
    let recovered = CertificateStore::new(state.clone());
    recovered.initialize().await.unwrap();
    assert!(recovered.material(ID).await.is_ok());
    assert!(!tombstone.exists());
    let backup = root.join(format!(".deleted-{UNKNOWN}-123-456"));
    let empty_tombstone = root.join(format!(".deleted-{UNKNOWN}-123-789"));
    std::fs::create_dir(&empty_tombstone).unwrap();
    std::fs::rename(root.join(ID), &backup).unwrap();
    recovered.collect_garbage().await.unwrap();
    assert!(
        !backup.exists(),
        "committed tombstone without an indexed ID is abandoned"
    );
    assert!(
        !empty_tombstone.exists(),
        "a cleanup interrupted after removing versions must be retried"
    );
}

#[tokio::test]
async fn certificate_gc_cleans_only_recognized_unleased_orphan_and_staging_material() {
    let state = test_state_dir();
    std::fs::create_dir_all(&state).unwrap();
    let store = CertificateStore::new(state.clone());
    store.initialize().await.unwrap();
    let (request, _) = import_request();
    let staged = store.stage_manual(UNKNOWN, request).await.unwrap();
    let material = store.staged_material(&staged).unwrap();
    let version = material.fullchain_path.parent().unwrap();
    let versions = version.parent().unwrap();
    let staging = versions.join(format!(".staging-{UNKNOWN}-{}-123-456", "a".repeat(64)));
    std::fs::create_dir(&staging).unwrap();
    std::fs::write(staging.join("fullchain.pem"), b"partial").unwrap();
    let unknown = versions.join(".staging-unrecognized");
    std::fs::create_dir(&unknown).unwrap();
    std::fs::write(unknown.join("notes"), b"keep").unwrap();
    store.collect_garbage().await.unwrap();
    assert!(
        staging.exists() && version.exists(),
        "lease must protect the entire ID"
    );
    store.discard_staged(&staged).await;
    store.collect_garbage().await.unwrap();
    assert!(!version.exists() && !staging.exists());
    assert_eq!(std::fs::read(unknown.join("notes")).unwrap(), b"keep");
}

#[tokio::test]
async fn certificate_gc_does_not_recursively_delete_unknown_files_and_retries_errors() {
    let state = test_state_dir();
    std::fs::create_dir_all(&state).unwrap();
    let store = CertificateStore::new(state.clone());
    store.initialize().await.unwrap();
    let version = state
        .join("certificates")
        .join(UNKNOWN)
        .join("versions")
        .join("b".repeat(64));
    std::fs::create_dir_all(version.join("fullchain.pem")).unwrap();
    assert_eq!(
        store.collect_garbage().await,
        Err(CertificateError::StoreUnavailable)
    );
    std::fs::remove_dir(version.join("fullchain.pem")).unwrap();
    let unowned = version.join("notes");
    std::fs::write(&unowned, b"keep").unwrap();
    store.collect_garbage().await.unwrap();
    assert!(unowned.exists());
    std::fs::remove_file(unowned).unwrap();
    std::fs::write(version.join("fullchain.pem"), b"partial").unwrap();
    for _ in 0..4 {
        store.collect_garbage().await.unwrap();
    }
    assert!(!version.exists());
}

#[tokio::test]
async fn certificate_gc_bounds_each_pass_and_advances_past_unknown_entries() {
    let state = test_state_dir();
    std::fs::create_dir_all(&state).unwrap();
    let store = CertificateStore::new(state.clone());
    store.initialize().await.unwrap();
    let (request, _) = import_request();
    let staged = store.stage_manual(ID, request).await.unwrap();
    store.commit_staged(&staged).await.unwrap();
    let active = store.material(ID).await.unwrap().fullchain_path;
    let versions = active.parent().unwrap().parent().unwrap();
    for n in 0..600 {
        std::fs::create_dir(versions.join(format!("unknown-{n}"))).unwrap();
    }
    let abandoned: Vec<_> = (0..130)
        .map(|n| versions.join(format!("{n:064x}")))
        .collect();
    for version in &abandoned {
        std::fs::create_dir(version).unwrap();
        std::fs::write(version.join("fullchain.pem"), b"abandoned").unwrap();
    }
    let mut previous = abandoned.len();
    for _ in 0..10 {
        store.collect_garbage().await.unwrap();
        let remaining = abandoned.iter().filter(|version| version.exists()).count();
        assert!(
            previous - remaining <= 64,
            "removal budget must apply to every pass"
        );
        previous = remaining;
    }
    assert_eq!(
        previous, 0,
        "retained scan cursor must eventually reach every orphan"
    );
    assert!(active.exists());
    assert!((0..600).all(|n| versions.join(format!("unknown-{n}")).exists()));
}

#[tokio::test]
async fn certificate_gc_protects_durable_and_memory_material_after_uncertain_index_commit() {
    let state = test_state_dir();
    std::fs::create_dir_all(&state).unwrap();
    let store = CertificateStore::new(state.clone());
    store.initialize().await.unwrap();
    let (request, _) = import_request();
    let staged = store.stage_manual(ID, request).await.unwrap();
    store.commit_staged(&staged).await.unwrap();
    let active_a = store.material(ID).await.unwrap().fullchain_path;
    let index_path = state.join("certificates/certificate-metadata.json");
    let index_a = std::fs::read(&index_path).unwrap();
    let (request, _) = import_request();
    let staged = store.stage_manual(ID, request).await.unwrap();
    store.commit_staged(&staged).await.unwrap();
    let active_b = store.material(ID).await.unwrap().fullchain_path;
    let index_b = std::fs::read(&index_path).unwrap();
    std::fs::write(&index_path, index_a).unwrap();
    store.collect_garbage().await.unwrap();
    assert!(active_a.exists() && active_b.exists());
    std::fs::write(index_path, index_b).unwrap();
    for _ in 0..4 {
        store.collect_garbage().await.unwrap();
    }
    assert!(!active_a.exists() && active_b.exists());
}

#[tokio::test]
async fn certificate_startup_preserves_ambiguous_deletion_recovery_artifacts() {
    let state = test_state_dir();
    std::fs::create_dir_all(&state).unwrap();
    let store = CertificateStore::new(state.clone());
    store.initialize().await.unwrap();
    let (request, _) = import_request();
    let staged = store.stage_manual(ID, request).await.unwrap();
    store.commit_staged(&staged).await.unwrap();
    drop(store);
    let root = state.join("certificates");
    let first = root.join(format!(".deleted-{ID}-123-456"));
    let second = root.join(format!(".deleted-{ID}-123-789"));
    std::fs::rename(root.join(ID), &first).unwrap();
    std::fs::create_dir(&second).unwrap();
    let reopened = CertificateStore::new(state);
    assert_eq!(
        reopened.initialize().await,
        Err(CertificateError::StoreUnavailable)
    );
    assert!(first.exists() && second.exists());
}

#[cfg(windows)]
#[tokio::test]
async fn certificate_gc_protects_case_insensitive_windows_material_references() {
    let state = test_state_dir();
    std::fs::create_dir_all(&state).unwrap();
    let store = CertificateStore::new(state.clone());
    store.initialize().await.unwrap();
    let (request, _) = import_request();
    let staged = store.stage_manual(ID, request).await.unwrap();
    store.commit_staged(&staged).await.unwrap();
    let material = store.material(ID).await.unwrap().fullchain_path;
    drop(store);
    let path = state.join("certificates/certificate-metadata.json");
    let mut index: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let hash = index["certificates"][ID]["materialId"]
        .as_str()
        .unwrap()
        .to_ascii_uppercase();
    index["certificates"][ID]["materialId"] = hash.into();
    std::fs::write(path, serde_json::to_vec(&index).unwrap()).unwrap();
    let reopened = CertificateStore::new(state);
    reopened.initialize().await.unwrap();
    reopened.collect_garbage().await.unwrap();
    assert!(material.exists());
    assert!(reopened.material(ID).await.is_ok());
}

#[cfg(unix)]
#[tokio::test]
async fn certificate_gc_rejects_links_without_touching_external_material() {
    let state = test_state_dir();
    std::fs::create_dir_all(&state).unwrap();
    let store = CertificateStore::new(state.clone());
    store.initialize().await.unwrap();
    let version = state
        .join("certificates")
        .join(UNKNOWN)
        .join("versions")
        .join("c".repeat(64));
    std::fs::create_dir_all(&version).unwrap();
    let outside = state.join("outside.pem");
    std::fs::write(&outside, b"keep").unwrap();
    std::os::unix::fs::symlink(&outside, version.join("fullchain.pem")).unwrap();
    assert_eq!(
        store.collect_garbage().await,
        Err(CertificateError::StoreUnavailable)
    );
    assert_eq!(std::fs::read(&outside).unwrap(), b"keep");
    std::fs::remove_file(version.join("fullchain.pem")).unwrap();
    std::fs::hard_link(&outside, version.join("fullchain.pem")).unwrap();
    let mut rejected = false;
    for _ in 0..3 {
        let result = store.collect_garbage().await;
        assert_eq!(std::fs::read(&outside).unwrap(), b"keep");
        assert_eq!(
            std::fs::read(version.join("fullchain.pem")).unwrap(),
            b"keep"
        );
        match result {
            Err(CertificateError::StoreUnavailable) => {
                rejected = true;
                break;
            }
            Ok(()) => {}
            other => panic!("unexpected cleanup result: {other:?}"),
        }
    }
    assert!(
        rejected,
        "retained cursor must eventually revisit the hard link"
    );
}
