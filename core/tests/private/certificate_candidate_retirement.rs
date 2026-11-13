use super::*;

fn invalidate_candidate(staged: &StagedCertificate, directory: &TestDirectory, expired: bool) {
    let version = directory
        .0
        .join("certificates")
        .join(ID)
        .join("versions")
        .join(staged.stored.material_id.as_ref().unwrap());
    if expired {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut parameters = rcgen::CertificateParams::new(vec!["example.com".into()]).unwrap();
        parameters.not_before = OffsetDateTime::now_utc() - time::Duration::days(2);
        parameters.not_after = OffsetDateTime::now_utc() - time::Duration::days(1);
        let certificate = parameters.self_signed(&key).unwrap();
        std::fs::write(version.join("fullchain.pem"), certificate.pem()).unwrap();
        std::fs::write(version.join("private-key.pem"), key.serialize_pem()).unwrap();
    } else {
        std::fs::write(version.join("fullchain.pem"), b"invalid certificate").unwrap();
    }
}

#[tokio::test]
async fn unusable_candidate_retires_once_and_same_id_can_be_renewed_across_restart() {
    for expired in [false, true] {
        for restart_before in [false, true] {
            let (mut store, staged, directory) = retryable_candidate().await;
            let active = store.get(ID).await.unwrap();
            let active_material = store.material(ID).await.unwrap();
            let active_bytes = std::fs::read(&active_material.fullchain_path).unwrap();
            invalidate_candidate(&staged, &directory, expired);
            if restart_before {
                store = CertificateStore::new(directory.0.clone());
                store.initialize().await.unwrap();
            }
            let events_before = store.events(None, 100).await.unwrap().events.len();
            let expected = if expired {
                CertificateError::CertificateExpired
            } else {
                CertificateError::InvalidCertificate
            };
            assert!(matches!(store.begin_candidate_activation(ID, false).await,
                Err(error) if error == expected));
            assert_lease_released(&store).await;
            assert!(store.pending_candidate_ids().await.unwrap().is_empty());
            assert_eq!(store.get(ID).await.unwrap().fingerprint, active.fingerprint);
            assert_eq!(
                std::fs::read(&active_material.fullchain_path).unwrap(),
                active_bytes
            );
            let terminal = store.events(None, 100).await.unwrap();
            assert_eq!(terminal.events.len(), events_before + 1);
            assert_eq!(
                terminal.events.last().unwrap().kind,
                CertificateEventKind::Failed
            );
            assert_eq!(
                terminal.events.last().unwrap().stage,
                CertificateOperationStage::Failed
            );
            for _ in 0..3 {
                assert!(
                    store
                        .begin_candidate_activation(ID, true)
                        .await
                        .unwrap()
                        .is_none()
                );
            }
            let reopened = CertificateStore::new(directory.0.clone());
            reopened.initialize().await.unwrap();
            assert!(reopened.pending_candidate_ids().await.unwrap().is_empty());
            assert_eq!(
                reopened.events(None, 100).await.unwrap().events,
                terminal.events
            );
            let (_, request) = reopened.begin_renewal(ID).await.unwrap();
            let leaf = rcgen::generate_simple_self_signed(request.domains.clone()).unwrap();
            let replacement = reopened
                .stage_acme(
                    ID,
                    &request,
                    leaf.cert.pem(),
                    leaf.signing_key.serialize_pem(),
                )
                .await
                .unwrap();
            let new_operation = replacement
                .stored
                .metadata
                .current_operation
                .as_ref()
                .unwrap()
                .id
                .clone();
            drop(reopened);
            let restarted = CertificateStore::new(directory.0.clone());
            restarted.initialize().await.unwrap();
            assert_eq!(
                restarted.index.lock().await.pending_candidates[ID]
                    .staged
                    .metadata
                    .current_operation
                    .as_ref()
                    .unwrap()
                    .id,
                new_operation
            );
            let replacement = restarted
                .begin_candidate_activation(ID, false)
                .await
                .unwrap()
                .unwrap();
            let renewed = restarted.commit_staged(&replacement).await.unwrap();
            assert_ne!(renewed.fingerprint, active.fingerprint);
        }
    }
}

#[tokio::test]
async fn retired_candidate_does_not_block_same_id_issue_import_or_delete() {
    for action in 0..3 {
        let (store, staged, directory) = retryable_candidate().await;
        invalidate_candidate(&staged, &directory, false);
        assert!(store.begin_candidate_activation(ID, false).await.is_err());
        let reopened = CertificateStore::new(directory.0.clone());
        reopened.initialize().await.unwrap();
        match action {
            0 => {
                reopened
                    .begin_issue(ID, issue_request(), false)
                    .await
                    .unwrap();
                reopened
                    .finish_failed(ID, CertificateError::AcmeFailed)
                    .await;
            }
            1 => {
                let leaf = rcgen::generate_simple_self_signed(vec!["example.com".into()]).unwrap();
                let imported = reopened
                    .stage_manual(
                        ID,
                        CertificateImportRequest {
                            certificate_pem: leaf.cert.pem(),
                            private_key_pem: leaf.signing_key.serialize_pem(),
                            chain_pem: None,
                            required_domains: None,
                        },
                    )
                    .await
                    .unwrap();
                reopened.commit_staged(&imported).await.unwrap();
                let after_import = CertificateStore::new(directory.0.clone());
                after_import.initialize().await.unwrap();
                assert_eq!(
                    after_import.get(ID).await.unwrap().source,
                    CertificateSource::Manual
                );
                assert!(
                    after_import
                        .pending_candidate_ids()
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
            _ => {
                reopened.delete_if_unused(ID, false).await.unwrap();
            }
        }
        assert_lease_released(&reopened).await;
    }
}

#[tokio::test]
async fn failed_retirement_preserves_live_state_and_recovers_durable_terminal_manifest() {
    for fail_manifest in [false, true] {
        let (store, staged, directory) = retryable_candidate().await;
        invalidate_candidate(&staged, &directory, false);
        let before = serde_json::to_value(&*store.index.lock().await).unwrap();
        let certificates = directory.0.join("certificates");
        let path = if fail_manifest {
            certificates.join(ID).join(CANDIDATE_MANIFEST_FILE)
        } else {
            certificates.join(CERTIFICATE_INDEX_FILE)
        };
        let bytes = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(matches!(
            store.begin_candidate_activation(ID, false).await,
            Err(CertificateError::StoreUnavailable)
        ));
        assert_eq!(
            serde_json::to_value(&*store.index.lock().await).unwrap(),
            before
        );
        assert_lease_released(&store).await;
        std::fs::remove_dir(&path).unwrap();
        std::fs::write(&path, bytes).unwrap();
        drop(store);
        let reopened = CertificateStore::new(directory.0.clone());
        reopened.initialize().await.unwrap();
        if fail_manifest {
            assert_eq!(reopened.pending_candidate_ids().await.unwrap(), [ID]);
            assert!(matches!(
                reopened.begin_candidate_activation(ID, false).await,
                Err(CertificateError::InvalidCertificate)
            ));
        }
        assert!(reopened.pending_candidate_ids().await.unwrap().is_empty());
        let events = reopened.events(None, 100).await.unwrap().events;
        assert_eq!(events.len(), before["events"].as_array().unwrap().len() + 1);
        let restarted = CertificateStore::new(directory.0.clone());
        restarted.initialize().await.unwrap();
        assert_eq!(restarted.events(None, 100).await.unwrap().events, events);
        assert!(restarted.begin_renewal(ID).await.is_ok());
        restarted
            .finish_failed(ID, CertificateError::AcmeFailed)
            .await;
    }
}

#[tokio::test]
async fn terminal_retirement_replays_over_busy_durable_index_after_failed_failure_and_retirement_writes()
 {
    for renewal in [false, true] {
        for missing in [false, true] {
            let (store, directory) = if renewal {
                active_store().await
            } else {
                let directory = TestDirectory(state_dir());
                std::fs::create_dir_all(&directory.0).unwrap();
                let store = CertificateStore::new(directory.0.clone());
                store.initialize().await.unwrap();
                (store, directory)
            };
            let active_fingerprint = if renewal {
                store.get(ID).await.unwrap().fingerprint
            } else {
                None
            };
            let request = if renewal {
                store.begin_renewal(ID).await.unwrap().1
            } else {
                let request = issue_request();
                store.begin_issue(ID, request.clone(), false).await.unwrap();
                request
            };
            let leaf = rcgen::generate_simple_self_signed(request.domains.clone()).unwrap();
            let staged = store
                .stage_acme(
                    ID,
                    &request,
                    leaf.cert.pem(),
                    leaf.signing_key.serialize_pem(),
                )
                .await
                .unwrap();
            let index_path = directory
                .0
                .join("certificates")
                .join(CERTIFICATE_INDEX_FILE);
            let durable_bytes = std::fs::read(&index_path).unwrap();
            let durable: CertificateIndex = serde_json::from_slice(&durable_bytes).unwrap();
            assert_eq!(
                durable.certificates[ID].metadata.operation,
                if renewal {
                    CertificateOperation::Renewing
                } else {
                    CertificateOperation::Issuing
                }
            );
            let operation_id = staged
                .stored
                .metadata
                .current_operation
                .as_ref()
                .unwrap()
                .id
                .clone();
            std::fs::remove_file(&index_path).unwrap();
            std::fs::create_dir(&index_path).unwrap();
            store
                .finish_candidate_failed(&staged, CertificateError::RuntimeApplyFailed)
                .await;
            assert_eq!(
                store.get(ID).await.unwrap().operation,
                CertificateOperation::Idle
            );
            if missing {
                let candidate = staged.material(&store.certificates_dir().unwrap()).unwrap();
                std::fs::remove_file(candidate.fullchain_path).unwrap();
            } else {
                invalidate_candidate(&staged, &directory, false);
            }
            let before_retirement = serde_json::to_value(&*store.index.lock().await).unwrap();
            assert!(matches!(
                store.begin_candidate_activation(ID, false).await,
                Err(CertificateError::StoreUnavailable)
            ));
            assert_eq!(
                serde_json::to_value(&*store.index.lock().await).unwrap(),
                before_retirement
            );
            assert!(
                recovery::read_candidate_manifest(&store.certificates_dir().unwrap(), ID)
                    .unwrap()
                    .unwrap()
                    .retired
            );
            assert_lease_released(&store).await;
            std::fs::remove_dir(&index_path).unwrap();
            std::fs::write(&index_path, durable_bytes).unwrap();
            drop(store);
            let reopened = CertificateStore::new(directory.0.clone());
            reopened.initialize().await.unwrap();
            assert!(reopened.pending_candidate_ids().await.unwrap().is_empty());
            let recovered = reopened.get(ID).await.unwrap();
            assert_eq!(recovered.operation, CertificateOperation::Idle);
            assert_eq!(recovered.fingerprint, active_fingerprint);
            assert_eq!(recovered.current_operation.unwrap().id, operation_id);
            let events = reopened.events(None, 100).await.unwrap().events;
            assert_eq!(events.len(), durable.events.len() + 1);
            assert_eq!(events.last().unwrap().operation_id, operation_id);
            assert_eq!(events.last().unwrap().kind, CertificateEventKind::Failed);
            assert_eq!(
                events.last().unwrap().stage,
                CertificateOperationStage::Failed
            );
            assert_eq!(
                events.last().unwrap().error_code.as_deref(),
                Some(CertificateError::InvalidCertificate.code())
            );
            drop(reopened);
            let restarted = CertificateStore::new(directory.0.clone());
            restarted.initialize().await.unwrap();
            assert_eq!(restarted.events(None, 100).await.unwrap().events, events);
            assert!(
                restarted
                    .begin_candidate_activation(ID, true)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_lease_released(&restarted).await;
            if renewal {
                restarted.begin_renewal(ID).await.unwrap();
            } else {
                restarted
                    .begin_issue(ID, issue_request(), false)
                    .await
                    .unwrap();
            }
            restarted
                .finish_failed(ID, CertificateError::AcmeFailed)
                .await;
        }
    }
}

#[tokio::test]
async fn invalid_candidate_cannot_retire_across_busy_or_changed_active_generation() {
    for busy in [false, true] {
        let (store, staged, directory) = retryable_candidate().await;
        invalidate_candidate(&staged, &directory, false);
        {
            let mut index = store.index.lock().await;
            let active = index.certificates.get_mut(ID).unwrap();
            if busy {
                active.metadata.operation = CertificateOperation::Renewing;
            } else {
                active.material_id = Some("0".repeat(64));
            }
        }
        let before = serde_json::to_value(&*store.index.lock().await).unwrap();
        assert!(store.begin_candidate_activation(ID, false).await.is_err());
        assert_eq!(
            serde_json::to_value(&*store.index.lock().await).unwrap(),
            before
        );
        assert!(
            !recovery::read_candidate_manifest(&store.certificates_dir().unwrap(), ID)
                .unwrap()
                .unwrap()
                .retired
        );
        assert_lease_released(&store).await;
    }
}

#[tokio::test]
async fn missing_candidate_material_can_be_retired_without_losing_active_material() {
    for missing in ["fullchain.pem", "private-key.pem", "version"] {
        let (store, staged, directory) = retryable_candidate().await;
        let version = directory
            .0
            .join("certificates")
            .join(ID)
            .join("versions")
            .join(staged.stored.material_id.as_ref().unwrap());
        let active = store.material(ID).await.unwrap();
        let active_bytes = std::fs::read(&active.fullchain_path).unwrap();
        if missing == "version" {
            std::fs::remove_dir_all(&version).unwrap();
        } else {
            std::fs::remove_file(version.join(missing)).unwrap();
        }
        assert!(matches!(
            store.begin_candidate_activation(ID, false).await,
            Err(CertificateError::InvalidCertificate)
        ));
        assert!(store.pending_candidate_ids().await.unwrap().is_empty());
        assert_eq!(std::fs::read(active.fullchain_path).unwrap(), active_bytes);
        assert_lease_released(&store).await;
        let reopened = CertificateStore::new(directory.0.clone());
        reopened.initialize().await.unwrap();
        assert!(reopened.begin_renewal(ID).await.is_ok());
        reopened
            .finish_failed(ID, CertificateError::AcmeFailed)
            .await;
    }
}

#[tokio::test]
async fn ambiguous_candidate_storage_failure_remains_pending_without_event_churn_until_repaired() {
    let (store, staged, directory) = retryable_candidate().await;
    let certificate = directory
        .0
        .join("certificates")
        .join(ID)
        .join("versions")
        .join(staged.stored.material_id.as_ref().unwrap())
        .join("fullchain.pem");
    let bytes = std::fs::read(&certificate).unwrap();
    std::fs::remove_file(&certificate).unwrap();
    std::fs::create_dir(&certificate).unwrap();
    let before = serde_json::to_value(&*store.index.lock().await).unwrap();
    for _ in 0..3 {
        assert!(matches!(
            store.begin_candidate_activation(ID, false).await,
            Err(CertificateError::StoreUnavailable)
        ));
        assert_eq!(
            serde_json::to_value(&*store.index.lock().await).unwrap(),
            before
        );
        assert_lease_released(&store).await;
    }
    let reopened = CertificateStore::new(directory.0.clone());
    reopened.initialize().await.unwrap();
    assert_eq!(reopened.pending_candidate_ids().await.unwrap(), [ID]);
    assert_eq!(
        reopened.events(None, 100).await.unwrap().events,
        store.events(None, 100).await.unwrap().events
    );
    std::fs::remove_dir(&certificate).unwrap();
    std::fs::write(&certificate, bytes).unwrap();
    let repaired = reopened
        .begin_candidate_activation(ID, false)
        .await
        .unwrap()
        .unwrap();
    reopened.commit_staged(&repaired).await.unwrap();
    assert!(reopened.pending_candidate_ids().await.unwrap().is_empty());
}

#[tokio::test]
async fn terminal_manifest_cannot_retire_an_indexed_candidate_for_a_different_operation() {
    let (store, staged, directory) = retryable_candidate().await;
    invalidate_candidate(&staged, &directory, false);
    assert!(store.begin_candidate_activation(ID, false).await.is_err());
    let mut other = staged.stored.clone();
    other.metadata.current_operation =
        Some(operations::new_operation(OperationKind::Renew).unwrap());
    let manifest = recovery::read_candidate_manifest(&store.certificates_dir().unwrap(), ID)
        .unwrap()
        .unwrap();
    let mut index = store.index.lock().await;
    let mut pending = manifest.clone();
    pending.retired = false;
    pending.staged = other;
    index
        .certificates
        .get_mut(ID)
        .unwrap()
        .metadata
        .current_operation = pending.staged.metadata.current_operation.clone();
    index.pending_candidates.insert(ID.into(), pending);
    let before = serde_json::to_value(&*index).unwrap();
    assert!(matches!(
        recovery::reconcile_candidate_manifests(&store.certificates_dir().unwrap(), &mut index),
        Err(CertificateError::StoreUnavailable)
    ));
    assert_eq!(serde_json::to_value(&*index).unwrap(), before);
}
