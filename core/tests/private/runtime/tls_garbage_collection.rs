use super::*;

fn ca_path(settings: &RuntimeSettings, ca: &TrustedCa) -> std::path::PathBuf {
    settings
        .state_dir
        .join("trusted-cas")
        .join(&ca.id)
        .join(format!(
            "{}.pem",
            ca.fingerprint_sha256.strip_prefix("sha256:").unwrap()
        ))
}

#[tokio::test]
async fn ca_gc_reclaims_rotation_deletion_and_failed_activation_material() {
    let engine = FakeCaddy::new();
    let (runtime, settings) = runtime(Some(engine.clone()));
    runtime.initialize().await;
    let ca_a = trusted_ca();
    let ca_b = trusted_ca();
    let a = ca_path(&settings, &ca_a);
    let b = ca_path(&settings, &ca_b);
    runtime
        .apply(upstream_configuration(ca_a.clone()))
        .await
        .unwrap();
    engine
        .loads
        .lock()
        .await
        .push_back(Err(EngineError::Rejected));
    runtime
        .apply(upstream_configuration(ca_b.clone()))
        .await
        .unwrap_err();
    assert!(a.exists());
    assert!(!b.exists(), "rejected material is abandoned after rollback");
    runtime.apply(upstream_configuration(ca_b)).await.unwrap();
    assert!(!a.exists(), "rotated CA must be reclaimed");
    assert!(b.exists());
    runtime.apply(configuration(4_000)).await.unwrap();
    assert!(!b.exists(), "deleted CA must be reclaimed");
}

#[tokio::test]
async fn ca_gc_protects_durable_recovery_and_defers_fenced_or_uncertain_cleanup() {
    let engine = FakeCaddy::new();
    let (runtime, settings) = runtime(Some(engine.clone()));
    runtime.initialize().await;
    let ca = trusted_ca();
    let config = upstream_configuration(ca.clone());
    runtime.apply(config).await.unwrap();
    let path = ca_path(&settings, &ca);
    runtime.shutdown().await;
    let restored = ProxyRuntime::new(settings.clone(), Some(FakeCaddy::new()));
    restored.initialize().await;
    assert!(
        path.exists(),
        "startup must retain durable recovery material"
    );
    let abandoned = trusted_ca();
    let abandoned_path = ca_path(&settings, &abandoned);
    std::fs::write(&abandoned_path, abandoned.pem.as_bytes()).unwrap();
    let fence = settings.state_dir.join("proxy-activation-pending");
    std::fs::write(&fence, b"pending\n").unwrap();
    restored.recover().await.unwrap();
    assert!(abandoned_path.exists());
    std::fs::remove_file(fence).unwrap();
    for _ in 0..4 {
        restored.recover().await.unwrap();
    }
    assert!(!abandoned_path.exists());
    assert!(path.exists());
}

#[tokio::test]
async fn ca_gc_preserves_unknown_files_and_retries_cleanup_errors() {
    let (runtime, settings) = runtime(Some(FakeCaddy::new()));
    runtime.initialize().await;
    let active = trusted_ca();
    runtime
        .apply(upstream_configuration(active.clone()))
        .await
        .unwrap();
    let abandoned = trusted_ca();
    let abandoned_path = ca_path(&settings, &abandoned);
    std::fs::create_dir(&abandoned_path).unwrap();
    let unknown = abandoned_path.parent().unwrap().join("notes.txt");
    std::fs::write(&unknown, b"unowned").unwrap();
    let temporary = abandoned_path
        .parent()
        .unwrap()
        .join(format!(".{}.pem.123.456.tmp", "a".repeat(64)));
    std::fs::write(&temporary, b"partial").unwrap();
    runtime.recover().await.unwrap();
    assert!(
        abandoned_path.is_dir(),
        "cleanup error must leave the candidate retryable"
    );
    assert!(ca_path(&settings, &active).exists());
    std::fs::remove_dir(&abandoned_path).unwrap();
    std::fs::write(&abandoned_path, abandoned.pem.as_bytes()).unwrap();
    for _ in 0..4 {
        runtime.recover().await.unwrap();
    }
    assert!(!abandoned_path.exists());
    assert!(!temporary.exists());
    assert_eq!(std::fs::read(unknown).unwrap(), b"unowned");
}

#[tokio::test]
async fn ca_gc_bounds_each_pass_and_advances_past_unknown_entries() {
    let (runtime, settings) = runtime(Some(FakeCaddy::new()));
    runtime.initialize().await;
    let ca = trusted_ca();
    runtime
        .apply(upstream_configuration(ca.clone()))
        .await
        .unwrap();
    let active = ca_path(&settings, &ca);
    let directory = active.parent().unwrap();
    for n in 0..600 {
        std::fs::write(directory.join(format!("unknown-{n}")), b"keep").unwrap();
    }
    let abandoned: Vec<_> = (0..130)
        .map(|n| directory.join(format!(".{n:064x}.pem.123.456.tmp")))
        .collect();
    for path in &abandoned {
        std::fs::write(path, b"partial").unwrap();
    }
    let mut previous = abandoned.len();
    for _ in 0..10 {
        runtime.recover().await.unwrap();
        let remaining = abandoned.iter().filter(|path| path.exists()).count();
        assert!(previous - remaining <= 64);
        previous = remaining;
    }
    assert_eq!(previous, 0);
    assert!(active.exists());
    assert!((0..600).all(|n| directory.join(format!("unknown-{n}")).exists()));
}

#[tokio::test]
async fn ca_gc_preserves_distinct_active_and_durable_references_but_reclaims_expired_history() {
    let (runtime, settings) = runtime(Some(FakeCaddy::new()));
    runtime.initialize().await;
    let active = trusted_ca();
    runtime
        .apply(upstream_configuration(active.clone()))
        .await
        .unwrap();
    let snapshot_path = settings.state_dir.join("active-proxy-snapshot.json");
    let original = std::fs::read(&snapshot_path).unwrap();
    let durable = trusted_ca();
    let config = upstream_configuration(durable.clone());
    let request = ProxyConfigRequest {
        version: 7,
        revision: config.revision,
        proxy_hosts: config.proxy_hosts,
        redirect_hosts: config.redirect_hosts,
        http_settings: config.http_settings,
        trusted_cas: config.trusted_cas,
        default_site: config.default_site,
    };
    let durable_path = ca_path(&settings, &durable);
    std::fs::write(&durable_path, durable.pem.as_bytes()).unwrap();
    std::fs::write(&snapshot_path, serde_json::to_vec(&request).unwrap()).unwrap();
    runtime.recover().await.unwrap();
    assert!(ca_path(&settings, &active).exists() && durable_path.exists());
    std::fs::write(&snapshot_path, original).unwrap();

    let mut parameters = rcgen::CertificateParams::new(vec!["expired.test".to_owned()]).unwrap();
    parameters.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    parameters.not_before = time::OffsetDateTime::from_unix_timestamp(946_684_800).unwrap();
    parameters.not_after = time::OffsetDateTime::from_unix_timestamp(978_307_200).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let pem = parameters.self_signed(&key).unwrap().pem();
    assert!(validate_trusted_ca_pem(&pem).is_err());
    let expired = TrustedCa {
        id: active.id.clone(),
        fingerprint_sha256: crate::proxy::trusted_ca_material_fingerprint(&pem).unwrap(),
        pem,
    };
    let expired_path = ca_path(&settings, &expired);
    std::fs::write(&expired_path, expired.pem.as_bytes()).unwrap();
    for _ in 0..4 {
        runtime.recover().await.unwrap();
    }
    assert!(!durable_path.exists() && !expired_path.exists());
    assert!(ca_path(&settings, &active).exists());
}

#[tokio::test]
async fn ca_gc_keeps_uncertain_activation_material_until_verified_restart() {
    let engine = FakeCaddy::new();
    let (runtime, settings) = runtime(Some(engine.clone()));
    runtime.initialize().await;
    let active = trusted_ca();
    let candidate = trusted_ca();
    runtime
        .apply(upstream_configuration(active.clone()))
        .await
        .unwrap();
    engine
        .loads
        .lock()
        .await
        .push_back(Err(EngineError::InvalidResponse));
    engine.shutdown_uncertain.store(true, Ordering::SeqCst);
    assert_eq!(
        runtime
            .apply(upstream_configuration(candidate.clone()))
            .await,
        Err(RuntimeError::ApplyFailed)
    );
    assert!(ca_path(&settings, &active).exists());
    assert!(
        ca_path(&settings, &candidate).exists(),
        "unconfirmed rollback cannot authorize collection"
    );
    engine.shutdown_uncertain.store(false, Ordering::SeqCst);
    runtime.shutdown().await;
    let recovered = ProxyRuntime::new(settings.clone(), Some(FakeCaddy::new()));
    recovered.initialize().await;
    assert!(recovered.is_ready().await);
    assert!(ca_path(&settings, &active).exists());
    assert!(!ca_path(&settings, &candidate).exists());
}

#[tokio::test]
async fn ca_gc_retries_partial_materialization_cleanup_while_preserving_active_policy() {
    let (runtime, settings) = runtime(Some(FakeCaddy::new()));
    runtime.initialize().await;
    let active = trusted_ca();
    runtime
        .apply(upstream_configuration(active.clone()))
        .await
        .unwrap();
    let candidate = trusted_ca();
    let mut blocked = trusted_ca();
    blocked.id = "0198d98a-0000-7000-8000-000000000002".to_owned();
    let blocked_path = ca_path(&settings, &blocked);
    std::fs::create_dir_all(&blocked_path).unwrap();
    let mut config = upstream_configuration(candidate.clone());
    config.trusted_cas.push(blocked);
    config.revision = revision_for_configuration_with_trusted_cas(
        &config.proxy_hosts,
        &config.http_settings,
        &config.trusted_cas,
    );
    assert_eq!(runtime.apply(config).await, Err(RuntimeError::ApplyFailed));
    for _ in 0..4 {
        runtime.recover().await.unwrap();
    }
    assert!(ca_path(&settings, &active).exists());
    assert!(!ca_path(&settings, &candidate).exists());
    assert!(
        blocked_path.is_dir(),
        "unrecognized damaged material must remain for repair"
    );
}
