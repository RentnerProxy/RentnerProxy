use super::*;

#[tokio::test]
async fn live_but_hung_caddy_is_recovered_by_the_owned_worker() {
    let engine = FakeCaddy::new();
    let (runtime, _) = runtime(Some(engine.clone()));
    runtime.initialize().await;
    let expected = configuration(4_000);
    runtime.apply(expected.clone()).await.unwrap();
    engine.probe_hang_once.store(true, Ordering::SeqCst);
    runtime.start_recovery_worker().await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while engine.start_count.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(runtime.is_ready().await);
    let status = runtime.status().await;
    assert_eq!(status.active_revision, Some(expected.revision));
    assert_eq!(status.recovery.faults, 1);
    assert_eq!(status.recovery.restarts_in_window, 1);
    assert!(runtime.recovery_progressing());
    runtime.shutdown().await;
    assert!(!runtime.recovery_progressing());
}

#[tokio::test]
async fn repeated_short_successes_exhaust_the_same_restart_budget() {
    let engine = FakeCaddy::new();
    let (runtime, _) = runtime(Some(engine.clone()));
    runtime.initialize().await;
    for _ in 0..3 {
        engine.running.store(false, Ordering::SeqCst);
        runtime.recover().await.unwrap();
        assert!(runtime.is_ready().await);
    }
    engine.running.store(false, Ordering::SeqCst);
    assert_eq!(runtime.recover().await, Err(RuntimeError::Unavailable));
    assert_eq!(engine.start_count.load(Ordering::SeqCst), 4);
    assert!(runtime.status().await.recovery.circuit_open);
    assert!(!runtime.is_ready().await);
    assert!(!runtime.recovery_progressing());
    runtime.shutdown().await;
}

#[tokio::test]
async fn uncertain_shutdown_never_spawns_a_replacement_and_latches_failure() {
    let engine = FakeCaddy::new();
    let (runtime, _) = runtime(Some(engine.clone()));
    runtime.initialize().await;
    engine
        .probes
        .lock()
        .await
        .push_back(Err(EngineError::TimedOut));
    engine.shutdown_uncertain.store(true, Ordering::SeqCst);
    assert_eq!(runtime.recover().await, Err(RuntimeError::Unavailable));
    assert_eq!(engine.start_count.load(Ordering::SeqCst), 1);
    assert!(engine.running.load(Ordering::SeqCst));
    assert!(runtime.status().await.recovery.circuit_open);
    engine.shutdown_uncertain.store(false, Ordering::SeqCst);
    assert_eq!(runtime.recover().await, Err(RuntimeError::Unavailable));
    assert_eq!(engine.start_count.load(Ordering::SeqCst), 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn failed_initialization_fallbacks_each_consume_a_recovery_attempt() {
    let engine = FakeCaddy::new();
    engine
        .starts
        .lock()
        .await
        .extend(std::iter::repeat_n(Err(EngineError::Unavailable), 10));
    let (runtime, _) = runtime(Some(engine.clone()));
    runtime.initialize().await;
    assert_eq!(engine.start_count.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.recover().await, Err(RuntimeError::Unavailable));
    assert_eq!(engine.start_count.load(Ordering::SeqCst), 4);
    assert_eq!(runtime.recover().await, Err(RuntimeError::Unavailable));
    assert_eq!(engine.start_count.load(Ordering::SeqCst), 5);
    let status = runtime.status().await;
    assert!(status.recovery.circuit_open);
    assert_eq!(status.recovery.restarts_in_window, 3);
    assert_eq!(runtime.recover().await, Err(RuntimeError::Unavailable));
    assert_eq!(engine.start_count.load(Ordering::SeqCst), 5);
    runtime.shutdown().await;
}

#[tokio::test]
async fn failed_configuration_requests_cannot_bypass_the_shared_restart_budget() {
    let engine = FakeCaddy::new();
    let (runtime, _) = runtime(Some(engine.clone()));
    runtime.initialize().await;
    runtime.apply(configuration(4_000)).await.unwrap();
    for port in 4_001..4_004 {
        engine
            .loads
            .lock()
            .await
            .push_back(Err(EngineError::InvalidResponse));
        assert_eq!(
            runtime.apply(configuration(port)).await,
            Err(RuntimeError::ApplyFailed)
        );
        assert!(runtime.is_ready().await);
    }
    engine
        .loads
        .lock()
        .await
        .push_back(Err(EngineError::InvalidResponse));
    assert_eq!(
        runtime.apply(configuration(4_004)).await,
        Err(RuntimeError::ApplyFailed)
    );
    assert_eq!(engine.start_count.load(Ordering::SeqCst), 4);
    assert!(runtime.status().await.recovery.circuit_open);
    assert!(!runtime.is_ready().await);
    runtime.shutdown().await;
}

#[tokio::test]
async fn crowdsec_switch_never_accepts_an_old_process_with_unconfirmed_shutdown() {
    const KEY: &str = "0123456789abcdef0123456789abcdef";
    let (api_url, lapi) = crowdsec_lapi(KEY).await;
    let engine = FakeCaddy::new();
    let (runtime, _) = runtime(Some(engine.clone()));
    runtime.initialize().await;
    engine.shutdown_uncertain.store(true, Ordering::SeqCst);
    assert_eq!(
        runtime
            .apply_crowdsec(CrowdSecConfigRequest {
                mode: CrowdSecMode::External,
                community_enabled: false,
                api_url: Some(api_url),
                api_key: Some(SecretString::new(KEY.to_owned())),
            })
            .await,
        Err(CrowdSecError::ApplyFailed)
    );
    assert_eq!(engine.start_count.load(Ordering::SeqCst), 1);
    assert!(runtime.status().await.recovery.circuit_open);
    assert!(
        !engine
            .configuration
            .lock()
            .await
            .contains("RENTNERPROXY_CROWDSEC_BOUNCER_KEY")
    );
    engine.shutdown_uncertain.store(false, Ordering::SeqCst);
    runtime.shutdown().await;
    lapi.abort();
}

#[tokio::test]
async fn intentional_provider_changes_do_not_spend_the_fault_recovery_budget() {
    const KEY: &str = "0123456789abcdef0123456789abcdef";
    let (api_url, lapi) = crowdsec_lapi(KEY).await;
    let engine = FakeCaddy::new();
    let (runtime, _) = runtime(Some(engine.clone()));
    runtime.initialize().await;
    for _ in 0..3 {
        for mode in [CrowdSecMode::External, CrowdSecMode::Disabled] {
            runtime
                .apply_crowdsec(CrowdSecConfigRequest {
                    mode,
                    community_enabled: false,
                    api_url: (mode == CrowdSecMode::External).then(|| api_url.clone()),
                    api_key: (mode == CrowdSecMode::External)
                        .then(|| SecretString::new(KEY.to_owned())),
                })
                .await
                .unwrap();
        }
    }
    let recovery = runtime.status().await.recovery;
    assert_eq!(engine.start_count.load(Ordering::SeqCst), 7);
    assert_eq!(recovery.restarts_in_window, 0);
    assert!(!recovery.circuit_open);
    assert!(runtime.is_ready().await);
    runtime.shutdown().await;
    lapi.abort();
}

#[tokio::test]
async fn failed_provider_changes_share_the_fault_rollback_budget() {
    const KEY: &str = "0123456789abcdef0123456789abcdef";
    let (api_url, lapi) = crowdsec_lapi(KEY).await;
    let engine = FakeCaddy::new();
    let (runtime, _) = runtime(Some(engine.clone()));
    runtime.initialize().await;
    for attempt in 0..4 {
        engine
            .starts
            .lock()
            .await
            .extend([Err(EngineError::Unavailable), Ok(())]);
        assert_eq!(
            runtime
                .apply_crowdsec(CrowdSecConfigRequest {
                    mode: CrowdSecMode::External,
                    community_enabled: false,
                    api_url: Some(api_url.clone()),
                    api_key: Some(SecretString::new(KEY.to_owned())),
                })
                .await,
            Err(CrowdSecError::ApplyFailed)
        );
        if attempt < 3 {
            assert!(runtime.is_ready().await);
        }
    }
    assert_eq!(engine.start_count.load(Ordering::SeqCst), 8);
    assert!(runtime.status().await.recovery.circuit_open);
    assert_eq!(runtime.status().await.recovery.restarts_in_window, 3);
    assert!(!runtime.is_ready().await);
    runtime.shutdown().await;
    lapi.abort();
}
