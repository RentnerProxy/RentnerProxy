use super::{
    BASELINE_PROBE_REVISION, CertificateStoreReadiness, EngineError, EngineFuture, ProxyEngine,
    ProxyRuntime, RenderPurpose, RuntimeError,
    renderer::render_config_with_crowdsec,
    state::{LAST_APPLY_FILE, prepare_state_dir, read_trimmed, state_dir},
};
use crate::models::ProxyRuntimeStatus;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::time::{Instant, sleep, timeout, timeout_at};
use tracing::{info, warn};

#[cfg(test)]
#[path = "../../tests/private/runtime_shutdown.rs"]
mod shutdown_tests;

impl ProxyRuntime {
    /// Serializes startup and records an unavailable state when ownership or initialization cannot be proven.
    pub(crate) async fn initialize(self: &Arc<Self>) {
        let _guard = self.apply_lock.lock().await;
        if let Err(error) = self.initialize_locked().await {
            self.mark_unavailable().await;
            warn!(stage = "startup", ?error, "Caddy runtime unavailable");
        }
    }

    /// Chooses initial-start or recovery semantics before restoring persisted proxy state.
    pub(super) async fn initialize_locked(self: &Arc<Self>) -> Result<(), RuntimeError> {
        let recovering = self.initialization_started.swap(true, Ordering::SeqCst);
        self.initialize_with_restart_budget(recovering).await
    }

    /// Validates state, claims the engine, and starts the restored or baseline configuration within the restart budget.
    async fn initialize_with_restart_budget(
        self: &Arc<Self>,
        recovering: bool,
    ) -> Result<(), RuntimeError> {
        self.initialization_started.store(true, Ordering::SeqCst);
        if self.stopping.load(Ordering::SeqCst) {
            return Err(RuntimeError::Unavailable);
        }
        self.storage(|runtime| prepare_state_dir(&runtime.settings.state_dir))
            .await?
            .map_err(|_| RuntimeError::Unavailable)?;
        if let Some(engine) = &self.engine
            && let Err(error) = self.run_stage(engine.claim_ownership()).await
        {
            if error == EngineError::OwnershipUncertain {
                self.recovery_health.open_circuit();
            }
            return Err(RuntimeError::Unavailable);
        }
        self.storage(|runtime| runtime.prepare_engine_directories())
            .await??;
        self.storage_async(|runtime| async move {
            if runtime.certificate_store.readiness().await != CertificateStoreReadiness::Ready {
                runtime
                    .certificate_store
                    .initialize()
                    .await
                    .map_err(|_| RuntimeError::Unavailable)?;
            }
            runtime
                .trusted_ca_store
                .initialize()
                .map_err(|_| RuntimeError::Unavailable)
        })
        .await??;
        self.initialize_crowdsec_locked().await;
        let provider = self.active_crowdsec_provider().await;
        let crowdsec = provider.render_settings();
        let baseline =
            render_config_with_crowdsec(None, &self.settings.render_settings(), crowdsec.as_ref())
                .map_err(|_| RuntimeError::ApplyFailed)?;
        let restored = self.restore_active_configuration().await;
        let restored = if let Some(configuration) = restored {
            self.render_proxy_config_for_apply(&configuration, None, RenderPurpose::Recovery)
                .await
                .ok()
                .map(|json| (configuration, json))
        } else {
            None
        };
        let (configuration, json) = restored.map_or((None, baseline.clone()), |(config, json)| {
            (Some(config), json)
        });
        let expected = configuration
            .as_ref()
            .map_or(BASELINE_PROBE_REVISION, |config| config.revision.as_str());
        let Some(engine) = &self.engine else {
            return Err(RuntimeError::Unavailable);
        };
        let started = if recovering {
            self.replace_engine(engine, &json, expected, provider.environment())
                .await
        } else {
            self.start_engine(engine, &json, expected, provider.environment())
                .await
        };
        let (configuration, json) = if started.is_ok() {
            (configuration, json)
        } else {
            if self.recovery_health.snapshot(Instant::now()).circuit_open {
                return Err(RuntimeError::Unavailable);
            }
            let fallback = if recovering {
                self.replace_engine(
                    engine,
                    &baseline,
                    BASELINE_PROBE_REVISION,
                    provider.environment(),
                )
                .await
            } else {
                self.stop_engine_for_restart(engine)
                    .await
                    .map_err(|_| RuntimeError::Unavailable)?;
                self.start_engine(
                    engine,
                    &baseline,
                    BASELINE_PROBE_REVISION,
                    provider.environment(),
                )
                .await
            };
            fallback.map_err(|_| RuntimeError::Unavailable)?;
            (None, baseline)
        };
        let host_sources = match configuration.as_ref() {
            Some(config) => self.render_active_host_sources(config).await?,
            None => Default::default(),
        };
        let last_apply_at = self
            .storage(|runtime| read_trimmed(&runtime.settings.state_dir.join(LAST_APPLY_FILE)))
            .await?;
        let mut state = self.state.lock().await;
        state.active_revision = configuration.as_ref().map(|config| config.revision.clone());
        state.last_apply_at = last_apply_at;
        state.active_json = json;
        state.host_sources = host_sources;
        state.initialized = true;
        state.engine_available = true;
        drop(state);
        *self.active_configuration.lock().await = configuration;
        Ok(())
    }

    fn prepare_engine_directories(&self) -> Result<(), RuntimeError> {
        let caddy = state_dir(&self.settings.state_dir)
            .and_then(|dir| dir.ensure_dir("caddy"))
            .map_err(|_| RuntimeError::Unavailable)?;
        caddy
            .ensure_dir("data")
            .map_err(|_| RuntimeError::Unavailable)?;
        let logs = state_dir(&self.settings.state_dir)
            .and_then(|dir| dir.ensure_dir("logs"))
            .map_err(|_| RuntimeError::Unavailable)?;
        match std::fs::symlink_metadata(logs.path().join("access.log")) {
            Ok(_) => {
                logs.open_file("access.log")
                    .map_err(|_| RuntimeError::Unavailable)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(RuntimeError::Unavailable),
        }
        let autosave_dir = caddy
            .ensure_dir("config")
            .and_then(|dir| dir.ensure_dir("caddy"))
            .map_err(|_| RuntimeError::Unavailable)?;
        let autosave_path = autosave_dir
            .child_path("autosave.json")
            .map_err(|_| RuntimeError::Unavailable)?;
        match std::fs::symlink_metadata(autosave_path) {
            Ok(_) => {
                let file = autosave_dir
                    .open_file("autosave.json")
                    .map_err(|_| RuntimeError::Unavailable)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    file.set_permissions(std::fs::Permissions::from_mode(0o600))
                        .map_err(|_| RuntimeError::Unavailable)?;
                }
                #[cfg(not(unix))]
                let _ = file;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(RuntimeError::Unavailable),
        }
        Ok(())
    }

    /// Runs the sole serialized recovery loop and updates worker liveness after each bounded attempt.
    pub(crate) async fn start_recovery_worker(self: &Arc<Self>) {
        let mut slot = self.recovery_task.lock().await;
        if slot.is_some() {
            return;
        }
        let runtime = Arc::clone(self);
        runtime.recovery_health.worker_started();
        let progress = super::recovery_health::WorkerProgress(Arc::clone(&runtime.recovery_health));
        *slot = Some(tokio::spawn(async move {
            let _progress = progress;
            let mut failures = 0u32;
            loop {
                let delay = if failures == 0 {
                    runtime.settings.recovery_interval
                } else {
                    Duration::from_secs((1u64 << failures.min(6)).min(60))
                };
                sleep(delay).await;
                if runtime.stopping.load(Ordering::SeqCst) {
                    break;
                }
                let result = runtime.recover().await;
                runtime.recovery_health.completed();
                if result.is_ok() || result == Err(RuntimeError::Busy) {
                    failures = 0;
                } else {
                    failures = failures.saturating_add(1);
                }
            }
        }));
    }

    /// Checks child ownership and active-revision serviceability before deciding whether recovery is needed.
    pub(crate) async fn recover(self: &Arc<Self>) -> Result<(), RuntimeError> {
        let Some(engine) = &self.engine else {
            return Err(RuntimeError::Unavailable);
        };
        if self.stopping.load(Ordering::SeqCst) {
            return Err(RuntimeError::Unavailable);
        }
        if self.recovery_health.snapshot(Instant::now()).circuit_open {
            return Err(RuntimeError::Unavailable);
        }
        let _guard = timeout(self.settings.lock_wait, self.apply_lock.lock())
            .await
            .map_err(|_| RuntimeError::Busy)?;
        if !self.state.lock().await.initialized {
            return self.initialize_with_restart_budget(true).await;
        }
        let running = match timeout(self.settings.lock_wait, engine.is_running()).await {
            Ok(running) => running,
            Err(_) => {
                self.recovery_health.open_circuit();
                return Err(RuntimeError::Unavailable);
            }
        };
        if !running {
            self.recovery_health.fault();
            self.mark_unavailable().await;
            return self.ensure_running_locked().await;
        }
        let revision = self
            .state
            .lock()
            .await
            .active_revision
            .clone()
            .unwrap_or_else(|| BASELINE_PROBE_REVISION.to_owned());
        // A live PID is not proof that Caddy can serve the active revision.
        // Tolerate brief saturation without spending a restart attempt. Only
        // this serialized worker advances the consecutive-failure threshold.
        let probed = self.run_stage(engine.probe(&revision)).await;
        if probed == Err(EngineError::OwnershipUncertain) {
            self.recovery_health.fault();
            self.mark_unavailable().await;
            self.recovery_health.open_circuit();
            return Err(RuntimeError::Unavailable);
        }
        if probed.is_err() {
            if !self.recovery_health.probe_failed() {
                return Ok(());
            }
            self.mark_unavailable().await;
        } else {
            self.recovery_health.probe_verified();
        }
        self.ensure_running_locked().await
    }

    /// Replaces an unavailable engine only after its prior child is confirmed stopped.
    pub(super) async fn ensure_running_locked(&self) -> Result<(), RuntimeError> {
        if self.stopping.load(Ordering::SeqCst)
            || self.recovery_health.snapshot(Instant::now()).circuit_open
        {
            return Err(RuntimeError::Unavailable);
        }
        let Some(engine) = &self.engine else {
            return Err(RuntimeError::Unavailable);
        };
        let (initialized, available, json, revision) = {
            let state = self.state.lock().await;
            (
                state.initialized,
                state.engine_available,
                state.active_json.clone(),
                state
                    .active_revision
                    .clone()
                    .unwrap_or_else(|| BASELINE_PROBE_REVISION.to_owned()),
            )
        };
        if !initialized {
            return Err(RuntimeError::Unavailable);
        }
        let running = match timeout(self.settings.lock_wait, engine.is_running()).await {
            Ok(running) => running,
            Err(_) => {
                self.recovery_health.open_circuit();
                return Err(RuntimeError::Unavailable);
            }
        };
        if running && available {
            return Ok(());
        }
        let environment = self.active_crowdsec_provider().await.environment();
        if self
            .replace_engine(engine, &json, &revision, environment)
            .await
            .is_err()
        {
            self.mark_unavailable().await;
            return Err(RuntimeError::Unavailable);
        }
        self.state.lock().await.engine_available = true;
        info!("Caddy recovered verified runtime configuration");
        Ok(())
    }

    /// Returns runtime state together with recovery circuit and worker telemetry.
    pub(crate) async fn status(&self) -> ProxyRuntimeStatus {
        let state = self.state.lock().await;
        let (active_revision, last_apply_at, available) = (
            state.active_revision.clone(),
            state.last_apply_at.clone(),
            state.engine_available && self.engine.is_some(),
        );
        drop(state);
        let running = match &self.engine {
            Some(engine) => engine.is_running().await,
            None => false,
        };
        let recovery = self.recovery_health.snapshot(Instant::now());
        ProxyRuntimeStatus {
            available: available && !recovery.circuit_open,
            running,
            active_revision,
            last_apply_at,
            recovery,
        }
    }

    /// Requires healthy certificate storage, an available child, and a successful active-revision probe.
    pub(crate) async fn is_ready(&self) -> bool {
        if self.recovery_health.snapshot(Instant::now()).circuit_open {
            return false;
        }
        if self.certificate_store.readiness().await != CertificateStoreReadiness::Ready {
            return false;
        }
        let Ok(_guard) = timeout(self.settings.lock_wait, self.apply_lock.lock()).await else {
            return false;
        };
        let status = self.status().await;
        if !status.available || !status.running {
            return false;
        }
        let Some(engine) = &self.engine else {
            return false;
        };
        self.run_stage(
            engine.probe(
                status
                    .active_revision
                    .as_deref()
                    .unwrap_or(BASELINE_PROBE_REVISION),
            ),
        )
        .await
        .is_ok()
    }

    /// Reports recent recovery-worker completion without claiming that proxy traffic is ready.
    pub(crate) fn recovery_progressing(&self) -> bool {
        !self.stopping.load(Ordering::SeqCst) && self.recovery_health.progressing(Instant::now())
    }

    /// Allows a replacement only after graceful shutdown has left no owned child running.
    pub(super) async fn stop_engine_for_restart(
        &self,
        engine: &Arc<dyn ProxyEngine>,
    ) -> Result<(), EngineError> {
        // Give Caddy's existing graceful + forced-reap phases their full bound.
        // A graceful error is safe only if the owned child is proven gone.
        let _ = timeout(Duration::from_secs(24), engine.shutdown()).await;
        match timeout(self.settings.lock_wait, engine.is_running()).await {
            Ok(false) => Ok(()),
            _ => {
                self.recovery_health.open_circuit();
                Err(EngineError::Unavailable)
            }
        }
    }

    /// Signals background subsystems to reject new work before the bounded shutdown drains them.
    pub(crate) fn begin_shutdown(&self) {
        self.background_jobs.stop_accepting();
        self.stopping.store(true, Ordering::SeqCst);
        self.ddns_automation.stop_accepting();
        self.docker_discovery.stop_accepting();
    }

    #[cfg(test)]
    pub(crate) async fn shutdown(&self) {
        self.shutdown_until(Instant::now() + crate::shutdown::SHUTDOWN_TIMEOUT)
            .await;
    }

    /// Stops workers and the engine within the caller's single shutdown deadline.
    pub(crate) async fn shutdown_until(&self, deadline: Instant) {
        self.begin_shutdown();
        // Background workers share a short phase, leaving the rest of the one
        // signal-time deadline for Caddy, including its forced-reap reserve.
        let workers_deadline = (Instant::now() + Duration::from_secs(3)).min(deadline);
        tokio::join!(
            self.background_jobs.stop_until(workers_deadline),
            self.docker_discovery.stop_until(workers_deadline),
            self.stop_ddns_scheduler_until(workers_deadline),
        );
        for slot in [&self.renewal_task, &self.recovery_task] {
            let Ok(mut slot) = timeout_at(workers_deadline, slot.lock()).await else {
                warn!(
                    stage = "shutdown",
                    "Background task slot exceeded shutdown deadline"
                );
                continue;
            };
            if let Some(task) = slot.take() {
                drop(slot);
                task.abort();
                let _ = timeout_at(workers_deadline, task).await;
            }
        }
        let lock_deadline = deadline
            .checked_sub(Duration::from_secs(2))
            .unwrap_or(deadline);
        let _guard = timeout_at(lock_deadline, self.apply_lock.lock()).await;
        if _guard.is_err() {
            warn!(
                stage = "shutdown",
                "Apply drain deadline exceeded; forcing engine shutdown"
            );
        }
        if let Some(engine) = &self.engine {
            let retained = match engine.retain_for_backup_until(deadline).await {
                Ok(retained) => retained,
                Err(_) => {
                    warn!(stage = "shutdown", "Caddy maintenance handoff failed");
                    false
                }
            };
            if !retained && engine.shutdown_until(deadline).await.is_err() {
                warn!(stage = "shutdown", "Caddy required forced termination");
            }
        }
        let _ = timeout_at(deadline, self.mark_unavailable()).await;
    }

    /// Admits a fault-recovery replacement through the shared rolling restart budget.
    pub(super) async fn replace_engine(
        &self,
        engine: &Arc<dyn ProxyEngine>,
        json: &str,
        revision: &str,
        environment: super::EngineEnvironment,
    ) -> Result<(), EngineError> {
        if self.stopping.load(Ordering::SeqCst)
            || !self.recovery_health.admit_restart(Instant::now())
        {
            self.mark_unavailable().await;
            return Err(EngineError::Unavailable);
        }
        self.reconfigure_engine(engine, json, revision, environment)
            .await
    }

    /// Restarts for an intentional provider change without charging that change as a runtime fault.
    pub(super) async fn reconfigure_engine(
        &self,
        engine: &Arc<dyn ProxyEngine>,
        json: &str,
        revision: &str,
        environment: super::EngineEnvironment,
    ) -> Result<(), EngineError> {
        // A deliberate provider change is not a runtime fault. Its failure
        // rollback uses replace_engine and shares the fault-recovery budget.
        if self.stopping.load(Ordering::SeqCst)
            || self.recovery_health.snapshot(Instant::now()).circuit_open
        {
            return Err(EngineError::Unavailable);
        }
        self.stop_engine_for_restart(engine).await?;
        self.start_engine(engine, json, revision, environment).await
    }

    /// Starts and probes a child, opening the circuit when ownership cannot be established.
    async fn start_engine(
        &self,
        engine: &Arc<dyn ProxyEngine>,
        json: &str,
        revision: &str,
        environment: super::EngineEnvironment,
    ) -> Result<(), EngineError> {
        engine.set_environment(environment);
        let result = self.run_stage(engine.start(json, revision)).await;
        if result == Err(EngineError::OwnershipUncertain) {
            self.recovery_health.open_circuit();
        }
        if result.is_err() {
            let _ = self.stop_engine_for_restart(engine).await;
        } else {
            self.recovery_health.probe_verified();
        }
        result
    }

    pub(super) async fn run_stage<'a>(&self, future: EngineFuture<'a>) -> Result<(), EngineError> {
        timeout(self.settings.stage_timeout, future)
            .await
            .map_err(|_| EngineError::TimedOut)?
    }

    pub(super) async fn mark_unavailable(&self) {
        self.state.lock().await.engine_available = false;
    }
}
