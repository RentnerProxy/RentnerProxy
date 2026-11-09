use serde::Serialize;
use std::{collections::VecDeque, sync::Mutex, time::Duration};
use tokio::time::Instant;

const RESTART_WINDOW: Duration = Duration::from_secs(300);
const MAX_RESTARTS: usize = 3;
const MAX_PROGRESS_AGE: Duration = Duration::from_secs(90);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecoveryStatus {
    pub(crate) worker_running: bool,
    pub(crate) last_completed_age_seconds: Option<u64>,
    pub(crate) circuit_open: bool,
    pub(crate) restarts_in_window: usize,
    pub(crate) faults: u64,
    pub(crate) completed_ticks: u64,
}

#[derive(Default)]
struct State {
    worker_running: bool,
    last_completed: Option<Instant>,
    circuit_open: bool,
    restarts: VecDeque<Instant>,
    faults: u64,
    completed_ticks: u64,
}

#[derive(Default)]
pub(super) struct RecoveryHealth(Mutex<State>);

impl RecoveryHealth {
    pub(super) fn worker_started(&self) {
        if let Ok(mut state) = self.0.lock() {
            state.worker_running = true;
        }
    }

    pub(super) fn worker_stopped(&self) {
        if let Ok(mut state) = self.0.lock() {
            state.worker_running = false;
        }
    }

    pub(super) fn completed(&self) {
        if let Ok(mut state) = self.0.lock() {
            state.last_completed = Some(Instant::now());
            state.completed_ticks = state.completed_ticks.saturating_add(1);
        }
    }

    pub(super) fn fault(&self) {
        if let Ok(mut state) = self.0.lock() {
            state.faults = state.faults.saturating_add(1);
        }
    }

    pub(super) fn open_circuit(&self) {
        if let Ok(mut state) = self.0.lock() {
            if !state.circuit_open {
                tracing::warn!(
                    stage = "caddy_recovery",
                    "Caddy ownership is unconfirmed; recovery circuit opened"
                );
            }
            state.circuit_open = true;
        }
    }

    pub(super) fn admit_restart(&self, now: Instant) -> bool {
        let Ok(mut state) = self.0.lock() else {
            return false;
        };
        while state
            .restarts
            .front()
            .is_some_and(|at| now.duration_since(*at) >= RESTART_WINDOW)
        {
            state.restarts.pop_front();
        }
        if state.circuit_open {
            return false;
        }
        if state.restarts.len() >= MAX_RESTARTS {
            state.circuit_open = true;
            tracing::warn!(
                stage = "caddy_recovery",
                max_restarts = MAX_RESTARTS,
                window_seconds = RESTART_WINDOW.as_secs(),
                "Caddy recovery circuit opened; appliance restart required"
            );
            return false;
        }
        state.restarts.push_back(now);
        true
    }

    pub(super) fn snapshot(&self, now: Instant) -> RecoveryStatus {
        match self.0.lock() {
            Ok(state) => RecoveryStatus {
                worker_running: state.worker_running,
                last_completed_age_seconds: state
                    .last_completed
                    .map(|at| now.duration_since(at).as_secs()),
                circuit_open: state.circuit_open,
                restarts_in_window: state
                    .restarts
                    .iter()
                    .filter(|at| now.duration_since(**at) < RESTART_WINDOW)
                    .count(),
                faults: state.faults,
                completed_ticks: state.completed_ticks,
            },
            Err(_) => RecoveryStatus {
                worker_running: false,
                last_completed_age_seconds: None,
                circuit_open: true,
                restarts_in_window: 0,
                faults: 0,
                completed_ticks: 0,
            },
        }
    }

    pub(super) fn progressing(&self, now: Instant) -> bool {
        let Ok(state) = self.0.lock() else {
            return false;
        };
        state.worker_running
            && !state.circuit_open
            && state
                .last_completed
                .is_some_and(|at| now.duration_since(at) <= MAX_PROGRESS_AGE)
    }
}

pub(super) struct WorkerProgress(pub(super) std::sync::Arc<RecoveryHealth>);

impl Drop for WorkerProgress {
    fn drop(&mut self) {
        self.0.worker_stopped();
    }
}

#[cfg(test)]
#[path = "../../tests/private/recovery_health.rs"]
mod tests;
