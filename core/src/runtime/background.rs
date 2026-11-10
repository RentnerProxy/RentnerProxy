use serde::Serialize;
use std::{
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const MAX: u64 = 9_007_199_254_740_991;
const IDS: [&str; 5] = [
    "caddy_recovery",
    "acme_renewal",
    "ddns",
    "docker_observation",
    "acme_orders",
];

#[derive(Clone, Copy)]
pub(super) enum Worker {
    Recovery,
    Renewal,
    Ddns,
    Docker,
    Orders,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum State {
    Unknown,
    Starting,
    Idle,
    Running,
    Degraded,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct Entry {
    #[serde(skip)]
    resident_alive: bool,
    id: &'static str,
    owner: &'static str,
    state: State,
    last_attempt: Option<u64>,
    last_success: Option<u64>,
    last_finished: Option<u64>,
    running_count: u64,
    attempts: u64,
    successes: u64,
    failures: u64,
    cancellations: u64,
    duration_buckets: [u64; 6],
    overflowed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Snapshot {
    epoch: Epoch,
    observed_at: u64,
    workers: [Entry; 5],
}
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct Epoch {
    started_at: u64,
    sequence: u64,
}

pub(crate) struct Registry {
    #[cfg(test)]
    pub(crate) observations: std::sync::atomic::AtomicU64,
    epoch: Epoch,
    entries: Mutex<[Entry; 5]>,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(MAX as u128) as u64
}
fn increment(value: &mut u64, overflowed: &mut bool) {
    if *value == MAX {
        *overflowed = true;
    } else {
        *value += 1;
    }
}

impl Registry {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            #[cfg(test)]
            observations: std::sync::atomic::AtomicU64::new(0),
            epoch: Epoch {
                started_at: now(),
                sequence: 0,
            },
            entries: Mutex::new(IDS.map(|id| Entry {
                id,
                resident_alive: false,
                owner: "controller",
                state: State::Unknown,
                last_attempt: None,
                last_success: None,
                last_finished: None,
                running_count: 0,
                attempts: 0,
                successes: 0,
                failures: 0,
                cancellations: 0,
                duration_buckets: [0; 6],
                overflowed: false,
            })),
        })
    }
    fn update(&self, worker: Worker, action: impl FnOnce(&mut Entry)) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        action(&mut entries[worker as usize]);
    }
    pub(crate) fn snapshot(&self) -> Snapshot {
        #[cfg(test)]
        self.observations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let workers = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        Snapshot {
            epoch: self.epoch,
            observed_at: now(),
            workers,
        }
    }
    pub(super) fn start(self: &Arc<Self>, worker: Worker) -> Resident {
        self.update(worker, |entry| {
            entry.resident_alive = true;
            if !matches!(entry.state, State::Stopping | State::Stopped) {
                entry.state = if entry.running_count > 0 {
                    State::Running
                } else {
                    State::Starting
                };
            }
        });
        Resident {
            registry: Arc::clone(self),
            worker,
        }
    }
    pub(super) fn begin(self: &Arc<Self>, worker: Worker) -> Pass {
        let mut failures_at_start = 0;
        self.update(worker, |entry| {
            failures_at_start = entry.failures;
            entry.last_attempt = Some(now());
            increment(&mut entry.attempts, &mut entry.overflowed);
            increment(&mut entry.running_count, &mut entry.overflowed);
            if matches!(entry.state, State::Stopping | State::Stopped) {
                entry.state = State::Stopping;
            } else if entry.state != State::Failed {
                entry.state = State::Running;
            }
        });
        Pass {
            registry: Arc::clone(self),
            worker,
            started: Instant::now(),
            finished: false,
            failures_at_start,
        }
    }
    pub(super) fn stop_worker(&self, worker: Worker) {
        self.update(worker, |entry| {
            entry.state = if !entry.resident_alive && entry.running_count == 0 {
                State::Stopped
            } else {
                State::Stopping
            };
        });
    }
    pub(super) fn stopping(&self) {
        for worker in [
            Worker::Recovery,
            Worker::Renewal,
            Worker::Ddns,
            Worker::Docker,
            Worker::Orders,
        ] {
            self.stop_worker(worker);
        }
    }
}

pub(super) struct Resident {
    registry: Arc<Registry>,
    worker: Worker,
}
impl Drop for Resident {
    fn drop(&mut self) {
        self.registry.update(self.worker, |entry| {
            entry.resident_alive = false;
            if !matches!(entry.state, State::Stopping | State::Stopped) {
                increment(&mut entry.failures, &mut entry.overflowed);
            }
            entry.state = if matches!(entry.state, State::Stopping | State::Stopped) {
                if entry.running_count > 0 {
                    State::Stopping
                } else {
                    State::Stopped
                }
            } else {
                State::Failed
            };
        });
    }
}

pub(super) enum Outcome {
    Idle,
    Success,
    Failure,
    Cancelled,
}
pub(super) struct Pass {
    registry: Arc<Registry>,
    worker: Worker,
    started: Instant,
    finished: bool,
    failures_at_start: u64,
}
impl Pass {
    pub(super) fn finish(mut self, outcome: Outcome) {
        self.complete(outcome);
    }
    fn complete(&mut self, outcome: Outcome) {
        self.finished = true;
        let bucket = [10, 100, 1000, 10000, 60000]
            .into_iter()
            .position(|bound| self.started.elapsed().as_millis() <= bound)
            .unwrap_or(5);
        self.registry.update(self.worker, |entry| {
            entry.running_count = entry.running_count.saturating_sub(1);
            entry.last_finished = Some(now());
            increment(&mut entry.duration_buckets[bucket], &mut entry.overflowed);
            let state = match outcome {
                Outcome::Idle => State::Idle,
                Outcome::Success => {
                    entry.last_success = entry.last_finished;
                    increment(&mut entry.successes, &mut entry.overflowed);
                    State::Idle
                }
                Outcome::Failure => {
                    increment(&mut entry.failures, &mut entry.overflowed);
                    State::Degraded
                }
                Outcome::Cancelled => {
                    increment(&mut entry.cancellations, &mut entry.overflowed);
                    State::Idle
                }
            };
            if entry.state == State::Stopping && !entry.resident_alive && entry.running_count == 0 {
                entry.state = State::Stopped;
            } else if !matches!(
                entry.state,
                State::Stopping | State::Stopped | State::Failed
            ) {
                entry.state = if entry.running_count > 0 {
                    State::Running
                } else if entry.failures > self.failures_at_start {
                    State::Degraded
                } else {
                    state
                };
            }
        });
    }
}
impl Drop for Pass {
    fn drop(&mut self) {
        if !self.finished {
            let stopping = {
                let entries = self
                    .registry
                    .entries
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                matches!(
                    entries[self.worker as usize].state,
                    State::Stopping | State::Stopped
                )
            };
            self.complete(if stopping && !std::thread::panicking() {
                Outcome::Cancelled
            } else {
                Outcome::Failure
            });
        }
    }
}

#[cfg(test)]
#[path = "../../tests/private/background.rs"]
mod tests;
