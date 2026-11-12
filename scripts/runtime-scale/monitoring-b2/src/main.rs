use rentnerproxy_monitoring_b2_experiment::{Claim, Error, Outbox, now_ms};
use serde_json::{Value, json};
use std::{
    env,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_postgres::{Client, NoTls, Statement, config::Host};

// One deadline starts before claim SQL and is shared by every returned claim.
// 11s work + 3s actual PG join leaves 1s below the owner's unchanged 15s drain.
const BATCH_WORK_BUDGET: Duration = Duration::from_secs(11);
const PG_JOIN_BUDGET: Duration = Duration::from_secs(3);

#[derive(Clone, Copy)]
struct WorkBudget {
    deadline: tokio::time::Instant,
}

impl WorkBudget {
    fn new() -> Self {
        Self {
            deadline: tokio::time::Instant::now() + BATCH_WORK_BUDGET,
        }
    }

    fn check(&self) -> Result<(), Error> {
        if tokio::time::Instant::now() >= self.deadline {
            return Err("B2 owned batch work budget expired".into());
        }
        Ok(())
    }

    async fn run<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T, Error>>,
    ) -> Result<T, Error> {
        // The timeout alone may poll a ready operation even after expiry. Check
        // before polling: no new SQL, blocking closure or HTTP starts after expiry.
        self.check()?;
        let value = tokio::time::timeout_at(self.deadline, operation).await??;
        self.check()?;
        Ok(value)
    }
}

/// A claim query admitted before stop retains ownership of every returned row.
async fn claim_batch<T>(
    interrupt: std::pin::Pin<&mut impl std::future::Future<Output = std::io::Result<()>>>,
    query: impl std::future::Future<Output = Result<T, Error>>,
) -> Result<Option<(T, WorkBudget)>, Error> {
    // The caller has just awaited SQLite availability. Recheck at the actual
    // claim admission boundary, then own the full query and all returned leases.
    if stop_requested(interrupt).await? {
        return Ok(None);
    }
    let budget = WorkBudget::new();
    Ok(Some((budget.run(query).await?, budget)))
}

/// Bounds a single PostgreSQL future, including connection establishment, to three seconds.
async fn database<T>(
    request: impl std::future::Future<Output = Result<T, tokio_postgres::Error>>,
) -> Result<T, Error> {
    Ok(tokio::time::timeout(Duration::from_secs(3), request).await??)
}

/// Serializes blocking SQLite operations outside the async executor with a bounded caller wait.
/// A timeout fails the caller but cannot cancel an already running blocking operation.
async fn persistence<T: Send + 'static>(
    store: Arc<Mutex<Outbox>>,
    operation: impl FnOnce(&mut Outbox) -> Result<T, Error> + Send + 'static,
) -> Result<T, Error> {
    tokio::time::timeout(
        Duration::from_secs(3),
        tokio::task::spawn_blocking(move || {
            let mut owner = store
                .lock()
                .map_err(|_| -> Error { "B2 store owner failed".into() })?;
            operation(&mut owner)
        }),
    )
    .await??
}

#[derive(Clone)]
struct Admission {
    client: Arc<Client>,
    statement: Statement,
    completed: Statement,
}

impl Admission {
    /// Offers durable raw completion to the restricted execution-slot release function.
    /// A false authority response preserves its lease fence; this never acknowledges replay.
    async fn raw_completed(&self, claim: &Claim) -> Result<(), Error> {
        // Raw completion releases only the scheduled execution slot; the original
        // policy token/45s result fence remains authoritative for Bun replay.
        let _recorded: bool = database(self.client.query_one(
            &self.completed,
            &[
                &claim.policy_id,
                &claim.policy_version,
                &claim.lease_token,
                &claim.store_id,
                &claim.controller_epoch,
                &claim.generation,
            ],
        ))
        .await?
        .get(0);
        Ok(())
    }
    /// Requests a fresh authority admission timestamp; absence denies network execution.
    async fn check(&self, claim: &Claim) -> Result<Option<i64>, Error> {
        Ok(database(self.client.query_one(
            &self.statement,
            &[
                &claim.policy_id,
                &claim.policy_version,
                &claim.lease_token,
                &claim.store_id,
                &claim.controller_epoch,
                &claim.generation,
            ],
        ))
        .await?
        .get(0))
    }
}

/// Parses the exact driver configuration used for the isolated database connection.
fn isolated_database(value: &str, appliance: bool) -> Result<tokio_postgres::Config, Error> {
    let url = reqwest::Url::parse(value)?;
    let expected_database = if appliance {
        "rentnerproxy"
    } else {
        "rentnerproxy_upstream_test"
    };
    // Driver URL query parameters can override the path/credentials or add hostaddr.
    // Only the fixture's optional explicit NoTls setting is supported.
    if !matches!(url.scheme(), "postgres" | "postgresql")
        || url.host_str() != Some("127.0.0.1")
        || url.path() != format!("/{expected_database}")
        || url.fragment().is_some()
        || url
            .query_pairs()
            .any(|(key, value)| key != "sslmode" || value != "disable")
    {
        return Err("B2 database outside isolated fixture".into());
    }
    let driver: tokio_postgres::Config = value.parse()?;
    if !matches!(driver.get_hosts(), [Host::Tcp(host)] if host == "127.0.0.1")
        || !driver.get_hostaddrs().is_empty()
        || driver.get_dbname() != Some(expected_database)
        || driver.get_ports() != [url.port().unwrap_or(5432)]
        || driver.get_options().is_some()
    {
        return Err("B2 effective database configuration outside isolated fixture".into());
    }
    Ok(driver)
}

/// Finishes each admitted completion before considering another startup recovery call.
async fn recover_completed<T, F: std::future::Future<Output = Result<(), Error>>>(
    claims: impl IntoIterator<Item = T>,
    mut complete: impl FnMut(T) -> F,
    mut interrupt: std::pin::Pin<&mut impl std::future::Future<Output = std::io::Result<()>>>,
) -> Result<bool, Error> {
    for claim in claims {
        if stop_requested(interrupt.as_mut()).await? {
            return Ok(true);
        }
        // Never drop an admitted SQL future as evidence of a clean drain.
        complete(claim).await?;
    }
    stop_requested(interrupt).await
}

/// Polls the persistent receiver without abandoning any admitted operation.
async fn stop_requested(
    interrupt: std::pin::Pin<&mut impl std::future::Future<Output = std::io::Result<()>>>,
) -> Result<bool, Error> {
    tokio::select! {
        biased;
        result = interrupt => { result?; Ok(true) }
        _ = std::future::ready(()) => Ok(false)
    }
}

struct Config {
    database: tokio_postgres::Config,
    namespace: String,
    controller: String,
    token: String,
    outbox: PathBuf,
    store: String,
    epoch: String,
    duration: Duration,
}

impl Config {
    /// Accepts only opted-in, bounded loopback fixture settings or the sealed appliance variant.
    /// Neither variant supplies defaults for missing identity, credentials or output paths.
    fn read() -> Result<Self, Error> {
        let get = |name| {
            env::var(name).map_err(|_| -> Error { "Missing B2 fixture configuration".into() })
        };
        if get("RENTNERPROXY_MONITORING_B2")? != "1" {
            return Err("B2 requires explicit fixture opt-in".into());
        }
        let database = get("B2_DATABASE_URL")?;
        let url = reqwest::Url::parse(&database)?;
        let namespace = get("B2_NAMESPACE")?;
        let controller = get("B2_CONTROLLER_URL")?;
        let token = get("B2_CONTROLLER_TOKEN")?;
        let duration: u64 = get("B2_DURATION_SECONDS")?.parse()?;
        let appliance = appliance_seal().is_ok();
        if !matches!(url.scheme(), "postgres" | "postgresql")
            || url.host_str() != Some("127.0.0.1")
            || (!appliance && url.path() != "/rentnerproxy_upstream_test")
            || (appliance && url.path() != "/rentnerproxy")
            || !namespace.starts_with("monitor_b2_")
            || namespace.len() > 60
            || !namespace
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            || (!appliance && controller != "http://127.0.0.1:55441")
            || (!appliance && token != "synthetic-monitoring-scale-token-392")
            || (appliance
                && (controller != "http://127.0.0.1:9024"
                    || token.len() != 64
                    || !token
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))))
            || !(1..=180).contains(&duration)
        {
            return Err("B2 configuration is not the isolated bounded fixture".into());
        }
        Ok(Self {
            database: isolated_database(&database, appliance)?,
            namespace,
            controller,
            token,
            outbox: PathBuf::from(get("B2_OUTBOX")?),
            store: get("B2_STORE_ID")?,
            epoch: get("B2_CONTROLLER_EPOCH")?,
            duration: Duration::from_secs(duration),
        })
    }
}

#[cfg(unix)]
/// Checks the Linux fixture marker, expected native UID/GID and configured state directory.
fn appliance_seal() -> Result<(), Error> {
    use std::os::unix::fs::MetadataExt;
    let run = env::var("B2_APPLIANCE_FIXTURE")?;
    if run.len() != 12
        || !run
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err("B2 appliance run seal invalid".into());
    }
    let directory = std::fs::symlink_metadata("/tmp/monitoring-b2")?;
    let metadata = std::fs::symlink_metadata("/tmp/monitoring-b2/mode.json")?;
    if !directory.is_dir()
        || directory.uid() != 0
        || directory.mode() & 0o022 != 0
        || !metadata.is_file()
        || metadata.uid() != 0
        || metadata.mode() & 0o022 != 0
        || metadata.len() > 4096
    {
        return Err("B2 appliance seal ownership mismatch".into());
    }
    let value: Value = serde_json::from_slice(&std::fs::read("/tmp/monitoring-b2/mode.json")?)?;
    if value["runId"] != run {
        return Err("B2 appliance seal mismatch".into());
    }
    let status = std::fs::read_to_string("/proc/self/status")?;
    for name in ["Uid:", "Gid:"] {
        let fields: Vec<_> = status
            .lines()
            .find(|line| line.starts_with(name))
            .ok_or("B2 native identity unavailable")?
            .split_whitespace()
            .skip(1)
            .collect();
        if fields.len() != 4 || fields.iter().any(|value| *value != "20004") {
            return Err("B2 native identity must be isolated UID/GID20004".into());
        }
    }
    let groups: Vec<_> = status
        .lines()
        .find(|line| line.starts_with("Groups:"))
        .ok_or("B2 native groups unavailable")?
        .split_whitespace()
        .skip(1)
        .collect();
    if groups.iter().any(|value| *value != "20004") {
        return Err("B2 native supplementary groups exceed raw-store scope".into());
    }
    let outbox = PathBuf::from(env::var("B2_OUTBOX")?);
    if outbox.parent() != Some(std::path::Path::new("/tmp/monitoring-b2/state")) {
        return Err("B2 appliance store outside sealed fixture".into());
    }
    Ok(())
}

#[cfg(not(unix))]
fn appliance_seal() -> Result<(), Error> {
    Err("B2 appliance requires Linux fixture".into())
}

/// Carries the already-admitted batch's authority, capacity and unchanged deadline.
struct ProbeOwner {
    admission: Admission,
    slots: Arc<Semaphore>,
    budget: WorkBudget,
}

/// Persists a start, refreshes authority and rechecks expiry before one bounded broker request.
/// Commits raw outcome evidence before offering slot release; Bun owns terminal result replay.
async fn probe(
    client: reqwest::Client,
    endpoint: String,
    token: String,
    claim: Claim,
    store: Arc<Mutex<Outbox>>,
    owner: ProbeOwner,
) -> Result<bool, Error> {
    let ProbeOwner {
        admission,
        slots,
        budget,
    } = owner;
    let _permit = budget
        .run(async { Ok(slots.acquire_owned().await?) })
        .await?;
    let started = now_ms()?;
    claim.validate(started)?;
    let owned = claim.clone();
    let durable = store.clone();
    budget
        .run(persistence(durable, move |owner| {
            owner.start(&owned, started)
        }))
        .await?;
    // Claiming reserved a lease. Only this fresh check after durable preparation admits HTTP,
    // matching A's context read directly before inspect; a later edit affects already-admitted work.
    let admitted = budget.run(admission.check(&claim)).await?;
    if let Some(admitted) = admitted {
        let owned = claim.clone();
        let durable = store.clone();
        budget
            .run(persistence(durable, move |owner| {
                owner.admitted(&owned, admitted)
            }))
            .await?;
    } else {
        budget
            .run(persistence(store, move |owner| {
                owner.finish(&claim, &json!({"error":"authority_withdrawn"}), now_ms()?)
            }))
            .await?;
        return Ok(false);
    }
    // A delayed fsync must never turn an expired reservation into a new network request.
    claim.validate(now_ms()?)?;
    let outcome = budget.run(async { Ok(async {
        let mut response = client.post(endpoint).bearer_auth(token).header("x-b2-attempt",claim.lease_token.clone()).json(&json!({
            "hostType": "proxy", "hostId": claim.host_id, "domain": claim.domain,
            "expectedRevision": claim.before["revision"], "upstreamOnly": true, "timeoutMs": claim.timeout_ms,
        })).send().await?;
        if !response.status().is_success() { return Ok::<Value, Error>(json!({"error":"runtime_unavailable"})); }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len() + chunk.len() > 2048 { return Ok(json!({"error":"invalid_native_result"})); }
            bytes.extend_from_slice(&chunk);
        }
        let payload: Value = match serde_json::from_slice(&bytes) { Ok(payload) => payload, Err(_) => return Ok(json!({"error":"invalid_native_result"})) };
        if payload["hostType"] != "proxy" || payload["hostId"] != claim.host_id || payload["domain"] != claim.domain || payload["expectedRevision"] != claim.before["revision"] {
            return Ok(json!({"error":"invalid_native_result"}));
        }
        Ok(payload)
    }.await.unwrap_or_else(|_| json!({"error":"runtime_unavailable"}))) }).await?;
    let completed_claim = claim.clone();
    budget
        .run(persistence(store, move |owner| {
            owner.finish(&claim, &outcome, now_ms()?)
        }))
        .await?;
    budget
        .run(admission.raw_completed(&completed_claim))
        .await?;
    Ok(true)
}

/// Owns one restricted PostgreSQL session and bounded probe tasks for the fixture duration.
/// Recovers interrupted starts first, then drains outstanding tasks and closes the session.
async fn run(config: Config) -> Result<(), Error> {
    // Register once before any work, retaining the receiver across SQL and SQLite awaits.
    // Recreating ctrl_c() per tick can lose a signal while no receiver is subscribed.
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    tokio::select! {
        biased;
        result = &mut interrupt => { result?; return Ok(()); }
        _ = std::future::ready(()) => {}
    }
    let (client, connection) = database(config.database.connect(NoTls)).await?;
    let pg = tokio::spawn(connection);
    let result = run_session(config, client, interrupt.as_mut()).await;
    // A timed-out SQL/blocking future is not a clean drain: its Err survives
    // this connection join and main exits nonzero, ending any remaining owners.
    // Only Ok means every probe and its blocking/SQL operations actually joined.
    // run_session owns all client/statement/task handles. Even a startup stop must
    // close the session and join the actual connection before reporting success.
    tokio::time::timeout(PG_JOIN_BUDGET, pg).await???;
    if let Some(summary) = result? {
        println!("{summary}");
    }
    Ok(())
}

async fn run_session(
    config: Config,
    client: Client,
    mut interrupt: std::pin::Pin<&mut impl std::future::Future<Output = std::io::Result<()>>>,
) -> Result<Option<Value>, Error> {
    let query = format!(
        "SELECT claim::text FROM {}.claim($1, $2, $3)",
        config.namespace
    );
    if stop_requested(interrupt.as_mut()).await? {
        return Ok(None);
    }
    let statement = database(client.prepare(&query)).await?;
    if stop_requested(interrupt.as_mut()).await? {
        return Ok(None);
    }
    let begin = database(client.prepare(&format!(
        "SELECT {}.begin_check($1::text::uuid,$2::text::uuid,$3::text::uuid,$4,$5,$6)",
        config.namespace
    )))
    .await?;
    let client = Arc::new(client);
    if stop_requested(interrupt.as_mut()).await? {
        return Ok(None);
    }
    let completed = database(client.prepare(&format!(
        "SELECT {}.record_raw_complete($1::text::uuid,$2::text::uuid,$3::text::uuid,$4,$5,$6)",
        config.namespace
    )))
    .await?;
    let admission = Admission {
        client: client.clone(),
        statement: begin,
        completed,
    };
    if stop_requested(interrupt.as_mut()).await? {
        return Ok(None);
    }
    let store = Arc::new(Mutex::new(Outbox::open(
        &config.outbox,
        &config.store,
        &config.epoch,
    )?));
    let recovered = persistence(store.clone(), |owner| {
        owner.recover_interrupted(now_ms()?)?;
        owner.completed_claims()
    })
    .await?;
    if recover_completed(
        recovered,
        |claim| {
            let admission = &admission;
            async move { admission.raw_completed(&claim).await }
        },
        interrupt.as_mut(),
    )
    .await?
    {
        return Ok(None);
    }
    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(12))
        .build()?;
    let endpoint = format!("{}/internal/v1/diagnostics/domain", config.controller);
    let slots = Arc::new(Semaphore::new(64));
    let mut tasks = JoinSet::new();
    let deadline = Instant::now() + config.duration;
    let mut started = 0usize;
    let mut finished = 0usize;
    let mut admitted = 0usize;
    let mut deferred = 0usize;
    while Instant::now() < deadline {
        if stop_requested(interrupt.as_mut()).await? {
            break;
        }
        while let Some(result) = tasks.try_join_next() {
            admitted += usize::from(result??);
            finished += 1;
        }
        let free = 64usize.saturating_sub(tasks.len());
        let durable = store.clone();
        let available = persistence(durable, |owner| owner.available(now_ms()?)).await?;
        // Pending tasks may not have fsynced their reservation yet. Conservative subtraction
        // reserves their maximum occupancy instead of overclaiming the last raw-store slots.
        let batch = free.min(available.saturating_sub(tasks.len())).min(16) as i32;
        if batch > 0 {
            // The single PG session reserves bounded fresh leases, never a cached policy list.
            // Connection failure aborts admission. No application credentials or raw tables are available.
            let Some((claims, budget)) = claim_batch(
                interrupt.as_mut(),
                database(client.query(&statement, &[&batch, &config.store, &config.epoch])),
            )
            .await?
            else {
                break;
            };
            // Stop after query admission does not abandon freshly returned leases.
            // Every row receives the same pre-query deadline, including claim time.
            for row in claims {
                let payload: String = row.get(0);
                if payload.len() > 2048 {
                    return Err("B2 claim size exceeded".into());
                }
                let claim: Claim = serde_json::from_str(&payload)?;
                claim.validate(now_ms()?)?;
                if claim.store_id != config.store || claim.controller_epoch != config.epoch {
                    return Err("B2 authority identity changed".into());
                }
                tasks.spawn(probe(
                    http.clone(),
                    endpoint.clone(),
                    config.token.clone(),
                    claim,
                    store.clone(),
                    ProbeOwner {
                        slots: slots.clone(),
                        admission: admission.clone(),
                        budget,
                    },
                ));
                started += 1;
            }
        } else {
            deferred += 1;
        }
        tokio::select! {
            biased;
            result = interrupt.as_mut() => { result?; break; }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
    while let Some(result) = tasks.join_next().await {
        admitted += usize::from(result??);
        finished += 1;
    }
    Ok(Some(
        json!({"runtime":"rust", "reserved":started, "durableResults":finished, "admitted":admitted, "deferred":deferred, "scheduledSlots":64, "interactiveSlotsReserved":2, "claimBatch":16, "databaseConnections":1}),
    ))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let result = match Config::read() {
        Ok(config) => run(config).await,
        Err(error) => Err(error),
    };
    if result.is_err() {
        // Driver errors can contain connection details. Only fixed diagnostics leave this fixture.
        eprintln!(
            "B2 experiment failed closed; inspect isolated fixture state (no credentials logged)"
        );
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn database_rejects_effective_destination_and_identity_overrides() {
        let base = "postgres://fixture:synthetic@127.0.0.1:5432/rentnerproxy_upstream_test";
        assert!(isolated_database(base, false).is_ok());
        assert!(
            isolated_database(
                "postgresql://fixture:synthetic@127.0.0.1/rentnerproxy",
                true
            )
            .is_ok()
        );
        assert!(isolated_database(&format!("{base}?sslmode=disable"), false).is_ok());
        for query in [
            "hostaddr=203.0.113.7",
            "dbname=other",
            "user=other",
            "password=other",
            "host=127.0.0.2",
            "port=5433",
            "options=-csearch_path=other",
            "hostaddr=127.0.0.1,203.0.113.7",
            "sslmode=require",
            "%64bname=other",
        ] {
            assert!(
                isolated_database(&format!("{base}?{query}"), false).is_err(),
                "override accepted: {query}"
            );
        }
    }

    #[tokio::test]
    async fn stop_during_availability_denies_the_next_claim_query() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let (stop, interrupted) = tokio::sync::oneshot::channel::<()>();
        let (release, available) = tokio::sync::oneshot::channel::<()>();
        let owner = tokio::spawn(async move {
            let interrupt = async { interrupted.await.map_err(std::io::Error::other) };
            tokio::pin!(interrupt);
            // This held availability completes only after stop is already pending.
            available.await.unwrap();
            claim_batch(interrupt.as_mut(), async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
        });
        stop.send(()).unwrap();
        release.send(()).unwrap();
        assert!(owner.await.unwrap().unwrap().is_none());
        assert_eq!(observed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn claim_time_and_probe_phases_share_one_budget_without_false_completion() {
        let budget = WorkBudget {
            deadline: tokio::time::Instant::now() + Duration::from_millis(100),
        };
        let completed = AtomicUsize::new(0);
        let phase = || async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            completed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        };
        // Each phase is individually far below the unchanged three-second cap.
        // Claim SQL already spent time before the probe inherits the same deadline.
        budget.run(phase()).await.unwrap();
        budget.run(phase()).await.unwrap();
        assert!(budget.run(phase()).await.is_err());
        assert_eq!(completed.load(Ordering::SeqCst), 2);
        let fresh_calls = AtomicUsize::new(0);
        assert!(
            budget
                .run(async {
                    fresh_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
                .is_err()
        );
        assert_eq!(fresh_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn recovery_checks_stop_before_first_call_and_preserves_signal_failure() {
        let calls = AtomicUsize::new(0);
        let interrupt = std::future::ready(Ok(()));
        tokio::pin!(interrupt);
        assert!(
            recover_completed(
                [1],
                |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Ok(()))
                },
                interrupt.as_mut()
            )
            .await
            .unwrap()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let interrupt = std::future::ready(Err(std::io::Error::other("synthetic signal failure")));
        tokio::pin!(interrupt);
        assert!(
            recover_completed([1], |_| std::future::ready(Ok(())), interrupt.as_mut())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn recovery_stop_settles_inflight_without_admitting_another_completion() {
        let (stop, interrupted) = tokio::sync::oneshot::channel::<()>();
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let (started, running) = tokio::sync::oneshot::channel::<()>();
        let calls = Arc::new(AtomicUsize::new(0));
        let settled = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();
        let observed_settled = settled.clone();
        let owner = tokio::spawn(async move {
            let interrupt = async { interrupted.await.map_err(std::io::Error::other) };
            tokio::pin!(interrupt);
            let mut first = Some((started, released));
            recover_completed(
                [1, 2, 3],
                move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let gate = first.take();
                    let settled = settled.clone();
                    async move {
                        if let Some((started, released)) = gate {
                            started.send(()).unwrap();
                            released.await.unwrap();
                        }
                        settled.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                interrupt.as_mut(),
            )
            .await
        });
        running.await.unwrap();
        stop.send(()).unwrap();
        tokio::task::yield_now().await;
        assert!(!owner.is_finished(), "inflight SQL was abandoned");
        release.send(()).unwrap();
        let stopped = tokio::time::timeout(Duration::from_secs(1), owner)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(stopped);
        assert_eq!(observed_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed_settled.load(Ordering::SeqCst), 1);
    }
}
