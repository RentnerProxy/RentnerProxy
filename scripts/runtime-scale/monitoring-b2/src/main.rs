use rentnerproxy_monitoring_b2_experiment::{Claim, Error, Outbox, now_ms};
use serde_json::{Value, json};
use std::{
    env,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_postgres::{Client, NoTls, Statement};

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

struct Config {
    database: String,
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
            database,
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

/// Persists a start, refreshes authority and rechecks expiry before one bounded broker request.
/// Commits raw outcome evidence before offering slot release; Bun owns terminal result replay.
async fn probe(
    client: reqwest::Client,
    endpoint: String,
    token: String,
    claim: Claim,
    store: Arc<Mutex<Outbox>>,
    slots: Arc<Semaphore>,
    admission: Admission,
) -> Result<bool, Error> {
    let _permit = slots.acquire_owned().await?;
    let started = now_ms()?;
    claim.validate(started)?;
    let owned = claim.clone();
    let durable = store.clone();
    persistence(durable, move |owner| owner.start(&owned, started)).await?;
    // Claiming reserved a lease. Only this fresh check after durable preparation admits HTTP,
    // matching A's context read directly before inspect; a later edit affects already-admitted work.
    let admitted = admission.check(&claim).await?;
    if let Some(admitted) = admitted {
        let owned = claim.clone();
        let durable = store.clone();
        persistence(durable, move |owner| owner.admitted(&owned, admitted)).await?;
    } else {
        persistence(store, move |owner| {
            owner.finish(&claim, &json!({"error":"authority_withdrawn"}), now_ms()?)
        })
        .await?;
        return Ok(false);
    }
    // A delayed fsync must never turn an expired reservation into a new network request.
    claim.validate(now_ms()?)?;
    let outcome = async {
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
    }.await.unwrap_or_else(|_| json!({"error":"runtime_unavailable"}));
    let completed_claim = claim.clone();
    persistence(store, move |owner| {
        owner.finish(&claim, &outcome, now_ms()?)
    })
    .await?;
    admission.raw_completed(&completed_claim).await?;
    Ok(true)
}

/// Owns one restricted PostgreSQL session and bounded probe tasks for the fixture duration.
/// Recovers interrupted starts first, then drains outstanding tasks and closes the session.
async fn run(config: Config) -> Result<(), Error> {
    let (client, connection) = database(tokio_postgres::connect(&config.database, NoTls)).await?;
    let pg = tokio::spawn(connection);
    let query = format!(
        "SELECT claim::text FROM {}.claim($1, $2, $3)",
        config.namespace
    );
    let statement = database(client.prepare(&query)).await?;
    let begin = database(client.prepare(&format!(
        "SELECT {}.begin_check($1::text::uuid,$2::text::uuid,$3::text::uuid,$4,$5,$6)",
        config.namespace
    )))
    .await?;
    let client = Arc::new(client);
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
    for claim in recovered {
        admission.raw_completed(&claim).await?;
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
            let claims =
                database(client.query(&statement, &[&batch, &config.store, &config.epoch])).await?;
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
                    slots.clone(),
                    admission.clone(),
                ));
                started += 1;
            }
        } else {
            deferred += 1;
        }
        tokio::select! { _ = tokio::time::sleep(Duration::from_millis(250)) => {}, _ = tokio::signal::ctrl_c() => break }
    }
    while let Some(result) = tasks.join_next().await {
        admitted += usize::from(result??);
        finished += 1;
    }
    drop(client);
    drop(admission);
    tokio::time::timeout(Duration::from_secs(3), pg).await???;
    println!(
        "{}",
        json!({"runtime":"rust", "reserved":started, "durableResults":finished, "admitted":admitted, "deferred":deferred, "scheduledSlots":64, "interactiveSlotsReserved":2, "claimBatch":16, "databaseConnections":1})
    );
    Ok(())
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
