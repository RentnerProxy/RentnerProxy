use crate::runtime::engine::{CaddyProcess, EngineError, ProxyEngine, ShutdownBudget};
use std::{
    path::PathBuf,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
    process::Command,
    task::JoinHandle,
    time::sleep,
};

struct ShutdownFixture {
    engine: CaddyProcess,
    root: PathBuf,
    control: JoinHandle<()>,
}

impl ShutdownFixture {
    async fn new(
        child_script: &str,
        control_delay: Duration,
        status: u16,
        exit_after_ack: bool,
    ) -> Self {
        let root = std::env::temp_dir().join(format!(
            "rp-shutdown-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let engine = CaddyProcess::new("unused-test-binary".into(), root.clone());
        let listener = UnixListener::bind(root.join("caddy-admin.sock")).unwrap();
        let exit_file = root.join("stop-acknowledged");
        let child = Command::new("sh")
            .args(["-c", child_script, "fixture"])
            .arg(&exit_file)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        *engine.child.lock().await = Some(child);
        let control = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
                assert!(request.len() < 4_096);
            }
            assert!(request.starts_with(b"POST /stop HTTP/1.1\r\n"));
            sleep(control_delay).await;
            let response = format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\n\r\n");
            // Timeout cases intentionally close the connection before this write.
            if stream.write_all(response.as_bytes()).await.is_ok() && exit_after_ack {
                std::fs::write(exit_file, b"acknowledged").unwrap();
            }
            let _ = stream.shutdown().await;
        });
        Self {
            engine,
            root,
            control,
        }
    }
}

impl Drop for ShutdownFixture {
    fn drop(&mut self) {
        self.control.abort();
        if self.root.parent() == Some(std::env::temp_dir().as_path())
            && self
                .root
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("rp-shutdown-"))
        {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

fn budget(control: u64, drain: u64) -> ShutdownBudget {
    ShutdownBudget {
        control: Duration::from_millis(control),
        drain: Duration::from_millis(drain),
        reap: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn final_shutdown_bounds_a_held_child_slot_without_losing_child_ownership() {
    let fixture = ShutdownFixture::new("exec sleep 60", Duration::ZERO, 200, false).await;
    let slot = fixture.engine.child.lock().await;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        fixture.engine.shutdown_until(deadline),
    )
    .await
    .unwrap();
    assert_eq!(result, Err(EngineError::TimedOut));
    assert!(
        slot.is_some(),
        "a contested child stays owned with kill_on_drop"
    );
    drop(slot);
    fixture
        .engine
        .terminate_child_bounded(Duration::from_secs(1))
        .await
        .unwrap();
    assert!(!fixture.engine.child_running().await);
    assert_eq!(
        fixture.engine.start_owned("{}", "unused").await,
        Err(EngineError::Unavailable)
    );
}

#[tokio::test]
async fn delayed_stop_acknowledgement_and_child_exit_complete_within_drain_budget() {
    let mut fixture = ShutdownFixture::new(
        "while [ ! -f \"$1\" ]; do sleep 0.01; done; exec sleep 0.2",
        Duration::from_millis(200),
        200,
        true,
    )
    .await;
    assert_eq!(
        fixture.engine.shutdown_owned(budget(600, 1_000)).await,
        Ok(())
    );
    assert!(!fixture.engine.child_running().await);
    tokio::time::timeout(Duration::from_secs(2), &mut fixture.control)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn stop_control_and_child_wait_share_one_absolute_deadline() {
    // A child exiting at 1.3s fits an incorrectly added 0.6s control + 1s wait,
    // but exceeds the actual shared 1s drain deadline.
    let fixture = ShutdownFixture::new(
        "while [ ! -f \"$1\" ]; do sleep 0.01; done; exec sleep 0.7",
        Duration::from_millis(600),
        200,
        true,
    )
    .await;
    assert_eq!(
        fixture.engine.shutdown_owned(budget(800, 1_000)).await,
        Err(EngineError::TimedOut)
    );
    assert!(!fixture.engine.child_running().await);
}

#[tokio::test]
async fn stalled_stop_control_is_bounded_and_kills_the_owned_child() {
    let fixture = ShutdownFixture::new("exec sleep 60", Duration::from_secs(60), 200, false).await;
    assert_eq!(
        fixture.engine.shutdown_owned(budget(50, 1_000)).await,
        Err(EngineError::TimedOut)
    );
    assert!(!fixture.engine.child_running().await);
}

#[tokio::test]
async fn rejected_stop_control_kills_the_owned_child_and_reports_failure() {
    let fixture = ShutdownFixture::new("exec sleep 60", Duration::ZERO, 500, false).await;
    assert_eq!(
        fixture.engine.shutdown_owned(budget(500, 1_000)).await,
        Err(EngineError::CommandFailed)
    );
    assert!(!fixture.engine.child_running().await);
}

#[tokio::test]
async fn unsuccessful_child_exit_is_not_reported_as_a_successful_drain() {
    let fixture = ShutdownFixture::new(
        "while [ ! -f \"$1\" ]; do sleep 0.01; done; exit 7",
        Duration::ZERO,
        200,
        true,
    )
    .await;
    assert_eq!(
        fixture.engine.shutdown_owned(budget(500, 1_000)).await,
        Err(EngineError::CommandFailed)
    );
    assert!(!fixture.engine.child_running().await);
}

#[tokio::test]
async fn absent_control_socket_is_bounded_and_kills_the_owned_child() {
    let fixture = ShutdownFixture::new("exec sleep 60", Duration::ZERO, 200, false).await;
    std::fs::remove_file(fixture.root.join("caddy-admin.sock")).unwrap();
    assert_eq!(
        fixture.engine.shutdown_owned(budget(500, 1_000)).await,
        Err(EngineError::Unavailable)
    );
    assert!(!fixture.engine.child_running().await);
}

#[tokio::test]
async fn forced_termination_reaps_the_child_and_is_idempotent() {
    let fixture = ShutdownFixture::new("exec sleep 60", Duration::ZERO, 200, false).await;
    assert_eq!(
        fixture
            .engine
            .terminate_child_bounded(Duration::from_secs(1))
            .await,
        Ok(())
    );
    assert!(fixture.engine.child.lock().await.is_none());
    assert_eq!(
        fixture
            .engine
            .terminate_child_bounded(Duration::from_secs(1))
            .await,
        Ok(())
    );
}

#[tokio::test]
/// Reserves time for forced child reaping when the graceful shutdown phase consumes its budget.
async fn expired_graceful_phase_keeps_the_forced_reap_reserve() {
    let fixture = ShutdownFixture::new("exec sleep 60", Duration::ZERO, 200, false).await;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    assert_eq!(
        fixture
            .engine
            .shutdown_owned_until(budget(20, 20), deadline)
            .await,
        Err(EngineError::TimedOut)
    );
    assert!(fixture.engine.child.lock().await.is_none());
    assert!(!fixture.engine.is_running().await);
}

#[cfg(target_os = "linux")]
#[tokio::test]
/// Ensures an externally reaped child remains uncertain and cannot authorize a replacement.
async fn externally_reaped_child_remains_unknown_and_never_authorizes_replacement() {
    let root = std::env::temp_dir().join(format!(
        "rp-external-reap-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let engine = CaddyProcess::new(root.join("must-not-spawn"), root.clone());
    let child = Command::new("/bin/true")
        .kill_on_drop(false)
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    // SAFETY: this current-thread fixture synchronously reaps its exact child
    // before Tokio's signal driver runs. ECHILD then models lost wait ownership
    // without changing global SIGCHLD handling or touching another process.
    let reaped = loop {
        // SAFETY: the exact-child ownership invariant above holds on every retry.
        let reaped = unsafe { libc::waitpid(pid as i32, std::ptr::null_mut(), 0) };
        if reaped < 0 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        break reaped;
    };
    assert_eq!(reaped, pid as i32);
    *engine.child.lock().await = Some(child);
    assert_eq!(
        engine.owned_child_running().await,
        Err(EngineError::OwnershipUncertain)
    );
    assert!(
        engine.is_running().await,
        "unknown ownership cannot be reported as absent"
    );
    assert_eq!(
        engine.start("{}", "none").await,
        Err(EngineError::OwnershipUncertain)
    );
    assert_eq!(
        engine.terminate_child_bounded(Duration::from_secs(1)).await,
        Err(EngineError::CommandFailed)
    );
    assert!(
        engine.child.lock().await.is_some(),
        "failed reaping must retain the original handle"
    );
    assert_eq!(
        engine.owned_child_running().await,
        Err(EngineError::OwnershipUncertain)
    );
    drop(engine);
    std::fs::remove_dir(root).unwrap();
}
