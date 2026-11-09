use super::*;
use crate::runtime::ProxyEngine;
use std::{
    os::unix::fs::{PermissionsExt, symlink},
    sync::atomic::{AtomicU64, Ordering},
};
#[cfg(target_os = "linux")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rp-owner-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }
    fn engine(&self) -> CaddyProcess {
        CaddyProcess::new(self.0.join("caddy-fixture"), self.0.clone())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn claim_after_prior_owners_close(engine: &CaddyProcess) {
    // Other parallel fixtures can inherit an FD between fork and exec. Closing
    // all prior descriptions must eventually release the kernel's lock.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match engine.claim_ownership().await {
                Ok(()) => break,
                Err(EngineError::OwnershipUncertain) => tokio::task::yield_now().await,
                Err(error) => panic!("ownership release failed: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn independent_controllers_cannot_claim_the_same_state_before_spawning() {
    let fixture = Fixture::new();
    let first = fixture.engine();
    let second = fixture.engine();
    first.claim_ownership().await.unwrap();
    assert_eq!(
        second.claim_ownership().await,
        Err(EngineError::OwnershipUncertain)
    );
    assert!(first.child.lock().await.is_none());
    assert!(second.child.lock().await.is_none());
    drop(first);
    claim_after_prior_owners_close(&second).await;
}

#[test]
fn linked_or_nonprivate_ownership_inodes_are_rejected() {
    for kind in ["symlink", "hardlink", "public"] {
        let fixture = Fixture::new();
        let path = fixture.0.join("caddy-owner.lock");
        let target = fixture.0.join("other");
        std::fs::write(&target, b"preserve").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600)).unwrap();
        match kind {
            "symlink" => symlink(&target, &path).unwrap(),
            "hardlink" => std::fs::hard_link(&target, &path).unwrap(),
            _ => {
                std::fs::write(&path, []).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            }
        }
        assert_eq!(
            fixture.engine().claim_state_ownership(),
            Err(EngineError::OwnershipUncertain)
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"preserve");
    }
}

#[tokio::test]
async fn occupied_private_sockets_are_not_unlinked_or_adopted() {
    for name in ["caddy-admin.sock", "runtime-probe.sock"] {
        let fixture = Fixture::new();
        let listener = UnixListener::bind(fixture.0.join(name)).unwrap();
        let metadata = std::fs::symlink_metadata(fixture.0.join(name)).unwrap();
        let engine = fixture.engine();
        assert_eq!(
            engine.start("{}", "none").await,
            Err(EngineError::OwnershipUncertain)
        );
        assert!(engine.child.lock().await.is_none());
        assert_eq!(
            std::fs::symlink_metadata(fixture.0.join(name))
                .unwrap()
                .ino(),
            metadata.ino()
        );
        drop(listener);
    }
}

#[tokio::test]
async fn stale_socket_inodes_are_allowed_without_destroying_them_during_preflight() {
    let fixture = Fixture::new();
    let mut identities = Vec::new();
    for name in ["caddy-admin.sock", "runtime-probe.sock"] {
        let path = fixture.0.join(name);
        let listener = UnixListener::bind(&path).unwrap();
        identities.push((
            path.clone(),
            std::fs::symlink_metadata(&path).unwrap().ino(),
        ));
        drop(listener);
        // Parallel process fixtures may briefly inherit the listener across
        // fork. Establish that the endpoint is actually stale before testing
        // preflight, rather than racing that child's close-on-exec cleanup.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match tokio::net::UnixStream::connect(&path).await {
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => break,
                    Ok(stream) => drop(stream),
                    Err(error) => panic!("stale socket fixture failed: {error}"),
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    let engine = fixture.engine();
    engine.claim_ownership().await.unwrap();
    engine.require_unoccupied_sockets().await.unwrap();
    for (path, inode) in identities {
        assert_eq!(std::fs::symlink_metadata(path).unwrap().ino(), inode);
    }
    assert!(engine.child.lock().await.is_none());
}

#[tokio::test]
async fn socket_paths_must_not_alias_regular_files_directories_or_symlinks() {
    for name in ["caddy-admin.sock", "runtime-probe.sock"] {
        for kind in ["regular", "directory", "symlink"] {
            let fixture = Fixture::new();
            let path = fixture.0.join(name);
            let target = fixture.0.join("preserved");
            std::fs::write(&target, b"keep existing state").unwrap();
            match kind {
                "regular" => std::fs::write(&path, b"not a socket").unwrap(),
                "directory" => std::fs::create_dir(&path).unwrap(),
                _ => symlink(&target, &path).unwrap(),
            }
            let before = std::fs::symlink_metadata(&path).unwrap();
            let engine = fixture.engine();
            assert_eq!(
                engine.start("{}", "none").await,
                Err(EngineError::OwnershipUncertain)
            );
            assert!(engine.child.lock().await.is_none());
            assert_eq!(
                std::fs::symlink_metadata(&path).unwrap().ino(),
                before.ino()
            );
            assert_eq!(std::fs::read(&target).unwrap(), b"keep existing state");
        }
    }
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn inherited_lock_blocks_takeover_after_controller_ownership_is_dropped() {
    let fixture = Fixture::new();
    std::fs::write(
        fixture.0.join("caddy-fixture"),
        "#!/bin/sh\ncat >/dev/null\nexec sleep 60\n",
    )
    .unwrap();
    std::fs::set_permissions(
        fixture.0.join("caddy-fixture"),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    let engine = fixture.engine();
    let respond = async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let pid = engine
                    .child
                    .lock()
                    .await
                    .as_ref()
                    .and_then(tokio::process::Child::id);
                if let Some(pid) = pid
                    && std::fs::read_link(format!("/proc/{pid}/exe"))
                        .ok()
                        .and_then(|path| path.file_name().map(|name| name == "sleep"))
                        == Some(true)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let listener = UnixListener::bind(fixture.0.join("runtime-probe.sock")).unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(stream.read_u8().await.unwrap());
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nnone\n")
            .await
            .unwrap();
    };
    let (started, ()) = tokio::join!(engine.start("{}", "none"), respond);
    started.unwrap();
    let mut child = engine.child.lock().await.take().unwrap();
    drop(engine);
    let successor = fixture.engine();
    assert_eq!(
        successor.claim_state_ownership(),
        Err(EngineError::OwnershipUncertain)
    );
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    claim_after_prior_owners_close(&successor).await;
}
