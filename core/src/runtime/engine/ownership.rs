use super::{CaddyProcess, EngineError};
use std::{
    fs::OpenOptions,
    os::{
        fd::{AsRawFd, RawFd},
        unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt},
    },
    time::Duration,
};

impl CaddyProcess {
    pub(super) fn claim_state_ownership(&self) -> Result<RawFd, EngineError> {
        let mut slot = self
            .ownership
            .lock()
            .map_err(|_| EngineError::OwnershipUncertain)?;
        if let Some(file) = slot.as_ref() {
            return Ok(file.as_raw_fd());
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(self.state_dir.join("caddy-owner.lock"))
            .map_err(|_| EngineError::OwnershipUncertain)?;
        let metadata = file
            .metadata()
            .map_err(|_| EngineError::OwnershipUncertain)?;
        // SAFETY: geteuid has no arguments or memory-safety preconditions.
        let uid = unsafe { libc::geteuid() };
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.uid() != uid
            || metadata.mode() & 0o7777 != 0o600
        {
            return Err(EngineError::OwnershipUncertain);
        }
        file.try_lock()
            .map_err(|_| EngineError::OwnershipUncertain)?;
        let fd = file.as_raw_fd();
        // Never explicitly unlock: the child inherits this open-file description
        // and must keep the lock after controller exit or backup handoff.
        *slot = Some(file);
        Ok(fd)
    }

    pub(super) async fn require_unoccupied_sockets(&self) -> Result<(), EngineError> {
        for path in [&self.admin_socket, &self.probe_socket] {
            match std::fs::symlink_metadata(path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Ok(metadata) if metadata.file_type().is_socket() => {}
                _ => return Err(EngineError::OwnershipUncertain),
            }
            // Caddy can unlink a Unix socket on bind. Never let a new process
            // steal a live controller's control or revision-probe channel.
            match tokio::time::timeout(
                Duration::from_millis(500),
                tokio::net::UnixStream::connect(path),
            )
            .await
            {
                Ok(Err(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                    ) => {}
                _ => return Err(EngineError::OwnershipUncertain),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/private/caddy_ownership.rs"]
mod tests;
