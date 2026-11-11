use super::{Error, FILE, MAX_BYTES, Registry, Store, validation, verification};
use crate::runtime::state::state_dir;
use std::{collections::BTreeSet, path::Path, sync::Arc};
use tokio::sync::OwnedMutexGuard;

impl Store {
    pub(super) async fn lock(&self) -> Result<OwnedMutexGuard<Option<Registry>>, Error> {
        let mut state = Arc::clone(&self.state).lock_owned().await;
        if state.is_some() {
            return Ok(state);
        }
        let path = self.path.clone();
        #[cfg(test)]
        let pause = self.io_pause.lock().unwrap().take();
        tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(pause) = pause {
                pause.wait();
            }
            *state = Some(Self::load(&path)?);
            Ok(state)
        })
        .await
        .map_err(|_| Error::StoreUnavailable)?
    }
    pub(super) fn registry(
        state: &mut OwnedMutexGuard<Option<Registry>>,
    ) -> Result<&mut Registry, Error> {
        state.as_mut().ok_or(Error::StoreUnavailable)
    }
    fn load(path: &Path) -> Result<Registry, Error> {
        let dir = state_dir(path).map_err(|_| Error::StoreUnavailable)?;
        let bytes = match dir.read_file(FILE, MAX_BYTES) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Registry {
                    version: 1,
                    connections: vec![],
                });
            }
            Err(_) => return Err(Error::StoreUnavailable),
        };
        let registry: Registry =
            serde_json::from_slice(&bytes).map_err(|_| Error::StoreUnavailable)?;
        if registry.version != 1 || registry.connections.len() > 100 {
            return Err(Error::StoreUnavailable);
        }
        let mut ids = BTreeSet::new();
        let mut names = BTreeSet::new();
        for entry in &registry.connections {
            validation::metadata(&entry.metadata).map_err(|_| Error::StoreUnavailable)?;
            verification::validate_pending(entry)?;
            if !ids.insert(&entry.metadata.id)
                || !names.insert(entry.metadata.name.to_lowercase())
                || entry.secret.version != 1
                || entry.secret.nonce.len() != 12
                || !(17..=528).contains(&entry.secret.ciphertext.len())
            {
                return Err(Error::StoreUnavailable);
            }
        }
        Ok(registry)
    }
    fn persist(path: &Path, registry: &Registry) -> Result<(), Error> {
        let bytes = serde_json::to_vec(registry).map_err(|_| Error::StoreUnavailable)?;
        if bytes.len() > MAX_BYTES {
            return Err(Error::StoreUnavailable);
        }
        state_dir(path)
            .and_then(|dir| dir.atomic_write(FILE, &bytes))
            .map_err(|_| Error::StoreUnavailable)?;
        Ok(())
    }
    pub(super) async fn commit(
        &self,
        mut state: OwnedMutexGuard<Option<Registry>>,
        registry: Registry,
    ) -> Result<(), Error> {
        let path = self.path.clone();
        #[cfg(test)]
        let fail_after_persist = Arc::clone(&self.fail_after_persist);
        #[cfg(test)]
        let pause = self.io_pause.lock().unwrap().take();
        tokio::task::spawn_blocking(move || {
            // Keep serialization, invalidation and publication together even if
            // the async caller is cancelled while replacement is still running.
            // A failed directory sync can follow a successful file replacement.
            *state = None;
            #[cfg(test)]
            if let Some(pause) = pause {
                pause.wait();
            }
            Self::persist(&path, &registry)?;
            #[cfg(test)]
            if fail_after_persist.swap(false, std::sync::atomic::Ordering::SeqCst) {
                return Err(Error::StoreUnavailable);
            }
            *state = Some(registry);
            Ok(())
        })
        .await
        .map_err(|_| Error::StoreUnavailable)?
    }
}
