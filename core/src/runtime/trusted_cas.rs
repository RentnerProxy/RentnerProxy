use std::{collections::BTreeSet, io::ErrorKind, path::PathBuf, sync::Arc};

use crate::{
    models::TrustedCa,
    proxy::{MAX_TRUSTED_CA_PEM_BYTES, is_canonical_uuid_v7},
};

use super::state::{SafeDir, state_dir, validate_component};
use super::tls_gc_cursor::{Entry, MaterialCursor, MAX_ENTRIES_PER_PASS};

const TRUSTED_CAS_DIRECTORY: &str = "trusted-cas";
const MAX_REMOVALS_PER_PASS: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TrustedCaMaterial {
    pub(crate) pem_path: PathBuf,
}

pub(crate) struct TrustedCaStore {
    state_dir: PathBuf,
    gc_cursor: std::sync::Mutex<MaterialCursor>,
}

impl TrustedCaStore {
    pub(crate) fn new(state_dir: PathBuf) -> Self {
        Self { state_dir, gc_cursor: std::sync::Mutex::new(MaterialCursor::default()) }
    }

    pub(crate) fn initialize(&self) -> Result<(), ()> {
        self.state_dir()?
            .ensure_dir(TRUSTED_CAS_DIRECTORY)
            .map(|_| ())
            .map_err(|_| ())
    }

    pub(crate) fn materialize(&self, trusted_ca: &TrustedCa) -> Result<TrustedCaMaterial, ()> {
        let validated = crate::proxy::validate_trusted_ca(trusted_ca).map_err(|_| ())?;
        if validated.pem != trusted_ca.pem {
            return Err(());
        }
        let (directory, filename) = self.material_directory(trusted_ca)?;
        match directory.read_file(&filename, MAX_TRUSTED_CA_PEM_BYTES) {
            Ok(existing) => (existing == validated.pem.as_bytes())
                .then_some(())
                .ok_or(())?,
            Err(error) if error.kind() == ErrorKind::NotFound => directory
                .atomic_write(&filename, validated.pem.as_bytes())
                .map_err(|_| ())?,
            Err(_) => return Err(()),
        }
        Ok(TrustedCaMaterial {
            pem_path: directory.child_path(&filename).map_err(|_| ())?,
        })
    }

    pub(crate) fn material_for(&self, trusted_ca: &TrustedCa) -> Result<TrustedCaMaterial, ()> {
        let filename = material_filename(trusted_ca)?;
        let pem_path = self
            .state_dir
            .join(TRUSTED_CAS_DIRECTORY)
            .join(&trusted_ca.id)
            .join(&filename);
        Ok(TrustedCaMaterial { pem_path })
    }

    fn collect_garbage(&self, protected: &[TrustedCa]) -> Result<(), ()> {
        let protected = protected.iter().map(|ca| {
            material_filename(ca).map(|filename| (ca.id.clone(), filename))
        }).collect::<Result<BTreeSet<_>, _>>()?;
        let root = self.state_dir()?.open_dir(TRUSTED_CAS_DIRECTORY).map_err(|_| ())?;
        let mut remaining = MAX_REMOVALS_PER_PASS;
        let mut cursor = self.gc_cursor.lock().map_err(|_| ())?;
        for _ in 0..MAX_ENTRIES_PER_PASS {
            let Some(entry) = cursor.next(&root).map_err(|_| ())? else { break };
            let (id, filename) = match entry {
                Entry::Directory(id) => {
                    if !is_canonical_uuid_v7(&id) { continue; }
                    root.owned_entry_metadata(&id).map_err(|_| ())?;
                    let directory = root.open_dir(&id).map_err(|_| ())?;
                    cursor.visit(id, &directory).map_err(|_| ())?;
                    continue;
                }
                Entry::FinishedDirectory(id) => {
                    match root.owned_entry_metadata(&id) {
                        Ok(_) => {}
                        Err(error) if error.kind() == ErrorKind::NotFound => continue,
                        Err(_) => return Err(()),
                    }
                    let directory = match root.open_dir(&id) {
                        Ok(directory) => directory,
                        Err(error) if error.kind() == ErrorKind::NotFound => continue,
                        Err(_) => return Err(()),
                    };
                    if std::fs::read_dir(directory.path()).map_err(|_| ())?.next().transpose().map_err(|_| ())?.is_none() {
                        root.remove_dir_tree(&id).map_err(|_| ())?;
                    }
                    continue;
                }
                Entry::Material(id, filename) => (id, filename),
            };
            root.owned_entry_metadata(&id).map_err(|_| ())?;
            let directory = match root.open_dir(&id) {
                Ok(directory) => directory,
                Err(error) if error.kind() == ErrorKind::NotFound => { cursor.skip_directory(); continue; }
                Err(_) => return Err(()),
            };
                let temporary = temporary_material_filename(&filename);
                let Some(hash) = temporary.or_else(|| filename.strip_suffix(".pem")) else { continue };
                if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
                    || protected.contains(&(id.clone(), filename.clone())) { continue; }
                directory.owned_entry_metadata(&filename).map_err(|_| ())?;
                if temporary.is_none() {
                    let bytes = directory.read_file(&filename, MAX_TRUSTED_CA_PEM_BYTES).map_err(|_| ())?;
                    let pem = std::str::from_utf8(&bytes).map_err(|_| ())?;
                    let fingerprint = crate::proxy::trusted_ca_material_fingerprint(pem).map_err(|_| ())?;
                    if fingerprint != format!("sha256:{hash}") { return Err(()); }
                }
                directory.remove_file(&filename).map_err(|_| ())?;
                remaining -= 1;
                if remaining == 0 { return Ok(()); }
        }
        Ok(())
    }

    fn state_dir(&self) -> Result<SafeDir, ()> {
        state_dir(&self.state_dir).map_err(|_| ())
    }

    fn material_directory(&self, trusted_ca: &TrustedCa) -> Result<(SafeDir, String), ()> {
        let filename = material_filename(trusted_ca)?;
        let trusted_cas = self
            .state_dir()?
            .open_dir(TRUSTED_CAS_DIRECTORY)
            .map_err(|_| ())?;
        let directory = trusted_cas.ensure_dir(&trusted_ca.id).map_err(|_| ())?;
        Ok((directory, filename))
    }
}

fn temporary_material_filename(filename: &str) -> Option<&str> {
    let (hash, suffix) = filename.strip_prefix('.')?.split_once(".pem.")?;
    let (pid, nanos) = suffix.strip_suffix(".tmp")?.split_once('.')?;
    (pid.bytes().all(|byte| byte.is_ascii_digit()) && pid.parse::<u32>().is_ok()
        && nanos.bytes().all(|byte| byte.is_ascii_digit()) && nanos.parse::<u128>().is_ok()).then_some(hash)
}

impl super::ProxyRuntime {
    pub(super) async fn collect_tls_garbage_locked(self: &Arc<Self>) {
        let state = self.state.lock().await;
        if !state.initialized || !state.engine_available { return; }
        drop(state);
        let mut protected = self.active_configuration.lock().await.as_ref()
            .map(|configuration| configuration.trusted_cas.clone()).unwrap_or_default();
        let result = self.storage_async(move |runtime| async move {
            if runtime.configuration_activation_is_fenced() { return Ok(()); }
            let directory = state_dir(&runtime.settings.state_dir).map_err(|_| ())?;
            match directory.read_file(super::ACTIVE_CONFIGURATION_FILE, super::renderer::MAX_RENDERED_PROXY_CONFIG_BYTES) {
                Ok(bytes) => {
                    let request = serde_json::from_slice::<crate::models::ProxyConfigRequest>(&bytes).map_err(|_| ())?;
                    protected.extend(request.trusted_cas);
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(_) => return Err(()),
            }
            let ca_result = runtime.trusted_ca_store.collect_garbage(&protected);
            let certificate_result = runtime.certificate_store.collect_garbage().await.map_err(|_| ());
            ca_result.and(certificate_result)
        }).await;
        if !matches!(result, Ok(Ok(()))) {
            tracing::warn!(stage = "tls_gc", "TLS material cleanup was incomplete; a later recovery pass will retry");
        }
    }
}

fn material_filename(trusted_ca: &TrustedCa) -> Result<String, ()> {
    if !is_canonical_uuid_v7(&trusted_ca.id) {
        return Err(());
    }
    validate_component(&trusted_ca.id).map_err(|_| ())?;
    let fingerprint = trusted_ca
        .fingerprint_sha256
        .strip_prefix("sha256:")
        .filter(|fingerprint| {
            fingerprint.len() == 64
                && fingerprint
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        })
        .ok_or(())?;
    let filename = format!("{fingerprint}.pem");
    validate_component(&filename).map_err(|_| ())?;
    Ok(filename)
}
