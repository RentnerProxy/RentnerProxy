mod setup;
pub(in crate::runtime) use setup::SetupSnapshot;
mod access;
mod desec;
mod hetzner;
mod models;
mod persistence;
mod references;
#[cfg(test)]
mod tests;
mod validation;
mod verification;

pub(crate) use super::dns::capabilities::{ProviderId, catalog as provider_catalog};
#[cfg(test)]
use super::state::state_dir;
use super::{
    clock::utc_now,
    dns::{EncryptedDnsConfig, encryption},
};
pub(crate) use models::Metadata;
pub(crate) use models::{
    Consumer, ConsumerType, DeleteRequest, Error, Reference, SaveRequest, TestRequest,
};
use models::{TestResult, ValidatedReference, Zone};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

const FILE: &str = "dns-connections.json";
const MAX_BYTES: usize = 1024 * 1024;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    metadata: Metadata,
    secret: EncryptedDnsConfig,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pending_checks: Vec<verification::PermissionCheck>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    version: u8,
    connections: Vec<Entry>,
}

pub(crate) struct Store {
    path: PathBuf,
    state: Arc<Mutex<Option<Registry>>>,
    pub(crate) bindings: Arc<Mutex<()>>,
    checks: Mutex<()>,
    key_io: Arc<Mutex<()>>,
    cleanup_cursor: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    key: Option<[u8; 32]>,
    #[cfg(test)]
    endpoint: Option<String>,
    #[cfg(test)]
    fail_after_persist: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    io_pause: std::sync::Mutex<Option<tests::IoPause>>,
}
impl Store {
    /// Creates a lazily loaded registry with separate registry, binding, check and key locks.
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            state: Arc::new(Mutex::new(None)),
            bindings: Arc::new(Mutex::new(())),
            checks: Mutex::new(()),
            key_io: Arc::new(Mutex::new(())),
            cleanup_cursor: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            key: None,
            #[cfg(test)]
            endpoint: None,
            #[cfg(test)]
            fail_after_persist: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            io_pause: std::sync::Mutex::new(None),
        }
    }
    /// Reads the current installation key on a blocking worker without caching it.
    ///
    /// The worker owns the key-I/O lock through completion even if its async waiter
    /// is cancelled. Key lookup and worker failures become `CredentialsUnavailable`.
    async fn key(&self) -> Result<[u8; 32], Error> {
        let guard = Arc::clone(&self.key_io).lock_owned().await;
        #[cfg(test)]
        let key = self.key;
        #[cfg(test)]
        let pause = self.io_pause.lock().unwrap().take();
        tokio::task::spawn_blocking(move || {
            // The key file may rotate: read on every use, retaining admission
            // until the read finishes even if the caller has been cancelled.
            let _guard = guard;
            #[cfg(test)]
            if let Some(pause) = pause {
                pause.wait();
            }
            #[cfg(test)]
            if let Some(key) = key {
                return Ok(key);
            }
            encryption::encryption_key_from_environment().map_err(|_| Error::CredentialsUnavailable)
        })
        .await
        .map_err(|_| Error::CredentialsUnavailable)?
    }
    #[cfg(test)]
    fn endpoint(&self) -> &str {
        self.endpoint
            .as_deref()
            .unwrap_or(super::dns::CLOUDFLARE_API_BASE)
    }
    fn provider_endpoint(&self, provider: ProviderId) -> Result<String, Error> {
        #[cfg(test)]
        if let Some(endpoint) = self.endpoint.as_deref() {
            return Ok(endpoint.to_owned());
        }
        if let Some(endpoint) = super::dns::connection_test_endpoint(provider)
            .map_err(|_| Error::ProviderUnavailable)?
        {
            return Ok(endpoint.to_string());
        }
        Ok(match provider {
            ProviderId::Cloudflare => super::dns::CLOUDFLARE_API_BASE,
            ProviderId::Desec => super::dns::DESEC_API_BASE,
            ProviderId::Hetzner => super::dns::HETZNER_API_BASE,
        }
        .to_owned())
    }
    /// Decrypts and validates a stored token using the current installation key.
    ///
    /// Key, authentication, UTF-8 and token-validation failures return
    /// `CredentialsUnavailable`, so an unreadable credential cannot be used.
    async fn decrypt(&self, entry: &Entry) -> Result<String, Error> {
        let aad = format!("dns-connection:{}:v1", entry.metadata.id);
        let bytes = encryption::decrypt_bytes(&entry.secret, &aad, &self.key().await?)
            .map_err(|_| Error::CredentialsUnavailable)?;
        let token = String::from_utf8(bytes).map_err(|_| Error::CredentialsUnavailable)?;
        validation::token(&token).map_err(|_| Error::CredentialsUnavailable)?;
        Ok(token)
    }
    pub(crate) async fn list(&self) -> Result<Vec<Metadata>, Error> {
        let mut state = self.lock().await?;
        Ok(Self::registry(&mut state)?
            .connections
            .iter()
            .map(|e| e.metadata.clone())
            .collect())
    }
    /// Checks provider access and selected zones without certifying DNS write permission.
    ///
    /// Uses a supplied token when present; otherwise decrypts the stored credential with
    /// the current key. Before returning a successful result for a stored connection,
    /// its revision is rechecked after the provider request so stale results conflict.
    pub(crate) async fn test(&self, request: TestRequest) -> Result<TestResult, Error> {
        if let Some(id) = &request.connection_id {
            validation::id(id)?;
        }
        if let Some(token) = &request.api_token {
            validation::token(token)?;
        }
        if request.connection_id.is_none() && request.api_token.is_none() {
            return Err(Error::TokenRequired);
        }
        let entry = {
            let mut state = self.lock().await?;
            let registry = Self::registry(&mut state)?;
            request
                .connection_id
                .as_ref()
                .map(|id| {
                    registry
                        .connections
                        .iter()
                        .find(|e| &e.metadata.id == id)
                        .cloned()
                        .ok_or(Error::ConnectionNotFound)
                })
                .transpose()?
        };
        let provider = resolve_provider(request.provider, entry.as_ref())?;
        let selected = request.zone_ids.unwrap_or_else(|| {
            entry
                .as_ref()
                .map(|e| e.metadata.zones.iter().map(|z| z.id.clone()).collect())
                .unwrap_or_default()
        });
        validation::selection(&selected, false)?;
        let token = match request.api_token {
            Some(token) => token,
            None => {
                self.decrypt(entry.as_ref().ok_or(Error::TokenRequired)?)
                    .await?
            }
        };
        if selected.iter().any(|id| !provider.valid_zone_id(id)) {
            return Err(Error::InvalidInput);
        }
        let zones =
            validation::permitted(provider, &self.provider_endpoint(provider)?, &token).await?;
        select(&zones, &selected)?;
        // A test against a stored credential must not silently describe a stale revision.
        if let Some(entry) = entry {
            let mut state = self.lock().await?;
            let registry = Self::registry(&mut state)?;
            check_revision(registry, &entry.metadata.id, entry.metadata.revision)?;
        }
        Ok(TestResult {
            zones,
            selected_zone_ids: selected,
            checked_at: utc_now(),
            dns_write_verified: false,
        })
    }
    /// Validates provider access and commits an encrypted, revision-checked connection.
    ///
    /// Rejects overlapping permission checks, removed zones still in use and registry
    /// limits. The final registry guard transfers into commit through disk replacement
    /// and cache publication; persistence errors may follow a successful replacement.
    pub(crate) async fn save(&self, id: String, request: SaveRequest) -> Result<Metadata, Error> {
        let _checks = self
            .checks
            .try_lock()
            .map_err(|_| Error::ConnectionConflict)?;
        validation::id(&id)?;
        validation::id(&request.actor_user_id)?;
        validation::name(&request.name)?;
        validation::selection(&request.zone_ids, true)?;
        if let Some(token) = &request.api_token {
            validation::token(token)?;
        }
        let old = {
            let mut state = self.lock().await?;
            let registry = Self::registry(&mut state)?;
            check_revision(registry, &id, request.expected_revision)?;
            check_name(registry, &id, &request.name)?;
            if request.expected_revision == 0 && registry.connections.len() >= 100 {
                return Err(Error::ConnectionLimit);
            }
            registry
                .connections
                .iter()
                .find(|e| e.metadata.id == id)
                .cloned()
        };
        let provider = resolve_provider(request.provider, old.as_ref())?;
        if request
            .zone_ids
            .iter()
            .any(|id| !provider.valid_zone_id(id))
        {
            return Err(Error::InvalidInput);
        }
        if old.as_ref().is_some_and(|e| {
            e.metadata
                .references
                .iter()
                .any(|r| !request.zone_ids.contains(&r.zone_id))
        }) {
            return Err(Error::ConnectionInUse);
        }
        if old.as_ref().is_some_and(|entry| {
            entry
                .pending_checks
                .iter()
                .any(|check| !request.zone_ids.contains(&check.zone_id))
        }) {
            return Err(Error::ConnectionInUse);
        }
        let token = match &request.api_token {
            Some(token) => token.clone(),
            None => {
                self.decrypt(old.as_ref().ok_or(Error::TokenRequired)?)
                    .await?
            }
        };
        let permitted =
            validation::permitted(provider, &self.provider_endpoint(provider)?, &token).await?;
        let zones = select(&permitted, &request.zone_ids)?;
        let revision = request
            .expected_revision
            .checked_add(1)
            .ok_or(Error::ConnectionConflict)?;
        let secret = encryption::encrypt_bytes(
            token.as_bytes(),
            &format!("dns-connection:{id}:v1"),
            &self.key().await?,
        )
        .map_err(|_| Error::CredentialsUnavailable)?;
        let mut state = self.lock().await?;
        let registry = Self::registry(&mut state)?;
        check_revision(registry, &id, request.expected_revision)?;
        check_name(registry, &id, &request.name)?;
        let current = registry.connections.iter().find(|e| e.metadata.id == id);
        if current.is_some_and(|e| {
            e.metadata
                .references
                .iter()
                .any(|r| !request.zone_ids.contains(&r.zone_id))
        }) {
            return Err(Error::ConnectionInUse);
        }
        if current.is_none() && registry.connections.len() >= 100 {
            return Err(Error::ConnectionLimit);
        }
        let mut now = utc_now();
        if let Some(entry) = &old {
            // Wall-clock correction must not move an established connection backwards.
            if validation::timestamp(&now)? < validation::timestamp(&entry.metadata.updated_at)? {
                now = entry.metadata.updated_at.clone();
            }
        }
        let verification = if request.api_token.is_none()
            && old
                .as_ref()
                .is_some_and(|entry| entry.metadata.zones == zones)
        {
            old.as_ref()
                .map(|entry| entry.metadata.verification.clone())
                .unwrap_or_default()
        } else {
            models::Verification::default()
        };
        let pending_checks = old
            .as_ref()
            .map(|entry| entry.pending_checks.clone())
            .unwrap_or_default();
        let metadata = Metadata {
            id: id.clone(),
            provider: provider.as_str().into(),
            ownership: "instance".into(),
            name: request.name,
            created_by_user_id: old
                .as_ref()
                .map(|e| e.metadata.created_by_user_id.clone())
                .unwrap_or(request.actor_user_id),
            revision,
            zones,
            references: current
                .map(|e| e.metadata.references.clone())
                .unwrap_or_default(),
            created_at: old
                .map(|e| e.metadata.created_at)
                .unwrap_or_else(|| now.clone()),
            updated_at: now,
            verification,
        };
        let mut next = registry.clone();
        next.connections.retain(|e| e.metadata.id != id);
        next.connections.push(Entry {
            metadata: metadata.clone(),
            secret,
            pending_checks,
        });
        self.commit(state, next).await?;
        Ok(metadata)
    }
    /// Deletes the expected revision only when no consumers or permission checks remain.
    ///
    /// Commit owns the registry guard through persistence and cache publication.
    /// A storage error may leave a replaced file, requiring a reload on the next access.
    pub(crate) async fn delete(&self, id: &str, expected_revision: u32) -> Result<(), Error> {
        validation::id(id)?;
        if expected_revision == 0 {
            return Err(Error::InvalidInput);
        }
        let mut state = self.lock().await?;
        let registry = Self::registry(&mut state)?;
        check_revision(registry, id, expected_revision)?;
        if registry
            .connections
            .iter()
            .find(|e| e.metadata.id == id)
            .is_some_and(|e| !e.metadata.references.is_empty() || !e.pending_checks.is_empty())
        {
            return Err(Error::ConnectionInUse);
        }
        let mut next = registry.clone();
        next.connections.retain(|e| e.metadata.id != id);
        self.commit(state, next).await?;
        Ok(())
    }
    /// Checks the selected connection and zone and verifies its token can be decrypted.
    ///
    /// The registry lock spans validation and key lookup. Missing or unreadable
    /// credentials fail closed; this check does not certify DNS write permission.
    pub(crate) async fn validate_reference(
        &self,
        reference: Reference,
    ) -> Result<ValidatedReference, Error> {
        validation::reference(&reference)?;
        let mut state = self.lock().await?;
        let registry = Self::registry(&mut state)?;
        let entry = registry
            .connections
            .iter()
            .find(|e| e.metadata.id == reference.connection_id)
            .ok_or(Error::ConnectionNotFound)?;
        let zone = entry
            .metadata
            .zones
            .iter()
            .find(|z| z.id == reference.zone_id)
            .cloned()
            .ok_or(Error::ZoneNotPermitted)?;
        // Checking decryption fails closed if the installation key was lost or rotated.
        self.decrypt(entry).await?;
        Ok(ValidatedReference {
            reference,
            revision: entry.metadata.revision,
            zone,
        })
    }
}
fn resolve_provider(
    requested: Option<ProviderId>,
    existing: Option<&Entry>,
) -> Result<ProviderId, Error> {
    let stored = existing
        .map(|entry| ProviderId::parse(&entry.metadata.provider).ok_or(Error::InvalidInput))
        .transpose()?;
    if requested
        .zip(stored)
        .is_some_and(|(requested, stored)| requested != stored)
    {
        return Err(Error::InvalidInput);
    }
    Ok(stored.or(requested).unwrap_or_default())
}

fn check_revision(registry: &Registry, id: &str, revision: u32) -> Result<(), Error> {
    match registry.connections.iter().find(|e| e.metadata.id == id) {
        Some(e) if e.metadata.revision == revision => Ok(()),
        None if revision == 0 => Ok(()),
        None => Err(Error::ConnectionNotFound),
        _ => Err(Error::ConnectionConflict),
    }
}
fn check_name(registry: &Registry, id: &str, name: &str) -> Result<(), Error> {
    if registry
        .connections
        .iter()
        .any(|e| e.metadata.id != id && e.metadata.name.to_lowercase() == name.to_lowercase())
    {
        Err(Error::ConnectionConflict)
    } else {
        Ok(())
    }
}
fn select(zones: &[Zone], ids: &[String]) -> Result<Vec<Zone>, Error> {
    ids.iter()
        .map(|id| {
            zones
                .iter()
                .find(|z| &z.id == id)
                .cloned()
                .ok_or(Error::ZoneNotPermitted)
        })
        .collect()
}
