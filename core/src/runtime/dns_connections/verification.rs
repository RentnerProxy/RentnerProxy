use super::{
    Entry, Error, Metadata, Store, check_revision,
    models::{ConnectionStatus, Verification},
    validation,
};
use crate::runtime::{
    certificates::CertificateError,
    clock::utc_now,
    dns::{DnsDelegationBinding, DnsProvider, DnsProviderConfig, DnsRecordIntent},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[cfg(test)]
mod tests;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct PermissionCheck {
    pub(super) zone_id: String,
    intent: DnsRecordIntent,
}

pub(super) fn validate_pending(entry: &Entry) -> Result<(), Error> {
    if entry.pending_checks.len() > 50 {
        return Err(Error::StoreUnavailable);
    }
    for check in &entry.pending_checks {
        let zone = entry
            .metadata
            .zones
            .iter()
            .find(|zone| zone.id == check.zone_id)
            .ok_or(Error::StoreUnavailable)?;
        let mut expected =
            DnsRecordIntent::new(&entry.metadata.id, &check.intent.name, &check.intent.value)
                .map_err(|_| Error::StoreUnavailable)?;
        let explicit_probe = if check.intent.delegation.is_some() {
            expected = permission_probe(&entry.metadata.id, &zone.name, &check.intent.value)
                .map_err(|_| Error::StoreUnavailable)?;
            true
        } else {
            false
        };
        let in_zone = explicit_probe
            || check.intent.name.ends_with(&format!(".{}", zone.name))
            || (matches!(entry.metadata.provider.as_str(), "desec" | "hetzner")
                && check.intent.name == zone.name);
        if expected != check.intent || !in_zone {
            return Err(Error::StoreUnavailable);
        }
    }
    Ok(())
}

fn permission_probe(
    id: &str,
    zone: &str,
    value: &str,
) -> Result<DnsRecordIntent, CertificateError> {
    if crate::proxy::is_canonical_domain(zone) {
        return Err(CertificateError::DnsProviderInvalid);
    }
    let intent = DnsRecordIntent::new(id, "rp-verify.invalid", value)?;
    let original_owner = "_acme-challenge.rp-verify.invalid".to_owned();
    let cname_chain = if original_owner == zone {
        vec![original_owner.clone()]
    } else {
        vec![original_owner.clone(), zone.to_owned()]
    };
    intent.with_delegation(DnsDelegationBinding {
        original_owner,
        target_owner: zone.to_owned(),
        zone_name: zone.to_owned(),
        cname_chain,
    })
}

fn provider_error(error: CertificateError) -> Error {
    match error {
        CertificateError::DnsProviderUnauthorized => Error::ProviderUnauthorized,
        CertificateError::DnsProviderInvalid => Error::ZoneNotPermitted,
        _ => Error::ProviderUnavailable,
    }
}

impl Store {
    /// Builds a permission-check provider using the stored entry and current key.
    ///
    /// Credential failures propagate; provider construction errors are mapped to
    /// connection verification errors without performing a DNS operation.
    async fn check_provider(&self, entry: &Entry, zone: &str) -> Result<DnsProvider, Error> {
        let config = match super::resolve_provider(None, Some(entry))? {
            super::ProviderId::Hetzner => DnsProviderConfig::Hetzner {
                zone_id: zone.to_owned(),
                api_token: self.decrypt(entry).await?,
            },
            super::ProviderId::Desec => DnsProviderConfig::Desec {
                zone_id: zone.to_owned(),
                api_token: self.decrypt(entry).await?,
            },
            super::ProviderId::Cloudflare => DnsProviderConfig::Cloudflare {
                zone_id: zone.to_owned(),
                api_token: self.decrypt(entry).await?,
            },
        };
        let endpoint = self.provider_endpoint(config.provider_id())?;
        DnsProvider::for_connection_check(config, &endpoint).map_err(provider_error)
    }

    /// Commits a revision-checked verification outcome and its checked timestamp.
    ///
    /// Success records all selected zones as verified; failures clear that list and
    /// distinguish denied access from other failures. Persistence errors propagate.
    async fn finish_check(
        &self,
        id: &str,
        revision: u32,
        error: Option<Error>,
    ) -> Result<Metadata, Error> {
        let mut state = self.lock().await?;
        let registry = Self::registry(&mut state)?;
        check_revision(registry, id, revision)?;
        let mut next = registry.clone();
        let entry = next
            .connections
            .iter_mut()
            .find(|entry| entry.metadata.id == id)
            .ok_or(Error::ConnectionNotFound)?;
        entry.metadata.verification = Verification {
            status: match error {
                None => ConnectionStatus::Connected,
                Some(Error::ProviderUnauthorized | Error::ZoneNotPermitted) => {
                    ConnectionStatus::Disconnected
                }
                Some(_) => ConnectionStatus::Failed,
            },
            checked_at: Some(utc_now()),
            zone_ids: if error.is_none() {
                entry
                    .metadata
                    .zones
                    .iter()
                    .map(|zone| zone.id.clone())
                    .collect()
            } else {
                Vec::new()
            },
            error,
        };
        let metadata = entry.metadata.clone();
        self.commit(state, next).await?;
        Ok(metadata)
    }

    /// Commits removal of a completed zone cleanup intent at the expected revision.
    ///
    /// A revision conflict preserves the current registry; persistence failures leave
    /// the cache invalidated for authoritative reload.
    async fn remove_check(&self, id: &str, revision: u32, zone_id: &str) -> Result<(), Error> {
        let mut state = self.lock().await?;
        let registry = Self::registry(&mut state)?;
        check_revision(registry, id, revision)?;
        let mut next = registry.clone();
        let entry = next
            .connections
            .iter_mut()
            .find(|entry| entry.metadata.id == id)
            .ok_or(Error::ConnectionNotFound)?;
        entry
            .pending_checks
            .retain(|check| check.zone_id != zone_id);
        self.commit(state, next).await
    }

    /// Cleans each persisted DNS intent before removing its registry ownership record.
    ///
    /// Stops on provider, revision or storage failure so remaining intents can be
    /// retried; successful earlier cleanups may already have been committed.
    async fn cleanup_checks(&self, entry: &Entry) -> Result<(), Error> {
        for check in &entry.pending_checks {
            self.check_provider(entry, &check.zone_id)
                .await?
                .cleanup_intents(std::slice::from_ref(&check.intent))
                .await
                .map_err(provider_error)?;
            self.remove_check(&entry.metadata.id, entry.metadata.revision, &check.zone_id)
                .await?;
        }
        Ok(())
    }

    // An explicit permission check creates and removes only an operation-owned
    // TXT record. Persist the intent before POST so cancellation/restart cannot
    // lose ownership or falsely report a successful permission check.
    pub(crate) async fn verify(&self, id: &str, revision: u32) -> Result<Metadata, Error> {
        validation::id(id)?;
        let _checks = self
            .checks
            .try_lock()
            .map_err(|_| Error::ConnectionConflict)?;
        let entry = {
            let mut state = self.lock().await?;
            let registry = Self::registry(&mut state)?;
            check_revision(registry, id, revision)?;
            registry
                .connections
                .iter()
                .find(|entry| entry.metadata.id == id)
                .cloned()
                .ok_or(Error::ConnectionNotFound)?
        };
        let result = tokio::time::timeout(Duration::from_secs(60), self.verify_inner(entry))
            .await
            .unwrap_or(Err(Error::ProviderUnavailable));
        match result {
            Err(
                error @ (Error::StoreUnavailable
                | Error::ConnectionConflict
                | Error::ConnectionNotFound),
            ) => Err(error),
            result => self.finish_check(id, revision, result.err()).await,
        }
    }

    /// Checks permitted zones and probes write access with operation-owned TXT records.
    ///
    /// Old intents are cleaned first; new intents and cleared verification are persisted
    /// before provider writes. Cleanup precedes each intent's removal; an intent remains
    /// recoverable while cleanup or removal is incomplete. A presentation error may be
    /// returned after successful cleanup has already removed its intent.
    async fn verify_inner(&self, mut entry: Entry) -> Result<(), Error> {
        self.cleanup_checks(&entry).await?;
        let provider = super::resolve_provider(None, Some(&entry))?;
        let permitted = validation::permitted(
            provider,
            &self.provider_endpoint(provider)?,
            &self.decrypt(&entry).await?,
        )
        .await?;
        super::select(
            &permitted,
            &entry
                .metadata
                .zones
                .iter()
                .map(|zone| zone.id.clone())
                .collect::<Vec<_>>(),
        )?;
        let mut random = [0_u8; 16];
        aws_lc_rs::rand::fill(&mut random).map_err(|_| Error::StoreUnavailable)?;
        let nonce: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
        entry.pending_checks = entry
            .metadata
            .zones
            .iter()
            .map(|zone| {
                if !crate::proxy::is_canonical_domain(&zone.name) {
                    return Ok(PermissionCheck {
                        zone_id: zone.id.clone(),
                        intent: permission_probe(&entry.metadata.id, &zone.name, &nonce)
                            .map_err(provider_error)?,
                    });
                }
                Ok(PermissionCheck {
                    zone_id: zone.id.clone(),
                    intent: DnsRecordIntent::new(
                        &entry.metadata.id,
                        &if matches!(
                            provider,
                            super::ProviderId::Desec | super::ProviderId::Hetzner
                        ) {
                            zone.name.clone()
                        } else {
                            format!("rp-verify-{}.{}", &nonce[..16], zone.name)
                        },
                        &nonce,
                    )
                    .map_err(provider_error)?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        {
            let mut state = self.lock().await?;
            let registry = Self::registry(&mut state)?;
            check_revision(registry, &entry.metadata.id, entry.metadata.revision)?;
            let mut next = registry.clone();
            let current = next
                .connections
                .iter_mut()
                .find(|current| current.metadata.id == entry.metadata.id)
                .ok_or(Error::ConnectionNotFound)?;
            current.pending_checks = entry.pending_checks.clone();
            current.metadata.verification = Verification::default();
            self.commit(state, next).await?;
        }
        let mut first_error = None;
        for check in &entry.pending_checks {
            let provider = self.check_provider(&entry, &check.zone_id).await?;
            let present = provider
                .present(&check.intent)
                .await
                .map_err(provider_error);
            provider
                .cleanup_intents(std::slice::from_ref(&check.intent))
                .await
                .map_err(provider_error)?;
            self.remove_check(&entry.metadata.id, entry.metadata.revision, &check.zone_id)
                .await?;
            if let Err(error) = present {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(in crate::runtime) async fn recover_permission_checks(&self) {
        let Ok(_checks) = self.checks.try_lock() else {
            return;
        };
        let mut entries = {
            let Ok(mut state) = self.lock().await else {
                return;
            };
            let Ok(registry) = Self::registry(&mut state) else {
                return;
            };
            registry
                .connections
                .iter()
                .filter(|entry| !entry.pending_checks.is_empty())
                .cloned()
                .collect::<Vec<_>>()
        };
        if entries.is_empty() {
            return;
        }
        let offset = self
            .cleanup_cursor
            .fetch_add(4, std::sync::atomic::Ordering::Relaxed)
            % entries.len();
        entries.rotate_left(offset);
        for entry in entries.into_iter().take(4) {
            let _ =
                tokio::time::timeout(Duration::from_secs(30), self.cleanup_checks(&entry)).await;
        }
    }
}
