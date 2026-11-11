use crate::runtime::{
    ProxyRuntime,
    certificates::{CertificateEnvironment, CertificateError, CertificateIssueRequest},
    dns::{DnsDelegationBinding, DnsProvider, DnsRecordIntent},
    dns_delegation,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::time::{Instant, sleep, timeout_at};

/// Checks the active delegated operation's persisted cancellation request.
///
/// Success permits the caller to proceed at this checkpoint; it does not reserve
/// against a later cancellation or reverse a provider mutation already accepted.
///
/// # Errors
/// Returns `AcmeCancelled` for a requested cancellation, `OperationInProgress` when
/// there is no current operation, or the certificate-store lookup error.
pub(super) async fn check_cancelled(
    runtime: &Arc<ProxyRuntime>,
    id: &str,
) -> Result<(), CertificateError> {
    let store = &runtime.certificate_store;
    let metadata = store.get(id).await?;
    let operation = metadata
        .current_operation
        .ok_or(CertificateError::OperationInProgress)?;
    if store.delegated_cancel_requested(id, &operation.id).await? {
        return Err(CertificateError::AcmeCancelled);
    }
    Ok(())
}

/// Persists cleanup authority before presenting one delegated DNS proof.
///
/// Mutates `intents` to retain ambiguous writes and clears their unconfirmed flag only
/// for a definitive provider outcome. Provider I/O runs between bounded store updates.
/// Cancellation is checked before and after presentation; an error after a write does
/// not imply the write was rolled back.
///
/// # Errors
/// Rejects replay of an identical ambiguous write with `DnsCleanupFailed` and
/// propagates cancellation, persistence, validation, and provider errors.
pub(super) async fn present(
    runtime: &Arc<ProxyRuntime>,
    id: &str,
    provider: &DnsProvider,
    intents: &mut Vec<DnsRecordIntent>,
    mut intent: DnsRecordIntent,
) -> Result<(), CertificateError> {
    check_cancelled(runtime, id).await?;
    if intents.iter().any(|pending| {
        pending.unconfirmed_write
            && pending.value == intent.value
            && pending.marker == intent.marker
            && pending
                .delegation
                .as_ref()
                .zip(intent.delegation.as_ref())
                .is_some_and(|(previous, current)| {
                    previous.zone_name == current.zone_name
                        && previous.target_owner == current.target_owner
                })
    }) {
        // Replaying the same ambiguous mutation could make a later read prove only
        // one of two writes, allowing the other to appear after authority is retired.
        return Err(CertificateError::DnsCleanupFailed);
    }
    intent.unconfirmed_write = true;
    intents.push(intent);
    acme_store!(runtime; [id, intents]; store => store.set_pending_dns_records(&id, intents))?;
    let outcome = provider
        .present_tracked(intents.last().ok_or(CertificateError::DnsProviderInvalid)?)
        .await;
    if !outcome.as_ref().is_err_and(|error| error.unconfirmed) {
        if let Some(intent) = intents.last_mut() {
            intent.unconfirmed_write = false;
        }
        acme_store!(runtime; [id, intents]; store => store.set_pending_dns_records(&id, intents))?;
    }
    outcome.map_err(|error| error.error)?;
    check_cancelled(runtime, id).await
}

pub(super) async fn bindings(
    request: &CertificateIssueRequest,
    provider: &DnsProvider,
) -> Result<BTreeMap<String, DnsDelegationBinding>, CertificateError> {
    let zone = provider.zone_name().await?;
    let mut bindings = BTreeMap::new();
    for domain in &request.domains {
        let name = domain.strip_prefix("*.").unwrap_or(domain);
        if bindings.contains_key(name) {
            continue;
        }
        let owner = format!("_acme-challenge.{name}");
        let binding = dns_delegation::resolve(&owner, &zone, request.environment).await?;
        bindings.insert(name.to_owned(), binding);
    }
    Ok(bindings)
}

pub(super) async fn recheck(
    binding: &DnsDelegationBinding,
    environment: CertificateEnvironment,
) -> Result<(), CertificateError> {
    let current =
        dns_delegation::resolve(&binding.original_owner, &binding.zone_name, environment).await?;
    if current != *binding {
        return Err(CertificateError::DnsDelegationInvalid);
    }
    Ok(())
}

/// Polls delegated proof visibility for up to sixty seconds.
///
/// Checks persisted cancellation between attempts and retries lookup/propagation
/// failures every two seconds. Success means the supplied records were observed.
///
/// # Errors
/// Returns cancellation or permanent validation errors immediately; at the deadline,
/// returns the latest lookup/propagation error rather than discarding its meaning.
pub(super) async fn wait_for_records(
    intents: &[DnsRecordIntent],
    environment: CertificateEnvironment,
    runtime: &Arc<ProxyRuntime>,
    id: &str,
) -> Result<(), CertificateError> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = CertificateError::DnsDelegationNotPropagated;
    loop {
        check_cancelled(runtime, id).await?;
        match timeout_at(
            deadline,
            dns_delegation::check_records(intents, environment),
        )
        .await
        {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(
                error @ (CertificateError::DnsDelegationNotPropagated
                | CertificateError::DnsDelegationLookupFailed),
            )) => last = error,
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(last),
        }
        if Instant::now() + Duration::from_secs(2) >= deadline {
            return Err(last);
        }
        sleep(Duration::from_secs(2)).await;
    }
}
