use super::{no_store_json, no_store_status_json};
use crate::{
    runtime::dns_connections::{DeleteRequest, Error, Reference, SaveRequest, TestRequest},
    server::AppState,
};
use axum::{
    body::Bytes,
    extract::{Path, rejection::BytesRejection},
    http::StatusCode,
    response::Response,
};
use serde::de::DeserializeOwned;
use std::{future::Future, sync::Arc};

/// Waits cancellably for the binding lock, then detaches an owned transaction.
///
/// Once admitted, the task retains the guard through reconciliation and mutation
/// even if the HTTP waiter disconnects. A join failure returns a no-store
/// `StoreUnavailable` response.
async fn owned_binding_transaction(
    bindings: Arc<tokio::sync::Mutex<()>>,
    transaction: impl Future<Output = Response> + Send + 'static,
) -> Response {
    // Waiting requests remain cancellable; only an admitted transaction detaches.
    let binding = bindings.lock_owned().await;
    // Reconciliation may continue in a storage worker after its waiter leaves.
    // Keep the binding guard alive through the associated mutation as well.
    tokio::spawn(async move {
        let _binding = binding;
        transaction.await
    })
    .await
    .unwrap_or_else(|failure| {
        tracing::warn!(
            cancelled = failure.is_cancelled(),
            panicked = failure.is_panic(),
            "DNS connection binding transaction failed"
        );
        error(Error::StoreUnavailable)
    })
}

fn error(error: Error) -> Response {
    let status = match error {
        Error::ConnectionNotFound => StatusCode::NOT_FOUND,
        Error::ConnectionConflict | Error::ConnectionInUse => StatusCode::CONFLICT,
        Error::ProviderUnauthorized
        | Error::ProviderUnavailable
        | Error::ProviderRateLimited
        | Error::StoreUnavailable
        | Error::CredentialsUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::BAD_REQUEST,
    };
    no_store_status_json(status, serde_json::json!({"error":error}))
}
fn parse<T: DeserializeOwned>(body: Result<Bytes, BytesRejection>) -> Result<T, Error> {
    let body = body.map_err(|_| Error::InvalidInput)?;
    if body.len() > 32 * 1024 {
        return Err(Error::InvalidInput);
    }
    serde_json::from_slice(&body).map_err(|_| Error::InvalidInput)
}
pub(in crate::server) async fn providers() -> Response {
    no_store_json(
        serde_json::json!({"providers": crate::runtime::dns_connections::provider_catalog()}),
    )
}

/// Lists connection metadata after reconciliation in an owned binding transaction.
pub(in crate::server) async fn list(state: AppState) -> Response {
    owned_binding_transaction(
        Arc::clone(&state.runtime.dns_connections.bindings),
        async move { list_inner(state).await },
    )
    .await
}

/// Reconciles certificate references before listing under the caller-owned binding lock.
///
/// Reconciliation and registry errors become no-store HTTP error responses.
async fn list_inner(state: AppState) -> Response {
    if let Err(e) = state.runtime.reconcile_provider_connections().await {
        return error(e);
    }
    match state.runtime.dns_connections.list().await {
        Ok(connections) => no_store_json(serde_json::json!({"connections":connections})),
        Err(e) => error(e),
    }
}
pub(in crate::server) async fn test(
    state: AppState,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let request = match parse::<TestRequest>(body) {
        Ok(v) => v,
        Err(e) => return error(e),
    };
    match state.runtime.dns_connections.test(request).await {
        Ok(v) => no_store_json(v),
        Err(e) => error(e),
    }
}
/// Parses a save request and admits reconciliation and mutation under the binding lock.
///
/// Waiting for the lock remains cancellable; an admitted transaction outlives
/// the HTTP waiter and returns connection or validation errors as no-store responses.
pub(in crate::server) async fn save(
    Path(id): Path<String>,
    state: AppState,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let request = match parse::<SaveRequest>(body) {
        Ok(v) => v,
        Err(e) => return error(e),
    };
    owned_binding_transaction(
        Arc::clone(&state.runtime.dns_connections.bindings),
        async move { save_inner(id, state, request).await },
    )
    .await
}

/// Reconciles references, then saves while the transaction owns the binding lock.
///
/// Returns the saved metadata or a mapped no-store error response.
async fn save_inner(id: String, state: AppState, request: SaveRequest) -> Response {
    if let Err(e) = state.runtime.reconcile_provider_connections().await {
        return error(e);
    }
    match state.runtime.dns_connections.save(id, request).await {
        Ok(v) => no_store_json(v),
        Err(e) => error(e),
    }
}
/// Parses a revision-checked deletion and admits it under the binding lock.
///
/// The admitted transaction owns reconciliation and deletion after a disconnect;
/// requests still queued for the lock remain cancellable.
pub(in crate::server) async fn delete(
    Path(id): Path<String>,
    state: AppState,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let request = match parse::<DeleteRequest>(body) {
        Ok(v) => v,
        Err(e) => return error(e),
    };
    owned_binding_transaction(
        Arc::clone(&state.runtime.dns_connections.bindings),
        async move { delete_inner(id, state, request).await },
    )
    .await
}

/// Reconciles references before deleting under the transaction-owned binding lock.
///
/// Returns confirmation or a mapped no-store error response.
async fn delete_inner(id: String, state: AppState, request: DeleteRequest) -> Response {
    if let Err(e) = state.runtime.reconcile_provider_connections().await {
        return error(e);
    }
    match state
        .runtime
        .dns_connections
        .delete(&id, request.expected_revision)
        .await
    {
        Ok(()) => no_store_json(serde_json::json!({"deleted":true})),
        Err(e) => error(e),
    }
}
/// Runs a revision-checked DNS write probe, then reconciles successful results.
///
/// Only post-verification reconciliation uses the owned binding transaction;
/// waiting for its lock remains cancellable. All responses disable caching.
pub(in crate::server) async fn verify(
    Path(id): Path<String>,
    state: AppState,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let request = match parse::<DeleteRequest>(body) {
        Ok(value) => value,
        Err(e) => return error(e),
    };
    match state
        .runtime
        .dns_connections
        .verify(&id, request.expected_revision)
        .await
    {
        Ok(connection) => {
            owned_binding_transaction(
                Arc::clone(&state.runtime.dns_connections.bindings),
                async move {
                    if let Err(e) = state.runtime.reconcile_provider_connections().await {
                        return error(e);
                    }
                    no_store_json(connection)
                },
            )
            .await
        }
        Err(e) => error(e),
    }
}
pub(in crate::server) async fn validate(
    state: AppState,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let request = match parse::<Reference>(body) {
        Ok(v) => v,
        Err(e) => return error(e),
    };
    match state
        .runtime
        .dns_connections
        .validate_reference(request)
        .await
    {
        Ok(v) => no_store_json(v),
        Err(e) => error(e),
    }
}
