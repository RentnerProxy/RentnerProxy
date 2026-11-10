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
use std::future::Future;

async fn owned_binding_transaction(
    transaction: impl Future<Output = Response> + Send + 'static,
) -> Response {
    // Reconciliation may continue in a storage worker after its waiter leaves.
    // Keep the binding guard alive through the associated mutation as well.
    tokio::spawn(transaction).await.unwrap_or_else(|failure| {
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

pub(in crate::server) async fn list(state: AppState) -> Response {
    owned_binding_transaction(async move { list_inner(state).await }).await
}

async fn list_inner(state: AppState) -> Response {
    let _binding = state.runtime.dns_connections.bindings.lock().await;
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
pub(in crate::server) async fn save(
    Path(id): Path<String>,
    state: AppState,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let request = match parse::<SaveRequest>(body) {
        Ok(v) => v,
        Err(e) => return error(e),
    };
    owned_binding_transaction(async move { save_inner(id, state, request).await }).await
}

async fn save_inner(id: String, state: AppState, request: SaveRequest) -> Response {
    let _binding = state.runtime.dns_connections.bindings.lock().await;
    if let Err(e) = state.runtime.reconcile_provider_connections().await {
        return error(e);
    }
    match state.runtime.dns_connections.save(id, request).await {
        Ok(v) => no_store_json(v),
        Err(e) => error(e),
    }
}
pub(in crate::server) async fn delete(
    Path(id): Path<String>,
    state: AppState,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let request = match parse::<DeleteRequest>(body) {
        Ok(v) => v,
        Err(e) => return error(e),
    };
    owned_binding_transaction(async move { delete_inner(id, state, request).await }).await
}

async fn delete_inner(id: String, state: AppState, request: DeleteRequest) -> Response {
    let _binding = state.runtime.dns_connections.bindings.lock().await;
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
            owned_binding_transaction(async move {
                let _binding = state.runtime.dns_connections.bindings.lock().await;
                if let Err(e) = state.runtime.reconcile_provider_connections().await {
                    return error(e);
                }
                no_store_json(connection)
            })
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
