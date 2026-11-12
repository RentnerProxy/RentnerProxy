use super::{no_store_json, no_store_status_json};
use crate::{
    runtime::docker_discovery::models::{Error, Inspect, Save, Watch},
    server::AppState,
};
use axum::extract::{Query, rejection::QueryRejection};
use axum::{body::Bytes, extract::rejection::BytesRejection, http::StatusCode, response::Response};

pub(in crate::server) async fn watch(
    state: AppState,
    query: Result<Query<Watch>, QueryRejection>,
) -> Response {
    let Ok(Query(request)) = query else {
        return error(Error::InvalidInput);
    };
    match state.runtime.docker_discovery.watch(request).await {
        Ok(value) => no_store_json(value),
        Err(e) => error(e),
    }
}

fn error(error: Error) -> Response {
    let status = match error {
        Error::InvalidInput => StatusCode::UNPROCESSABLE_ENTITY,
        Error::Conflict => StatusCode::CONFLICT,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    no_store_status_json(status, serde_json::json!({"error": error}))
}
pub(in crate::server) async fn status(state: AppState) -> Response {
    match state.runtime.docker_discovery.status().await {
        Ok(value) => no_store_json(value),
        Err(e) => error(e),
    }
}
pub(in crate::server) async fn inspect(
    state: AppState,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let Some(request) = body
        .ok()
        .and_then(|body| serde_json::from_slice::<Inspect>(&body).ok())
    else {
        return error(Error::InvalidInput);
    };
    match state.runtime.docker_discovery.inspect(request).await {
        Ok(candidates) => no_store_json(serde_json::json!({"candidates": candidates})),
        Err(e) => error(e),
    }
}
pub(in crate::server) async fn save(
    state: AppState,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let Some(request) = body
        .ok()
        .and_then(|body| serde_json::from_slice::<Save>(&body).ok()) else { return error(Error::InvalidInput) };
    match state.runtime.docker_discovery.save(request).await {
        Ok(value) => no_store_json(value),
        Err(e) => error(e),
    }
}
