use crate::utils::helpers::query_response_to_csv;
use crate::utils::{
    error::DoubledeckerError,
    helpers::{handle_file_upload, parse_batch_to_json},
    statics::{AppState, QueryRequest, QueryResponse},
};
use axum::body::Body;
use axum::extract::{Json, Multipart, State};
use axum::http::header;
use axum::response::Response;
use std::path::Path;

pub async fn upload_csv(
    State(state): State<AppState>,
    multipart: Multipart,
) -> Result<String, DoubledeckerError> {
    let file_path = handle_file_upload(multipart)
        .await
        .map_err(|e| DoubledeckerError::MultipartError(e.to_string()))?;

    let table_name = Path::new(&file_path)
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or(DoubledeckerError::InvalidFilePath)?;

    state.executor.load_csv(&file_path, table_name).await?;
    *state.current_table.write().await = Some(table_name.to_string());

    Ok(String::from(table_name))
}

pub async fn execute_query(
    State(state): State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, DoubledeckerError> {
    let table_name_guard = state.current_table.read().await;
    let table_name = table_name_guard
        .as_ref()
        .ok_or(DoubledeckerError::Internal("No table loaded".to_string()))?;
    let batches = state
        .executor
        .execute_operations(table_name, payload.operations)
        .await
        .map_err(|e| DoubledeckerError::QueryExecution(e.to_string()))?;
    let response = parse_batch_to_json(batches).await?;
    Ok(Json(response))
}

pub async fn describe_table(
    State(state): State<AppState>,
) -> Result<Json<QueryResponse>, DoubledeckerError> {
    let table_name_guard = state.current_table.read().await;
    let table_name = table_name_guard
        .as_ref()
        .ok_or(DoubledeckerError::Internal("No table loaded".to_string()))?;

    let description = state.executor.describe_table(table_name).await?;
    Ok(Json(description))
}

pub async fn execute_query_csv(
    State(state): State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> Result<axum::response::Response, DoubledeckerError> {
    let table_name_guard = state.current_table.read().await;
    let table_name = table_name_guard
        .as_ref()
        .ok_or(DoubledeckerError::Internal("No table loaded".to_string()))?;

    let batches = state
        .executor
        .execute_operations(table_name, payload.operations)
        .await
        .map_err(|e| DoubledeckerError::QueryExecution(e.to_string()))?;

    let response = parse_batch_to_json(batches).await?;

    let csv_data = query_response_to_csv(&response);

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "text/csv; charset=utf-8")
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"query_results.csv\"",
        )
        .body(Body::from(csv_data))
        .unwrap())
}
