use crate::utils::{
    error::DoubledeckerError,
    helpers::{handle_file_upload, parse_batch_to_json},
    statics::{AppState, QueryRequest, QueryResponse},
};
use axum::extract::{Json, Multipart, State};
use std::path::Path;

pub async fn upload_csv(
    State(state): State<AppState>,
    multipart: Multipart,
) -> Result<String, DoubledeckerError> {
    eprintln!("Starting CSV upload...");
    let start = std::time::Instant::now();

    let file_path = handle_file_upload(multipart)
        .await
        .map_err(|e| DoubledeckerError::MultipartError(e.to_string()))?;

    eprintln!("File upload completed in {:?}", start.elapsed());
    eprintln!("File saved to: {}", file_path);

    let table_name = Path::new(&file_path)
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or(DoubledeckerError::InvalidFilePath)?;

    eprintln!("Parsing CSV into table '{}'...", table_name);
    let load_start = std::time::Instant::now();
    state.executor.load_csv(&file_path, table_name).await?;
    eprintln!("CSV parsing completed in {:?}", load_start.elapsed());

    *state.current_table.write().await = Some(table_name.to_string());

    eprintln!("Total upload time: {:?}", start.elapsed());

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

pub async fn describe_table(State(state): State<AppState>) -> Result<Json<QueryResponse>, DoubledeckerError> {
    let table_name_guard = state.current_table.read().await;
    let table_name = table_name_guard.as_ref().ok_or(DoubledeckerError::Internal("No table loaded".to_string()))?;
    let description = state.executor.describe_table(table_name).await?;
    Ok(Json(description))
} 
