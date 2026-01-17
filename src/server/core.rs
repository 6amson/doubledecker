use crate::db::operations::{create_upload, get_upload_by_table_name};
use crate::db::operations::{increment_file_count, increment_query_count};
use crate::server::middleware::AuthenticatedUser;
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
// use axum_macros::debug_handler;

use axum_macros::debug_handler;

#[debug_handler]
pub async fn upload_csv(
    auth_user: AuthenticatedUser,
    State(state): State<AppState>,
    multipart: Multipart,
) -> Result<Json<serde_json::Value>, DoubledeckerError> {
    eprintln!("Received upload request from user: {}", auth_user.user_id);

    let (file_path, file_name, file_size) = handle_file_upload(multipart).await.map_err(|e| {
        eprintln!("❌ File upload failed: {}", e);
        DoubledeckerError::MultipartError(e.to_string())
    })?;

    eprintln!("File uploaded to S3: {}", file_path);

    let table_name = Path::new(&file_path)
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or(DoubledeckerError::InvalidFilePath)?
        .to_string();

    eprintln!("✓ Table name extracted: {}", table_name);

    let s3_key = file_path
        .strip_prefix("s3://")
        .unwrap_or(&file_path)
        .to_string();

    eprintln!("✓ S3 key: {}", s3_key);

    // Create database record
    create_upload(
        &state.db_pool,
        auth_user.user_id,
        file_name.clone(),
        s3_key.clone(),
        file_size
            .try_into()
            .map_err(|_| DoubledeckerError::FileUpload("File size too large".to_string()))?,
        "csv".to_string(),
        table_name.clone(),
    )
    .await
    .map_err(|e| {
        eprintln!("Database insert failed: {}", e);
        e
    })?;

    eprintln!("Upload record created in database and S3. Table not loaded into memory.");

    increment_file_count(&state.db_pool, auth_user.user_id)
        .await
        .map_err(|e| {
            eprintln!("Failed to increment file count: {}", e);
            e
        })?;

    eprintln!("File count incremented");
    eprintln!("Upload completed successfully for table: {}", table_name);

    let s3_uploader = crate::utils::s3::S3Uploader::new().await;
    let file_link = s3_uploader.generate_presigned_url(&s3_key, None).await.ok();

    Ok(Json(serde_json::json!({
        "table_name": table_name,
        "file_name": file_name,
        "file_size": file_size,
        "file_link": file_link,
        "success": true
    })))
}

pub async fn execute_query(
    auth_user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, DoubledeckerError> {
    eprintln!(
        "execute_query handler called for user: {}",
        auth_user.user_id
    );

    let table_name = payload.table_name.ok_or(DoubledeckerError::Internal(
        "table_name is required".to_string(),
    ))?;

    eprintln!("Stateless query: Loading table '{}'", table_name);
    let upload = get_upload_by_table_name(&state.db_pool, &table_name, auth_user.user_id).await?;
    let s3_path = format!("s3://{}", upload.s3_key);

    // Create a fresh executor for this request to avoid concurrent query conflicts
    let executor = crate::server::executor::QueryExecutor::new();

    executor
        .load_csv(&s3_path, &table_name)
        .await
        .map_err(|e| DoubledeckerError::DataFusionError(e.to_string()))?;

    let batches = executor
        .execute_operations(&table_name, payload.operations)
        .await
        .map_err(|e| DoubledeckerError::QueryExecution(e.to_string()))?;

    let response = parse_batch_to_json(batches).await?;

    // Track query execution
    increment_query_count(&state.db_pool, auth_user.user_id).await?;

    Ok(Json(response))
}

pub async fn download_query_csv(
    auth_user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> Result<Response, DoubledeckerError> {
    let table_name = payload.table_name.ok_or(DoubledeckerError::Internal(
        "table_name is required".to_string(),
    ))?;

    eprintln!("Stateless download: Loading table '{}'", table_name);
    let upload = get_upload_by_table_name(&state.db_pool, &table_name, auth_user.user_id).await?;
    let s3_path = format!("s3://{}", upload.s3_key);

    // Create a fresh executor for this request to avoid concurrent query conflicts
    let executor = crate::server::executor::QueryExecutor::new();

    executor
        .load_csv(&s3_path, &table_name)
        .await
        .map_err(|e| DoubledeckerError::DataFusionError(e.to_string()))?;

    let batches = executor
        .execute_operations(&table_name, payload.operations)
        .await
        .map_err(|e| DoubledeckerError::QueryExecution(e.to_string()))?;

    let response = parse_batch_to_json(batches).await?;

    // Track query execution
    increment_query_count(&state.db_pool, auth_user.user_id).await?;

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
