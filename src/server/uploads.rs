use crate::db::models::{PaginatedResponse, PaginationParams, WorkspaceRole};
use crate::db::queries::{create_dataset, get_dataset_by_id, get_datasets, update_dataset_status};
use crate::server::dtos::common::DatasetResponse;
use crate::server::dtos::uploads::*;
use crate::server::extractors::verify_workspace_access;
use crate::server::middleware::AuthenticatedUser;
use crate::server::state::AppState;
use crate::utils::error::DoubledeckerError;
use axum::extract::{Multipart, Path, Query, State};
use axum::Json;
use uuid::Uuid;

/// Path A (<50MB): Direct multipart upload endpoint
#[utoipa::path(
    post,
    path = "/api/workspaces/{workspace_id}/datasets/upload",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID")
    ),
    responses(
        (status = 200, description = "Dataset uploaded directly", body = DatasetResponse)
    ),
    tag = "datasets"
)]
pub async fn upload_dataset_direct(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<DatasetResponse>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Admin).await?;

    let mut distributor_source = "auto".to_string();
    let mut file_content: Option<Vec<u8>> = None;
    let mut filename = "upload.csv".to_string();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| DoubledeckerError::BadRequest(format!("Multipart error: {}", e)))?
    {
        let name = field.name().unwrap_or("").to_string();
        if name == "distributor_source" || name == "source" {
            if let Ok(text) = field.text().await {
                distributor_source = text;
            }
        } else if name == "file" || name == "csv" {
            if let Some(fn_str) = field.file_name() {
                filename = fn_str.to_string();
            }
            if let Ok(bytes) = field.bytes().await {
                file_content = Some(bytes.to_vec());
            }
        }
    }

    let content = file_content.ok_or_else(|| DoubledeckerError::BadRequest("No file uploaded".to_string()))?;
    let file_size_bytes = content.len() as i64;

    if file_size_bytes > 50 * 1024 * 1024 {
        return Err(DoubledeckerError::BadRequest(
            "File exceeds 50MB limit for direct upload. Please use Path B (presigned URL upload).".to_string(),
        ));
    }

    let dataset_id = Uuid::new_v4();
    let staging_key = format!("workspaces/{}/staging/{}.csv", workspace_id, dataset_id);
    let parquet_key = format!("workspaces/{}/processed/{}.parquet", workspace_id, dataset_id);

    // 1. Upload staging file to S3
    state.uploader.upload_csv_with_key(&staging_key, content).await?;

    // 2. Create dataset in DB as QUEUED
    let dataset = create_dataset(
        &state.db_pool,
        workspace_id,
        distributor_source,
        filename,
        parquet_key,
        file_size_bytes,
        "QUEUED".to_string(),
    )
    .await?;

    // 3. Send event to Inngest for background workflow orchestration
    let evt = inngest::event::Event::new(
        "dataset/uploaded",
        serde_json::json!({
            "workspace_id": workspace_id,
            "dataset_id": dataset.id,
            "staging_key": staging_key,
        }),
    );
    let _ = state.inngest_client.send_event(&evt).await;

    Ok(Json(DatasetResponse::from_dataset(dataset)))
}

/// Path B (>50MB): Generate Presigned PUT URL for direct-to-S3 client upload
#[utoipa::path(
    post,
    path = "/api/workspaces/{workspace_id}/datasets/presigned_url",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID")
    ),
    request_body = PresignedUrlRequest,
    responses(
        (status = 200, description = "Presigned URL generated", body = PresignedUrlResponse)
    ),
    tag = "datasets"
)]
pub async fn generate_presigned_url_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    State(state): State<AppState>,
    Json(payload): Json<PresignedUrlRequest>,
) -> Result<Json<PresignedUrlResponse>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Admin).await?;

    let dataset_id = Uuid::new_v4();
    let staging_key = format!("workspaces/{}/staging/{}.csv", workspace_id, dataset_id);
    let parquet_key = format!("workspaces/{}/processed/{}.parquet", workspace_id, dataset_id);

    // 1. Generate presigned PUT URL
    let presigned_url = state
        .uploader
        .generate_presigned_put_url(&staging_key, Some(3600))
        .await?;

    // 2. Create dataset as PENDING_UPLOAD
    let _dataset = create_dataset(
        &state.db_pool,
        workspace_id,
        payload.distributor_source.unwrap_or_else(|| "auto".to_string()),
        payload.filename.clone(),
        parquet_key,
        payload.file_size_bytes,
        "PENDING_UPLOAD".to_string(),
    )
    .await?;

    Ok(Json(PresignedUrlResponse {
        dataset_id,
        presigned_url,
        staging_key,
    }))
}

/// Path B (>50MB): Confirm direct upload completion & enqueue processing
#[utoipa::path(
    post,
    path = "/api/workspaces/{workspace_id}/datasets/confirm",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID")
    ),
    request_body = ConfirmUploadRequest,
    responses(
        (status = 200, description = "Dataset upload confirmed", body = DatasetResponse)
    ),
    tag = "datasets"
)]
pub async fn confirm_upload_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    State(state): State<AppState>,
    Json(payload): Json<ConfirmUploadRequest>,
) -> Result<Json<DatasetResponse>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Admin).await?;

    let _ = update_dataset_status(&state.db_pool, payload.dataset_id, "QUEUED", 0, None).await?;
    
    // Send event to Inngest for background workflow orchestration
    let evt = inngest::event::Event::new(
        "dataset/uploaded",
        serde_json::json!({
            "workspace_id": workspace_id,
            "dataset_id": payload.dataset_id,
            "staging_key": payload.staging_key,
        }),
    );
    let _ = state.inngest_client.send_event(&evt).await;

    let dataset = get_dataset_by_id(&state.db_pool, workspace_id, payload.dataset_id).await?;
    Ok(Json(DatasetResponse::from_dataset(dataset)))
}

#[utoipa::path(
    get,
    path = "/api/workspaces/{workspace_id}/datasets",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID")
    ),
    responses(
        (status = 200, description = "List workspace datasets", body = PaginatedDatasets)
    ),
    tag = "datasets"
)]
pub async fn list_datasets_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    Query(pagination): Query<PaginationParams>,
    State(state): State<AppState>,
) -> Result<Json<PaginatedResponse<DatasetResponse>>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Viewer).await?;

    let limit = pagination.effective_limit();
    let paginated_datasets = get_datasets(&state.db_pool, workspace_id, pagination.cursor, limit).await?;
    let responses = PaginatedResponse {
        data: paginated_datasets.data.into_iter().map(DatasetResponse::from_dataset).collect(),
        pagination: paginated_datasets.pagination,
    };
    Ok(Json(responses))
}
