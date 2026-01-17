use crate::db::operations::get_uploads_by_user_paginated;
use crate::server::middleware::AuthenticatedUser;
use crate::utils::error::DoubledeckerError;
use crate::utils::s3::S3Uploader;
use crate::utils::statics::{AppState, PaginatedResponse, PaginationParams, UploadResponse};
use axum::Json;
use axum::extract::{Query, State};

pub async fn list_uploads_handler(
    State(state): State<AppState>,
    auth_user: AuthenticatedUser,
    Query(params): Query<PaginationParams>,
) -> Result<Json<PaginatedResponse<UploadResponse>>, DoubledeckerError> {
    let page = if params.page < 1 { 1 } else { params.page };
    let page_size = if params.page_size < 1 {
        10
    } else if params.page_size > 100 {
        100
    } else {
        params.page_size
    };

    let (uploads, total) =
        get_uploads_by_user_paginated(&state.db_pool, auth_user.user_id, page, page_size).await?;

    // Generate presigned URLs for each upload
    let s3_uploader = S3Uploader::new().await;
    let mut upload_responses = Vec::new();

    for upload in uploads {
        let file_link = s3_uploader
            .generate_presigned_url(&upload.s3_key, None)
            .await
            .ok();
        upload_responses.push(UploadResponse::from_upload(upload, file_link));
    }

    let total_pages = (total as f64 / page_size as f64).ceil() as i64;

    Ok(Json(PaginatedResponse {
        data: upload_responses,
        total,
        page,
        page_size,
        total_pages,
    }))
}
