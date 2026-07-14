use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Deserialize, ToSchema)]
pub struct PresignedUrlRequest {
    pub filename: String,
    pub distributor_source: Option<String>,
    pub file_size_bytes: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PresignedUrlResponse {
    pub dataset_id: Uuid,
    pub presigned_url: String,
    pub staging_key: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ConfirmUploadRequest {
    pub dataset_id: Uuid,
    pub staging_key: String,
}
