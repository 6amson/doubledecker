use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Serialize, ToSchema)]
pub struct DeleteResponse {
    pub message: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DatasetResponse {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub distributor_source: String,
    pub filename: String,
    pub s3_parquet_key: String,
    pub file_size_bytes: i64,
    pub row_count: i64,
    pub status: String,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl DatasetResponse {
    pub fn from_dataset(dataset: crate::db::models::Dataset) -> Self {
        Self {
            id: dataset.id,
            workspace_id: dataset.workspace_id,
            distributor_source: dataset.distributor_source,
            filename: dataset.filename,
            s3_parquet_key: dataset.s3_parquet_key,
            file_size_bytes: dataset.file_size_bytes,
            row_count: dataset.row_count,
            status: dataset.status,
            error_message: dataset.error_message,
            created_at: dataset.created_at,
            updated_at: dataset.updated_at,
        }
    }
}
