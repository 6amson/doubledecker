use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum FilterOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Contains,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum AggFunc {
    Sum,
    Avg,
    Max,
    Min,
    Count,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Aggregation {
    pub function: AggFunc,
    pub column: String,
    pub alias: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum TransformOp {
    Multiply,
    Divide,
    Add,
    Subtract,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
pub enum Operations {
    Select {
        columns: Vec<String>,
    },
    Filter {
        column: String,
        operator: FilterOp,
        value: String,
    },
    GroupBy {
        columns: Vec<String>,
        aggregations: Vec<Aggregation>,
    },
    Sort {
        column: String,
        ascending: bool,
    },
    Limit {
        count: usize,
    },
    Transform {
        column: String,
        operation: TransformOp,
        value: f64,
        alias: String,
    },
}

#[derive(Clone)]
pub struct AppState {
    pub db_pool: PgPool,
}

#[derive(Deserialize)]
pub struct QueryRequest {
    pub table_name: Option<String>,
    pub operations: Vec<Operations>,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct AuthResponse {
    pub token: String,
    pub user: UserInfo,
}

#[derive(Debug, Serialize)]
pub struct UserInfo {
    pub id: Uuid,
    pub email: String,
    pub total_queries: i32,
    pub total_files_processed: i32,
    pub total_saved_queries: i32,
}

#[derive(Debug, Deserialize)]
pub struct CreateSavedQueryRequest {
    pub name: String,
    pub description: Option<String>,
    pub query: Vec<Operations>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateSavedQueryRequest {
    pub name: String,
    pub description: Option<String>,
    pub query: Vec<Operations>,
}

#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_page_size")]
    pub page_size: i64,
}

fn default_page() -> i64 {
    1
}

fn default_page_size() -> i64 {
    10
}

#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T> {
    pub data: Vec<T>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
    pub total_pages: i64,
}

#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub id: Uuid,
    pub user_id: Uuid,
    pub file_name: String,
    pub s3_key: String,
    pub file_size: i64,
    pub file_type: String,
    pub table_name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub file_link: Option<String>,
}

impl UploadResponse {
    pub fn from_upload(upload: crate::db::models::Upload, file_link: Option<String>) -> Self {
        Self {
            id: upload.id,
            user_id: upload.user_id,
            file_name: upload.file_name,
            s3_key: upload.s3_key,
            file_size: upload.file_size,
            file_type: upload.file_type,
            table_name: upload.table_name,
            created_at: upload.created_at,
            updated_at: upload.updated_at,
            file_link,
        }
    }
}
