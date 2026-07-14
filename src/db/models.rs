use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use utoipa::ToSchema;
use uuid::Uuid;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum UserType {
    Artist,
    Songwriter,
    Producer,
    Instrumentalist,
    Dj,
    Engineer,
    Manager,
    AAndR,
    Publisher,
    Label,
    Attorney,
    Distributor,
    Other,
}

impl fmt::Display for UserType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UserType::Artist => write!(f, "ARTIST"),
            UserType::Songwriter => write!(f, "SONGWRITER"),
            UserType::Producer => write!(f, "PRODUCER"),
            UserType::Instrumentalist => write!(f, "INSTRUMENTALIST"),
            UserType::Dj => write!(f, "DJ"),
            UserType::Engineer => write!(f, "ENGINEER"),
            UserType::Manager => write!(f, "MANAGER"),
            UserType::AAndR => write!(f, "A_AND_R"),
            UserType::Publisher => write!(f, "PUBLISHER"),
            UserType::Label => write!(f, "LABEL"),
            UserType::Attorney => write!(f, "ATTORNEY"),
            UserType::Distributor => write!(f, "DISTRIBUTOR"),
            UserType::Other => write!(f, "OTHER"),
        }
    }
}

impl FromStr for UserType {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SONGWRITER" => Ok(UserType::Songwriter),
            "PRODUCER" => Ok(UserType::Producer),
            "INSTRUMENTALIST" => Ok(UserType::Instrumentalist),
            "DJ" => Ok(UserType::Dj),
            "ENGINEER" => Ok(UserType::Engineer),
            "MANAGER" => Ok(UserType::Manager),
            "A_AND_R" | "A&R" | "AANDR" => Ok(UserType::AAndR),
            "PUBLISHER" => Ok(UserType::Publisher),
            "LABEL" => Ok(UserType::Label),
            "ATTORNEY" => Ok(UserType::Attorney),
            "DISTRIBUTOR" => Ok(UserType::Distributor),
            "OTHER" => Ok(UserType::Other),
            _ => Ok(UserType::Artist),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum WorkspaceRole {
    Viewer = 1,
    Manager = 2,
    Admin = 3,
    Owner = 4,
}

impl fmt::Display for WorkspaceRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkspaceRole::Viewer => write!(f, "VIEWER"),
            WorkspaceRole::Manager => write!(f, "MANAGER"),
            WorkspaceRole::Admin => write!(f, "ADMIN"),
            WorkspaceRole::Owner => write!(f, "OWNER"),
        }
    }
}

impl FromStr for WorkspaceRole {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "OWNER" => Ok(WorkspaceRole::Owner),
            "ADMIN" => Ok(WorkspaceRole::Admin),
            "MANAGER" => Ok(WorkspaceRole::Manager),
            _ => Ok(WorkspaceRole::Viewer),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct WorkspaceMember {
    pub workspace_id: Uuid,
    pub user_id: Uuid,
    pub role: String,
    pub created_at: DateTime<Utc>,
}

impl WorkspaceMember {
    pub fn get_role(&self) -> WorkspaceRole {
        WorkspaceRole::from_str(&self.role).unwrap_or(WorkspaceRole::Viewer)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct QueryHistoryRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub user_id: Option<Uuid>,
    pub query_id: String,
    pub sql_executed: String,
    pub row_count: i64,
    pub execution_time_ms: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct User {
    pub id: Uuid,
    pub name: String,
    pub email: String,
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub user_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl User {
    pub fn get_user_type(&self) -> UserType {
        UserType::from_str(&self.user_type).unwrap_or(UserType::Artist)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct Workspace {
    pub id: Uuid,
    pub owner_user_id: Uuid,
    pub name: String,
    pub storage_used_bytes: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct Artist {
    pub id: Uuid,
    pub owner_user_id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct Album {
    pub id: Uuid,
    pub owner_user_id: Uuid,
    pub artist_id: Uuid,
    pub title: String,
    pub upc: Option<String>,
    pub release_date: Option<NaiveDate>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct Track {
    pub id: Uuid,
    pub owner_user_id: Uuid,
    pub artist_id: Uuid,
    pub album_id: Option<Uuid>,
    pub isrc: String,
    pub title: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct Payee {
    pub id: Uuid,
    pub owner_user_id: Uuid,
    pub name: String,
    pub email: Option<String>,
    pub bank_account: Option<String>,
    pub tax_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct CascadingSplit {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub artist_id: Option<Uuid>,
    pub album_id: Option<Uuid>,
    pub track_id: Option<Uuid>,
    pub payee_id: Option<Uuid>,
    pub payee_name: String,
    pub percentage: Decimal,
    pub effective_from: Option<NaiveDate>,
    pub effective_to: Option<NaiveDate>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct Dataset {
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

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct FxRate {
    pub date: NaiveDate,
    pub currency_code: String,
    pub rate_to_usd: Decimal,
}

// #[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
// pub struct BackgroundJob {
//     pub id: i64,
//     pub workspace_id: Uuid,
//     pub dataset_id: Uuid,
//     pub s3_staging_key: String,
//     pub status: String,
//     pub attempts: i32,
//     pub max_attempts: i32,
//     pub run_at: DateTime<Utc>,
//     pub error_log: Option<String>,
//     pub created_at: DateTime<Utc>,
// }

#[derive(Debug, Clone, Deserialize, ToSchema, utoipa::IntoParams)]
pub struct PaginationParams {
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

impl PaginationParams {
    pub fn effective_limit(&self) -> usize {
        self.limit.unwrap_or(50).clamp(1, 100)
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PaginationMeta {
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[aliases(
    PaginatedWorkspaces = PaginatedResponse<Workspace>,
    PaginatedWorkspaceMembers = PaginatedResponse<WorkspaceMember>,
    PaginatedArtists = PaginatedResponse<Artist>,
    PaginatedAlbums = PaginatedResponse<Album>,
    PaginatedTracks = PaginatedResponse<Track>,
    PaginatedPayees = PaginatedResponse<Payee>,
    PaginatedSplits = PaginatedResponse<CascadingSplit>,
    PaginatedDatasets = PaginatedResponse<crate::server::dtos::common::DatasetResponse>,
    PaginatedQueryHistory = PaginatedResponse<QueryHistoryRecord>
)]
pub struct PaginatedResponse<T> {
    pub data: Vec<T>,
    pub pagination: PaginationMeta,
}

impl<T> PaginatedResponse<T> {
    pub fn new(data: Vec<T>, next_cursor: Option<String>, has_more: bool, limit: usize) -> Self {
        Self {
            data,
            pagination: PaginationMeta {
                next_cursor,
                has_more,
                limit,
            },
        }
    }
}
