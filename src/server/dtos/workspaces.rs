use crate::db::models::WorkspaceRole;
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateWorkspaceRequest {
    pub name: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateWorkspaceRequest {
    pub name: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct AddWorkspaceMemberRequest {
    pub user_id: Uuid,
    pub role: WorkspaceRole,
}
