use crate::db::models::{PaginatedResponse, PaginationParams, Workspace, WorkspaceMember, WorkspaceRole};
use crate::db::queries::{
    add_workspace_member, create_workspace, delete_workspace, get_workspaces_for_user,
    list_workspace_members, update_workspace,
};
use crate::server::dtos::DeleteResponse;
use crate::server::extractors::verify_workspace_access;
use crate::server::middleware::AuthenticatedUser;
use crate::server::state::AppState;
use crate::utils::error::DoubledeckerError;
use axum::extract::{Path, Query, State};
use axum::Json;
use crate::server::dtos::workspaces::*;
use uuid::Uuid;

#[utoipa::path(
    post,
    path = "/api/workspaces",
    request_body = CreateWorkspaceRequest,
    responses(
        (status = 200, description = "Workspace created", body = Workspace)
    ),
    tag = "workspaces"
)]
pub async fn create_workspace_handler(
    auth_user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(payload): Json<CreateWorkspaceRequest>,
) -> Result<Json<Workspace>, DoubledeckerError> {
    if payload.name.trim().is_empty() {
        return Err(DoubledeckerError::BadRequest(
            "Workspace name cannot be empty".to_string(),
        ));
    }

    let workspace = create_workspace(&state.db_pool, auth_user.user_id, payload.name).await?;
    Ok(Json(workspace))
}

#[utoipa::path(
    get,
    path = "/api/workspaces",
    params(PaginationParams),
    responses(
        (status = 200, description = "List user workspaces", body = PaginatedWorkspaces)
    ),
    tag = "workspaces"
)]
pub async fn list_workspaces_handler(
    auth_user: AuthenticatedUser,
    Query(pagination): Query<PaginationParams>,
    State(state): State<AppState>,
) -> Result<Json<PaginatedResponse<Workspace>>, DoubledeckerError> {
    let limit = pagination.effective_limit();
    let workspaces = get_workspaces_for_user(&state.db_pool, auth_user.user_id, pagination.cursor, limit).await?;
    Ok(Json(workspaces))
}

#[utoipa::path(
    put,
    path = "/api/workspaces/{workspace_id}",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID")
    ),
    request_body = UpdateWorkspaceRequest,
    responses(
        (status = 200, description = "Workspace updated successfully", body = Workspace)
    ),
    tag = "workspaces"
)]
pub async fn update_workspace_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    State(state): State<AppState>,
    Json(payload): Json<UpdateWorkspaceRequest>,
) -> Result<Json<Workspace>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Owner).await?;
    let updated = update_workspace(&state.db_pool, workspace_id, &payload.name).await?;
    Ok(Json(updated))
}

#[utoipa::path(
    delete,
    path = "/api/workspaces/{workspace_id}",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID")
    ),
    responses(
        (status = 200, description = "Workspace deleted successfully", body = DeleteResponse)
    ),
    tag = "workspaces"
)]
pub async fn delete_workspace_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    State(state): State<AppState>,
) -> Result<Json<DeleteResponse>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Owner).await?;
    let _ = delete_workspace(&state.db_pool, workspace_id).await?;
    Ok(Json(DeleteResponse {
        message: "Workspace deleted successfully".to_string(),
    }))
}

#[utoipa::path(
    post,
    path = "/api/workspaces/{workspace_id}/members",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID")
    ),
    request_body = AddWorkspaceMemberRequest,
    responses(
        (status = 200, description = "Member added", body = WorkspaceMember)
    ),
    tag = "workspaces"
)]
pub async fn add_workspace_member_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    State(state): State<AppState>,
    Json(payload): Json<AddWorkspaceMemberRequest>,
) -> Result<Json<WorkspaceMember>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Admin).await?;

    let member = add_workspace_member(&state.db_pool, workspace_id, payload.user_id, payload.role).await?;
    Ok(Json(member))
}

#[utoipa::path(
    get,
    path = "/api/workspaces/{workspace_id}/members",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID"),
        PaginationParams,
    ),
    responses(
        (status = 200, description = "List workspace members", body = PaginatedWorkspaceMembers)
    ),
    tag = "workspaces"
)]
pub async fn list_workspace_members_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    Query(pagination): Query<PaginationParams>,
    State(state): State<AppState>,
) -> Result<Json<PaginatedResponse<WorkspaceMember>>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Viewer).await?;

    let limit = pagination.effective_limit();
    let members = list_workspace_members(&state.db_pool, workspace_id, pagination.cursor, limit).await?;
    Ok(Json(members))
}
