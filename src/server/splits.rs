use crate::db::models::{CascadingSplit, PaginatedResponse, PaginationParams, WorkspaceRole};
use crate::db::queries::{create_split, delete_split, get_splits_for_target, update_split};
use crate::server::dtos::DeleteResponse;
use crate::server::extractors::verify_workspace_access;
use crate::server::middleware::AuthenticatedUser;
use crate::server::state::AppState;
use crate::utils::error::DoubledeckerError;
use axum::extract::{Path, Query, State};
use axum::Json;
use crate::server::dtos::splits::*;
use uuid::Uuid;

#[utoipa::path(
    post,
    path = "/api/workspaces/{workspace_id}/splits",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID")
    ),
    request_body = CreateSplitRequest,
    responses(
        (status = 200, description = "Split created", body = CascadingSplit)
    ),
    tag = "splits"
)]
pub async fn create_split_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    State(state): State<AppState>,
    Json(payload): Json<CreateSplitRequest>,
) -> Result<Json<CascadingSplit>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Manager).await?;

    if payload.payee_name.trim().is_empty() {
        return Err(DoubledeckerError::BadRequest(
            "Payee name cannot be empty".to_string(),
        ));
    }

    let split = create_split(
        &state.db_pool,
        workspace_id,
        payload.artist_id,
        payload.album_id,
        payload.track_id,
        payload.payee_id,
        payload.payee_name,
        payload.percentage,
        payload.effective_from,
        payload.effective_to,
    )
    .await?;
    Ok(Json(split))
}

#[utoipa::path(
    get,
    path = "/api/workspaces/{workspace_id}/splits",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID"),
        PaginationParams,
    ),
    responses(
        (status = 200, description = "List splits", body = PaginatedSplits)
    ),
    tag = "splits"
)]
pub async fn list_splits_handler(
    auth_user: AuthenticatedUser,
    Path(workspace_id): Path<Uuid>,
    Query(pagination): Query<PaginationParams>,
    State(state): State<AppState>,
) -> Result<Json<PaginatedResponse<CascadingSplit>>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Viewer).await?;

    let limit = pagination.effective_limit();
    let splits = get_splits_for_target(&state.db_pool, workspace_id, None, None, None, pagination.cursor, limit).await?;
    Ok(Json(splits))
}

#[utoipa::path(
    delete,
    path = "/api/workspaces/{workspace_id}/splits/{split_id}",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID"),
        ("split_id" = Uuid, Path, description = "Split ID")
    ),
    responses(
        (status = 200, description = "Split deleted", body = DeleteResponse)
    ),
    tag = "splits"
)]
pub async fn delete_split_handler(
    auth_user: AuthenticatedUser,
    Path((workspace_id, split_id)): Path<(Uuid, Uuid)>,
    State(state): State<AppState>,
) -> Result<Json<DeleteResponse>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Manager).await?;

    let affected = delete_split(&state.db_pool, workspace_id, split_id).await?;
    if affected == 0 {
        return Err(DoubledeckerError::NotFound("Cascading split not found".to_string()));
    }

    Ok(Json(DeleteResponse {
        message: "Split successfully deleted".to_string(),
    }))
}

#[utoipa::path(
    put,
    path = "/api/workspaces/{workspace_id}/splits/{split_id}",
    params(
        ("workspace_id" = Uuid, Path, description = "Workspace ID"),
        ("split_id" = Uuid, Path, description = "Split ID")
    ),
    request_body = UpdateSplitRequest,
    responses(
        (status = 200, description = "Split updated", body = CascadingSplit)
    ),
    tag = "splits"
)]
pub async fn update_split_handler(
    auth_user: AuthenticatedUser,
    Path((workspace_id, split_id)): Path<(Uuid, Uuid)>,
    State(state): State<AppState>,
    Json(payload): Json<UpdateSplitRequest>,
) -> Result<Json<CascadingSplit>, DoubledeckerError> {
    verify_workspace_access(&state, workspace_id, auth_user.user_id, WorkspaceRole::Manager).await?;

    let updated = update_split(
        &state.db_pool,
        workspace_id,
        split_id,
        payload.percentage,
        payload.payee_id,
        payload.payee_name,
        payload.effective_from,
        payload.effective_to,
    )
    .await?;

    Ok(Json(updated))
}
