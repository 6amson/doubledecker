use crate::db::models::WorkspaceRole;
use crate::db::queries::verify_workspace_permission;
use crate::server::state::AppState;
use crate::utils::error::DoubledeckerError;
use uuid::Uuid;

pub async fn verify_workspace_access(
    state: &AppState,
    workspace_id: Uuid,
    user_id: Uuid,
    required_role: WorkspaceRole,
) -> Result<(), DoubledeckerError> {
    verify_workspace_permission(&state.db_pool, workspace_id, user_id, required_role).await?;
    Ok(())
}
