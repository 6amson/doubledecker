use crate::db::models::{PaginatedResponse, WorkspaceMember, WorkspaceRole};
use crate::db::queries::common::paginate_rows;
use crate::utils::error::DoubledeckerError;
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

pub async fn verify_workspace_permission(
    pool: &PgPool,
    workspace_id: Uuid,
    user_id: Uuid,
    required_role: WorkspaceRole,
) -> Result<WorkspaceRole, DoubledeckerError> {
    let workspace_owner: Option<(Uuid,)> = sqlx::query_as(
        "SELECT owner_user_id FROM workspaces WHERE id = $1",
    )
    .bind(workspace_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| DoubledeckerError::Internal(e.to_string()))?;

    if let Some((owner_id,)) = workspace_owner {
        if owner_id == user_id {
            return Ok(WorkspaceRole::Owner);
        }
    } else {
        return Err(DoubledeckerError::NotFound("Workspace not found".to_string()));
    }

    let member_role_row: Option<(String,)> = sqlx::query_as(
        "SELECT role FROM workspace_members WHERE workspace_id = $1 AND user_id = $2",
    )
    .bind(workspace_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| DoubledeckerError::Internal(e.to_string()))?;

    let user_role = match member_role_row {
        Some((r_str,)) => WorkspaceRole::from_str(&r_str).unwrap_or(WorkspaceRole::Viewer),
        None => return Err(DoubledeckerError::Unauthorized),
    };

    if user_role >= required_role {
        Ok(user_role)
    } else {
        Err(DoubledeckerError::Forbidden(format!(
            "Insufficient permissions: required {}, but user has {}",
            required_role, user_role
        )))
    }
}

pub async fn add_workspace_member(
    pool: &PgPool,
    workspace_id: Uuid,
    user_id: Uuid,
    role: WorkspaceRole,
) -> Result<WorkspaceMember, DoubledeckerError> {
    let member = sqlx::query_as::<_, WorkspaceMember>(
        r#"
        INSERT INTO workspace_members (workspace_id, user_id, role)
        VALUES ($1, $2, $3)
        ON CONFLICT (workspace_id, user_id)
        DO UPDATE SET role = EXCLUDED.role
        RETURNING workspace_id, user_id, role, created_at
        "#,
    )
    .bind(workspace_id)
    .bind(user_id)
    .bind(role.to_string())
    .fetch_one(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(member)
}

pub async fn list_workspace_members(
    pool: &PgPool,
    workspace_id: Uuid,
    cursor: Option<String>,
    limit: usize,
) -> Result<PaginatedResponse<WorkspaceMember>, DoubledeckerError> {
    let cursor_uuid = cursor.and_then(|c| Uuid::from_str(&c).ok());
    let fetch_limit = (limit + 1) as i64;

    let rows = sqlx::query_as::<_, WorkspaceMember>(
        r#"
        SELECT workspace_id, user_id, role, created_at
        FROM workspace_members
        WHERE workspace_id = $1
          AND ($2::uuid IS NULL OR (created_at, user_id) > (SELECT created_at, user_id FROM workspace_members WHERE workspace_id = $1 AND user_id = $2))
        ORDER BY created_at ASC, user_id ASC
        LIMIT $3
        "#,
    )
    .bind(workspace_id)
    .bind(cursor_uuid)
    .bind(fetch_limit)
    .fetch_all(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(paginate_rows(rows, limit, |item| item.user_id.to_string()))
}
