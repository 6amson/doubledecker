use crate::db::models::{PaginatedResponse, Workspace};
use crate::db::queries::common::paginate_rows;
use crate::utils::error::DoubledeckerError;
use chrono::Utc;
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

pub async fn create_workspace(
    pool: &PgPool,
    owner_user_id: Uuid,
    name: String,
) -> Result<Workspace, DoubledeckerError> {
    let workspace = sqlx::query_as::<_, Workspace>(
        r#"
        INSERT INTO workspaces (owner_user_id, name)
        VALUES ($1, $2)
        RETURNING id, owner_user_id, name, storage_used_bytes, created_at, updated_at
        "#,
    )
    .bind(owner_user_id)
    .bind(&name)
    .fetch_one(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(workspace)
}

pub async fn get_workspaces_for_user(
    pool: &PgPool,
    owner_user_id: Uuid,
    cursor: Option<String>,
    limit: usize,
) -> Result<PaginatedResponse<Workspace>, DoubledeckerError> {
    let cursor_uuid = cursor.and_then(|c| Uuid::from_str(&c).ok());
    let fetch_limit = (limit + 1) as i64;

    let rows = sqlx::query_as::<_, Workspace>(
        r#"
        SELECT id, owner_user_id, name, storage_used_bytes, created_at, updated_at
        FROM workspaces
        WHERE owner_user_id = $1
          AND ($2::uuid IS NULL OR (created_at, id) < (SELECT created_at, id FROM workspaces WHERE id = $2))
        ORDER BY created_at DESC, id DESC
        LIMIT $3
        "#,
    )
    .bind(owner_user_id)
    .bind(cursor_uuid)
    .bind(fetch_limit)
    .fetch_all(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(paginate_rows(rows, limit, |item| item.id.to_string()))
}

pub async fn get_workspace_by_id(
    pool: &PgPool,
    workspace_id: Uuid,
    owner_user_id: Uuid,
) -> Result<Workspace, DoubledeckerError> {
    let workspace = sqlx::query_as::<_, Workspace>(
        r#"
        SELECT id, owner_user_id, name, storage_used_bytes, created_at, updated_at
        FROM workspaces
        WHERE id = $1 AND owner_user_id = $2
        "#,
    )
    .bind(workspace_id)
    .bind(owner_user_id)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DoubledeckerError::NotFound("Workspace not found".to_string()),
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(workspace)
}

pub async fn update_workspace_storage(
    pool: &PgPool,
    workspace_id: Uuid,
    delta_bytes: i64,
) -> Result<(), DoubledeckerError> {
    sqlx::query(
        r#"
        UPDATE workspaces
        SET storage_used_bytes = storage_used_bytes + $2,
            updated_at = $3
        WHERE id = $1
        "#,
    )
    .bind(workspace_id)
    .bind(delta_bytes)
    .bind(Utc::now())
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(())
}

pub async fn update_workspace(
    pool: &PgPool,
    workspace_id: Uuid,
    name: &str,
) -> Result<Workspace, DoubledeckerError> {
    let ws = sqlx::query_as::<_, Workspace>(
        r#"
        UPDATE workspaces
        SET name = $2, updated_at = NOW()
        WHERE id = $1
        RETURNING id, owner_user_id, name, storage_used_bytes, created_at, updated_at
        "#,
    )
    .bind(workspace_id)
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    ws.ok_or_else(|| DoubledeckerError::NotFound("Workspace not found".to_string()))
}

pub async fn delete_workspace(
    pool: &PgPool,
    workspace_id: Uuid,
) -> Result<u64, DoubledeckerError> {
    let res = sqlx::query(
        r#"
        DELETE FROM workspaces
        WHERE id = $1
        "#,
    )
    .bind(workspace_id)
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(res.rows_affected())
}
