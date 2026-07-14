use crate::db::models::{PaginatedResponse, QueryHistoryRecord};
use crate::db::queries::common::paginate_rows;
use crate::utils::error::DoubledeckerError;
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

pub async fn record_query_history(
    pool: &PgPool,
    workspace_id: Uuid,
    user_id: Option<Uuid>,
    query_id: &str,
    sql_executed: &str,
    row_count: i64,
    execution_time_ms: i64,
) -> Result<QueryHistoryRecord, DoubledeckerError> {
    let rec = sqlx::query_as::<_, QueryHistoryRecord>(
        r#"
        INSERT INTO query_history (workspace_id, user_id, query_id, sql_executed, row_count, execution_time_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, workspace_id, user_id, query_id, sql_executed, row_count, execution_time_ms, created_at
        "#,
    )
    .bind(workspace_id)
    .bind(user_id)
    .bind(query_id)
    .bind(sql_executed)
    .bind(row_count)
    .bind(execution_time_ms)
    .fetch_one(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(rec)
}

pub async fn list_query_history(
    pool: &PgPool,
    workspace_id: Uuid,
    cursor: Option<String>,
    limit: usize,
) -> Result<PaginatedResponse<QueryHistoryRecord>, DoubledeckerError> {
    let cursor_uuid = cursor.and_then(|c| Uuid::from_str(&c).ok());
    let fetch_limit = (limit + 1) as i64;

    let rows = sqlx::query_as::<_, QueryHistoryRecord>(
        r#"
        SELECT id, workspace_id, user_id, query_id, sql_executed, row_count, execution_time_ms, created_at
        FROM query_history
        WHERE workspace_id = $1
          AND ($2::uuid IS NULL OR (created_at, id) < (SELECT created_at, id FROM query_history WHERE id = $2))
        ORDER BY created_at DESC, id DESC
        LIMIT $3
        "#,
    )
    .bind(workspace_id)
    .bind(cursor_uuid)
    .bind(fetch_limit)
    .fetch_all(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(paginate_rows(rows, limit, |item| item.id.to_string()))
}

pub async fn get_query_history_by_id(
    pool: &PgPool,
    workspace_id: Uuid,
    query_id: &str,
) -> Result<QueryHistoryRecord, DoubledeckerError> {
    let rec = sqlx::query_as::<_, QueryHistoryRecord>(
        r#"
        SELECT id, workspace_id, user_id, query_id, sql_executed, row_count, execution_time_ms, created_at
        FROM query_history
        WHERE workspace_id = $1 AND query_id = $2
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(workspace_id)
    .bind(query_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    rec.ok_or_else(|| DoubledeckerError::NotFound("Query history record not found".to_string()))
}
