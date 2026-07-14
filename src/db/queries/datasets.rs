use crate::db::models::{Dataset, PaginatedResponse};
use crate::db::queries::common::paginate_rows;
use crate::utils::error::DoubledeckerError;
use chrono::Utc;
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

pub async fn create_dataset(
    pool: &PgPool,
    workspace_id: Uuid,
    distributor_source: String,
    filename: String,
    s3_parquet_key: String,
    file_size_bytes: i64,
    status: String,
) -> Result<Dataset, DoubledeckerError> {
    let dataset = sqlx::query_as::<_, Dataset>(
        r#"
        INSERT INTO datasets (workspace_id, distributor_source, filename, s3_parquet_key, file_size_bytes, status)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, workspace_id, distributor_source, filename, s3_parquet_key, file_size_bytes, row_count, status, error_message, created_at, updated_at
        "#,
    )
    .bind(workspace_id)
    .bind(&distributor_source)
    .bind(&filename)
    .bind(&s3_parquet_key)
    .bind(file_size_bytes)
    .bind(&status)
    .fetch_one(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(dataset)
}

pub async fn get_datasets(
    pool: &PgPool,
    workspace_id: Uuid,
    cursor: Option<String>,
    limit: usize,
) -> Result<PaginatedResponse<Dataset>, DoubledeckerError> {
    let cursor_uuid = cursor.and_then(|c| Uuid::from_str(&c).ok());
    let fetch_limit = (limit + 1) as i64;

    let rows = sqlx::query_as::<_, Dataset>(
        r#"
        SELECT id, workspace_id, distributor_source, filename, s3_parquet_key, file_size_bytes, row_count, status, error_message, created_at, updated_at
        FROM datasets
        WHERE workspace_id = $1
          AND ($2::uuid IS NULL OR (created_at, id) < (SELECT created_at, id FROM datasets WHERE id = $2))
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

pub async fn get_dataset_by_id(
    pool: &PgPool,
    workspace_id: Uuid,
    dataset_id: Uuid,
) -> Result<Dataset, DoubledeckerError> {
    let dataset = sqlx::query_as::<_, Dataset>(
        r#"
        SELECT id, workspace_id, distributor_source, filename, s3_parquet_key, file_size_bytes, row_count, status, error_message, created_at, updated_at
        FROM datasets
        WHERE id = $1 AND workspace_id = $2
        "#,
    )
    .bind(dataset_id)
    .bind(workspace_id)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DoubledeckerError::NotFound("Dataset not found".to_string()),
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(dataset)
}

pub async fn update_dataset_status(
    pool: &PgPool,
    dataset_id: Uuid,
    status: &str,
    row_count: i64,
    error_message: Option<String>,
) -> Result<(), DoubledeckerError> {
    sqlx::query(
        r#"
        UPDATE datasets
        SET status = $2,
            row_count = $3,
            error_message = $4,
            updated_at = $5
        WHERE id = $1
        "#,
    )
    .bind(dataset_id)
    .bind(status)
    .bind(row_count)
    .bind(error_message)
    .bind(Utc::now())
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(())
}
