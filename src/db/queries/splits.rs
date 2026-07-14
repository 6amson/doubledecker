use crate::db::models::{CascadingSplit, PaginatedResponse};
use crate::db::queries::common::paginate_rows;
use crate::utils::error::DoubledeckerError;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

pub async fn create_split(
    pool: &PgPool,
    workspace_id: Uuid,
    artist_id: Option<Uuid>,
    album_id: Option<Uuid>,
    track_id: Option<Uuid>,
    payee_id: Option<Uuid>,
    payee_name: String,
    percentage: Decimal,
    effective_from: Option<NaiveDate>,
    effective_to: Option<NaiveDate>,
) -> Result<CascadingSplit, DoubledeckerError> {
    let target_count = (artist_id.is_some() as u8) + (album_id.is_some() as u8) + (track_id.is_some() as u8);
    if target_count != 1 {
        return Err(DoubledeckerError::BadRequest(
            "Exactly one target (artist_id, album_id, or track_id) must be specified".to_string(),
        ));
    }

    let existing_splits = get_splits_for_target(pool, workspace_id, artist_id, album_id, track_id, None, 1000).await?;
    if existing_splits.data.len() >= 50 {
        return Err(DoubledeckerError::BadRequest(
            "Maximum of 50 payees allowed per target".to_string(),
        ));
    }

    let current_sum: Decimal = existing_splits.data.iter().map(|s| s.percentage).sum();
    if current_sum + percentage > Decimal::new(10000, 2) {
        return Err(DoubledeckerError::BadRequest(
            format!("Total assigned percentage exceeds 100.00% (Current sum: {}%, New: {}%)", current_sum, percentage),
        ));
    }

    let split = sqlx::query_as::<_, CascadingSplit>(
        r#"
        INSERT INTO cascading_splits (
            workspace_id, artist_id, album_id, track_id, payee_id, payee_name, percentage, effective_from, effective_to
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id, workspace_id, artist_id, album_id, track_id, payee_id, payee_name, percentage, effective_from, effective_to, created_at
        "#,
    )
    .bind(workspace_id)
    .bind(artist_id)
    .bind(album_id)
    .bind(track_id)
    .bind(payee_id)
    .bind(&payee_name)
    .bind(percentage)
    .bind(effective_from)
    .bind(effective_to)
    .fetch_one(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(split)
}

pub async fn get_splits_for_target(
    pool: &PgPool,
    workspace_id: Uuid,
    artist_id: Option<Uuid>,
    album_id: Option<Uuid>,
    track_id: Option<Uuid>,
    cursor: Option<String>,
    limit: usize,
) -> Result<PaginatedResponse<CascadingSplit>, DoubledeckerError> {
    let cursor_uuid = cursor.and_then(|c| Uuid::from_str(&c).ok());
    let fetch_limit = (limit + 1) as i64;

    let rows = sqlx::query_as::<_, CascadingSplit>(
        r#"
        SELECT id, workspace_id, artist_id, album_id, track_id, payee_id, payee_name, percentage, effective_from, effective_to, created_at
        FROM cascading_splits
        WHERE workspace_id = $1
          AND ($2::uuid IS NULL OR artist_id = $2)
          AND ($3::uuid IS NULL OR album_id = $3)
          AND ($4::uuid IS NULL OR track_id = $4)
          AND ($5::uuid IS NULL OR (created_at, id) > (SELECT created_at, id FROM cascading_splits WHERE id = $5))
        ORDER BY created_at ASC, id ASC
        LIMIT $6
        "#,
    )
    .bind(workspace_id)
    .bind(artist_id)
    .bind(album_id)
    .bind(track_id)
    .bind(cursor_uuid)
    .bind(fetch_limit)
    .fetch_all(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(paginate_rows(rows, limit, |item| item.id.to_string()))
}

pub async fn delete_split(
    pool: &PgPool,
    workspace_id: Uuid,
    split_id: Uuid,
) -> Result<u64, DoubledeckerError> {
    let result = sqlx::query(
        "DELETE FROM cascading_splits WHERE id = $1 AND workspace_id = $2",
    )
    .bind(split_id)
    .bind(workspace_id)
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(result.rows_affected())
}

pub async fn update_split(
    pool: &PgPool,
    workspace_id: Uuid,
    split_id: Uuid,
    percentage: Option<Decimal>,
    payee_id: Option<Uuid>,
    payee_name: Option<String>,
    effective_from: Option<NaiveDate>,
    effective_to: Option<NaiveDate>,
) -> Result<CascadingSplit, DoubledeckerError> {
    let existing: CascadingSplit = sqlx::query_as::<_, CascadingSplit>(
        "SELECT id, workspace_id, artist_id, album_id, track_id, payee_id, payee_name, percentage, effective_from, effective_to, created_at FROM cascading_splits WHERE id = $1 AND workspace_id = $2",
    )
    .bind(split_id)
    .bind(workspace_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?
    .ok_or_else(|| DoubledeckerError::NotFound("Cascading split not found".to_string()))?;

    if let Some(new_pct) = percentage {
        let existing_splits = get_splits_for_target(
            pool,
            workspace_id,
            existing.artist_id,
            existing.album_id,
            existing.track_id,
            None,
            1000,
        )
        .await?;

        let other_sum: Decimal = existing_splits
            .data.iter()
            .filter(|s| s.id != split_id)
            .map(|s| s.percentage)
            .sum();

        if other_sum + new_pct > Decimal::new(10000, 2) {
            return Err(DoubledeckerError::BadRequest(format!(
                "Total assigned percentage exceeds 100.00% (Other payees sum: {}%, Updated: {}%)",
                other_sum, new_pct
            )));
        }
    }

    let updated = sqlx::query_as::<_, CascadingSplit>(
        r#"
        UPDATE cascading_splits
        SET
            percentage = COALESCE($3, percentage),
            payee_id = COALESCE($4, payee_id),
            payee_name = COALESCE($5, payee_name),
            effective_from = COALESCE($6, effective_from),
            effective_to = COALESCE($7, effective_to)
        WHERE id = $1 AND workspace_id = $2
        RETURNING id, workspace_id, artist_id, album_id, track_id, payee_id, payee_name, percentage, effective_from, effective_to, created_at
        "#,
    )
    .bind(split_id)
    .bind(workspace_id)
    .bind(percentage)
    .bind(payee_id)
    .bind(payee_name)
    .bind(effective_from)
    .bind(effective_to)
    .fetch_one(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(updated)
}

pub async fn get_effective_splits(
    pool: &PgPool,
    workspace_id: Uuid,
    reporting_date: NaiveDate,
) -> Result<Vec<CascadingSplit>, DoubledeckerError> {
    let splits = sqlx::query_as::<_, CascadingSplit>(
        r#"
        SELECT id, workspace_id, artist_id, album_id, track_id, payee_id, payee_name, percentage, effective_from, effective_to, created_at
        FROM cascading_splits
        WHERE workspace_id = $1
          AND COALESCE(effective_from, '1900-01-01') <= $2
          AND COALESCE(effective_to, '2099-12-31') >= $2
        "#,
    )
    .bind(workspace_id)
    .bind(reporting_date)
    .fetch_all(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(splits)
}
