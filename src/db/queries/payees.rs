use crate::db::models::{PaginatedResponse, Payee};
use crate::db::queries::common::paginate_rows;
use crate::utils::error::DoubledeckerError;
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

pub async fn create_payee(
    pool: &PgPool,
    owner_user_id: Uuid,
    name: String,
    email: Option<String>,
    bank_account: Option<String>,
    tax_id: Option<String>,
) -> Result<Payee, DoubledeckerError> {
    let payee = sqlx::query_as::<_, Payee>(
        r#"
        INSERT INTO payees (owner_user_id, name, email, bank_account, tax_id)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, owner_user_id, name, email, bank_account, tax_id, created_at
        "#,
    )
    .bind(owner_user_id)
    .bind(&name)
    .bind(email)
    .bind(bank_account)
    .bind(tax_id)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
            DoubledeckerError::DatabaseError("Payee name already exists in your contact book".to_string())
        }
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(payee)
}

pub async fn upsert_payee(
    pool: &PgPool,
    owner_user_id: Uuid,
    name: String,
) -> Result<Payee, DoubledeckerError> {
    let payee = sqlx::query_as::<_, Payee>(
        r#"
        INSERT INTO payees (owner_user_id, name)
        VALUES ($1, $2)
        ON CONFLICT (owner_user_id, name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id, owner_user_id, name, email, bank_account, tax_id, created_at
        "#,
    )
    .bind(owner_user_id)
    .bind(&name)
    .fetch_one(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(payee)
}

pub async fn get_payees(
    pool: &PgPool,
    owner_user_id: Uuid,
    cursor: Option<String>,
    limit: usize,
) -> Result<PaginatedResponse<Payee>, DoubledeckerError> {
    let cursor_uuid = cursor.and_then(|c| Uuid::from_str(&c).ok());
    let fetch_limit = (limit + 1) as i64;

    let rows = sqlx::query_as::<_, Payee>(
        r#"
        SELECT id, owner_user_id, name, email, bank_account, tax_id, created_at
        FROM payees
        WHERE owner_user_id = $1
          AND ($2::uuid IS NULL OR (created_at, id) < (SELECT created_at, id FROM payees WHERE id = $2))
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

pub async fn update_payee(
    pool: &PgPool,
    payee_id: Uuid,
    name: Option<&str>,
    email: Option<&str>,
    bank_account: Option<&str>,
    tax_id: Option<&str>,
) -> Result<Payee, DoubledeckerError> {
    let payee = sqlx::query_as::<_, Payee>(
        r#"
        UPDATE payees
        SET name = COALESCE($2, name),
            email = COALESCE($3, email),
            bank_account = COALESCE($4, bank_account),
            tax_id = COALESCE($5, tax_id)
        WHERE id = $1
        RETURNING id, owner_user_id, name, email, bank_account, tax_id, created_at
        "#,
    )
    .bind(payee_id)
    .bind(name)
    .bind(email)
    .bind(bank_account)
    .bind(tax_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    payee.ok_or_else(|| DoubledeckerError::NotFound("Payee not found".to_string()))
}

pub async fn delete_payee(
    pool: &PgPool,
    payee_id: Uuid,
) -> Result<u64, DoubledeckerError> {
    let res = sqlx::query(
        r#"
        DELETE FROM payees
        WHERE id = $1
        "#,
    )
    .bind(payee_id)
    .execute(pool)
    .await
    .map_err(|e| DoubledeckerError::DatabaseError(e.to_string()))?;

    Ok(res.rows_affected())
}
