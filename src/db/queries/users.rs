use crate::db::models::User;
use crate::utils::error::DoubledeckerError;
use bcrypt::{hash, verify, DEFAULT_COST};
use sqlx::PgPool;
use uuid::Uuid;

pub async fn create_user(
    pool: &PgPool,
    name: String,
    email: String,
    password: String,
    user_type: Option<String>,
) -> Result<User, DoubledeckerError> {
    let password_hash = hash(password, DEFAULT_COST)
        .map_err(|e| DoubledeckerError::Internal(format!("Password hashing failed: {}", e)))?;

    let user_type_str = user_type.unwrap_or_else(|| "ARTIST".to_string()).to_uppercase();

    let user = sqlx::query_as::<_, User>(
        r#"
        INSERT INTO users (name, email, password_hash, user_type)
        VALUES ($1, $2, $3, $4)
        RETURNING id, name, email, password_hash, user_type, created_at, updated_at
        "#,
    )
    .bind(&name)
    .bind(&email)
    .bind(&password_hash)
    .bind(&user_type_str)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(db_err) if db_err.is_unique_violation() => {
            DoubledeckerError::DatabaseError("Email already exists".to_string())
        }
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(user)
}

pub async fn get_user_by_email(pool: &PgPool, email: &str) -> Result<User, DoubledeckerError> {
    let user = sqlx::query_as::<_, User>(
        r#"
        SELECT id, name, email, password_hash, user_type, created_at, updated_at
        FROM users
        WHERE email = $1
        "#,
    )
    .bind(email)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DoubledeckerError::NotFound("User not found".to_string()),
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(user)
}

pub async fn get_user_by_id(pool: &PgPool, id: Uuid) -> Result<User, DoubledeckerError> {
    let user = sqlx::query_as::<_, User>(
        r#"
        SELECT id, name, email, password_hash, user_type, created_at, updated_at
        FROM users
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::RowNotFound => DoubledeckerError::NotFound("User not found".to_string()),
        _ => DoubledeckerError::DatabaseError(e.to_string()),
    })?;

    Ok(user)
}

pub fn verify_password(password: &str, hash_str: &str) -> Result<bool, DoubledeckerError> {
    verify(password, hash_str)
        .map_err(|e| DoubledeckerError::Internal(format!("Password verification failed: {}", e)))
}
